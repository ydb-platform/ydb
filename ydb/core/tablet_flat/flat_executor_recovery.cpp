#include "flat_executor_recovery.h"

#include "flat_cxx_database.h"
#include "tablet_flat_executed.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/protos/recoveryshard_config.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/file.h>


namespace NKikimr::NTabletFlatExecutor::NRecovery {

using NTabletFlatExecutor::TTabletExecutedFlat;

namespace {

template<typename TSchemeType>
TRawTypeValue ToRawTypeValue(const typename TSchemeType::TValueType& value, TMemoryPool& pool) {
    auto* p = pool.Allocate<typename TSchemeType::TValueType>();
    *p = value;
    return TSchemeType::ToRawTypeValue(*p);
}

TRawTypeValue MakeTypeValueFromJson(NScheme::TTypeInfo type, const NJson::TJsonValue& value, TMemoryPool& pool) {
    if (value.IsNull()) {
        return TRawTypeValue();
    }

    NScheme::TTypeId typeId = type.GetTypeId();
    switch (typeId) {
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Uint32:
        case NScheme::NTypeIds::Int64:
        case NScheme::NTypeIds::Uint64:
        case NScheme::NTypeIds::Uint8:
        case NScheme::NTypeIds::Int8:
        case NScheme::NTypeIds::Int16:
        case NScheme::NTypeIds::Uint16:
        case NScheme::NTypeIds::Bool:
        case NScheme::NTypeIds::Double:
        case NScheme::NTypeIds::Float:
        case NScheme::NTypeIds::Date:
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Timestamp:
        case NScheme::NTypeIds::Interval:
        case NScheme::NTypeIds::Date32:
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::JsonDocument: {
            auto* cell = pool.Allocate<TCell>();
            TString err;
            if (!NFormats::MakeCell(*cell, value, type, pool, err)) {
                throw yexception() << "Failed to parse " << value << " for type " << typeId << ": " << err;
            }
            if (!NFormats::CheckCellValue(*cell, type)) {
                throw yexception() << "Invalid value " << value << " for type " << typeId;
            }
            return TRawTypeValue(cell->Data(), cell->Size(), typeId);
        }
        case NScheme::NTypeIds::PairUi64Ui64: {
            const auto& arr = value.GetArraySafe();
            if (arr.size() != 2) {
                throw yexception() << "Expected array of size 2 for PairUi64Ui64 type";
            }
            std::pair<ui64, ui64> pair = {arr[0].GetUIntegerSafe(), arr[1].GetUIntegerSafe()};
            return ToRawTypeValue<NScheme::TPairUi64Ui64>(pair, pool);
        }
        case NScheme::NTypeIds::ActorId: {
            TActorId actorId;
            const auto& v = value.GetStringSafe();
            if (!actorId.Parse(v.data(), v.size())) {
                throw yexception() << "Failed to parse ActorId from string: " << v;
            }
            return ToRawTypeValue<NScheme::TActorId>(actorId, pool);
        }
        case NScheme::NTypeIds::String:
        default: {
            TString decoded = Base64StrictDecode(value.GetStringSafe());
            auto saved = pool.AppendString(TStringBuf(decoded));
            return TRawTypeValue(saved, typeId);
        }
    }
}

const NTable::TScheme::TTableInfo* FindTableFromJson(const NJson::TJsonValue& json, TTransactionContext& txc) {
    if (!json.IsMap()) {
        throw yexception() << TStringBuilder() << "Invalid JSON format, expected object: " << json;
    }
    const auto& jsonMap = json.GetMap();

    auto it = jsonMap.find("table");
    if (it == jsonMap.end()) {
        throw yexception() << "Table name is not found in json: " << json;
    }

    if (!it->second.IsString()) {
        throw yexception() << "Table name is not string in json: " << json;
    }

    TString tableName = it->second.GetString();
    const auto& scheme = txc.DB.GetScheme();
    auto tableIt = scheme.TableNames.find(tableName);
    if (tableIt == scheme.TableNames.end()) {
        throw yexception() << "Table " << tableName << " not found in database schema";
    }

    return &scheme.Tables.at(tableIt->second);
}

NTable::ERowOp FindOpFromJson(const NJson::TJsonValue& json) {
    if (!json.IsMap()) {
        throw yexception() << TStringBuilder() << "Invalid JSON format, expected object: " << json;
    }
    const auto& jsonMap = json.GetMap();

    auto it = jsonMap.find("op");
    if (it == jsonMap.end()) {
        throw yexception() << "Operation name is not found in json: " << json;
    }

    if (!it->second.IsString()) {
        throw yexception() << "Operation name is not string in json: " << json;
    }

    TString opName = it->second.GetString();
    if (opName == "upsert") {
        return NTable::ERowOp::Upsert;
    } else if (opName == "erase") {
        return NTable::ERowOp::Erase;
    } else if (opName == "replace") {
        return NTable::ERowOp::Reset;
    } else {
        throw yexception() << "Unknown operation name: " << opName << " in json: " << json;
    }
}

void UploadData(const NJson::TJsonValue& json, const NTable::TScheme::TTableInfo* table,
                std::optional<NTable::ERowOp> op, TMemoryPool& pool, TTransactionContext &txc)
{
    if (table == nullptr) {
        table = FindTableFromJson(json, txc);
    }

    if (!op.has_value()) {
        op = FindOpFromJson(json);
    }

    TVector<TRawTypeValue> key(table->KeyColumns.size());
    TVector<NTable::TUpdateOp> ops;

    if (!json.IsMap()) {
        throw yexception() << TStringBuilder() << "Invalid JSON format, expected object: " << json;
    }
    const auto& columns = json.GetMap();
    for (const auto& [columnName, value] : columns) {
        if (columnName == "table" || columnName == "op") {
            continue;
        }

        auto it = table->ColumnNames.find(columnName);
        if (it == table->ColumnNames.end()) {
            throw yexception() << "Column " << columnName << " not found in table " << table->Name;
        }

        const auto& column = table->Columns.at(it->second);

        if (column.KeyOrder != Max<ui32>()) {
            key.at(column.KeyOrder) = MakeTypeValueFromJson(column.PType.GetTypeId(), value, pool);
        } else {
            ops.emplace_back(NIceDb::TUpdateOp(
                column.Id,
                NTable::ECellOp::Set,
                MakeTypeValueFromJson(column.PType.GetTypeId(), value, pool)
            ));
        }
    }

    try {
        txc.DB.Update(table->Id, *op, key, ops);
    } catch (const std::exception& e) {
        throw yexception() << "Failed to update table " << table->Name << " with value " << json << ": " << e.what();
    }
}

ui64 RestoreTxMaxRedoBytes() {
    return AppData()->RecoveryShardConfig.GetRestoreTxMaxRedoBytes();
}

ui64 RestoreTxMaxLines() {
    return AppData()->RecoveryShardConfig.GetRestoreTxMaxLines();
}

ui64 RestoreInFlightBytes() {
    return AppData()->RecoveryShardConfig.GetRestoreInFlightBytes();
}

} // anonymous namespace

class TRecoveryShard : public TActor<TRecoveryShard>, public TTabletExecutedFlat {
public:
    friend class TTxUploadSchema;
    friend class TTxUploadSnapshot;
    friend class TTxUploadChangelog;

    ITransaction *CreateTxUploadSchema(TEvSchemaData::TPtr ev);
    ITransaction *CreateTxUploadSnapshot(TEvSnapshotData::TPtr ev, size_t startLine = 0);
    ITransaction *CreateTxUploadChangelog(TEvChangelogData::TPtr ev, size_t startLine = 0);

    explicit TRecoveryShard(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateWork)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {}

    void OnActivateExecutor(const TActorContext &ctx) override {
        SignalTabletActive(ctx);

        if (RestoreState == ERestoreState::InProgress) {
            Send(BackupReader, new TEvReadBackup);
        }
    }

    void OnDetach(const TActorContext &) override {
        PassAway();
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &) override {
        PassAway();
    }

    void PassAway() override {
        if (BackupReader) {
            Send(BackupReader, new TEvents::TEvPoisonPill);
        }
        TActor::PassAway();
    }

    void DefaultSignalTabletActive(const TActorContext &) override {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRestoreBackup, Handle);
            hFunc(TEvBackupReaderResult, Handle);
            hFunc(TEvBackupInfo, Handle);

            hFunc(TEvSchemaData, Handle);
            hFunc(TEvSnapshotData, Handle);
            hFunc(TEvChangelogData, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
        }
    }

    void Handle(TEvRestoreBackup::TPtr& ev) {
        if (RestoreState == ERestoreState::NotStarted) {
            StartRestore(ev->Get()->BackupPath, ev->Sender);
        }
    }

    void Handle(TEvBackupReaderResult::TPtr& ev) {
        const auto* msg = ev->Get();
        CompleteRestore(msg->Success, msg->Error);
    }

    void Handle(TEvBackupInfo::TPtr& ev) {
        TotalBytes = ev->Get()->TotalBytes;

        using EMode = TEvTablet::TEvCompleteRecoveryBoot::EMode;
        Send(Tablet(), new TEvTablet::TEvCompleteRecoveryBoot(EMode::WipeAllData));
    }

    void Handle(TEvSchemaData::TPtr& ev) {
        Execute(CreateTxUploadSchema(ev));
    }

    void Handle(TEvSnapshotData::TPtr& ev) {
        Execute(CreateTxUploadSnapshot(ev));
    }

    void Handle(TEvChangelogData::TPtr& ev) {
        Execute(CreateTxUploadChangelog(ev));
    }

    void StartRestore(const TString& backupPath, TActorId subscriber = {}) {
        RestoreState = ERestoreState::InProgress;

        BackupPath = backupPath;
        RestoreSubscriber = subscriber;

        BackupReader = Register(CreateBackupReader(SelfId(), backupPath), TMailboxType::HTSwap, AppData()->IOPoolId);
    }

    void CompleteRestore(bool success, const TString& error) {
        if (success) {
            if (error) {
                RestoreState = ERestoreState::DoneWithWarning;
                Error = error;
            } else {
                RestoreState = ERestoreState::Done;
            }
        } else {
            RestoreState = ERestoreState::Error;
            Error = error;
        }

        if (RestoreSubscriber) {
            Send(RestoreSubscriber, new TEvRestoreCompleted(success, error));
        }
    }

    using TRenderer = std::function<void(IOutputStream&)>;

    static void Alert(IOutputStream& str, TStringBuf type, TStringBuf text) {
        HTML(str) {
            DIV_CLASS(TStringBuilder() << "alert alert-" << type) {
                if (type == "warning") {
                    STRONG() {
                        str << "Warning: ";
                    }
                }
                str << text;
            }
        }
    }

    static void Warning(IOutputStream& str, TStringBuf text) {
        Alert(str, "warning", text);
    }
    static void Danger(IOutputStream& str, TStringBuf text) {
        Alert(str, "danger", text);
    }
    static void Info(IOutputStream& str, TStringBuf text) {
        Alert(str, "info", text);
    }
    static void Success(IOutputStream& str, TStringBuf text) {
        Alert(str, "success", text);
    }

    template <typename T>
    static void Header(IOutputStream& str, const T& title) {
        HTML(str) {
            DIV_CLASS("page-header") {
                TAG(TH3) {
                    str << title;
                }
            }
        }
    }

    static void Panel(IOutputStream& str, TRenderer title, TRenderer body) {
        HTML(str) {
            DIV_CLASS("panel panel-default") {
                DIV_CLASS("panel-heading") {
                    H4_CLASS("panel-title") {
                        title(str);
                    }
                }
                body(str);
            }
        }
    }

    static void SimplePanel(IOutputStream& str, const TStringBuf title, TRenderer body) {
        auto titleRenderer = [&title](IOutputStream& str) {
            HTML(str) {
                str << title;
            }
        };

        auto bodyRenderer = [body = std::move(body)](IOutputStream& str) {
            HTML(str) {
                DIV_CLASS("panel-body") {
                    body(str);
                }
            }
        };

        Panel(str, titleRenderer, bodyRenderer);
    }

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override {
        if (!ev) {
            return true;
        }

        auto cgi = ev->Get()->Cgi();
        if (const auto& path = cgi.Get("restoreBackup")) {
            if (RestoreState == ERestoreState::NotStarted) {
                StartRestore(path);
            }
        }

        TStringStream str;
        HTML(str) {
            SimplePanel(str, "Restore Tablet", [&](IOutputStream& str) {
                HTML(str) {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "restoreBackup") {
                                str << "Backup Path";
                            }
                            DIV_CLASS("col-sm-10") {
                                str << "<input type='text' id='restoreBackup' name='restoreBackup' class='form-control' "
                                    << "placeholder='/tmp/backup/gen_312' required>";
                            }
                        }

                        DIV_CLASS("form-group") {
                            DIV_CLASS("col-sm-offset-2 col-sm-10") {
                                const char* state = RestoreState != ERestoreState::NotStarted ? "disabled" : "";
                                str << "<button type='submit' name='startRestore' class='btn btn-danger' " << state << ">"
                                    << "Start Restore"
                                << "</button>";
                            }
                        }
                    }

                    if (RestoreState != ERestoreState::NotStarted) {
                        if (RestoreState == ERestoreState::InProgress) {
                            Info(str, TStringBuilder() << "Restoring from " << BackupPath.GetPath().Quote());

                            double p = TotalBytes != 0 ? (100.0 * ProcessedBytes / TotalBytes) : 0;
                            DIV_CLASS_ID("progress", "restoreProgress") {
                                TAG_CLASS_STYLE(TDiv, "progress-bar progress-bar-info", TStringBuilder() << "width:" << p << "%;") {
                                    str << Sprintf("%.2f%%", p);
                                }
                            }

                            TAG_ATTRS(TDiv, {{"id", "restoreDetails"}}) {
                                str << "Processed Bytes: " << ProcessedBytes
                                    << " / Total Bytes: " << TotalBytes;
                            }
                        } else if (RestoreState == ERestoreState::Done) {
                            Success(str, TStringBuilder() << "Restore from " << BackupPath.GetPath().Quote() << " completed successfully");
                        } else if (RestoreState == ERestoreState::Error) {
                            Danger(str, TStringBuilder() << "Restore from " << BackupPath.GetPath().Quote() << " failed: " << Error << ". Restart tablet to try again.");
                        } else if (RestoreState == ERestoreState::DoneWithWarning) {
                            Warning(str, TStringBuilder() << "Restore from " << BackupPath.GetPath().Quote() << " completed, but changelog is not fully restored: " << Error << ". Restart tablet to try again.");
                        }
                    }

                    str << R"(
                    <script>
                    $(document).ready(function() {
                        $('button[name="startRestore"]').click(function(e) {
                            e.preventDefault();

                            var btn = this;
                            var form = $(btn.form);

                            if (!confirm('Are you sure you want to start restore? This will override existing data')) {
                                return;
                            }

                            $.ajax({
                                type: "GET",
                                url: window.location.href,
                                data: form.serialize(),
                                success: function(response) {
                                    $('body').html(response);
                                },
                                error: function(xhr, status, error) {
                                    alert('Failed to start restore: ' + error);
                                }
                            });
                        });
                    });
                    </script>
                    )";
                }
            });
        }

        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

private:
    TFsPath BackupPath;
    ERestoreState RestoreState = ERestoreState::NotStarted;
    TString Error;

    TActorId BackupReader;
    ui64 ProcessedBytes = 0;
    ui64 TotalBytes = 0;

    TActorId RestoreSubscriber; // only for tests
}; // TRecoveryShard

class TUploadTxResult {
public:
    enum class EState : ui8 {
        Error,
        PartialDone,
        Done,
    };

    void Error(const TString& error) {
        State = EState::Error;
        ErrorMessage = error;
    }

    void Done(size_t processedBytes) {
        State = EState::Done;
        ProcessedBytes = processedBytes;
    }

    void PartialDone(size_t processedBytes) {
        State = EState::PartialDone;
        ProcessedBytes = processedBytes;
    }

    bool IsError() const {
        return State == EState::Error;
    }

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsPartialDone() const {
        return State == EState::PartialDone;
    }

    TString GetErrorMessage() const {
        return ErrorMessage;
    }

    ui64 GetProcessedBytes() const {
        return ProcessedBytes;
    }

private:
    EState State = EState::Error;
    TString ErrorMessage;
    ui64 ProcessedBytes = 0;
};

class TTxUploadSchema : public TTransactionBase<TRecoveryShard> {
public:
    TTxUploadSchema(TRecoveryShard* self, TEvSchemaData::TPtr& schema)
        : TBase(self)
        , Schema(schema)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        try {
            NTable::TSchemeChanges protoSchema;
            NProtobufJson::Json2Proto(Schema->Get()->Data, protoSchema, {
                .FieldNameMode = NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense,
                .AllowUnknownFields = false,
                .MapAsObject = true,
                .EnumValueMode = NProtobufJson::TJson2ProtoConfig::EnumSnakeCaseInsensitive,
            });

            txc.DB.Alter().Merge(protoSchema);
        } catch (const yexception& e) {
            Error = e.what();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Error) {
            Self->CompleteRestore(false, Error);
            ctx.Send(Schema->Sender, new TEvDataAck(false, Error));
        } else {
            Self->ProcessedBytes += Schema->Get()->Data.size();
            ctx.Send(Schema->Sender, new TEvDataAck(true));
        }
    }
private:
    TEvSchemaData::TPtr Schema;
    TString Error;
}; // TTxUploadSchema

class TTxUploadSnapshot : public TTransactionBase<TRecoveryShard> {
public:
    TTxUploadSnapshot(TRecoveryShard* self, TEvSnapshotData::TPtr& snapshot, size_t startLine)
        : TBase(self)
        , Snapshot(snapshot)
        , Pool(1_MB)
        , StartLine(startLine)
    {}

    size_t ProcessedLines(size_t currentLine) const {
        return currentLine - StartLine;
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        auto it = txc.DB.GetScheme().TableNames.find(Snapshot->Get()->TableName);
        if (it == txc.DB.GetScheme().TableNames.end()) {
            Result.Error(TStringBuilder() << "Table " << Snapshot->Get()->TableName << " not found in database schema");
            return true;
        }

        const auto& table = txc.DB.GetScheme().Tables.at(it->second);

        size_t processedBytes = 0;
        size_t i = StartLine;
        while (i < Snapshot->Get()->Lines.size()) {
            const auto& line = Snapshot->Get()->Lines[i];

            try {
                NJson::TJsonValue json;
                NJson::ReadJsonTree(line, &json, true);
                UploadData(json, &table, NTable::ERowOp::Upsert, Pool, txc);

                processedBytes += line.size() + 1; // +1 for newline
                ++i;

                if (ProcessedLines(i) >= RestoreTxMaxLines() || txc.DB.GetCommitRedoBytes() >= RestoreTxMaxRedoBytes()) {
                    break;
                }
            } catch (const std::exception& e) {
                Result.Error(TStringBuilder() << "Failed to upload snapshot data: " << e.what() << ", line: " << line);
                return true;
            }
        }

        if (i < Snapshot->Get()->Lines.size()) {
            // Start new tx to upload the rest data
            Self->Execute(Self->CreateTxUploadSnapshot(std::move(Snapshot), i));
            Result.PartialDone(processedBytes);
        } else {
            Result.Done(processedBytes);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Result.IsDone()) {
            Self->ProcessedBytes += Result.GetProcessedBytes();
            ctx.Send(Snapshot->Sender, new TEvDataAck(true));
        } else if (Result.IsPartialDone()) {
            Self->ProcessedBytes += Result.GetProcessedBytes();
         } else if (Result.IsError()) {
            Self->CompleteRestore(false, Result.GetErrorMessage());
            ctx.Send(Snapshot->Sender, new TEvDataAck(false, Result.GetErrorMessage()));
        }
    }

private:
    TEvSnapshotData::TPtr Snapshot;
    TUploadTxResult Result;
    TMemoryPool Pool;
    size_t StartLine;
}; // TTxUploadSnapshot

class TTxUploadChangelog : public TTransactionBase<TRecoveryShard> {
public:
    TTxUploadChangelog(TRecoveryShard* self, TEvChangelogData::TPtr& changelog, size_t startLine)
        : TBase(self)
        , Changelog(changelog)
        , Pool(1_MB)
        , StartLine(startLine)
    {}

    size_t ProcessedLines(size_t currentLine) const {
        return currentLine - StartLine;
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        size_t processedBytes = 0;
        size_t i = StartLine;

        while (i < Changelog->Get()->Lines.size()) {
            const auto& line = Changelog->Get()->Lines[i];

            NJson::TJsonValue json;
            try {
                NJson::ReadJsonTree(line, &json, true);
            } catch (const std::exception& e) {
                Result.Error(TStringBuilder() << "Failed to parse changelog: " << e.what() << ", line: " << line);
                return true;
            }

            if (!json.IsMap()) {
                Result.Error(TStringBuilder() << "Invalid JSON format in changelog line: " << line);
                return true;
            }

            if (json.Has("schema_changes")) {
                auto changesJson = json["schema_changes"];
                if (!changesJson.IsArray()) {
                    Result.Error(TStringBuilder() << "Invalid schema changes format in changelog line: " << line);
                    return true;
                }

                if (txc.DB.GetCommitRedoBytes() > 0) {
                    // Schema changes can't be applied after data changes in the same transaction.
                    // Restart from this line in a new transaction.
                    break;
                }

                const auto& changesArray = changesJson.GetArray();
                try {
                    NTable::TSchemeChanges protoSchema;

                    for (const auto& change : changesArray) {
                        NProtobufJson::Json2Proto(change, *protoSchema.AddDelta(), {
                            .FieldNameMode = NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense,
                            .AllowUnknownFields = false,
                            .MapAsObject = true,
                            .EnumValueMode = NProtobufJson::TJson2ProtoConfig::EnumSnakeCaseInsensitive,
                        });
                    }

                    txc.DB.Alter().Merge(protoSchema);
                } catch (const std::exception& e) {
                    Result.Error(TStringBuilder() << "Failed to upload changelog schema: " << e.what() << ", line: " << line);
                    return true;
                }
            }

            if (json.Has("data_changes")) {
                auto changesJson = json["data_changes"];
                if (!changesJson.IsArray()) {
                    Result.Error(TStringBuilder() << "Invalid data changes format in changelog line: " << line);
                    return true;
                }

                const auto& changesArray = changesJson.GetArray();
                try {
                    for (const auto& change : changesArray) {
                        UploadData(change, nullptr, std::nullopt, Pool, txc);
                    }
                } catch (const std::exception& e) {
                    Result.Error(TStringBuilder() << "Failed to upload changelog data: " << e.what() << ", line: " << line);
                    return true;
                }
            }

            processedBytes += line.size() + 1; // +1 for newline;
            ++i;

            if (ProcessedLines(i) >= RestoreTxMaxLines() || txc.DB.GetCommitRedoBytes() >= RestoreTxMaxRedoBytes()) {
                break;
            }
        }

        if (i < Changelog->Get()->Lines.size()) {
            // Start new tx to upload the rest data
            Self->Execute(Self->CreateTxUploadChangelog(std::move(Changelog), i));
            Result.PartialDone(processedBytes);
        } else {
            Result.Done(processedBytes);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
         if (Result.IsDone()) {
            Self->ProcessedBytes += Result.GetProcessedBytes();
            ctx.Send(Changelog->Sender, new TEvDataAck(true));
        } else if (Result.IsPartialDone()) {
            Self->ProcessedBytes += Result.GetProcessedBytes();
        } else if (Result.IsError()) {
            Self->CompleteRestore(true, Result.GetErrorMessage()); // changelog errors are warnings
            ctx.Send(Changelog->Sender, new TEvDataAck(false, Result.GetErrorMessage()));
        }
    }
private:
    TEvChangelogData::TPtr Changelog;
    TUploadTxResult Result;
    TMemoryPool Pool;
    size_t StartLine;
}; // TTxUploadChangelog

ITransaction* TRecoveryShard::CreateTxUploadSchema(TEvSchemaData::TPtr ev) {
    return new TTxUploadSchema(this, ev);
}

ITransaction* TRecoveryShard::CreateTxUploadSnapshot(TEvSnapshotData::TPtr ev, size_t startLine) {
    return new TTxUploadSnapshot(this, ev, startLine);
}

ITransaction* TRecoveryShard::CreateTxUploadChangelog(TEvChangelogData::TPtr ev, size_t startLine) {
    return new TTxUploadChangelog(this, ev, startLine);
}

class TBackupReader : public TActorBootstrapped<TBackupReader> {
public:
    TBackupReader(TActorId owner, const TString& backupPath)
        : Owner(owner)
        , BackupPath(backupPath)
    {}

    void Bootstrap() {
        auto snapshotDir = BackupPath.Child("snapshot");
        if (!snapshotDir.Exists()) {
            return SendResultAndDie(false, TStringBuilder() << "Snapshot dir doesn't exist: " << snapshotDir);
        }

        auto schemaFile = snapshotDir.Child("schema.json");
        if (!schemaFile.Exists()) {
            return SendResultAndDie(false, TStringBuilder() << "Snapshot schema file doesn't exist: " << schemaFile);
        }

        auto changelogFile = BackupPath.Child("changelog.json");
        if (!changelogFile.Exists()) {
            return SendResultAndDie(false, TStringBuilder() << "Changelog file doesn't exist: " << changelogFile);
        }

        // Calculate total size and collect snapshot files
        ui64 totalBytes = 0;

        try {
            TFileStat stat(changelogFile);
            totalBytes += stat.Size;
        } catch (const TIoException& e) {
            return SendResultAndDie(false, TStringBuilder() << "Cannot get size of " << changelogFile << ": " << e.what());
        }

        TVector<TFsPath> snapshotFiles;
        snapshotDir.List(snapshotFiles);

        for (const auto& file : snapshotFiles) {
            if (file.Basename() != "schema.json") {
                SnapshotFiles.push_back(file);
            }

            try {
                TFileStat stat(file);
                totalBytes += stat.Size;
            } catch (const TIoException& e) {
                return SendResultAndDie(false, TStringBuilder() << "Cannot get size of " << file << ": " << e.what());
            }
        }

        SchemaFilePath = schemaFile;
        ChangelogFilePath = changelogFile;

        Send(Owner, new TEvBackupInfo(totalBytes));
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvReadBackup, Handle);
            hFunc(TEvDataAck, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    void Handle(TEvReadBackup::TPtr&) {
        try {
            TString schemaData = TFileInput(SchemaFilePath).ReadAll();
            Send(Owner, new TEvSchemaData(std::move(schemaData)));
        } catch (const TIoException& e) {
            return SendResultAndDie(false, TStringBuilder() << "Failed to read schema file " << SchemaFilePath << ": " << e.what());
        }
    }

    void Handle(TEvDataAck::TPtr& ev) {
        const auto* msg = ev->Get();

        if (!msg->Success) {
            return PassAway();
        }

        ProcessNextChunk();
    }

    void ProcessNextChunk() {
        while (true) {
            // Get current file or open next one
            if (!CurrentFileInput) {
                if (!SnapshotFiles.empty()) {
                    CurrentFilePath = SnapshotFiles.front();
                    SnapshotFiles.pop_front();

                    CurrentTableName = CurrentFilePath.Basename();
                    // Remove .json extension
                    if (CurrentTableName.EndsWith(".json")) {
                        CurrentTableName = CurrentTableName.substr(0, CurrentTableName.size() - 5);
                    }

                    try {
                        CurrentFileInput = MakeHolder<TFileInput>(CurrentFilePath, 1_MB);
                    } catch (const TIoException& e) {
                        return SendResultAndDie(false, TStringBuilder() << "Failed to open snapshot file " << CurrentFilePath << ": " << e.what());
                    }
                } else if (!ChangelogProcessed) {
                    CurrentFilePath = ChangelogFilePath;
                    CurrentTableName.clear();
                    ChangelogProcessed = true;

                    try {
                        CurrentFileInput = MakeHolder<TFileInput>(CurrentFilePath, 1_MB);
                    } catch (const TIoException& e) {
                        return SendResultAndDie(false, TStringBuilder() << "Failed to open changelog file " << CurrentFilePath << ": " << e.what());
                    }
                } else {
                    // All files processed
                    return SendResultAndDie(true);
                }
            }

            // Read lines until buffer is full
            TVector<TString> lines;
            ui64 linesSize = 0;
            try {
                TString line;
                while (linesSize < RestoreInFlightBytes() && CurrentFileInput->ReadLine(line)) {
                    linesSize += line.size() + 1; // +1 for \n
                    lines.push_back(std::move(line));
                }
            } catch (const TIoException& e) {
                return SendResultAndDie(false, TStringBuilder() << "Failed to read from file " << CurrentFilePath << ": " << e.what());
            }

            if (lines.empty()) {
                // End of file reached, try next file
                CurrentFileInput.Reset();
                continue;
            }

            if (CurrentTableName) {
                Send(Owner, new TEvSnapshotData(CurrentTableName, std::move(lines)));
            } else {
                Send(Owner, new TEvChangelogData(std::move(lines)));
            }
            return;
        }
    }

    void SendResultAndDie(bool success, const TString& error = "") {
        Send(Owner, new TEvBackupReaderResult(success, error));
        PassAway();
    }

private:
    const TActorId Owner;
    const TFsPath BackupPath;

    TFsPath SchemaFilePath;
    TFsPath ChangelogFilePath;
    TDeque<TFsPath> SnapshotFiles;

    TFsPath CurrentFilePath;
    TString CurrentTableName;
    THolder<TFileInput> CurrentFileInput;
    bool ChangelogProcessed = false;
}; // TBackupReader

IActor* CreateRecoveryShard(const TActorId &tablet, TTabletStorageInfo *info) {
    return new TRecoveryShard(tablet, info);
}

IActor* CreateBackupReader(TActorId owner, const TString& backupPath) {
    return new TBackupReader(owner, backupPath);
}

} //namespace NKikimr::NTabletFlatExecutor::NRecovery
