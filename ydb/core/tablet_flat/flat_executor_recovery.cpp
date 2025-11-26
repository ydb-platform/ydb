#include "flat_executor_recovery.h"

#include "flat_cxx_database.h"
#include "tablet_flat_executed.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/file.h>
#include <variant>


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
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Int32 type";
            }
            return ToRawTypeValue<NScheme::TInt32>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Uint32:
            if (!value.IsUInteger()) {
                throw yexception() << "Expected unsigned integer value for Uint32 type";
            }
            return ToRawTypeValue<NScheme::TUint32>(value.GetUInteger(), pool);
        case NScheme::NTypeIds::Int64:
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Int64 type";
            }
            return ToRawTypeValue<NScheme::TInt64>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Uint64:
            if (!value.IsUInteger()) {
                throw yexception() << "Expected unsigned integer value for Uint64 type";
            }
            return ToRawTypeValue<NScheme::TUint64>(value.GetUInteger(), pool);
        case NScheme::NTypeIds::Uint8:
            if (!value.IsUInteger()) {
                throw yexception() << "Expected unsigned integer value for Uint8 type";
            }
            return ToRawTypeValue<NScheme::TUint8>(value.GetUInteger(), pool);
        case NScheme::NTypeIds::Int8:
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Int8 type";
            }
            return ToRawTypeValue<NScheme::TInt8>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Int16:
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Int16 type";
            }
            return ToRawTypeValue<NScheme::TInt16>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Uint16:
            if (!value.IsUInteger()) {
                throw yexception() << "Expected unsigned integer value for Uint16 type";
            }
            return ToRawTypeValue<NScheme::TUint16>(value.GetUInteger(), pool);
        case NScheme::NTypeIds::Bool:
            if (!value.IsBoolean()) {
                throw yexception() << "Expected boolean value for Bool type";
            }
            return ToRawTypeValue<NScheme::TBool>(value.GetBoolean(), pool);
        case NScheme::NTypeIds::Double:
            if (!value.IsDouble()) {
                throw yexception() << "Expected double value for Double type";
            }
            return ToRawTypeValue<NScheme::TDouble>(value.GetDouble(), pool);
        case NScheme::NTypeIds::Float:
            if (!value.IsDouble()) {
                throw yexception() << "Expected double value for Float type";
            }
            return ToRawTypeValue<NScheme::TFloat>(value.GetDouble(), pool);
        case NScheme::NTypeIds::Date:
            if (!value.IsUInteger()) {
                throw yexception() << "Expected unsigned integer value for Date type";
            }
            return ToRawTypeValue<NScheme::TDate>(value.GetUInteger(), pool);
        case NScheme::NTypeIds::Datetime:
            if (!value.IsUInteger()) {
                throw yexception() << "Expected unsigned integer value for Datetime type";
            }
            return ToRawTypeValue<NScheme::TDatetime>(value.GetUInteger(), pool);
        case NScheme::NTypeIds::Timestamp:
            if (!value.IsUInteger()) {
                throw yexception() << "Expected unsigned integer value for Timestamp type";
            }
            return ToRawTypeValue<NScheme::TTimestamp>(value.GetUInteger(), pool);
        case NScheme::NTypeIds::Interval:
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Interval type";
            }
            return ToRawTypeValue<NScheme::TInterval>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Date32:
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Date32 type";
            }
            return ToRawTypeValue<NScheme::TDate32>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Datetime64:
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Datetime64 type";
            }
            return ToRawTypeValue<NScheme::TDatetime64>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Timestamp64:
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Timestamp64 type";
            }
            return ToRawTypeValue<NScheme::TTimestamp64>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Interval64:
            if (!value.IsInteger()) {
                throw yexception() << "Expected integer value for Interval64 type";
            }
            return ToRawTypeValue<NScheme::TInterval64>(value.GetInteger(), pool);
        case NScheme::NTypeIds::Utf8: {
            if (!value.IsString()) {
                throw yexception() << "Expected string value for Utf8 type";
            }
            auto saved = pool.AppendString(TStringBuf(value.GetString()));
            return TRawTypeValue(saved, typeId);
        }
        case NScheme::NTypeIds::Json: {
            if (!value.IsString()) {
                throw yexception() << "Expected string value for Json type";
            }
            auto saved = pool.AppendString(TStringBuf(value.GetString()));
            return TRawTypeValue(saved, typeId);
        }
        case NScheme::NTypeIds::JsonDocument: {
            if (!value.IsString()) {
                throw yexception() << "Expected string value for JsonDocument type";
            }
            TString decoded = Base64StrictDecode(value.GetString());
            auto maybeBinaryJson = NBinaryJson::SerializeToBinaryJson(decoded);
            if (std::holds_alternative<TString>(maybeBinaryJson)) {
                throw yexception() << "Invalid JSON for JsonDocument provided: " + std::get<TString>(maybeBinaryJson);
            }
            const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(maybeBinaryJson);
            auto saved = pool.AppendString(TStringBuf(binaryJson.Data(), binaryJson.Size()));
            return TRawTypeValue(saved, typeId);
        }
        case NScheme::NTypeIds::PairUi64Ui64: {
            if (!value.IsArray()) {
                throw yexception() << "Expected array value for PairUi64Ui64 type";
            }
            const auto& arr = value.GetArray();
            if (arr.size() != 2) {
                throw yexception() << "Expected array of size 2 for PairUi64Ui64 type";
            }
            if (!arr[0].IsUInteger() || !arr[1].IsUInteger()) {
                throw yexception() << "Expected unsigned integer values in array for PairUi64Ui64 type";
            }
            std::pair<ui64, ui64> pair = {arr[0].GetUInteger(), arr[1].GetUInteger()};
            return ToRawTypeValue<NScheme::TPairUi64Ui64>(pair, pool);
        }
        case NScheme::NTypeIds::ActorId: {
            if (!value.IsString()) {
                throw yexception() << "Expected string value for ActorId type";
            }
            TActorId actorId;

            const auto& v = value.GetString();
            if (!actorId.Parse(v.data(), v.size())) {
                throw yexception() << "Failed to parse ActorId from string: " << v;
            }
            return ToRawTypeValue<NScheme::TActorId>(actorId, pool);
        }
        case NScheme::NTypeIds::String: {
            if (!value.IsString()) {
                throw yexception() << "Expected string value for String type";
            }

            TString decoded = Base64StrictDecode(value.GetString());
            auto saved = pool.AppendString(TStringBuf(decoded));
            return TRawTypeValue(saved, typeId);
        }
        default: {
            if (!value.IsString()) {
                throw yexception() << "Expected string value for type id " << typeId;
            }
            TString decoded = Base64StrictDecode(value.GetString());
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

void UploadData(const NJson::TJsonValue& json,  const NTable::TScheme::TTableInfo* table,
                std::optional<NTable::ERowOp> op, TMemoryPool& pool, TTransactionContext &txc) {

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

} // anonymous namespace

class TRecoveryShard : public TActor<TRecoveryShard>, public TTabletExecutedFlat {
public:
    friend class TTxUploadSchema;
    friend class TTxUploadSnapshot;
    friend class TTxUploadChangelog;

    ITransaction *CreateTxUploadSchema(TEvSchemaData::TPtr& ev);
    ITransaction *CreateTxUploadSnapshot(TEvSnapshotData::TPtr& ev);
    ITransaction *CreateTxUploadChangelog(TEvChangelogData::TPtr& ev);

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
    TTxUploadSnapshot(TRecoveryShard* self, TEvSnapshotData::TPtr& snapshot)
        : TBase(self)
        , Snapshot(snapshot)
        , Pool(1_MB)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        auto it = txc.DB.GetScheme().TableNames.find(Snapshot->Get()->TableName);
        if (it == txc.DB.GetScheme().TableNames.end()) {
            Error = TStringBuilder() << "Table " << Snapshot->Get()->TableName << " not found in database schema";
            return true;
        }

        const auto& table = txc.DB.GetScheme().Tables.at(it->second);

        for (const auto& line : Snapshot->Get()->Lines) {
            NJson::TJsonValue json;

            try {
                NJson::ReadJsonTree(line, &json);
                UploadData(json, &table, NTable::ERowOp::Upsert, Pool, txc);
            } catch (const std::exception& e) {
                Error = TStringBuilder() << "Failed to upload snapshot data: " << e.what() << ", line: " << line;
                return true;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Error) {
            Self->CompleteRestore(false, Error);
            ctx.Send(Snapshot->Sender, new TEvDataAck(false, Error));
        } else {
            Self->ProcessedBytes += Snapshot->Get()->Size;
            ctx.Send(Snapshot->Sender, new TEvDataAck(true));
        }
    }
private:
    TEvSnapshotData::TPtr Snapshot;
    TString Error;
    TMemoryPool Pool;
}; // TTxUploadSnapshot

class TTxUploadChangelog : public TTransactionBase<TRecoveryShard> {
public:
    TTxUploadChangelog(TRecoveryShard* self, TEvChangelogData::TPtr& changelog)
        : TBase(self)
        , Changelog(changelog)
        , Pool(1_MB)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        for (const auto& line : Changelog->Get()->Lines) {
            NJson::TJsonValue json;

            try {
                NJson::ReadJsonTree(line, &json);
            } catch (const std::exception& e) {
                Error = TStringBuilder() << "Failed to parse changelog line: " << e.what();
                return true;
            }

            if (!json.IsMap()) {
                Error = TStringBuilder() << "Invalid JSON format in changelog line: " << line;
                return true;
            }

            if (json.Has("schema_changes")) {
                auto changesJson = json["schema_changes"];
                if (!changesJson.IsArray()) {
                    Error = TStringBuilder() << "Invalid schema changes format in changelog line: " << line;
                    return true;
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
                } catch (const yexception& e) {
                    Error = TStringBuilder() << "Failed to upload changelog schema: " << e.what() << ", line: " << line;
                    return true;
                }
            }

            if (json.Has("data_changes")) {
                auto changesJson = json["data_changes"];
                if (!changesJson.IsArray()) {
                    Error = TStringBuilder() << "Invalid data changes format in changelog line: " << line;
                    return true;
                }

                const auto& changesArray = changesJson.GetArray();
                try {
                    for (const auto& change : changesArray) {
                        UploadData(change, nullptr, std::nullopt, Pool, txc);
                    }
                } catch (const std::exception& e) {
                    Error = TStringBuilder() << "Failed to upload changelog data: " << e.what() << ", line: " << line;
                    return true;
                }
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Error) {
            Self->CompleteRestore(true, Error); // changelog errors are warnings
            ctx.Send(Changelog->Sender, new TEvDataAck(false, Error));
        } else {
            Self->ProcessedBytes += Changelog->Get()->Size;
            ctx.Send(Changelog->Sender, new TEvDataAck(true));
        }
    }
private:
    TEvChangelogData::TPtr Changelog;
    TString Error;
    TMemoryPool Pool;
}; // TTxUploadChangelog

ITransaction* TRecoveryShard::CreateTxUploadSchema(TEvSchemaData::TPtr& ev) {
    return new TTxUploadSchema(this, ev);
}

ITransaction* TRecoveryShard::CreateTxUploadSnapshot(TEvSnapshotData::TPtr& ev) {
    return new TTxUploadSnapshot(this, ev);
}

ITransaction* TRecoveryShard::CreateTxUploadChangelog(TEvChangelogData::TPtr& ev) {
    return new TTxUploadChangelog(this, ev);
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
                while (linesSize < 1_MB && CurrentFileInput->ReadLine(line)) {
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
                Send(Owner, new TEvSnapshotData(CurrentTableName, std::move(lines), linesSize));
            } else {
                Send(Owner, new TEvChangelogData(std::move(lines), linesSize));
            }
            return;
        }
    }

    void SendResultAndDie(bool success, const TString& error = "") {
        Send(Owner, new TEvBackupReaderResult(success, error));
        PassAway();
    }

private:
    TActorId Owner;
    TFsPath BackupPath;

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
