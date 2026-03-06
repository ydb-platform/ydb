#include "flat_executor_recovery.h"

#include "flat_executor_backup_common.h"
#include "flat_cxx_database.h"
#include "flat_part_iface.h"
#include "tablet_flat_executed.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/protos/recoveryshard_config.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/util.h>
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

    for (size_t i = 0; i < key.size(); ++i) {
        if (key[i].IsEmpty()) {
            ui32 keyColId = table->KeyColumns.at(i);
            const auto& col = table->Columns.at(keyColId);
            throw yexception() << "Key column " << col.Name << " is missing in table " << table->Name;
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

class TDryRunExecutor
    : public NFlatExecutorSetup::IExecutor
    , public IExecuting
{
    struct TDryRunPages : public NTable::IPages {
        TResult Locate(const NTable::TMemTable*, ui64, ui32) override { Y_TABLET_ERROR("Not supported"); }
        TResult Locate(const NTable::TPart*, ui64, NTable::ELargeObj) override { Y_TABLET_ERROR("Not supported"); }
        const TSharedData* TryGetPage(const NTable::TPart*, TPageId, TGroupId) override { Y_TABLET_ERROR("Not supported"); }
    };

    struct TDryRunStats : public TExecutorStats {};

public:
    explicit TDryRunExecutor(ui64 tabletId)
        : TabletId(tabletId)
    {}

    void Execute(TAutoPtr<ITransaction> transaction, const TActorContext &ctx) override {
        if (Executing) {
            PendingTx.push_back(transaction);
            return;
        }
        Executing = true;
        ExecuteImpl(transaction, ctx);
        while (!PendingTx.empty()) {
            auto next = std::move(PendingTx.front());
            PendingTx.pop_front();
            ExecuteImpl(next, ctx);
        }
        Executing = false;
    }

    const NTable::TScheme& Scheme() const override { return DB.GetScheme(); }
    const TExecutorStats& GetStats() const override { return Stats; }
    void DetachTablet() override {}
    TExecutorCounters* GetCounters() override { return nullptr; }
    void UpdateConfig(TEvTablet::TEvUpdateConfig::TPtr&) override {}

    void RenderHtmlPage(NMon::TEvRemoteHttpInfo::TPtr& ev) const override {
        TActivationContext::Send(new IEventHandle(ev->Sender, ev->Recipient,
            new NMon::TEvRemoteHttpInfoRes("Not supported")));
    }
    void RenderHtmlCounters(NMon::TEvRemoteHttpInfo::TPtr& ev) const override {
        TActivationContext::Send(new IEventHandle(ev->Sender, ev->Recipient,
            new NMon::TEvRemoteHttpInfoRes("Not supported")));
    }
    void RenderHtmlDb(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext&) const override {
        TActivationContext::Send(new IEventHandle(ev->Sender, ev->Recipient,
            new NMon::TEvRemoteHttpInfoRes("Not supported")));
    }
    void GetTabletCounters(TEvTablet::TEvGetCounters::TPtr& ev) override {
        TActivationContext::Send(new IEventHandle(ev->Sender, ev->Recipient,
            new TEvTablet::TEvGetCountersResponse()));
    }

    void Boot(TEvTablet::TEvBoot::TPtr&, const TActorContext&) override { Y_TABLET_ERROR("Not supported"); }
    void Restored(TEvTablet::TEvRestored::TPtr&, const TActorContext&) override { Y_TABLET_ERROR("Not supported"); }
    void FollowerBoot(TEvTablet::TEvFBoot::TPtr&, const TActorContext&) override { Y_TABLET_ERROR("Not supported"); }
    void FollowerUpdate(THolder<TEvTablet::TFUpdateBody>) override { Y_TABLET_ERROR("Not supported"); }
    void FollowerAuxUpdate(TString) override { Y_TABLET_ERROR("Not supported"); }
    void FollowerAttached(ui32) override { Y_TABLET_ERROR("Not supported"); }
    void FollowerDetached(ui32) override { Y_TABLET_ERROR("Not supported"); }
    void FollowerSyncComplete() override { Y_TABLET_ERROR("Not supported"); }
    void FollowerGcApplied(ui32, TDuration) override { Y_TABLET_ERROR("Not supported"); }

    ui64 Enqueue(TAutoPtr<ITransaction>) override { Y_TABLET_ERROR("Not supported"); }
    ui64 EnqueueLowPriority(TAutoPtr<ITransaction>) override { Y_TABLET_ERROR("Not supported"); }
    bool CancelTransaction(ui64) override { Y_TABLET_ERROR("Not supported"); }
    void ConfirmReadOnlyLease(TMonotonic) override { Y_TABLET_ERROR("Not supported"); }
    void ConfirmReadOnlyLease(TMonotonic, std::function<void()>) override { Y_TABLET_ERROR("Not supported"); }
    void ConfirmReadOnlyLease(std::function<void()>) override { Y_TABLET_ERROR("Not supported"); }
    TString BorrowSnapshot(ui32, const TTableSnapshotContext&, TRawVals, TRawVals, ui64) const override { Y_TABLET_ERROR("Not supported"); }
    ui64 MakeScanSnapshot(ui32) override { Y_TABLET_ERROR("Not supported"); }
    void DropScanSnapshot(ui64) override { Y_TABLET_ERROR("Not supported"); }
    ui64 QueueScan(ui32, TAutoPtr<NTable::IScan>, ui64, const TScanOptions&) override { Y_TABLET_ERROR("Not supported"); }
    bool CancelScan(ui32, ui64) override { Y_TABLET_ERROR("Not supported"); }
    TFinishedCompactionInfo GetFinishedCompactionInfo(ui32) const override { Y_TABLET_ERROR("Not supported"); }
    bool HasSchemaChanges(ui32) const override { Y_TABLET_ERROR("Not supported"); }
    ui64 CompactBorrowed(ui32) override { Y_TABLET_ERROR("Not supported"); }
    ui64 CompactMemTable(ui32) override { Y_TABLET_ERROR("Not supported"); }
    ui64 CompactTable(ui32) override { Y_TABLET_ERROR("Not supported"); }
    bool CompactTables() override { Y_TABLET_ERROR("Not supported"); }
    void AllowBorrowedGarbageCompaction(ui32) override { Y_TABLET_ERROR("Not supported"); }
    void RegisterExternalTabletCounters(TAutoPtr<TTabletCountersBase>) override { Y_TABLET_ERROR("Not supported"); }
    void SendUserAuxUpdateToFollowers(TString, const TActorContext&) override { Y_TABLET_ERROR("Not supported"); }
    THashMap<TLogoBlobID, TVector<ui64>> GetBorrowedParts() const override { Y_TABLET_ERROR("Not supported"); }
    bool HasLoanedParts() const override { Y_TABLET_ERROR("Not supported"); }
    bool HasBorrowed(ui32, ui64) const override { Y_TABLET_ERROR("Not supported"); }
    void OnYellowChannels(TVector<ui32>, TVector<ui32>) override { Y_TABLET_ERROR("Not supported"); }
    NMetrics::TResourceMetrics* GetResourceMetrics() const override { Y_TABLET_ERROR("Not supported"); }
    float GetRejectProbability() const override { Y_TABLET_ERROR("Not supported"); }
    void SetPreloadTablesData(THashSet<ui32>) override { Y_TABLET_ERROR("Not supported"); }
    void StartVacuum(ui64) override { Y_TABLET_ERROR("Not supported"); }
    void MakeSnapshot(TIntrusivePtr<TTableSnapshotContext>) override { Y_TABLET_ERROR("Not supported"); }
    void DropSnapshot(TIntrusivePtr<TTableSnapshotContext>) override { Y_TABLET_ERROR("Not supported"); }
    void MoveSnapshot(const TTableSnapshotContext&, ui32, ui32) override { Y_TABLET_ERROR("Not supported"); }
    void ClearSnapshot(const TTableSnapshotContext&) override { Y_TABLET_ERROR("Not supported"); }
    void LoanTable(ui32, const TString&) override { Y_TABLET_ERROR("Not supported"); }
    void CleanupLoan(const TLogoBlobID&, ui64) override { Y_TABLET_ERROR("Not supported"); }
    void ConfirmLoan(const TLogoBlobID&, const TLogoBlobID&) override { Y_TABLET_ERROR("Not supported"); }
    void EnableReadMissingReferences() override { Y_TABLET_ERROR("Not supported"); }
    void DisableReadMissingReferences() override { Y_TABLET_ERROR("Not supported"); }
    ui64 MissingReferencesSize() const override { Y_TABLET_ERROR("Not supported"); }

private:
    void ExecuteImpl(TAutoPtr<ITransaction>& transaction, const TActorContext &ctx) {
        ++Step0;
        NTable::TTxStamp stamp(Generation0, Step0);
        DB.Begin(stamp, Pages);
        NWilson::TSpan span;
        TTransactionContext txc(TabletId, Generation0, Step0, DB, *this, Max<ui64>(), 0, span);
        bool ready = transaction->Execute(txc, ctx);
        DB.Commit(stamp, ready);
        if (ready) {
            transaction->Complete(ctx);
        } else {
            PendingTx.push_back(std::move(transaction));
        }
    }

    NTable::TDatabase DB;
    TDryRunPages Pages;
    TDryRunStats Stats;
    ui64 TabletId;
    bool Executing = false;
    TDeque<TAutoPtr<ITransaction>> PendingTx;
};

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
            HFunc(TEvBackupInfo, Handle);

            hFunc(TEvSchemaData, Handle);
            hFunc(TEvSnapshotData, Handle);
            hFunc(TEvChangelogData, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
        }
    }

    void Handle(TEvRestoreBackup::TPtr& ev) {
        if (RestoreState == ERestoreState::NotStarted) {
            StartRestore(ev->Get()->BackupPath, ev->Sender, ev->Get()->SkipChecksumValidation, ev->Get()->DryRun);
        }
    }

    void Handle(TEvBackupReaderResult::TPtr& ev) {
        const auto* msg = ev->Get();
        CompleteRestore(msg->Success, msg->Error);
    }

    void Handle(TEvBackupInfo::TPtr& ev, const TActorContext& ctx) {
        TotalBytes = ev->Get()->TotalBytes;
        PreviousChangelogChecksum = NBackup::ComputeInitialChecksum(ev->Get()->Generation, ev->Get()->Step);

        if (DryRun) {
            DryRunExec = std::make_unique<TDryRunExecutor>(TabletID());
            Executor0 = DryRunExec.get();
            ITablet::ExecutorActorID = ctx.SelfID;
            OnActivateExecutor(ctx);
        } else {
            using EMode = TEvTablet::TEvCompleteRecoveryBoot::EMode;
            Send(Tablet(), new TEvTablet::TEvCompleteRecoveryBoot(EMode::WipeAllData));
        }
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

    void StartRestore(const TString& backupPath, TActorId subscriber = {}, bool skipChecksumValidation = false, bool dryRun = false) {
        RestoreState = ERestoreState::InProgress;
        SkipChecksumValidation = skipChecksumValidation;
        DryRun = dryRun;

        BackupPath = backupPath;
        RestoreSubscriber = subscriber;

        BackupReader = Register(CreateBackupReader(SelfId(), backupPath, TabletType(), TabletID(), skipChecksumValidation), TMailboxType::HTSwap, AppData()->IOPoolId);
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
                bool skipChecksum = cgi.Has("skipChecksumValidation");
                bool dryRun = cgi.Has("dryRun");
                StartRestore(path, {}, skipChecksum, dryRun);
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
                                str << "<div class='checkbox'><label>"
                                    << "<input type='checkbox' id='dryRun' name='dryRun'>"
                                    << " Dry Run"
                                    << "</label></div>";
                                str << "<div class='checkbox'><label>"
                                    << "<input type='checkbox' id='skipChecksumValidation' name='skipChecksumValidation'>"
                                    << " Skip Checksum Validation"
                                    << "</label></div>";
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

                            var isDryRun = $('#dryRun').is(':checked');
                            var msg = isDryRun
                                ? 'Are you sure you want to start dry-run restore?'
                                : 'Are you sure you want to start restore? This will override existing data';
                            if (!confirm(msg)) {
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

    TString PreviousChangelogChecksum;
    bool SkipChecksumValidation = false;
    bool DryRun = false;
    std::unique_ptr<TDryRunExecutor> DryRunExec;
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
            TString lineChecksum;

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

            if (!Self->SkipChecksumValidation) {
                if (!json.Has("sha256")) {
                    Result.Error(TStringBuilder() << "Changelog line is missing 'sha256' field: " << line);
                    return true;
                }

                if (!json["sha256"].IsString()) {
                    Result.Error(TStringBuilder() << "Invalid 'sha256' field in changelog line: " << line);
                    return true;
                }

                const TString& expectedChecksum = json["sha256"].GetString();

                static constexpr TStringBuf sha256Prefix = "\"sha256\":\"";
                auto pos = line.rfind(sha256Prefix);
                if (pos == TString::npos) {
                    Result.Error(TStringBuilder() << "Changelog line is missing 'sha256' field: " << line);
                    return true;
                }

                auto valueStart = pos + sha256Prefix.size();
                auto valueEnd = line.find('"', valueStart);
                if (valueEnd == TString::npos) {
                    Result.Error(TStringBuilder() << "Malformed sha256 field in changelog line: " << line);
                    return true;
                }

                TStringBuf before(line.data(), pos);
                TStringBuf after(line.data() + valueEnd + 1, line.size() - valueEnd - 1);
                lineChecksum = NBackup::ComputeChecksum(before, after, Self->PreviousChangelogChecksum);
    
                if (lineChecksum != expectedChecksum) {
                    Result.Error(TStringBuilder() << "Changelog checksum mismatch:"
                        << " expected " << expectedChecksum << ", got " << lineChecksum << ", line: " << line);
                    return true;
                }
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

            Self->PreviousChangelogChecksum = std::move(lineChecksum);
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
    TBackupReader(TActorId owner, const TString& backupPath,
                  TTabletTypes::EType tabletType, ui64 tabletId,
                  bool skipChecksumValidation)
        : Owner(owner)
        , BackupPath(backupPath)
        , SnapshotDirPath(BackupPath.Child("snapshot"))
        , SchemaFilePath(SnapshotDirPath.Child("schema.json"))
        , ChangelogFilePath(BackupPath.Child("changelog.json"))
        , ExpectedTabletType(tabletType)
        , ExpectedTabletId(tabletId)
        , SkipChecksumValidation(skipChecksumValidation)
    {}

    void Bootstrap() {
        if (!BackupPath.Exists()) {
            return SendResultAndDie(false, TStringBuilder() << "Backup dir doesn't exist: " << BackupPath);
        }

        if (!SnapshotDirPath.Exists()) {
            return SendResultAndDie(false, TStringBuilder() << "Snapshot dir doesn't exist: " << SnapshotDirPath);
        }

        if (!ValidateManifest(SnapshotDirPath)) {
            return;
        }

        if (!SchemaFilePath.Exists()) {
            return SendResultAndDie(false, TStringBuilder() << "Snapshot schema file doesn't exist: " << SchemaFilePath);
        }

        if (!ChangelogFilePath.Exists()) {
            return SendResultAndDie(false, TStringBuilder() << "Changelog file doesn't exist: " << ChangelogFilePath);
        }

        ui64 totalBytes = 0;
        try {
            totalBytes = CalculateTotalSize();
        } catch (const TIoException& e) {
            return SendResultAndDie(false, TStringBuilder() << "Cannot calculate total size: " << e.what());
        }

        Send(Owner, new TEvBackupInfo(totalBytes, Generation, Step));
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

    bool ValidateManifest(const TFsPath& snapshotDir) {
        auto manifestFile = snapshotDir.Child("manifest.json");
        if (!manifestFile.Exists()) {
            SendResultAndDie(false, TStringBuilder() << "Manifest file doesn't exist: " << manifestFile);
            return false;
        }

        TString manifestStr;
        try {
            manifestStr = TFileInput(manifestFile).ReadAll();
        } catch (const TIoException& e) {
            SendResultAndDie(false, TStringBuilder() << "Failed to read manifest " << manifestFile << ": " << e.what());
            return false;
        }

        if (!SkipChecksumValidation) {
            auto manifestChecksumFile = snapshotDir.Child("manifest.json.sha256");
            if (!manifestChecksumFile.Exists()) {
                SendResultAndDie(false, TStringBuilder() << "Manifest checksum file doesn't exist: " << manifestChecksumFile);
                return false;
            }

            TString expectedManifestChecksum;
            try {
                expectedManifestChecksum = TFileInput(manifestChecksumFile).ReadAll();
            } catch (const TIoException& e) {
                SendResultAndDie(false, TStringBuilder() << "Failed to read manifest checksum: " << manifestChecksumFile << ": " << e.what());
                return false;
            }

            TString actualManifestChecksum = NBackup::ComputeChecksum(manifestStr);
            if (actualManifestChecksum != expectedManifestChecksum) {
                SendResultAndDie(false, TStringBuilder() << "Manifest checksum mismatch: "
                                                         << "expected " << expectedManifestChecksum
                                                         << ", got " << actualManifestChecksum);
                return false;
            }
        }

        NJson::TJsonValue manifest;
        try {
            NJson::ReadJsonTree(manifestStr, &manifest, true);
        } catch (const std::exception& e) {
            SendResultAndDie(false, TStringBuilder() << "Failed to parse manifest: " << manifestFile << ": " << e.what());
            return false;
        }

        if (!manifest.Has("tablet_type") || !manifest["tablet_type"].IsString()) {
            SendResultAndDie(false, TStringBuilder() << "Manifest is missing 'tablet_type' field or it is not a string: " << manifest);
            return false;
        }

        TString actualTypeName = manifest["tablet_type"].GetString();
        TString expectedTypeName = TTabletTypes::EType_Name(ExpectedTabletType);
        NProtobufJson::ToSnakeCaseDense(&expectedTypeName);
        if (actualTypeName != expectedTypeName) {
            SendResultAndDie(false, TStringBuilder()
                << "Manifest tablet type mismatch: expected " << expectedTypeName
                << ", got " << actualTypeName);
            return false;
        }

        if (!manifest.Has("tablet_id") || !manifest["tablet_id"].IsUInteger()) {
            SendResultAndDie(false, TStringBuilder() << "Manifest is missing 'tablet_id' field or it is not an unsigned integer: " << manifest);
            return false;
        }

        ui64 actualTabletId = manifest["tablet_id"].GetUInteger();
        if (actualTabletId != ExpectedTabletId) {
            SendResultAndDie(false, TStringBuilder()
                << "Manifest tablet id mismatch: expected " << ExpectedTabletId
                << ", got " << actualTabletId);
            return false;
        }

        if (!manifest.Has("generation") || !manifest["generation"].IsUInteger()) {
            SendResultAndDie(false, TStringBuilder() << "Manifest is missing 'generation' field or it is not an unsigned integer: " << manifest);
            return false;
        }
        Generation = manifest["generation"].GetUInteger();

        if (!manifest.Has("step") || !manifest["step"].IsUInteger()) {
            SendResultAndDie(false, TStringBuilder() << "Manifest is missing 'step' field or it is not an unsigned integer: " << manifest);
            return false;
        }
        Step = manifest["step"].GetUInteger();

        if (!manifest.Has("files") || !manifest["files"].IsArray()) {
            SendResultAndDie(false, TStringBuilder() << "Manifest is missing 'files' array or it is not an array: " << manifest);
            return false;
        }

        const auto& files = manifest["files"].GetArray();
        for (const auto& fileEntry : files) {
            if (!fileEntry.Has("name") || !fileEntry["name"].IsString()) {
                SendResultAndDie(false, TStringBuilder() << "Manifest file entry is missing 'name' field: " << fileEntry);
                return false;
            }

            TString name = fileEntry["name"].GetString();

            auto filePath = snapshotDir.Child(name);
            if (!filePath.Exists()) {
                SendResultAndDie(false, TStringBuilder() << "File listed in manifest not found: " << filePath);
                return false;
            }

            TString basename = filePath.Basename();
            if (basename != "schema.json") {
                SnapshotFiles.push_back(filePath);
            }

            if (!SkipChecksumValidation) {
                if (!fileEntry.Has("sha256") || !fileEntry["sha256"].IsString()) {
                    SendResultAndDie(false, TStringBuilder() << "Manifest file entry is missing 'sha256' field: " << fileEntry);
                    return false;
                }

                TString expectedFileSha256 = fileEntry["sha256"].GetString();

                NOpenSsl::NSha256::TCalcer calcer;
                try {
                    TFileInput input(filePath, 1_MB);
                    char buf[64_KB];
                    size_t bytesRead;
                    while ((bytesRead = input.Read(buf, sizeof(buf))) > 0) {
                        calcer.Update(buf, bytesRead);
                    }
                } catch (const TIoException& e) {
                    SendResultAndDie(false, TStringBuilder() << "Failed to read file " << filePath << " for checksum validation: " << e.what());
                    return false;
                }

                auto fileDigest = calcer.Final();
                TString actualFileSha256 = NBackup::FormatChecksumDigest(fileDigest);
                if (actualFileSha256 != expectedFileSha256) {
                    SendResultAndDie(false, TStringBuilder() << "Checksum mismatch for " << filePath
                                             << ": expected " << expectedFileSha256
                                             << ", got " << actualFileSha256);
                    return false;
                }
            }
        }

        return true;
    }

    ui64 CalculateTotalSize() const {
        ui64 totalBytes = 0;
        totalBytes += GetFileSize(ChangelogFilePath);
        totalBytes += GetFileSize(SchemaFilePath);
        for (const auto& file : SnapshotFiles) {
            totalBytes += GetFileSize(file);
        }
        return totalBytes;
    }

    ui64 GetFileSize(const TFsPath& path) const {
        TFileStat stat(path);
        return stat.Size;
    }

    void SendResultAndDie(bool success, const TString& error = "") {
        Send(Owner, new TEvBackupReaderResult(success, error));
        PassAway();
    }

private:
    const TActorId Owner;
    const TFsPath BackupPath;

    const TFsPath SnapshotDirPath;
    const TFsPath SchemaFilePath;
    const TFsPath ChangelogFilePath;

    const TTabletTypes::EType ExpectedTabletType;
    const ui64 ExpectedTabletId;
    const bool SkipChecksumValidation;

    TDeque<TFsPath> SnapshotFiles;

    ui32 Generation = 0;
    ui32 Step = 0;

    TFsPath CurrentFilePath;
    TString CurrentTableName;
    THolder<TFileInput> CurrentFileInput;
    bool ChangelogProcessed = false;
}; // TBackupReader

IActor* CreateRecoveryShard(const TActorId &tablet, TTabletStorageInfo *info) {
    return new TRecoveryShard(tablet, info);
}

IActor* CreateBackupReader(TActorId owner, const TString& backupPath,
                           TTabletTypes::EType tabletType, ui64 tabletId,
                           bool skipChecksumValidation) {
    return new TBackupReader(owner, backupPath, tabletType, tabletId, skipChecksumValidation);
}

} //namespace NKikimr::NTabletFlatExecutor::NRecovery
