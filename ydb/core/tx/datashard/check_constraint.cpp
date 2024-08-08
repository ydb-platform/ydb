#include "scan_common.h"
#include "datashard_impl.h"
#include "range_ops.h"
#include "upload_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/kqp/common/kqp_types.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/core/ydb_convert/ydb_convert.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <ydb/core/ydb_convert/table_description.h>


namespace NKikimr::NDataShard {

#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)

bool CheckNotNullConstraint(const TConstArrayRef<TCell>& cells) {
    for (const auto& cell : cells) {
        if (cell.IsNull()) {
            return false;
        }
    }

    return true;
}

class TCheckColumnScan final: public TActor<TCheckColumnScan>, public NTable::IScan {
private:
    ui64 BuildIndexId;
    TString TargetTable;
    const TScanRecord::TSeqNo SeqNo;
    ui64 DataShardId;
    const TActorId ProgressActorId;

    const TSerializedTableRange RequestedRange;
    const TVector<NScheme::TTypeInfo> KeyTypes;
    const TSerializedTableRange TableRange;

    IDriver* Driver = nullptr;

    TTags ScanTags;

    enum ECheckingNotNullStatus {
        Ok,
        NullFound
    } CheckingNotNullStatus = ECheckingNotNullStatus::Ok;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHECK_CONSTRAINT_ACTOR;
    }

    TCheckColumnScan(
        ui64 buildIndexId,
        TString targetTable,
        const TScanRecord::TSeqNo& seqNo,
        ui64 dataShardId,
        const TActorId& progressActorId,
        const TUserTable& tableInfo,
        const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings,
        const TSerializedTableRange& range
    )
        : TActor(&TThis::StateWork)
        , BuildIndexId(buildIndexId)
        , TargetTable(targetTable)
        , SeqNo(seqNo)
        , DataShardId(dataShardId)
        , ProgressActorId(progressActorId)
        , RequestedRange(range)
        , KeyTypes(tableInfo.KeyColumnTypes)
        , TableRange(tableInfo.Range)
    {
        Y_ABORT_UNLESS(checkingNotNullSettings.ColumnsSize() > 0);

        TVector<TString> columnNames;
        columnNames.reserve(checkingNotNullSettings.GetColumns().size());
        for (auto& col : checkingNotNullSettings.GetColumns()) {
            columnNames.push_back(col.GetColumnName());
        }

        ScanTags = BuildTags(tableInfo, std::move(columnNames));
    }

    ~TCheckColumnScan() final = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final {
        LOG_D("Prepare " << Debug());
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

        Driver = driver;

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final {
        LOG_D("Seek " << Debug());
        Y_ABORT_UNLESS(seq == 0);

        auto scanRange = Intersect(KeyTypes, RequestedRange.ToTableRange(), TableRange.ToTableRange());

        if (scanRange.From) {
            auto seek = scanRange.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            lead.To(ScanTags, scanRange.From, seek);
        } else {
            lead.To(ScanTags, {}, NTable::ESeek::Lower);
        }

        if (scanRange.To) {
            lead.Until(scanRange.To, scanRange.InclusiveTo);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell>, const TRow& row) noexcept final {
        LOG_D("Feed");

        const TConstArrayRef<TCell> rowCells = *row;
        if (!CheckNotNullConstraint(rowCells)) {
            CheckingNotNullStatus = ECheckingNotNullStatus::NullFound;
            return EScan::Final;
        }

        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final {
        LOG_D("Finish " << Debug());
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());

        auto progress = MakeHolder<TEvDataShard::TEvCheckConstraintProgressResponse>();
        progress->Record.SetBuildIndexId(BuildIndexId);
        progress->Record.SetTabletId(DataShardId);
        progress->Record.SetRequestSeqNoGeneration(SeqNo.Generation);
        progress->Record.SetRequestSeqNoRound(SeqNo.Round);

        if (abort != EAbort::None) {
            progress->Record.SetStatus(NKikimrTxDataShard::EBuildIndexStatus::ABORTED);
            Ydb::Issue::IssueMessage* issue = progress->Record.AddIssues();
            issue->set_message("Aborted by scan host env");
            issue->set_severity(NYql::TSeverityIds::S_ERROR);

            LOG_W(Debug());
        }

        switch (CheckingNotNullStatus) {
            case ECheckingNotNullStatus::NullFound:
            {
                progress->Record.SetStatus(NKikimrTxDataShard::EBuildIndexStatus::CHECKING_NOT_NULL_ERROR);
                Ydb::Issue::IssueMessage* issue = progress->Record.AddIssues();
                issue->set_message("Column contains null value, so not-null constraint was not set.");
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                break;
            }
            case ECheckingNotNullStatus::Ok:
                progress->Record.SetStatus(NKikimrTxDataShard::EBuildIndexStatus::DONE);
                break;
        }

        ctx.Send(ProgressActorId, progress.Release());

        Driver = nullptr;
        PassAway();
        return nullptr;
    }

    EScan Exhausted() noexcept final {
        LOG_D("Exhausted " << Debug());
        return EScan::Final;
    }

    void Describe(IOutputStream& out) const noexcept final {
        out << Debug();
    }

    TString Debug() const {
        return TStringBuilder() << "TCheckColumnScan: "
                                << "datashard id: " << DataShardId
                                << ", generation: " << SeqNo.Generation
                                << ", round: " << SeqNo.Round
                                << ", requested range: " << DebugPrintRange(KeyTypes, RequestedRange.ToTableRange(), *AppData()->TypeRegistry);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            default:
                LOG_E("TCheckColumnScan: StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString() << " " << Debug());
        }
    }
};

TAutoPtr<NTable::IScan> CreateCheckConstraintScan(
    ui64 buildIndexId,
    TString targetTable,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TUserTable& tableInfo,
    const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings,
    const TSerializedTableRange& range
)
{
    return new TCheckColumnScan(
        buildIndexId,
        targetTable,
        seqNo,
        dataShardId,
        progressActorId,
        tableInfo,
        checkingNotNullSettings,
        range
    );
}

class TDataShard::TTxHandleSafeCheckConstraintScan : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeCheckConstraintScan(TDataShard* self, TEvDataShard::TEvCheckConstraintCreateRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(TTransactionContext&, const TActorContext& ctx) {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) {
        // nothing
    }

private:
    TEvDataShard::TEvCheckConstraintCreateRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvCheckConstraintCreateRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeCheckConstraintScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvCheckConstraintCreateRequest::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    TRowVersion rowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion,
                                                     std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }

    auto response = MakeHolder<TEvDataShard::TEvCheckConstraintProgressResponse>();
    response->Record.SetBuildIndexId(record.GetBuildIndexId());
    response->Record.SetTabletId(TabletID());
    response->Record.SetStatus(NKikimrTxDataShard::EBuildIndexStatus::ACCEPTED);

    TScanRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);

    auto badRequest = [&](const TString& error) {
        response->Record.SetStatus(NKikimrTxDataShard::EBuildIndexStatus::BAD_REQUEST);
        auto issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);
        ctx.Send(ev->Sender, std::move(response));
    };

    const ui64 buildIndexId = record.GetBuildIndexId();
    const ui64 shardId = record.GetTabletId();
    const auto tableId = TTableId(record.GetOwnerId(), record.GetPathId());

    if (shardId != TabletID()) {
        badRequest(TStringBuilder() << "Wrong shard " << shardId << " this is " << TabletID());
        return;
    }

    if (!GetUserTables().contains(tableId.PathId.LocalPathId)) {
        badRequest(TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
        return;
    }

    const auto& userTable = *GetUserTables().at(tableId.PathId.LocalPathId);

    if (const auto* recCard = ScanManager.Get(buildIndexId)) {
        if (recCard->SeqNo == seqNo) {
            // do no start one more scan
            ctx.Send(ev->Sender, std::move(response));
            return;
        }

        CancelScan(userTable.LocalTid, recCard->ScanId);
        ScanManager.Drop(buildIndexId);
    }

    TSerializedTableRange requestedRange;
    requestedRange.Load(record.GetKeyRange());

    auto scanRange = Intersect(userTable.KeyColumnTypes, requestedRange.ToTableRange(), userTable.Range.ToTableRange());

    if (scanRange.IsEmptyRange(userTable.KeyColumnTypes)) {
        badRequest(TStringBuilder() << " requested range doesn't intersect with table range"
                                    << " requestedRange: " << DebugPrintRange(userTable.KeyColumnTypes, requestedRange.ToTableRange(), *AppData()->TypeRegistry)
                                    << " tableRange: " << DebugPrintRange(userTable.KeyColumnTypes, userTable.Range.ToTableRange(), *AppData()->TypeRegistry)
                                    << " scanRange: " << DebugPrintRange(userTable.KeyColumnTypes, scanRange, *AppData()->TypeRegistry));
        return;
    }

    if (!record.HasSnapshotStep() || !record.HasSnapshotTxId()) {
        badRequest(TStringBuilder() << " request doesn't have Shapshot Step or TxId");
        return;
    }

    const TSnapshotKey snapshotKey(tableId.PathId, rowVersion.Step, rowVersion.TxId);
    const TSnapshot* snapshot = SnapshotManager.FindAvailable(snapshotKey);
    if (!snapshot) {
        badRequest(TStringBuilder()
                   << "no snapshot has been found"
                   << " , path id is " << tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
                   << " , snapshot step is " << snapshotKey.Step
                   << " , snapshot tx is " << snapshotKey.TxId);
        return;
    }

    if (!IsStateActive()) {
        badRequest(TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
        return;
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10); // TODO(flown4qqqq) Should be different group?

    const auto scanId = QueueScan(userTable.LocalTid,
                                  CreateCheckConstraintScan(
                                                       buildIndexId,
                                                       record.GetTargetName(),
                                                       seqNo,
                                                       shardId,
                                                       ev->Sender,
                                                       userTable,
                                                       record.GetCheckingNotNullSettings(),
                                                       requestedRange
                                                    ),
                                  ev->Cookie,
                                  scanOpts);

    TScanRecord recCard = {scanId, seqNo};

    ScanManager.Set(buildIndexId, recCard);

    ctx.Send(ev->Sender, std::move(response));
}

} // namespace NKikimr::NDataShard
