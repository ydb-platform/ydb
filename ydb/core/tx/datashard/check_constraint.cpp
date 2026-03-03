#include "scan_common.h"
#include "datashard_impl.h"
#include "range_ops.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NKikimr::NDataShard {

using namespace NKikimr::NDataShard::NKikimrTxDataShard;

class TCheckColumnsScan final : public TActor<TCheckColumnsScan>, public NTable::IScan {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHECK_CONSTRAINT_ACTOR;
    }

    TCheckColumnsScan(
        const TEvCheckConstraintRequest::ProtoRecordType& request,
        const TActorId& sender,
        ui64 tabletId,
        const TUserTable& tableInfo
    )
        : TActor(&TThis::StateWork)
        , Request(request)
        , Sender(sender)
        , TabletId(tabletId)
    {
        TVector<TString> columnNames;
        columnNames.reserve(Request.CheckingColumnsSize());
        for (const auto& col : Request.GetCheckingColumns()) {
            columnNames.push_back(col.GetColumnName());
        }
        ScanTags = BuildTags(tableInfo, std::move(columnNames));
    }

    ~TCheckColumnsScan() final = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final {
        Y_ABORT_UNLESS(seq == 0);
        lead.To(ScanTags, {}, NTable::ESeek::Lower);
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> cells, const TRow& row) noexcept final {
        const TConstArrayRef<TCell> rowCells = *row;
        for (const auto& cell : rowCells) {
            if (cell.IsNull()) {
                Status = NKikimrSetColumnConstraint::ECheckStatus::ERROR;
                return EScan::Final;
            }
        }
        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final {
        if (abort != EAbort::None) {
            PassAway();
            return nullptr;
        }

        auto response = MakeHolder<TEvCheckConstraintResponse>();
        response->Record.SetId(Request.GetId());
        response->Record.SetTabletId(TabletId);

        if (Status == NKikimrSetColumnConstraint::ECheckStatus::INVALID) {
            Status = NKikimrSetColumnConstraint::ECheckStatus::SUCCESS;
        }

        response->Record.SetStatus(Status);

        if (Status == NKikimrSetColumnConstraint::ECheckStatus::ERROR) {
            auto* issue = response->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message("Constraint violation: NULL value found.");
        }

        TActivationContext::Send(new IEventHandle(Sender, SelfId(), response.Release()));
        PassAway();
        return nullptr;
    }

    EScan Exhausted() noexcept final {
        return EScan::Final;
    }

    void Describe(IOutputStream& out) const noexcept final {
        out << "TCheckColumnsScan";
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            default:
                break;
        }
    }

    const TEvCheckConstraintRequest::ProtoRecordType Request;
    const TActorId Sender;
    const ui64 TabletId;
    TTags ScanTags;
    NKikimrSetColumnConstraint::ECheckStatus Status = NKikimrSetColumnConstraint::ECheckStatus::INVALID;
};

class TDataShard::TTxHandleSafeCheckConstraintScan : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeCheckConstraintScan(TDataShard* self, TEvCheckConstraintRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) override {}

private:
    TEvCheckConstraintRequest::TPtr Ev;
};

void TDataShard::Handle(TEvCheckConstraintRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeCheckConstraintScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvCheckConstraintRequest::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    TRowVersion rowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }

    auto sendResponse = [&](NKikimrSetColumnConstraint::ECheckStatus status, const TString& error = "") {
        auto response = MakeHolder<TEvCheckConstraintResponse>();
        response->Record.SetId(record.GetId());
        response->Record.SetTabletId(TabletID());
        response->Record.SetStatus(status);
        if (!error.empty()) {
            auto* issue = response->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(error);
        }
        ctx.Send(ev->Sender, std::move(response));
    };

    if (record.GetTabletId() != TabletID()) {
        sendResponse(NKikimrSetColumnConstraint::ECheckStatus::BAD_REQUEST, TStringBuilder() << "Wrong shard " << record.GetTabletId() << " this is " << TabletID());
        return;
    }

    const auto tableId = TTableId(record.GetOwnerId(), record.GetPathId());
    if (!GetUserTables().contains(tableId.PathId.LocalPathId)) {
        sendResponse(NKikimrSetColumnConstraint::ECheckStatus::BAD_REQUEST, TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
        return;
    }

    const auto& userTable = *GetUserTables().at(tableId.PathId.LocalPathId);

    TScanRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};
    if (const auto* recCard = ScanManager.Get(record.GetId())) {
        if (recCard->SeqNo == seqNo) {
            sendResponse(NKikimrSetColumnConstraint::ECheckStatus::IN_PROGRESS);
            return;
        }
        CancelScan(userTable.LocalTid, recCard->ScanId);
        ScanManager.Drop(record.GetId());
    }

    if (!record.HasSnapshotStep() || !record.HasSnapshotTxId()) {
        sendResponse(NKikimrSetColumnConstraint::ECheckStatus::BAD_REQUEST, "Request doesn't have Snapshot Step or TxId");
        return;
    }

    const TSnapshotKey snapshotKey(tableId.PathId, rowVersion.Step, rowVersion.TxId);
    const TSnapshot* snapshot = SnapshotManager.FindAvailable(snapshotKey);
    if (!snapshot) {
        sendResponse(NKikimrSetColumnConstraint::ECheckStatus::BAD_REQUEST, TStringBuilder()
                   << "No snapshot has been found"
                   << ", path id is " << tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
                   << ", snapshot step is " << snapshotKey.Step
                   << ", snapshot tx is " << snapshotKey.TxId);
        return;
    }

    if (!IsStateActive()) {
        sendResponse(NKikimrSetColumnConstraint::ECheckStatus::BAD_REQUEST, TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
        return;
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10);

    auto scan = new TCheckColumnsScan(record, ev->Sender, TabletID(), userTable);
    
    ui64 scanId = QueueScan(userTable.LocalTid, scan, ev->Cookie, scanOpts);
    
    TScanRecord scanRecord = {scanId, seqNo};
    ScanManager.Set(record.GetId(), scanRecord);

    sendResponse(NKikimrSetColumnConstraint::ECheckStatus::ACCEPTED);
}

} // namespace NKikimr::NDataShard