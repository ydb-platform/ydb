#include "scan_common.h"
#include "datashard_impl.h"
#include "range_ops.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <ydb/core/protos/index_builder.pb.h>

#include <ydb/core/tx/datashard/build_index/common_helper.h>

namespace NKikimr::NDataShard {

class TValidateRowConditionScan final : public TActor<TValidateRowConditionScan>, public NTable::IScan {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_DATASHARD_ACTOR;
    }

    TValidateRowConditionScan(
        const NKikimrTxDataShard::TEvValidateRowConditionRequest& request,
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
        columnNames.reserve(Request.NotNullColumnsSize());
        for (const auto& col : Request.GetNotNullColumns()) {
            columnNames.push_back(col);
        }
        ScanTags = BuildTags(tableInfo, std::move(columnNames));
    }

    ~TValidateRowConditionScan() final = default;

    TInitialState Prepare(IDriver*, TIntrusiveConstPtr<TScheme>) noexcept final {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(static_cast<IActor*>(this));
        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final {
        Y_ABORT_UNLESS(seq == 0);
        lead.To(ScanTags, {}, NTable::ESeek::Lower);
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell>, const TRow& row) noexcept final {
        const TConstArrayRef<TCell> rowCells = *row;
        for (const auto& cell : rowCells) {
            if (cell.IsNull()) {
                IsValid = false;
                Status = NKikimrIndexBuilder::EBuildStatus::DONE;
                return EScan::Final;
            }
        }
        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(NTable::IScan::EStatus) noexcept final {
        auto response = MakeHolder<TEvDataShard::TEvValidateRowConditionResponse>();
        response->Record.SetId(Request.GetId());
        response->Record.SetTabletId(TabletId);

        if (Status == NKikimrIndexBuilder::EBuildStatus::INVALID) {
            Status = NKikimrIndexBuilder::EBuildStatus::DONE;
        }

        response->Record.SetStatus(Status);
        response->Record.SetIsValid(IsValid);

        if (!IsValid) {
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
        out << "TValidateRowConditionScan";
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            default:
                break;
        }
    }

    const NKikimrTxDataShard::TEvValidateRowConditionRequest Request;
    const TActorId Sender;
    const ui64 TabletId;
    TTags ScanTags;
    NKikimrIndexBuilder::EBuildStatus Status = NKikimrIndexBuilder::EBuildStatus::INVALID;
    bool IsValid = true;
};

class TDataShard::TTxHandleSafeValidateRowConditionScan : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeValidateRowConditionScan(TDataShard* self, TEvDataShard::TEvValidateRowConditionRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) override {}

private:
    TEvDataShard::TEvValidateRowConditionRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvValidateRowConditionRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeValidateRowConditionScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvValidateRowConditionRequest::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    const ui64 id = record.GetId();
    auto rowVersion = GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};

    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }

    auto sendResponse = [&](NKikimrIndexBuilder::EBuildStatus status, const TString& error = "") {
        auto response = MakeHolder<TEvDataShard::TEvValidateRowConditionResponse>();
        response->Record.SetId(id);
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
        sendResponse(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST, TStringBuilder() << "Wrong shard " << record.GetTabletId() << " this is " << TabletID());
        return;
    }

    const auto tableId = TTableId(record.GetOwnerId(), record.GetPathId());
    if (!GetUserTables().contains(tableId.PathId.LocalPathId)) {
        sendResponse(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST, TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
        return;
    }

    if (!IsStateActive()) {
        sendResponse(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST, TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
        return;
    }

    const auto& userTable = *GetUserTables().at(tableId.PathId.LocalPathId);

    auto scan = new TValidateRowConditionScan(record, ev->Sender, TabletID(), userTable);
    StartScan(this, scan, id, seqNo, rowVersion, userTable.LocalTid);
}

} // namespace NKikimr::NDataShard