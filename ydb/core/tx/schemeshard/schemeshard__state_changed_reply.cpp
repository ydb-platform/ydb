#include "schemeshard_impl.h"

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxShardStateChanged : public TSchemeShard::TRwTxBase {
    TEvDataShard::TEvStateChanged::TPtr Ev;
    TSideEffects SideEffects;

    TTxShardStateChanged(TSelf *self, TEvDataShard::TEvStateChanged::TPtr& ev)
        : TRwTxBase(self)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_DATASHARD_STATE_RESULT; }

    void DeleteShard(TTabletId tabletId, const TActorContext &ctx) {
        auto shardIdx = Self->GetShardIdx(tabletId);
        if (shardIdx == InvalidShardIdx) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "TTxShardStateChanged DoExecute"
                           << "Unknown shardIdx for tabletId,"
                           << ", tabletId: " << tabletId
                           << ", state: " << DatashardStateName(NDataShard::TShardState::Offline));
            return;
        }

        SideEffects.DeleteShard(shardIdx);
    }

    void ProgressDependentOperation(TTabletId tabletId, const TActorContext &ctx) {
        for(auto txIdNum: Self->PipeTracker.FindTx(ui64(tabletId))) {
            auto txId = TTxId(txIdNum);
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TTxShardStateChanged DoExecute"
                            << "Operation should be restarted in case missing one of shard"
                            << ", txId: " << txId
                            << ", tabletId: " << tabletId);

            if (!Self->Operations.contains(txId)) {
                continue;
            }

            TOperation::TPtr operation = Self->Operations.at(txId);

            for (ui64 pipeTrackerCookie : Self->PipeTracker.FindCookies(ui64(txId), ui64(tabletId))) {
                auto opId = TOperationId(txId, pipeTrackerCookie);
                SideEffects.ActivateTx(opId);
            }

            if (!operation->PipeBindedMessages.contains(tabletId)) {
                continue;
            }

            for (auto& item: operation->PipeBindedMessages.at(tabletId)) {
                auto msgCookie = item.first;
                auto& msg = item.second;
                SideEffects.UnbindMsgFromPipe(msg.OpId, tabletId, msgCookie);
            }
        }
    }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        auto tabletId = TTabletId(Ev->Get()->Record.GetTabletId());
        auto state = Ev->Get()->Record.GetState();

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxShardStateChanged DoExecute"
                       << ", datashard informs about state changing"
                       << ", datashardId: " << tabletId
                       << ", state: " << DatashardStateName(state)
                       << ", at schemeshard: " << Self->TabletID());

        // Ack state change notification
        auto event = MakeHolder<TEvDataShard::TEvStateChangedResult>(Self->TabletID(), state);
        SideEffects.Send(Ev->Get()->GetSource(), std::move(event));

        if (state == NDataShard::TShardState::Offline) {
            DeleteShard(tabletId, ctx);
            ProgressDependentOperation(tabletId, ctx);
        }

        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxShardStateChanged(
    TEvDataShard::TEvStateChanged::TPtr& ev)
{
    return new TTxShardStateChanged(this, ev);
}

}}
