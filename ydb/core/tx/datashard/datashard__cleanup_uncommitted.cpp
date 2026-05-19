#include "datashard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr::NDataShard {

using namespace NTabletFlatExecutor;

class TDataShard::TTxCleanupUncommitted : public TTransactionBase<TDataShard> {
public:
    TTxCleanupUncommitted(TDataShard* self)
        : TTransactionBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (Self->State != TShardState::Ready) {
            // We need to be very careful about cleaning up uncommitted changes
            // Avoid mistakes by waiting until shard restarts in a Ready state
            return true;
        }

        size_t removed = 0;
        for (const auto& pr : Self->TableInfos) {
            if (pr.second->IsReplicated() || pr.second->IsIncrementalRestore()) {
                // Replicated tables use uncommitted changes for replication
                // Since we don't track them we cannot know whether they leaked or not
                continue;
            }

            auto localTid = pr.second->LocalTid;
            if (!txc.DB.GetScheme().GetTableInfo(localTid)) {
                // Note: this check is likely not needed, since all user tables
                // must be present in the Ready state, but make sure we don't
                // trip since this code always runs at startup.
                continue;
            }

            auto openTxs = txc.DB.GetOpenTxs(localTid);
            for (ui64 txId : openTxs) {
                if (Self->SysLocksTable().GetLocks().contains(txId)) {
                    // Changes are associated with a known lock
                    continue;
                }
                if (Self->GetVolatileTxManager().FindByCommitTxId(txId)) {
                    // Changes are associated with a known volatile tx
                    continue;
                }

                // Changes are neither committed nor removed and are not tracked
                if (removed >= 1000) {
                    // Avoid removing more than 1k transactions per transaction
                    Reschedule = true;
                    break;
                }

                // Remove otherwise untracked changes
                txc.DB.RemoveTx(localTid, txId);
                ++removed;
            }

            if (Reschedule) {
                break;
            }
        }

        if (removed > 0) {
            YDB_LOG_CTX_WARN(ctx, "DataShard removed untracked uncommitted changes",
                {"TabletID", Self->TabletID()},
                {"removed", removed});
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Reschedule) {
            Self->CleanupUncommitted(ctx);
        }
    }

private:
    bool Reschedule = false;
};

void TDataShard::CleanupUncommitted(const TActorContext& ctx) {
    if (State == TShardState::Ready) {
        Execute(new TTxCleanupUncommitted(this), ctx);
    }
}

} // namespace NKikimr::NDataShard
