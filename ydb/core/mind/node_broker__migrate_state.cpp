#include "node_broker_impl.h"

#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr::NNodeBroker {

constexpr size_t MAX_NODES_BATCH_SIZE = 1000;

class TNodeBroker::TTxMigrateState : public TTransactionBase<TNodeBroker> {
public:
    TTxMigrateState(TNodeBroker *self, TDbChanges&& dbChanges)
        : TBase(self)
        , DbChanges(std::move(dbChanges))
    {
    }

    TTxType GetTxType() const override { return TXTYPE_MIGRATE_STATE; }

    void FinalizeMigration(TTransactionContext &txc, const TActorContext &ctx)
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxMigrateState FinalizeMigration");

        if (DbChanges.UpdateEpoch) {
            Self->Dirty.DbUpdateEpoch(Self->Dirty.Epoch, txc);
        }

        if (DbChanges.UpdateApproxEpochStart) {
            Self->Dirty.DbUpdateApproxEpochStart(Self->Dirty.ApproxEpochStart, txc);
        }

        if (DbChanges.UpdateMainNodesTable) {
            Self->Dirty.DbUpdateMainNodesTable(txc);
        }

        // Move epoch if required.
        auto now = ctx.Now();
        while (now > Self->Dirty.Epoch.End) {
            TStateDiff diff;
            Self->Dirty.ComputeNextEpochDiff(diff);
            Self->Dirty.ApplyStateDiff(diff);
            Self->Dirty.DbApplyStateDiff(diff, txc);
        }

        Finalized = true;
    }

    void ProcessMigrationBatch(TTransactionContext &txc, const TActorContext &ctx)
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, TStringBuilder()
            << "TTxMigrateState ProcessMigrationBatch"
            << "  UpdateNodes left " << DbChanges.UpdateNodes.size()
            << ", NewVersionUpdateNodes left " << DbChanges.NewVersionUpdateNodes.size());

        size_t nodesBatchSize = 0;
        while (nodesBatchSize < MAX_NODES_BATCH_SIZE && !DbChanges.UpdateNodes.empty()) {
            Self->Dirty.DbUpdateNode(DbChanges.UpdateNodes.back(), txc);
            DbChanges.UpdateNodes.pop_back();
            ++nodesBatchSize;
        }

        const bool newVersionInBatch = nodesBatchSize < MAX_NODES_BATCH_SIZE
            && !DbChanges.NewVersionUpdateNodes.empty()
            && DbChanges.UpdateEpoch;

        if (newVersionInBatch) {
            Self->Dirty.DbUpdateEpoch(Self->Dirty.Epoch, txc);
            DbChanges.UpdateEpoch = false;
            // Changing version may affect uncommitted approximate epoch start
            if (DbChanges.UpdateApproxEpochStart) {
                Self->Dirty.DbUpdateApproxEpochStart(Self->Dirty.ApproxEpochStart, txc);
                DbChanges.UpdateApproxEpochStart = false;
            }
        }

        while (nodesBatchSize < MAX_NODES_BATCH_SIZE && !DbChanges.NewVersionUpdateNodes.empty()) {
            Self->Dirty.DbUpdateNode(DbChanges.NewVersionUpdateNodes.back(), txc);
            DbChanges.NewVersionUpdateNodes.pop_back();
            ++nodesBatchSize;
        }
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxMigrateState Execute");

        ProcessMigrationBatch(txc, ctx);
        if (!DbChanges.HasNodeUpdates()) {
            FinalizeMigration(txc, ctx);
        }
        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxMigrateState Complete");

        if (Finalized) {
            Self->Committed = Self->Dirty;
            Self->Become(&TNodeBroker::StateWork);
            Self->SubscribeForConfigUpdates(ctx);
            Self->ScheduleEpochUpdate(ctx);
            Self->ScheduleProcessSubscribersQueue(ctx);
            Self->PrepareEpochCache();
            Self->PrepareUpdateNodesLog();

            NKikimrNodeBroker::TVersionInfo versionInfo;
            versionInfo.SetSupportDeltaProtocol(true);
            Self->SignalTabletActive(ctx, versionInfo.SerializeAsString());

            Self->UpdateCommittedStateCounters();
        } else {
            Self->Execute(Self->CreateTxMigrateState(std::move(DbChanges)));
        }
    }

private:
    TDbChanges DbChanges;
    bool Finalized = false;
};

ITransaction *TNodeBroker::CreateTxMigrateState(TDbChanges&& dbChanges)
{
    return new TTxMigrateState(this, std::move(dbChanges));
}

} // namespace NKikimr::NNodeBroker
