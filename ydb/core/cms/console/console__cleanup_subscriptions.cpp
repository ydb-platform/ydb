#include "console_impl.h"
#include "console_configs_manager.h"
#include "console_tenants_manager.h"

#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/cms/console/validators/registry.h>

namespace NKikimr::NConsole {

class TConfigsManager::TTxCleanupSubscriptions : public TTransactionBase<TConfigsManager> {
public:
    TTxCleanupSubscriptions(TEvInterconnect::TEvNodesInfo::TPtr &ev,
                            TConfigsManager *self)
        : TBase(self)
        , Nodes(ev)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConsole::TTxCleanupSubscriptions");

        THashSet<ui32> nodes;
        for (auto &node : Nodes->Get()->Nodes)
            nodes.insert(node.NodeId);

        for (auto pr : Self->SubscriptionIndex.GetSubscriptions()) {
            auto nodeId = pr.second->Subscriber.ServiceId.NodeId();
            if (nodeId && !nodes.contains(nodeId)) {
                Self->PendingSubscriptionModifications.RemovedSubscriptions.insert(pr.first);

                LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                            "Subscription " << pr.first << " has subscriber from unknown node "
                            << nodeId << " and will be removed");
            }
        }

        // Update database.
        Self->DbApplyPendingSubscriptionModifications(txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS,
                  "TConsole::TTxCleanupSubscriptions Complete");

        if (!Self->PendingSubscriptionModifications.IsEmpty())
            Self->ApplyPendingSubscriptionModifications(ctx);
        Self->ScheduleSubscriptionsCleanup(ctx);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvInterconnect::TEvNodesInfo::TPtr Nodes;
};

ITransaction *TConfigsManager::CreateTxCleanupSubscriptions(TEvInterconnect::TEvNodesInfo::TPtr &ev)
{
    return new TTxCleanupSubscriptions(ev, this);
}

} // namespace NKikimr::NConsole
