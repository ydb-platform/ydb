#include "console_impl.h"
#include "console_configs_manager.h"
#include "console_tenants_manager.h"

#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/cms/console/validators/registry.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_CONFIGS

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
        YDB_LOG_CTX_DEBUG(ctx, "TConsole::TTxCleanupSubscriptions");

        THashSet<ui32> nodes;
        for (auto &node : Nodes->Get()->Nodes)
            nodes.insert(node.NodeId);

        for (auto pr : Self->SubscriptionIndex.GetSubscriptions()) {
            auto nodeId = pr.second->Subscriber.ServiceId.NodeId();
            if (nodeId && !nodes.contains(nodeId)) {
                Self->PendingSubscriptionModifications.RemovedSubscriptions.insert(pr.first);

                YDB_LOG_CTX_DEBUG(ctx, "Subscription has subscriber from unknown node and will be removed",
                    {"first", pr.first},
                    {"nodeId", nodeId});
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
