#include "validator_bootstrap.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/tablet/resource_broker.h>

#include <util/string/builder.h>

namespace NKikimr::NConsole {

TBootstrapConfigValidator::TBootstrapConfigValidator()
    : IConfigValidator("bootstrap",
                       { (ui32)NKikimrConsole::TConfigItem::NameserviceConfigItem,
                         (ui32)NKikimrConsole::TConfigItem::BootstrapConfigItem,
                         (ui32)NKikimrConsole::TConfigItem::ResourceBrokerConfigItem, })
{
}

TString TBootstrapConfigValidator::GetDescription() const
{
    return "Check Resource Broker config and required system tablets.";
}

bool TBootstrapConfigValidator::CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                                            const NKikimrConfig::TAppConfig &newConfig,
                                            TVector<Ydb::Issue::IssueMessage> &issues) const
{
    Y_UNUSED(oldConfig);

    if (!oldConfig.HasBootstrapConfig() && !newConfig.HasBootstrapConfig())
        return true;

    if (!newConfig.HasBootstrapConfig()) {
        AddError(issues, "missing bootstrap config");
        return false;
    }

    if (!CheckTablets(newConfig, issues))
        return false;

    if (!CheckResourceBrokerConfig(newConfig, issues))
        return false;

    if (!CheckResourceBrokerOverrides(newConfig, issues))
        return false;

    if (newConfig.GetBootstrapConfig().HasCompactionBroker()) {
        AddError(issues, "deprecated compaction broker config is used");
        return false;
    }

    return true;
}

bool TBootstrapConfigValidator::CheckTablets(const NKikimrConfig::TAppConfig &config,
                                             TVector<Ydb::Issue::IssueMessage> &issues) const
{
    THashSet<NKikimrConfig::TBootstrap::ETabletType> requiredTablets = {
        NKikimrConfig::TBootstrap::FLAT_BS_CONTROLLER,
        NKikimrConfig::TBootstrap::FLAT_SCHEMESHARD,
        NKikimrConfig::TBootstrap::FLAT_TX_COORDINATOR,
        NKikimrConfig::TBootstrap::TX_MEDIATOR,
        NKikimrConfig::TBootstrap::TX_ALLOCATOR,
        NKikimrConfig::TBootstrap::CONSOLE
    };
    THashSet<NKikimrConfig::TBootstrap::ETabletType> importantTablets = {
        NKikimrConfig::TBootstrap::CMS,
        NKikimrConfig::TBootstrap::NODE_BROKER,
        NKikimrConfig::TBootstrap::TENANT_SLOT_BROKER,
    };

    auto &cfg = config.GetBootstrapConfig();
    auto &nsCfg = config.GetNameserviceConfig();
    THashSet<ui32> nodeIds;

    for (auto &node : nsCfg.GetNode())
        nodeIds.insert(node.GetNodeId());

    for (auto &tablet : cfg.GetTablet()) {
        auto type = tablet.GetType();

        requiredTablets.erase(type);
        importantTablets.erase(type);

        for (auto id : tablet.GetNode()) {
            if (!nodeIds.contains(id)) {
                AddError(issues,
                         TStringBuilder() << "Missing node " << id
                         << " is used for tablet " << type);
                return false;
            }
        }

        if (!tablet.NodeSize()) {
            AddError(issues,
                     TStringBuilder() << "Empty nodes list for tablet " << type);
            return false;
        }
    }

    if (!requiredTablets.empty()) {
        for (auto type : requiredTablets)
            AddError(issues, TStringBuilder() << "Missing required tablet " << type);
        return false;
    }

    for (auto type : importantTablets)
        AddWarning(issues, TStringBuilder() << "Missing important tablet " << type);

    return true;
}

bool TBootstrapConfigValidator::CheckResourceBrokerConfig(const NKikimrConfig::TAppConfig &config,
                                                          TVector<Ydb::Issue::IssueMessage> &issues) const
{
    if (!config.GetBootstrapConfig().HasResourceBroker())
        return true;

    auto &cfg = config.GetBootstrapConfig().GetResourceBroker();

    THashSet<TString> queues;
    for (auto &queue : cfg.GetQueues()) {
        if (!queue.GetName()) {
            AddError(issues, "queue with empty name is not allowed");
            return false;
        }
        if (queues.contains(queue.GetName())) {
            AddError(issues, Sprintf("multiple queues with '%s' name",
                                     queue.GetName().data()));
            return false;
        }
        if (!queue.GetWeight()) {
            AddError(issues, Sprintf("queue '%s' should have non-zero weight",
                                     queue.GetName().data()));
            return false;
        }
        if (IsUnlimitedResource(queue.GetLimit())) {
            AddWarning(issues, Sprintf("unlimited resources for queue '%s'",
                                       queue.GetName().data()));
        }
        queues.insert(queue.GetName());
    }

    THashSet<TString> tasks;
    THashSet<TString> unusedQueues = queues;
    for (auto &task : cfg.GetTasks()) {
        if (!task.GetName()) {
            AddError(issues, "task with empty name is not allowed");
            return false;
        }
        if (tasks.contains(task.GetName())) {
            AddError(issues, Sprintf("multiple tasks with '%s' name",
                                     task.GetName().data()));
            return false;
        }
        if (!queues.contains(task.GetQueueName())) {
            AddError(issues, Sprintf("task '%s' uses unknown queue '%s'",
                                     task.GetName().data(), task.GetQueueName().data()));
            return false;
        }
        if (!task.GetDefaultDuration()) {
            AddError(issues, Sprintf("task '%s' should have non-zero default duration",
                                     task.GetName().data()));
            return false;
        }
        tasks.insert(task.GetName());
        unusedQueues.erase(task.GetQueueName());
    }

    if (!queues.contains(NLocalDb::DefaultQueueName)) {
        AddError(issues, Sprintf("config should have '%s' queue defined",
                                 NLocalDb::DefaultQueueName.data()));
        return false;
    }
    if (!tasks.contains(NLocalDb::UnknownTaskName)) {
        AddError(issues, Sprintf("config should have '%s' task defined",
                                 NLocalDb::UnknownTaskName.data()));
        return false;
    }

    for (auto &queue : unusedQueues) {
        if (queue != NLocalDb::DefaultQueueName)
            AddWarning(issues, Sprintf("queue '%s' is not used by any task", queue.data()));
    }

    if (IsUnlimitedResource(cfg.GetResourceLimit()))
        AddWarning(issues, "unlimited total resources");

    return true;
}

bool TBootstrapConfigValidator::CheckResourceBrokerOverrides(
        const NKikimrConfig::TAppConfig &config,
        TVector<Ydb::Issue::IssueMessage> &issues) const
{
    if (!config.HasResourceBrokerConfig())
        return true;

    auto defaultConfig = NResourceBroker::MakeDefaultConfig();
    const auto &overrides = config.GetResourceBrokerConfig();

    THashSet<TString> defaultQueues;
    for (const auto &queue : defaultConfig.GetQueues()) {
        defaultQueues.insert(queue.GetName());
    }

    THashSet<TString> queues;
    for (const auto &queue : overrides.GetQueues()) {
        if (!queue.GetName()) {
            AddError(issues, "queue with empty name is not allowed");
            return false;
        }
        if (queues.contains(queue.GetName())) {
            AddError(issues, Sprintf("multiple queues with '%s' name",
                                     queue.GetName().data()));
            return false;
        }
        if (!defaultQueues.contains(queue.GetName())) {
            if (!queue.GetWeight()) {
                AddError(issues, Sprintf("queue '%s' should have non-zero weight",
                                        queue.GetName().data()));
                return false;
            }
            if (IsUnlimitedResource(queue.GetLimit())) {
                AddWarning(issues, Sprintf("unlimited resources for queue '%s'",
                                        queue.GetName().data()));
            }
        }
        queues.insert(queue.GetName());
    }

    THashSet<TString> defaultTasks;
    for (const auto &task : defaultConfig.GetTasks()) {
        defaultTasks.insert(task.GetName());
    }

    THashSet<TString> tasks;
    for (auto &task : overrides.GetTasks()) {
        if (!task.GetName()) {
            AddError(issues, "task with empty name is not allowed");
            return false;
        }
        if (tasks.contains(task.GetName())) {
            AddError(issues, Sprintf("multiple tasks with '%s' name",
                                     task.GetName().data()));
            return false;
        }
        if (!defaultQueues.contains(task.GetQueueName()) && !queues.contains(task.GetQueueName())) {
            AddError(issues, Sprintf("task '%s' uses unknown queue '%s'",
                                     task.GetName().data(), task.GetQueueName().data()));
            return false;
        }
        if (!defaultTasks.contains(task.GetName())) {
            if (!task.GetDefaultDuration()) {
                AddError(issues, Sprintf("task '%s' should have non-zero default duration",
                                        task.GetName().data()));
                return false;
            }
        }
        tasks.insert(task.GetName());
    }

    return true;
}

bool TBootstrapConfigValidator::IsUnlimitedResource(const NKikimrResourceBroker::TResources &limit) const
{
    for (auto res : limit.GetResource()) {
        if (res > 0 && res < Max<ui64>())
            return false;
    }
    if (limit.HasCpu() && limit.GetCpu() < Max<ui64>())
        return false;
    if (limit.HasMemory() && limit.GetMemory() < Max<ui64>())
        return false;
    return true;
}

} // namespace NKikimr::NConsole
