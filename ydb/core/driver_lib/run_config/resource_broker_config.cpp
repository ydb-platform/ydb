#include "resource_broker_config.h"

#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/resource_broker.pb.h>

namespace NKikimr::NKikimrConfigHelpers {

NMemory::TResourceBrokerConfig CreateMemoryControllerResourceBrokerConfig(const NKikimrConfig::TAppConfig& config) {
    NMemory::TResourceBrokerConfig resourceBrokerSelfConfig; // for backward compatibility
    auto mergeResourceBrokerConfigs = [&](const NKikimrResourceBroker::TResourceBrokerConfig& resourceBrokerConfig) {
        if (resourceBrokerConfig.HasResourceLimit() && resourceBrokerConfig.GetResourceLimit().HasMemory()) {
            resourceBrokerSelfConfig.LimitBytes = resourceBrokerConfig.GetResourceLimit().GetMemory();
        }
        for (const auto& queue : resourceBrokerConfig.GetQueues()) {
            if (queue.HasLimit() && queue.GetLimit().HasMemory()) {
                resourceBrokerSelfConfig.QueueLimits[queue.GetName()] = queue.GetLimit().GetMemory();
            }
        }
    };
    if (config.HasBootstrapConfig() && config.GetBootstrapConfig().HasResourceBroker()) {
        mergeResourceBrokerConfigs(config.GetBootstrapConfig().GetResourceBroker());
    }
    if (config.HasResourceBrokerConfig()) {
        mergeResourceBrokerConfigs(config.GetResourceBrokerConfig());
    }
    return resourceBrokerSelfConfig;
}

} // namespace NKikimr::NKikimrConfigHelpers
