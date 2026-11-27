#include "setup.h"

#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {

void ConfigureBlobStorage(NActors::TTestActorRuntime& runtime, TBlobStorageSubsystemFactory factory) {
    Y_ABORT_UNLESS(factory);
    auto previous = std::move(runtime.SetupNodeSubSystems);
    runtime.SetupNodeSubSystems = [factory = std::move(factory), previous = std::move(previous)](
            ui32 nodeIndex, NActors::TActorSystemSetup* setup) {
        if (previous) {
            previous(nodeIndex, setup);
        }
        InstallBlobStorageSubsystem(*setup, factory(nodeIndex));
    };
}

void SetupTabletServicesWithBlobStorage(TTestActorRuntime& runtime, TAppPrepare* app) {
    TAppPrepare defaults(TAppPrepare::TLightweightTag{});
    if (!app) {
        app = &defaults;
    }
    if (!app->Domains->Domain) {
        app->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1").Release());
        app->AddHive(0);
    }

    // Validate the implementation before starting any node actor system.
    auto previous = std::move(runtime.SetupNodeSubSystems);
    runtime.SetupNodeSubSystems = [previous = std::move(previous)](ui32 nodeIndex, NActors::TActorSystemSetup* setup) {
        if (previous) {
            previous(nodeIndex, setup);
        }
        Y_ABORT_UNLESS(NActors::GetSubSystem<IBlobStorageSubsystem>(setup->SubSystems),
            "tablet setup requires an explicitly configured BlobStorage subsystem");
    };

    while (app->Icb.size() < runtime.GetNodeCount()) {
        app->Icb.emplace_back(new TControlBoard);
    }
    for (const auto& board : app->Icb) {
        board->CreateConfigControls(true);
        board->UpdateControls(app->ImmediateControlsConfig);
    }

    NSharedCache::TSharedCacheConfig cache;
    cache.SetMemoryLimit(32_MB);
    for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
        SetupStateStorageGroups(runtime, node);
        SetupTabletResolver(runtime, node);
        SetupTabletPipePerNodeCaches(runtime, node, false);
        SetupResourceBroker(runtime, node, app->ResourceBrokerConfig);
        SetupSharedPageCache(runtime, node, cache);
        SetupMonitoringProxy(runtime, node);
        SetupNodeWhiteboard(runtime, node);
        SetupNodeTabletMonitor(runtime, node);
        runtime.AddLocalService(MakeTabletCountersAggregatorID(runtime.GetNodeId(node)),
            NActors::TActorSetupCmd(CreateTabletCountersAggregator(false), NActors::TMailboxType::Revolving, 0), node);
    }
    runtime.Initialize(app->Unwrap());
}


}
