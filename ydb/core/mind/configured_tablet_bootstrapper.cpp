#include "configured_tablet_bootstrapper.h"

#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

// for 'create' funcs
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/backup/controller/tablet.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/replication/controller/controller.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceshard/sequenceshard.h>
#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/cms/cms.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/node_broker.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/kesus/tablet/tablet.h>
#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/test_tablet/test_tablet.h>
#include <ydb/core/tablet/simple_tablet.h>
#include <ydb/core/blob_depot/blob_depot.h>
#include <ydb/core/statistics/aggregator/aggregator.h>
#include <ydb/core/tablet_flat/flat_executor_recovery.h>

#include <ydb/library/actors/core/hfunc.h>

#include <google/protobuf/util/message_differencer.h>

namespace NKikimr {

class TConfiguredTabletBootstrapper : public TActorBootstrapped<TConfiguredTabletBootstrapper> {

    struct TTabletState {
        TActorId BootstrapperInstance;
        NKikimrConfig::TBootstrap::TTablet Config;
    };

    THashMap<ui64, TTabletState> TabletStates;

    void ProcessTabletsFromConfig(const NKikimrConfig::TBootstrap &bootstrapConfig) {
        THashSet<ui64> currentTabletIds;
        for (const auto &tablet : bootstrapConfig.GetTablet()) {
            ui64 tabletId = tablet.GetInfo().GetTabletID();
            currentTabletIds.insert(tabletId);
            UpdateTablet(tablet);
        }

        // remove tablets that are no longer in config
        for (auto it = TabletStates.begin(); it != TabletStates.end(); ) {
            if (!currentTabletIds.contains(it->first)) {
                StopBootstrapper(it->first, it->second);
                TabletStates.erase(it++);
            } else {
                ++it;
            }
        }
    }

    void StopBootstrapper(ui64 tabletId, TTabletState& state) {
        if (!state.BootstrapperInstance) {
            return;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "Stopping tablet " << tabletId << " bootstrapper on node " << SelfId().NodeId());

        Send(state.BootstrapperInstance, new TEvents::TEvPoisonPill());
        TActivationContext::ActorSystem()->RegisterLocalService(MakeBootstrapperID(tabletId, SelfId().NodeId()), TActorId());
        state.BootstrapperInstance = TActorId();
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev) {
        const auto &record = ev->Get()->Record;

        if (record.GetConfig().HasBootstrapConfig()) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "Received bootstrap config update on node " << SelfId().NodeId());
            const auto &bootstrapConfig = record.GetConfig().GetBootstrapConfig();

            ProcessTabletsFromConfig(bootstrapConfig);
        }

        Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
    }

    void UpdateTablet(const NKikimrConfig::TBootstrap::TTablet &tabletConfig) {
        ui64 tabletId = tabletConfig.GetInfo().GetTabletID();
        auto &state = TabletStates[tabletId];

        google::protobuf::util::MessageDifferencer md;
        if (!state.BootstrapperInstance || !md.Compare(state.Config, tabletConfig)) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "Tablet " << tabletId << " config changed, recreating bootstrapper on node " << SelfId().NodeId());
            state.Config.CopyFrom(tabletConfig);
            RecreateBootstrapper(tabletId, tabletConfig);
        }
    }

    void RecreateBootstrapper(ui64 tabletId, const NKikimrConfig::TBootstrap::TTablet &config) {
        auto &state = TabletStates[tabletId];
        const ui32 selfNode = SelfId().NodeId();

        StopBootstrapper(tabletId, state);

        // not apply config
        if (Find(config.GetNode(), selfNode) != config.GetNode().end()) {
            TIntrusivePtr<TTabletStorageInfo> storageInfo = TabletStorageInfoFromProto(config.GetInfo());
            if (config.HasBootType()) {
                storageInfo->BootType = BootTypeFromProto(config.GetBootType());
            }

            const auto *appData = AppData();

            // extract from kikimr_services_initializer
            const TTabletTypes::EType tabletType = BootstrapperTypeToTabletType(config.GetType());

            if (storageInfo->TabletType == TTabletTypes::TypeInvalid)
                storageInfo->TabletType = tabletType;

            TIntrusivePtr<TTabletSetupInfo> tabletSetupInfo = MakeTabletSetupInfo(tabletType, storageInfo->BootType,
                appData->UserPoolId, appData->SystemPoolId);

            TIntrusivePtr<TBootstrapperInfo> bi = new TBootstrapperInfo(tabletSetupInfo.Get());
            for (ui32 node : config.GetNode()) {
                bi->Nodes.emplace_back(node);
            }
            if (config.HasWatchThreshold())
                bi->WatchThreshold = TDuration::MilliSeconds(config.GetWatchThreshold());
            if (config.HasStartFollowers())
                bi->StartFollowers = config.GetStartFollowers();

            bool standby = config.GetStandBy();
            state.BootstrapperInstance = Register(CreateBootstrapper(storageInfo.Get(), bi.Get(), standby), TMailboxType::HTSwap, appData->SystemPoolId);

            TActivationContext::ActorSystem()->RegisterLocalService(MakeBootstrapperID(tabletId, selfNode), state.BootstrapperInstance);

            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "Started tablet " << tabletId << " bootstrapper on node " << selfNode);

        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CONFIGURED_BOOTSTRAPPER;
    }

    NKikimrConfig::TBootstrap InitialBootstrapConfig;

    TConfiguredTabletBootstrapper(const NKikimrConfig::TBootstrap &bootstrapConfig)
        : InitialBootstrapConfig(bootstrapConfig)
    {
    }

    void Bootstrap() {
        ProcessTabletsFromConfig(InitialBootstrapConfig);

        // and subscribe for changes
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(NKikimrConsole::TConfigItem::BootstrapConfigItem));
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        }
    }
};


TTabletTypes::EType BootstrapperTypeToTabletType(ui32 type) {
    switch (type) {
    case NKikimrConfig::TBootstrap::TX_DUMMY:
        return TTabletTypes::Dummy;
    case NKikimrConfig::TBootstrap::HIVE:
    case NKikimrConfig::TBootstrap::FLAT_HIVE:
        return TTabletTypes::Hive;
    case NKikimrConfig::TBootstrap::TX_COORDINATOR:
    case NKikimrConfig::TBootstrap::FLAT_TX_COORDINATOR:
        return TTabletTypes::Coordinator;
    case NKikimrConfig::TBootstrap::TX_MEDIATOR:
        return TTabletTypes::Mediator;
    case NKikimrConfig::TBootstrap::BS_DOMAINCONTROLLER:
    case NKikimrConfig::TBootstrap::FLAT_BS_CONTROLLER:
        return TTabletTypes::BSController;
    case NKikimrConfig::TBootstrap::DATASHARD:
    case NKikimrConfig::TBootstrap::FAKE_DATASHARD:
        return TTabletTypes::DataShard;
    case NKikimrConfig::TBootstrap::SCHEMESHARD:
    case NKikimrConfig::TBootstrap::FLAT_SCHEMESHARD:
        return TTabletTypes::SchemeShard;
    case NKikimrConfig::TBootstrap::KEYVALUEFLAT:
        return TTabletTypes::KeyValue;
    case NKikimrConfig::TBootstrap::TX_PROXY:
    case NKikimrConfig::TBootstrap::FLAT_TX_PROXY:
    case NKikimrConfig::TBootstrap::TX_ALLOCATOR:
        return TTabletTypes::TxAllocator;
    case NKikimrConfig::TBootstrap::CMS:
        return TTabletTypes::Cms;
    case NKikimrConfig::TBootstrap::NODE_BROKER:
        return TTabletTypes::NodeBroker;
    case NKikimrConfig::TBootstrap::TENANT_SLOT_BROKER:
        return TTabletTypes::TenantSlotBroker;
    case NKikimrConfig::TBootstrap::CONSOLE:
        return TTabletTypes::Console;
    default:
        Y_ABORT("unknown tablet type");
    }
    return TTabletTypes::TypeInvalid;
}

TIntrusivePtr<TTabletSetupInfo> MakeTabletSetupInfo(
        TTabletTypes::EType tabletType,
        ETabletBootType bootType,
        ui32 poolId, ui32 tabletPoolId)
{
    TTabletSetupInfo::TTabletCreationFunc createFunc;

    switch (tabletType) {
    case TTabletTypes::BSController:
        createFunc = &CreateFlatBsController;
        break;
    case TTabletTypes::Hive:
        createFunc = &CreateDefaultHive;
        break;
    case TTabletTypes::Coordinator:
        createFunc = &CreateFlatTxCoordinator;
        break;
    case TTabletTypes::Mediator:
        createFunc = &CreateTxMediator;
        break;
    case TTabletTypes::TxAllocator:
        createFunc = &CreateTxAllocator;
        break;
    case TTabletTypes::DataShard:
        createFunc = &CreateDataShard;
        break;
    case TTabletTypes::SchemeShard:
        createFunc = &CreateFlatTxSchemeShard;
        break;
    case TTabletTypes::KeyValue:
        createFunc = &CreateKeyValueFlat;
        break;
    case TTabletTypes::Cms:
        createFunc = &NCms::CreateCms;
        break;
    case TTabletTypes::NodeBroker:
        createFunc = &NNodeBroker::CreateNodeBroker;
        break;
    case TTabletTypes::TenantSlotBroker:
        createFunc = &NTenantSlotBroker::CreateTenantSlotBroker;
        break;
    case TTabletTypes::Console:
        createFunc = &NConsole::CreateConsole;
        break;
    case TTabletTypes::Kesus:
        createFunc = &NKesus::CreateKesusTablet;
        break;
    case TTabletTypes::SysViewProcessor:
        createFunc = &NSysView::CreateSysViewProcessor;
        break;
    case TTabletTypes::TestShard:
        createFunc = &NTestShard::CreateTestShard;
        break;
    case TTabletTypes::SequenceShard:
        createFunc = &NSequenceShard::CreateSequenceShard;
        break;
    case TTabletTypes::ReplicationController:
        createFunc = &NReplication::CreateController;
        break;
    case TTabletTypes::BlobDepot:
        createFunc = &NBlobDepot::CreateBlobDepot;
        break;
    case TTabletTypes::StatisticsAggregator:
        createFunc = &NStat::CreateStatisticsAggregator;
        break;
    case TTabletTypes::BackupController:
        createFunc = &NBackup::CreateBackupController;
        break;
    case TTabletTypes::Dummy:
        createFunc = &CreateSimpleTablet;
        break;
    default:
        return nullptr;
    }

    if (bootType == ETabletBootType::Recovery) {
        createFunc = &NTabletFlatExecutor::NRecovery::CreateRecoveryShard;
    }

    return new TTabletSetupInfo(createFunc, TMailboxType::ReadAsFilled, poolId, TMailboxType::ReadAsFilled, tabletPoolId);
}

IActor* CreateConfiguredTabletBootstrapper(const NKikimrConfig::TBootstrap &bootstrapConfig) {
    return new TConfiguredTabletBootstrapper(bootstrapConfig);
}

}
