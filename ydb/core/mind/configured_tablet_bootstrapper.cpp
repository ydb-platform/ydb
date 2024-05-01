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
#include <ydb/core/blob_depot/blob_depot.h>
#include <ydb/core/statistics/aggregator/aggregator.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {

class TConfiguredTabletBootstrapper : public TActorBootstrapped<TConfiguredTabletBootstrapper> {
    const ui64 TabletId;
    const ::NKikimrConfig::TBootstrap::TTablet DefaultConfig;
    TActorId BootstrapperInstance;
    TString CurrentConfig;

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev) {
        const auto &record = ev->Get()->Record;

        NKikimrConfig::TBootstrap::TTablet tabletConfig;
        if (record.GetConfig().HasBootstrapConfig()) {
            for (const NKikimrConfig::TBootstrap::TTablet &x : record.GetConfig().GetBootstrapConfig().GetTablet()) {
                if (x.GetInfo().GetTabletID() == TabletId) {
                    tabletConfig.CopyFrom(x);
                    break;
                }
            }
        }

        CheckChanged(tabletConfig);

        Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
    }

    void CheckChanged(const NKikimrConfig::TBootstrap::TTablet &config) {
        TString x = config.SerializeAsString();
        if (CurrentConfig == x)
            return;

        if (BootstrapperInstance) {
            Send(BootstrapperInstance, new TEvents::TEvPoisonPill());
            TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(MakeBootstrapperID(TabletId, SelfId().NodeId()), TActorId());
            BootstrapperInstance = TActorId();
        }

        CurrentConfig = x;

        // not apply config
        const ui32 selfNode = SelfId().NodeId();
        if (Find(config.GetNode(), selfNode) != config.GetNode().end()) {
            TIntrusivePtr<TTabletStorageInfo> storageInfo = TabletStorageInfoFromProto(config.GetInfo());
            const auto *appData = AppData();

            // extract from kikimr_services_initializer
            const TTabletTypes::EType tabletType = BootstrapperTypeToTabletType(config.GetType());

            if (storageInfo->TabletType == TTabletTypes::TypeInvalid)
                storageInfo->TabletType = tabletType;

            TIntrusivePtr<TTabletSetupInfo> tabletSetupInfo = MakeTabletSetupInfo(tabletType, appData->UserPoolId, appData->SystemPoolId);

            TIntrusivePtr<TBootstrapperInfo> bi = new TBootstrapperInfo(tabletSetupInfo.Get());
            if (config.NodeSize() != 1) {
                for (ui32 node : config.GetNode()) {
                    if (node != selfNode)
                        bi->OtherNodes.emplace_back(node);
                }
                if (config.HasWatchThreshold())
                    bi->WatchThreshold = TDuration::MilliSeconds(config.GetWatchThreshold());
                if (config.HasStartFollowers())
                    bi->StartFollowers = config.GetStartFollowers();
            }

            BootstrapperInstance = Register(CreateBootstrapper(storageInfo.Get(), bi.Get(), false), TMailboxType::HTSwap, appData->SystemPoolId);

            TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(MakeBootstrapperID(TabletId, SelfId().NodeId()), BootstrapperInstance);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CONFIGURED_BOOTSTRAPPER;
    }

    TConfiguredTabletBootstrapper(const ::NKikimrConfig::TBootstrap::TTablet &defaultConfig)
        : TabletId(defaultConfig.GetInfo().GetTabletID())
        , DefaultConfig(defaultConfig)
    {}

    void Bootstrap() {
        // start with initial config (as we can start CMS itself - it could be not possible to get actual config at all)
        CheckChanged(DefaultConfig);

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
    default:
        return nullptr;
    }

    return new TTabletSetupInfo(createFunc, TMailboxType::ReadAsFilled, poolId, TMailboxType::ReadAsFilled, tabletPoolId);
}

IActor* CreateConfiguredTabletBootstrapper(const ::NKikimrConfig::TBootstrap::TTablet &defaultConfig) {
    return new TConfiguredTabletBootstrapper(defaultConfig);
}

}
