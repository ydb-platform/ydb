#include "appdata.h"
#include "tablet_types.h"

namespace NKikimr {

TAppData::TAppData(
        ui32 sysPoolId, ui32 userPoolId, ui32 ioPoolId, ui32 batchPoolId,
        TMap<TString, ui32> servicePools,
        const NScheme::TTypeRegistry* typeRegistry,
        const NMiniKQL::IFunctionRegistry* functionRegistry,
        const TFormatFactory* formatFactory,
        TProgramShouldContinue *kikimrShouldContinue)
    : Magic(MagicTag)
    , SystemPoolId(sysPoolId)
    , UserPoolId(userPoolId)
    , IOPoolId(ioPoolId)
    , BatchPoolId(batchPoolId)
    , ServicePools(servicePools)
    , TypeRegistry(typeRegistry)
    , FunctionRegistry(functionRegistry)
    , FormatFactory(formatFactory)
    , ProxySchemeCacheNodes(Max<ui64>() / 4)
    , ProxySchemeCacheDistrNodes(Max<ui64>() / 4)
    , CompilerSchemeCachePaths(Max<ui64>() / 4)
    , CompilerSchemeCacheTables(Max<ui64>() / 4)
    , Mon(nullptr)
    , BusMonPage(nullptr)
    , Icb(new TControlBoard())
    , InFlightLimiterRegistry(new NGRpcService::TInFlightLimiterRegistry(Icb))
    , StaticBlobStorageConfig(new NKikimrBlobStorage::TNodeWardenServiceSet)
    , KikimrShouldContinue(kikimrShouldContinue)
{}

TAppData::TDefaultTabletTypes::TDefaultTabletTypes()
    : SchemeShard(TTabletTypes::FLAT_SCHEMESHARD)
    , DataShard(TTabletTypes::FLAT_DATASHARD)
    , KeyValue(TTabletTypes::KEYVALUEFLAT)
    , PersQueue(TTabletTypes::PERSQUEUE) 
    , PersQueueReadBalancer(TTabletTypes::PERSQUEUE_READ_BALANCER) 
    , Dummy(TTabletTypes::TX_DUMMY)
    , Coordinator(TTabletTypes::FLAT_TX_COORDINATOR)
    , Mediator(TTabletTypes::TX_MEDIATOR)
    , Kesus(TTabletTypes::KESUS)
    , Hive(TTabletTypes::Hive)
    , SysViewProcessor(TTabletTypes::SysViewProcessor)
    , ColumnShard(TTabletTypes::COLUMNSHARD)
    , TestShard(TTabletTypes::TestShard)
    , SequenceShard(TTabletTypes::SequenceShard)
    , ReplicationController(TTabletTypes::ReplicationController)
{
}

TIntrusivePtr<IRandomProvider> TAppData::RandomProvider = CreateDefaultRandomProvider();
TIntrusivePtr<ITimeProvider> TAppData::TimeProvider = CreateDefaultTimeProvider();

}
