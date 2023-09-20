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
    , MonotonicTimeProvider(CreateDefaultMonotonicTimeProvider())
    , ProxySchemeCacheNodes(Max<ui64>() / 4)
    , ProxySchemeCacheDistrNodes(Max<ui64>() / 4)
    , CompilerSchemeCachePaths(Max<ui64>() / 4)
    , CompilerSchemeCacheTables(Max<ui64>() / 4)
    , Mon(nullptr)
    , Icb(new TControlBoard())
    , InFlightLimiterRegistry(new NGRpcService::TInFlightLimiterRegistry(Icb))
    , KikimrShouldContinue(kikimrShouldContinue)
{}

TIntrusivePtr<IRandomProvider> TAppData::RandomProvider = CreateDefaultRandomProvider();
TIntrusivePtr<ITimeProvider> TAppData::TimeProvider = CreateDefaultTimeProvider();

}
