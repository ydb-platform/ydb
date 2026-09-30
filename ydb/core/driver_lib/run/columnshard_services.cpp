#include "kikimr_services_initializers.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/general_cache/service/service.h>
#include <ydb/core/tx/general_cache/usage/service.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_service.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/priorities/usage/config.h>
#include <ydb/core/tx/priorities/service/service.h>
#include <ydb/core/tx/priorities/usage/service.h>

namespace NKikimr::NKikimrServicesInitializers {

TBlobCacheInitializer::TBlobCacheInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TBlobCacheInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> blobCacheGroup = tabletGroup->GetSubgroup("type", "BLOB_CACHE");

    const NBlobCache::TBlobCacheSettings settings = NBlobCache::TBlobCacheSettings::FromProto(Config.GetBlobCacheConfig());
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(NBlobCache::MakeBlobCacheServiceId(),
        TActorSetupCmd(NBlobCache::CreateBlobCache(settings, blobCacheGroup), TMailboxType::ReadAsFilled, appData->UserPoolId)));
}

TGeneralCachePortionsMetadataInitializer::TGeneralCachePortionsMetadataInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TGeneralCachePortionsMetadataInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    auto serviceConfig = NGeneralCache::NPublic::TConfig::BuildFromProto(Config.GetPortionsMetadataCache());
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse portions metadata cache config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NGeneralCache::NPublic::TConfig::BuildDefault().DebugString());
        serviceConfig = NGeneralCache::NPublic::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_GENERAL_CACHE_PORTIONS_METADATA");

    auto service = NGeneralCache::CreateService<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>(*serviceConfig, conveyorGroup);

    setup->LocalServices.push_back(
        std::make_pair(NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
}

TGeneralCacheColumnDataInitializer::TGeneralCacheColumnDataInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TGeneralCacheColumnDataInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    auto serviceConfig = NGeneralCache::NPublic::TConfig::BuildFromProto(Config.GetColumnDataCache());
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse column data cache config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NGeneralCache::NPublic::TConfig::BuildDefault().DebugString());
        serviceConfig = NGeneralCache::NPublic::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_GENERAL_CACHE_COLUMN_DATA");

    auto service = NGeneralCache::CreateService<NOlap::NGeneralCache::TColumnDataCachePolicy>(*serviceConfig, conveyorGroup);

    setup->LocalServices.push_back(
        std::make_pair(NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TColumnDataCachePolicy>::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
}

TOverloadManagerInitializer::TOverloadManagerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TOverloadManagerInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup = tabletGroup->GetSubgroup("type", "CS_OVERLOAD_MANAGER");

    setup->LocalServices.push_back(std::make_pair(NColumnShard::NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        TActorSetupCmd(NColumnShard::NOverload::TOverloadManagerServiceOperator::CreateService(countersGroup), TMailboxType::HTSwap, appData->UserPoolId)));
}

TFlowControlManagerInitializer::TFlowControlManagerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TFlowControlManagerInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup =
        NColumnShard::NFlowControl::TFlowControlManagerServiceOperator::BuildCountersGroup(appData->Counters);

    setup->LocalServices.push_back(std::make_pair(NColumnShard::NFlowControl::TFlowControlManagerServiceOperator::MakeServiceId(NodeId),
        TActorSetupCmd(NColumnShard::NFlowControl::TFlowControlManagerServiceOperator::CreateService(countersGroup), TMailboxType::HTSwap, appData->UserPoolId)));
}

TCompPrioritiesInitializer::TCompPrioritiesInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TCompPrioritiesInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NPrioritiesQueue::TConfig serviceConfig;
    if (Config.HasCompPrioritiesConfig()) {
        Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto(Config.GetCompPrioritiesConfig()));
    }

    if (serviceConfig.IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_COMP_PRIORITIES");

        auto service = NPrioritiesQueue::CreateService<NPrioritiesQueue::TCompConveyorPolicy>(serviceConfig, conveyorGroup);

        setup->LocalServices.push_back(std::make_pair(
            NPrioritiesQueue::TCompServiceOperator::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TCompositeConveyorInitializer::TCompositeConveyorInitializer(const TKikimrRunConfig& runConfig)
	: IKikimrServicesInitializer(runConfig) {
}

void TCompositeConveyorInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    const NKikimrConfig::TCompositeConveyorConfig protoConfig = [&]() {
        NKikimrConfig::TCompositeConveyorConfig result;
        if (Config.HasCompConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            protoWorkersPool.SetName("WP::" + ::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            protoLink.SetWeight(1);
            if (Config.GetCompConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetCompConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetCompConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetCompConveyorConfig().GetWorkersCount());
            } else if (Config.GetCompConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetCompConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.33);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            protoWorkersPool.SetName("WP::" + ::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.33);
            protoWorkersPool.SetMaxBatchSize(1);
        }

        if (Config.HasInsertConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            protoWorkersPool.SetName("WP::" + ::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            protoLink.SetWeight(1);
            if (Config.GetInsertConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetInsertConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetInsertConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetInsertConveyorConfig().GetWorkersCount());
            } else if (Config.GetInsertConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetInsertConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.2);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            protoWorkersPool.SetName("WP::" + ::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.2);
            protoWorkersPool.SetMaxBatchSize(1);
        }
        if (Config.HasScanConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            protoWorkersPool.SetName("WP::" + ::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            protoLink.SetWeight(1);
            if (Config.GetScanConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetScanConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetScanConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetScanConveyorConfig().GetWorkersCount());
            } else if (Config.GetScanConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetScanConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.4);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            protoWorkersPool.SetName("WP::" + ::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.4);
        }

        {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Deduplication));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            protoWorkersPool.SetName("WP::" + ::ToString(NConveyorComposite::ESpecialTaskCategory::Deduplication));
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Deduplication));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.3);
        }

        {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Normalizer));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            protoWorkersPool.SetName("WP::" + ::ToString(NConveyorComposite::ESpecialTaskCategory::Normalizer));
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Normalizer));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.33);
        }

        if (!Config.HasCompositeConveyorConfig()) {
            return result;
        }
        auto overlaid = NConveyorComposite::NConfig::TConfig::OverlayYamlOnDefaults(result, Config.GetCompositeConveyorConfig());
        if (overlaid.IsFail()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot overlay composite conveyor config")(
                "error", overlaid.GetErrorMessage())("action", "keeping synthesized composite conveyor defaults");
            return result;
        }
        return overlaid.DetachResult();
    }();

    auto serviceConfig = NConveyorComposite::NConfig::TConfig::BuildFromProto(protoConfig);
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse composite conveyor config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NConveyorComposite::NConfig::TConfig::BuildDefault().DebugString());
        serviceConfig = NConveyorComposite::NConfig::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    if (serviceConfig->IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_COMPOSITE_CONVEYOR");

        const auto registerService = [&](const ui32 poolId, bool useBatchPool) {
            auto poolConveyorGroup = conveyorGroup->GetSubgroup("actor_system_pool_id", ::ToString(poolId));
            auto service = NConveyorComposite::CreateService(*serviceConfig, poolConveyorGroup);
            setup->LocalServices.push_back(std::make_pair(
                NConveyorComposite::TServiceOperator::MakeServiceId(NodeId, useBatchPool),
                TActorSetupCmd(service, TMailboxType::HTSwap, poolId)));
        };

        registerService(appData->UserPoolId, false);
        registerService(appData->BatchPoolId, true);
    }
}

} // namespace NKikimr::NKikimrServicesInitializers
