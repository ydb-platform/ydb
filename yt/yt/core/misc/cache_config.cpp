#include "cache_config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSlruCacheConfigPtr TSlruCacheConfig::CreateWithCapacity(i64 capacity)
{
    auto result = New<TSlruCacheConfig>();
    result->Capacity = capacity;
    return result;
}

void TSlruCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("capacity", &TThis::Capacity)
        .Default(0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("younger_size_fraction", &TThis::YoungerSizeFraction)
        .Default(0.25)
        .InRange(0.0, 1.0);
    registrar.Parameter("shard_count", &TThis::ShardCount)
        .Default(16)
        .GreaterThan(0);
    registrar.Parameter("touch_buffer_capacity", &TThis::TouchBufferCapacity)
        .Default(65536)
        .GreaterThan(0);
    registrar.Parameter("small_ghost_cache_ratio", &TThis::SmallGhostCacheRatio)
        .Default(0.5)
        .GreaterThanOrEqual(0.0);
    registrar.Parameter("large_ghost_cache_ratio", &TThis::LargeGhostCacheRatio)
        .Default(2.0)
        .GreaterThanOrEqual(0.0);
    registrar.Parameter("enable_ghost_caches", &TThis::EnableGhostCaches)
        .Default(true);

    registrar.Postprocessor([] (TThis* config) {
        if (!IsPowerOf2(config->ShardCount)) {
            THROW_ERROR_EXCEPTION("\"shard_count\" must be power of two, actual: %v",
                config->ShardCount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSlruCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("capacity", &TThis::Capacity)
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.Parameter("younger_size_fraction", &TThis::YoungerSizeFraction)
        .Optional()
        .InRange(0.0, 1.0);
    registrar.Parameter("enable_ghost_caches", &TThis::EnableGhostCaches)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncExpiringCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expire_after_access_time", &TThis::ExpireAfterAccessTime)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
        .Alias("success_expiration_time")
        .Default(TDuration::Seconds(15));
    registrar.Parameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
        .Alias("failure_expiration_time")
        .Default(TDuration::Seconds(15));
    registrar.Parameter("refresh_time", &TThis::RefreshTime)
        .Alias("success_probation_time")
        .Default(TDuration::Seconds(10));
    registrar.Parameter("batch_update", &TThis::BatchUpdate)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->RefreshTime && *config->RefreshTime && *config->RefreshTime > config->ExpireAfterSuccessfulUpdateTime) {
            THROW_ERROR_EXCEPTION("\"refresh_time\" must be less than \"expire_after_successful_update_time\"")
                << TErrorAttribute("refresh_time", config->RefreshTime)
                << TErrorAttribute("expire_after_successful_update_time", config->ExpireAfterSuccessfulUpdateTime);
        }
    });
}

void TAsyncExpiringCacheConfig::ApplyDynamicInplace(
    const TAsyncExpiringCacheDynamicConfigPtr& dynamicConfig)
{
    ExpireAfterAccessTime = dynamicConfig->ExpireAfterAccessTime.value_or(ExpireAfterAccessTime);
    ExpireAfterSuccessfulUpdateTime = dynamicConfig->ExpireAfterSuccessfulUpdateTime.value_or(ExpireAfterSuccessfulUpdateTime);
    ExpireAfterFailedUpdateTime = dynamicConfig->ExpireAfterFailedUpdateTime.value_or(ExpireAfterFailedUpdateTime);
    RefreshTime = dynamicConfig->RefreshTime.has_value()
        ? dynamicConfig->RefreshTime
        : RefreshTime;
    BatchUpdate = dynamicConfig->BatchUpdate.value_or(BatchUpdate);
}

TAsyncExpiringCacheConfigPtr TAsyncExpiringCacheConfig::ApplyDynamic(
    const TAsyncExpiringCacheDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    mergedConfig->ApplyDynamicInplace(dynamicConfig);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncExpiringCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expire_after_access_time", &TThis::ExpireAfterAccessTime)
        .Optional();
    registrar.Parameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
        .Optional();
    registrar.Parameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
        .Optional();
    registrar.Parameter("refresh_time", &TThis::RefreshTime)
        .Optional();
    registrar.Parameter("batch_update", &TThis::BatchUpdate)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
