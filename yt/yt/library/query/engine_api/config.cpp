#include "config.h"

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TExecutorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cg_cache", &TThis::CGCache)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->CGCache->Capacity = 512;
        config->CGCache->ShardCount = 1;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TColumnEvaluatorCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cg_cache", &TThis::CGCache)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->CGCache->Capacity = 512;
        config->CGCache->ShardCount = 1;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TColumnEvaluatorCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cg_cache", &TThis::CGCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
