#include "yql_dq_integration.h"

#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {

std::unordered_set<IDqIntegration*> GetUniqueIntegrations(const TTypeAnnotationContext& typesCtx) {
    std::unordered_set<IDqIntegration*> uniqueIntegrations(typesCtx.DataSources.size() + typesCtx.DataSinks.size());
    for (const auto& provider : typesCtx.DataSources) {
        if (auto* dqIntegration = provider->GetDqIntegration()) {
            uniqueIntegrations.emplace(dqIntegration);
        }
    }

    for (const auto& provider : typesCtx.DataSinks) {
        if (auto* dqIntegration = provider->GetDqIntegration()) {
            uniqueIntegrations.emplace(dqIntegration);
        }
    }

    return uniqueIntegrations;
}

} // namespace NYql
