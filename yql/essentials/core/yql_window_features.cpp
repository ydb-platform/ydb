#include "yql_window_features.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>

namespace NYql {

bool IsRangeWindowFrameEnabled(TTypeAnnotationContext& types) {
    return IsWindowNewPipelineEnabled(types) && types.LangVer >= MakeLangVersion(2025, 5);
}

bool IsWindowNewPipelineEnabled(TTypeAnnotationContext& types) {
    if (types.WindowNewPipeline && NKikimr::NMiniKQL::RuntimeVersion >= 73u) {
        // The new window pipeline generates code that is not robust to the absence of ForbidConstantDepends.
        // Therefore, we must ensure that it is enabled.
        YQL_ENSURE(IsForbidConstantDependsEnabled(types), "This feature must be enabled.");
        return true;
    }
    return false;
}

} // namespace NYql
