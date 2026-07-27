#include "yql_window_features.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/langver/feature.gen.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>

namespace NYql {

bool IsRangeWindowFrameEnabled(TTypeAnnotationContext& types) {
    return IsWindowNewPipelineEnabled(types) &&
           types.LangVer >= NFeature::YqlRangeWindows.MinLangVer;
}

bool IsWindowNewPipelineEnabled(TTypeAnnotationContext& types) {
    return types.WindowNewPipeline && NKikimr::NMiniKQL::RuntimeVersion >= 76U;
}

} // namespace NYql
