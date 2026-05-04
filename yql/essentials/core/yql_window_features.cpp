#include "yql_window_features.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>

namespace NYql {

bool IsRangeWindowFrameEnabled(TTypeAnnotationContext& types) {
    return IsWindowNewPipelineEnabled(types) && types.LangVer >= MakeLangVersion(2026, 1);
}

bool IsWindowNewPipelineEnabled(TTypeAnnotationContext& types) {
    if (types.WindowNewPipeline && NKikimr::NMiniKQL::RuntimeVersion >= 76u) {
        return true;
    }
    return false;
}

} // namespace NYql
