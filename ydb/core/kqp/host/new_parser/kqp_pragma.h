#pragma once

#include "kqp_name_resolver.h"

namespace NYql {
struct TExprContext;
struct TTypeAnnotationContext;
}

namespace NKikimr::NKqp::NNewYql {

// Apply literal provider configuration in source order, using the same
// transformers and policies as the legacy frontend. Call before metadata IO
// and annotation. Does not mark the legacy pipeline's Configure step done.
bool ApplyProviderPragmas(const TScript& script, const TProviderPragmas& pragmas,
    NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& types);

} // namespace NKikimr::NKqp::NNewYql
