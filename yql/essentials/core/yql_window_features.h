#pragma once

namespace NYql {

struct TTypeAnnotationContext;

bool IsRangeWindowFrameEnabled(TTypeAnnotationContext& types);

bool IsWindowNewPipelineEnabled(TTypeAnnotationContext& types);

} // namespace NYql
