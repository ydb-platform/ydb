#pragma once

#include "py_ctx.h"

namespace NPython {

TPyObjectPtr ToPyDynamicLinear(
    const TPyCastContext::TPtr& castCtx,
    const NKikimr::NUdf::TType* itemType,
    const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyDynamicLinear(
    const TPyCastContext::TPtr& castCtx,
    const NKikimr::NUdf::TType* itemType,
    TPyObjectPtr value);

} // namespace NPython
