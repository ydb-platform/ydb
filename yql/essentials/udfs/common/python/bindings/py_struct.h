#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

TPyObjectPtr ToPyStruct(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyStruct(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type, PyObject* value);

} // namespace NPython
