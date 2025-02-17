#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

extern const char ResourceCapsuleName[];

TPyObjectPtr ToPyResource(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyResource(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type,
        PyObject* value);

} // namspace NPython
