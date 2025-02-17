#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

extern PyTypeObject PyCallableType;

TPyObjectPtr ToPyCallable(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyCallable(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        PyObject* value);

void SetupCallableSettings(const TPyCastContext::TPtr& castCtx, PyObject* value);

} // namspace NPython
