#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

extern PyTypeObject PyStreamType;
extern PyObject* PyYieldIterationException;

TPyObjectPtr ToPyStream(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyStream(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        const TPyObjectPtr& value,
        const TPyObjectPtr& originalCallable,
        const TPyObjectPtr& originalCallableClosure,
        const TPyObjectPtr& originalCallableArgs);

} // namespace NPython
