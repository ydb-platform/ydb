#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

extern PyTypeObject PyVoidType;
extern PyObject PyVoidObject;

TPyObjectPtr ToPyVoid(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyVoid(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type,
        PyObject* value);

} // namspace NPython
