#pragma once

#include "py_ctx.h"

namespace NPython {

TPyObjectPtr ToPyVariant(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyVariant(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        PyObject* value);

} // namspace NPython
