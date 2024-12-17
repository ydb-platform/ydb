#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

TPyObjectPtr ToPyDecimal(const TPyCastContext::TPtr& castCtx, const NKikimr::NUdf::TUnboxedValuePod& value, ui8 precision, ui8 scale);

NKikimr::NUdf::TUnboxedValue FromPyDecimal(const TPyCastContext::TPtr& castCtx, PyObject* value, ui8 precision, ui8 scale);

} // namespace NPython
