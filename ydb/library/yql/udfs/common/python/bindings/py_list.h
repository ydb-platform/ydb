#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

extern PyTypeObject PyLazyListIteratorType;
extern PyTypeObject PyLazyListType;
extern PyTypeObject PyThinListIteratorType;
extern PyTypeObject PyThinListType;

TPyObjectPtr ToPyLazyList(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* itemType,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyLazyGenerator(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        TPyObjectPtr callableObj);

NKikimr::NUdf::TUnboxedValue FromPyLazyIterable(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        TPyObjectPtr iterableObj);

NKikimr::NUdf::TUnboxedValue FromPyLazyIterator(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* type,
        TPyObjectPtr iteratorObj);

} // namspace NPython
