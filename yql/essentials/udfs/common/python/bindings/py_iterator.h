#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

extern PyTypeObject PyIteratorType;
extern PyTypeObject PyPairIteratorType;

TPyObjectPtr ToPyIterator(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* itemType,
        const NKikimr::NUdf::TUnboxedValuePod& value);

TPyObjectPtr ToPyIterator(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* keyType,
        const NKikimr::NUdf::TType* payloadType,
        const NKikimr::NUdf::TUnboxedValuePod& value);


} // namspace NPython
