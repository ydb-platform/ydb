#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

namespace NPython {

extern PyTypeObject PyLazyDictType;
extern PyTypeObject PyLazySetType;

TPyObjectPtr ToPyLazyDict(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* keyType,
        const NKikimr::NUdf::TType* payloadType,
        const NKikimr::NUdf::TUnboxedValuePod& value);

TPyObjectPtr ToPyLazySet(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* itemType,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyMapping(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* keyType,
        const NKikimr::NUdf::TType* payType,
        PyObject* map);

NKikimr::NUdf::TUnboxedValue FromPyDict(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* keyType,
        const NKikimr::NUdf::TType* payType,
        PyObject* dict);

NKikimr::NUdf::TUnboxedValue FromPySet(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* itemType,
        PyObject* set);

NKikimr::NUdf::TUnboxedValue FromPySequence(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* keyType,
        PyObject* sequence);

NKikimr::NUdf::TUnboxedValue FromPySequence(
        const TPyCastContext::TPtr& castCtx,
        const NKikimr::NUdf::TType* itemType,
        const NKikimr::NUdf::TDataTypeId keyType,
        PyObject* sequence);

} // namspace NPython
