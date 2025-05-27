#pragma once

#include "py_ptr.h"
#include "py_ctx.h"

#include <util/generic/typetraits.h>

namespace NPython {

template <typename T>
TPyObjectPtr PyCast(typename TTypeTraits<T>::TFuncParam value);

template <typename T>
T PyCast(PyObject* value);

template <typename T>
bool TryPyCast(PyObject* value, T& result);

template <typename T>
TPyObjectPtr ToPyUnicode(const T& value);

TPyObjectPtr ToPyObject(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod& value);

NKikimr::NUdf::TUnboxedValue FromPyObject(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type,
        PyObject* value);

TPyObjectPtr ToPyArgs(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type,
        const NKikimr::NUdf::TUnboxedValuePod* args,
        const NKikimr::NUdf::TCallableTypeInspector& inspector);

void FromPyArgs(
        const TPyCastContext::TPtr& ctx,
        const NKikimr::NUdf::TType* type,
        PyObject* pyArgs,
        NKikimr::NUdf::TUnboxedValue* cArgs,
        const NKikimr::NUdf::TCallableTypeInspector& inspector);

} // namspace NPython
