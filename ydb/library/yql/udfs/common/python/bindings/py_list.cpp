#include "py_list.h"
#include "py_dict.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

using namespace NKikimr;

#if PY_MAJOR_VERSION >= 3
#define SLICEOBJ(obj) obj
#else
#define SLICEOBJ(obj) (reinterpret_cast<PySliceObject*>(obj))
// See details about need for backports in ya.make
#include "py27_backports.h"
#endif

namespace NPython {

namespace {
inline Py_ssize_t CastIndex(PyObject* key, const char* name)
{
    Py_ssize_t index = -1;
    if (PyIndex_Check(key)) {
        index = PyNumber_AsSsize_t(key, PyExc_IndexError);
    }
    if (index < 0) {
        const TPyObjectPtr value = PyUnicode_FromFormat("argument of %s must be positive integer or long", name);
        PyErr_SetObject(PyExc_IndexError, value.Get());
    }

    return index;
}
}

//////////////////////////////////////////////////////////////////////////////
// TPyLazyList interface
//////////////////////////////////////////////////////////////////////////////
struct TPyLazyList
{
    using TPtr = NUdf::TRefCountedPtr<TPyLazyList, TPyPtrOps<TPyLazyList>>;

    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    const NUdf::TType* ItemType;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Value;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Dict;
    Py_ssize_t Step;
    Py_ssize_t CachedLength;

    inline static TPyLazyList* Cast(PyObject* o) {
        return reinterpret_cast<TPyLazyList*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    static PyObject* New(
            const TPyCastContext::TPtr& castCtx,
            const NUdf::TType* itemType,
            NUdf::IBoxedValuePtr value,
            Py_ssize_t step = 1,
            Py_ssize_t size = -1);

    static int Bool(PyObject* self);
    static PyObject* Repr(PyObject* self);
    static PyObject* Iter(PyObject* self);
    static Py_ssize_t Len(PyObject* self);
    static PyObject* Subscript(PyObject* self, PyObject* slice);
    static PyObject* ToIndexDict(PyObject* self, PyObject* /* arg */);
    static PyObject* Reversed(PyObject* self, PyObject* /* arg */);
    static PyObject* Take(PyObject* self, PyObject* arg);
    static PyObject* Skip(PyObject* self, PyObject* arg);
    static PyObject* HasFastLen(PyObject* self, PyObject* /* arg */);
    static PyObject* HasItems(PyObject* self, PyObject* /* arg */);
};

PyMappingMethods LazyListMapping = {
    INIT_MEMBER(mp_length, TPyLazyList::Len),
    INIT_MEMBER(mp_subscript, TPyLazyList::Subscript),
    INIT_MEMBER(mp_ass_subscript, nullptr),
};

PyNumberMethods LazyListNumbering = {
     INIT_MEMBER(nb_add, nullptr),
     INIT_MEMBER(nb_subtract, nullptr),
     INIT_MEMBER(nb_multiply, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_divide, nullptr),
#endif
     INIT_MEMBER(nb_remainder, nullptr),
     INIT_MEMBER(nb_divmod, nullptr),
     INIT_MEMBER(nb_power, nullptr),
     INIT_MEMBER(nb_negative, nullptr),
     INIT_MEMBER(nb_positive, nullptr),
     INIT_MEMBER(nb_absolute, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_bool, TPyLazyList::Bool),
#else
     INIT_MEMBER(nb_nonzero, TPyLazyList::Bool),
#endif
     INIT_MEMBER(nb_invert, nullptr),
     INIT_MEMBER(nb_lshift, nullptr),
     INIT_MEMBER(nb_rshift, nullptr),
     INIT_MEMBER(nb_and, nullptr),
     INIT_MEMBER(nb_xor, nullptr),
     INIT_MEMBER(nb_or, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_coerce, nullptr),
#endif
     INIT_MEMBER(nb_int, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_reserved, nullptr),
#else
     INIT_MEMBER(nb_long, nullptr),
#endif
     INIT_MEMBER(nb_float, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_oct, nullptr),
     INIT_MEMBER(nb_hex, nullptr),
#endif

     INIT_MEMBER(nb_inplace_add, nullptr),
     INIT_MEMBER(nb_inplace_subtract, nullptr),
     INIT_MEMBER(nb_inplace_multiply, nullptr),
     INIT_MEMBER(nb_inplace_remainder, nullptr),
     INIT_MEMBER(nb_inplace_power, nullptr),
     INIT_MEMBER(nb_inplace_lshift, nullptr),
     INIT_MEMBER(nb_inplace_rshift, nullptr),
     INIT_MEMBER(nb_inplace_and, nullptr),
     INIT_MEMBER(nb_inplace_xor, nullptr),
     INIT_MEMBER(nb_inplace_or, nullptr),

     INIT_MEMBER(nb_floor_divide, nullptr),
     INIT_MEMBER(nb_true_divide, nullptr),
     INIT_MEMBER(nb_inplace_floor_divide, nullptr),
     INIT_MEMBER(nb_inplace_true_divide, nullptr),

     INIT_MEMBER(nb_index, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_matrix_multiply, nullptr),
     INIT_MEMBER(nb_inplace_matrix_multiply, nullptr),
#endif
};

PyDoc_STRVAR(reversed__doc__, "DEPRECATED: use reversed(list) or list[::-1] instead.");
PyDoc_STRVAR(take__doc__, "DEPRECATED: use slice list[:n] instead.");
PyDoc_STRVAR(skip__doc__, "DEPRECATED: use slice list[n:] instead.");
PyDoc_STRVAR(to_index_dict__doc__, "DEPRECATED: use list[n] instead.");
PyDoc_STRVAR(has_fast_len__doc__, "DEPRECATED: do not use.");
PyDoc_STRVAR(has_items__doc__, "DEPRECATED: test list as bool instead.");

static PyMethodDef TPyLazyListMethods[] = {
    { "__reversed__",  TPyLazyList::Reversed, METH_NOARGS, nullptr },
    { "to_index_dict",  TPyLazyList::ToIndexDict, METH_NOARGS, to_index_dict__doc__ },
    { "reversed",  TPyLazyList::Reversed, METH_NOARGS, reversed__doc__ },
    { "take",  TPyLazyList::Take, METH_O, take__doc__ },
    { "skip",  TPyLazyList::Skip, METH_O, skip__doc__ },
    { "has_fast_len",  TPyLazyList::HasFastLen, METH_NOARGS, has_fast_len__doc__ },
    { "has_items",  TPyLazyList::HasItems, METH_NOARGS, has_items__doc__ },
    { nullptr, nullptr, 0, nullptr }    /* sentinel */
};

#if PY_MAJOR_VERSION >= 3
#define Py_TPFLAGS_HAVE_ITER 0
#endif

PyTypeObject PyLazyListType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TList"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyLazyList)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyLazyList::Dealloc),
#if PY_VERSION_HEX < 0x030800b4
    INIT_MEMBER(tp_print          , nullptr),
#else
    INIT_MEMBER(tp_vectorcall_offset, 0),
#endif
    INIT_MEMBER(tp_getattr        , nullptr),
    INIT_MEMBER(tp_setattr        , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_as_async       , nullptr),
#else
    INIT_MEMBER(tp_compare        , nullptr),
#endif
    INIT_MEMBER(tp_repr           , TPyLazyList::Repr),
    INIT_MEMBER(tp_as_number      , &LazyListNumbering),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , &LazyListMapping),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , Py_TPFLAGS_HAVE_ITER),
    INIT_MEMBER(tp_doc            , "yql.TList object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , TPyLazyList::Iter),
    INIT_MEMBER(tp_iternext       , nullptr),
    INIT_MEMBER(tp_methods        , TPyLazyListMethods),
    INIT_MEMBER(tp_members        , nullptr),
    INIT_MEMBER(tp_getset         , nullptr),
    INIT_MEMBER(tp_base           , nullptr),
    INIT_MEMBER(tp_dict           , nullptr),
    INIT_MEMBER(tp_descr_get      , nullptr),
    INIT_MEMBER(tp_descr_set      , nullptr),
    INIT_MEMBER(tp_dictoffset     , 0),
    INIT_MEMBER(tp_init           , nullptr),
    INIT_MEMBER(tp_alloc          , nullptr),
    INIT_MEMBER(tp_new            , nullptr),
    INIT_MEMBER(tp_free           , nullptr),
    INIT_MEMBER(tp_is_gc          , nullptr),
    INIT_MEMBER(tp_bases          , nullptr),
    INIT_MEMBER(tp_mro            , nullptr),
    INIT_MEMBER(tp_cache          , nullptr),
    INIT_MEMBER(tp_subclasses     , nullptr),
    INIT_MEMBER(tp_weaklist       , nullptr),
    INIT_MEMBER(tp_del            , nullptr),
    INIT_MEMBER(tp_version_tag    , 0),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_finalize       , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b1
    INIT_MEMBER(tp_vectorcall     , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
    INIT_MEMBER(tp_print          , nullptr),
#endif
};

//////////////////////////////////////////////////////////////////////////////
// TPyLazyListIterator interface
//////////////////////////////////////////////////////////////////////////////
struct TPyLazyListIterator
{
    PyObject_HEAD;
    TPyLazyList::TPtr List;
    TPyCleanupListItem<NUdf::TUnboxedValue> Iterator;
    Py_ssize_t Length;
    TPyCastContext::TPtr CastCtx;

    inline static TPyLazyListIterator* Cast(PyObject* o) {
        return reinterpret_cast<TPyLazyListIterator*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        auto obj = Cast(self);
        auto ctx = obj->CastCtx;
        ctx->MemoryLock->Acquire();
        delete obj;
        ctx->MemoryLock->Release();
    }

    inline static PyObject* Repr(PyObject* self) {
        Y_UNUSED(self);
        return PyRepr("<yql.TListIterator>").Release();
    }

    static PyObject* New(TPyLazyList* list);
    static PyObject* Next(PyObject* self);
};

PyTypeObject PyLazyListIteratorType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TListIterator"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyLazyListIterator)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyLazyListIterator::Dealloc),
#if PY_VERSION_HEX < 0x030800b4
    INIT_MEMBER(tp_print          , nullptr),
#else
    INIT_MEMBER(tp_vectorcall_offset, 0),
#endif
    INIT_MEMBER(tp_getattr        , nullptr),
    INIT_MEMBER(tp_setattr        , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_as_async       , nullptr),
#else
    INIT_MEMBER(tp_compare        , nullptr),
#endif
    INIT_MEMBER(tp_repr           , TPyLazyListIterator::Repr),
    INIT_MEMBER(tp_as_number      , nullptr),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , nullptr),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , Py_TPFLAGS_HAVE_ITER),
    INIT_MEMBER(tp_doc            , "yql.ListIterator object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , PyObject_SelfIter),
    INIT_MEMBER(tp_iternext       , TPyLazyListIterator::Next),
    INIT_MEMBER(tp_methods        , nullptr),
    INIT_MEMBER(tp_members        , nullptr),
    INIT_MEMBER(tp_getset         , nullptr),
    INIT_MEMBER(tp_base           , nullptr),
    INIT_MEMBER(tp_dict           , nullptr),
    INIT_MEMBER(tp_descr_get      , nullptr),
    INIT_MEMBER(tp_descr_set      , nullptr),
    INIT_MEMBER(tp_dictoffset     , 0),
    INIT_MEMBER(tp_init           , nullptr),
    INIT_MEMBER(tp_alloc          , nullptr),
    INIT_MEMBER(tp_new            , nullptr),
    INIT_MEMBER(tp_free           , nullptr),
    INIT_MEMBER(tp_is_gc          , nullptr),
    INIT_MEMBER(tp_bases          , nullptr),
    INIT_MEMBER(tp_mro            , nullptr),
    INIT_MEMBER(tp_cache          , nullptr),
    INIT_MEMBER(tp_subclasses     , nullptr),
    INIT_MEMBER(tp_weaklist       , nullptr),
    INIT_MEMBER(tp_del            , nullptr),
    INIT_MEMBER(tp_version_tag    , 0),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_finalize       , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b1
    INIT_MEMBER(tp_vectorcall     , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
    INIT_MEMBER(tp_print          , nullptr),
#endif
};

//////////////////////////////////////////////////////////////////////////////
// TPyLazyList implementation
//////////////////////////////////////////////////////////////////////////////
PyObject* TPyLazyList::New(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        NUdf::IBoxedValuePtr value,
        Py_ssize_t step,
        Py_ssize_t size)
{
    TPyLazyList* list = new TPyLazyList;
    PyObject_INIT(list, &PyLazyListType);

    list->CastCtx = castCtx;
    list->ItemType = itemType;
    list->Value.Set(castCtx->PyCtx, value);
    list->Step = step;
    list->CachedLength = size;

    return reinterpret_cast<PyObject*>(list);
}

PyObject* TPyLazyList::Repr(PyObject*)
{
    return PyRepr("<yql.TList>").Release();
}

PyObject* TPyLazyList::Iter(PyObject* self)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        return TPyLazyListIterator::New(list);
    } PY_CATCH(nullptr)
}

Py_ssize_t TPyLazyList::Len(PyObject* self)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        if (list->CachedLength == -1) {
            list->CachedLength = static_cast<Py_ssize_t>(NUdf::TBoxedValueAccessor::GetListLength(*list->Value.Get()));
        }
        return (list->CachedLength + list->Step - 1) / list->Step;
    } PY_CATCH(-1)
}

PyObject* TPyLazyList::Subscript(PyObject* self, PyObject* slice)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        const auto vb = list->CastCtx->ValueBuilder;

        if (PyIndex_Check(slice)) {
            Py_ssize_t index = PyNumber_AsSsize_t(slice, PyExc_IndexError);

            if (!list->Dict.IsSet()) {
                list->Dict.Set(list->CastCtx->PyCtx, vb->ToIndexDict(NUdf::TUnboxedValuePod(list->Value.Get().Get())).AsBoxed());
            }

            if (index < 0) {
                if (list->CachedLength == -1) {
                    list->CachedLength = static_cast<Py_ssize_t>(NUdf::TBoxedValueAccessor::GetDictLength(*list->Dict.Get()));
                }

                ++index *= list->Step;
                --index += list->CachedLength;
            } else {
                index *= list->Step;
            }

            if (index < 0 || (list->CachedLength != -1 && index >= list->CachedLength)) {
                const TPyObjectPtr error = PyUnicode_FromFormat("index %zd out of bounds, list size: %zd", index, list->CachedLength);
                PyErr_SetObject(PyExc_IndexError, error.Get());
                return nullptr;
            }

            if (const auto item = NUdf::TBoxedValueAccessor::Lookup(*list->Dict.Get(), NUdf::TUnboxedValuePod(ui64(index)))) {
                return ToPyObject(list->CastCtx, list->ItemType, item.GetOptionalValue()).Release();
            }

            const TPyObjectPtr error = PyUnicode_FromFormat("index %zd out of bounds", index);
            PyErr_SetObject(PyExc_IndexError, error.Get());
            return nullptr;
        }

        if (PySlice_Check(slice)) {
            Py_ssize_t start, stop, step, size;

            if (list->CachedLength >= 0) {
                if (PySlice_GetIndicesEx(SLICEOBJ(slice), (list->CachedLength + list->Step - 1) / list->Step, &start, &stop, &step, &size) < 0) {
                    return nullptr;
                }
            } else {
                if (PySlice_Unpack(slice, &start, &stop, &step) < 0) {
                    return nullptr;
                }

                if (step < -1 || step > 1 || (start < 0 && start > PY_SSIZE_T_MIN) || (stop < 0 && stop > PY_SSIZE_T_MIN)) {
                    list->CachedLength = static_cast<Py_ssize_t>(NUdf::TBoxedValueAccessor::GetListLength(*list->Value.Get()));
                    size = PySlice_AdjustIndices((list->CachedLength + list->Step - 1) / list->Step, &start, &stop, step);
                } else {
                    size = PySlice_AdjustIndices(PY_SSIZE_T_MAX, &start, &stop, step);
                }
            }

            if (!step) {
                PyErr_SetString(PyExc_ValueError, "slice step cannot be zero");
                return nullptr;
            }

            const Py_ssize_t hi = PY_SSIZE_T_MAX / list->Step;
            const Py_ssize_t lo = PY_SSIZE_T_MIN / list->Step;
            step = step > lo && step < hi ? step * list->Step : (step > 0 ? PY_SSIZE_T_MAX : PY_SSIZE_T_MIN);

            NUdf::TUnboxedValue newList;
            if (size > 0) {
                size = step > 0 ?
                    (size < PY_SSIZE_T_MAX / step ? --size * step + 1 : PY_SSIZE_T_MAX):
                    (size < PY_SSIZE_T_MAX / -step ? --size * -step + 1 : PY_SSIZE_T_MAX);

                start = start < hi ? start * list->Step : PY_SSIZE_T_MAX;
                const Py_ssize_t skip = step > 0 ? start : start - size + 1;

                newList = NUdf::TUnboxedValuePod(list->Value.Get().Get());
                if (skip > 0) {
                    newList = vb->SkipList(newList, skip);
                }

                if (size < PY_SSIZE_T_MAX && (list->CachedLength == -1 || list->CachedLength - skip > size)) {
                    newList = vb->TakeList(newList, size);
                }

                if (step < 0) {
                    step = -step;
                    newList = vb->ReverseList(newList);
                }
            } else {
                newList = vb->NewEmptyList();
            }

            return New(list->CastCtx, list->ItemType, newList.AsBoxed(), step, size);
        }

        const TPyObjectPtr type = PyObject_Type(slice);
        const TPyObjectPtr repr = PyObject_Repr(type.Get());
        const TPyObjectPtr error = PyUnicode_FromFormat("Unsupported slice object type: %R", repr.Get());
        PyErr_SetObject(PyExc_TypeError, error.Get());
        return nullptr;
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyList::ToIndexDict(PyObject* self, PyObject* /* arg */)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        if (!list->Dict.IsSet()) {
            list->Dict.Set(list->CastCtx->PyCtx, list->CastCtx->ValueBuilder->ToIndexDict(NUdf::TUnboxedValuePod(list->Value.Get().Get())).AsBoxed());
        }

        return ToPyLazyDict(list->CastCtx, nullptr, list->ItemType, NUdf::TUnboxedValuePod(list->Dict.Get().Get())).Release();
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyList::Reversed(PyObject* self, PyObject* /* arg */)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        const auto newList = list->CastCtx->ValueBuilder->ReverseList(NUdf::TUnboxedValuePod(list->Value.Get().Get()));
        return New(list->CastCtx, list->ItemType, newList.AsBoxed(), list->Step);
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyList::Take(PyObject* self, PyObject* arg)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        Py_ssize_t count = CastIndex(arg, "take");
        if (count < 0) {
            return nullptr;
        }
        count *= list->Step;

        auto vb = list->CastCtx->ValueBuilder;
        NUdf::TUnboxedValue value(NUdf::TUnboxedValuePod(list->Value.Get().Get()));
        auto newList = vb->TakeList(value, static_cast<ui64>(count));
        return New(list->CastCtx, list->ItemType, newList.AsBoxed(), list->Step);
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyList::Skip(PyObject* self, PyObject* arg)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        Py_ssize_t count = CastIndex(arg, "skip");
        if (count < 0) {
            return nullptr;
        }
        count *= list->Step;

        NUdf::TUnboxedValue value(NUdf::TUnboxedValuePod(list->Value.Get().Get()));
        const auto newList = list->CastCtx->ValueBuilder->SkipList(value, static_cast<ui64>(count));
        return New(list->CastCtx, list->ItemType, newList.AsBoxed(), list->Step);
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyList::HasFastLen(PyObject* self, PyObject* /* arg */)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        if (NUdf::TBoxedValueAccessor::HasFastListLength(*list->Value.Get())) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_FALSE;
    } PY_CATCH(nullptr)
}

PyObject* TPyLazyList::HasItems(PyObject* self, PyObject* /* arg */)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        if (NUdf::TBoxedValueAccessor::HasListItems(*list->Value.Get())) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_FALSE;
    } PY_CATCH(nullptr)
}

int TPyLazyList::Bool(PyObject* self)
{
    PY_TRY {
        TPyLazyList* list = Cast(self);
        if (list->CachedLength == -1) {
            return NUdf::TBoxedValueAccessor::HasListItems(*list->Value.Get()) ? 1 : 0;
        } else {
            return list->CachedLength > 0 ? 1 : 0;
        }
    } PY_CATCH(-1)
}

//////////////////////////////////////////////////////////////////////////////
// TPyLazyListIterator implementation
//////////////////////////////////////////////////////////////////////////////
PyObject* TPyLazyListIterator::New(TPyLazyList* list)
{
    TPyLazyListIterator* listIter = new TPyLazyListIterator;
    PyObject_INIT(listIter, &PyLazyListIteratorType);
    listIter->List.Reset(list);
    listIter->Iterator.Set(list->CastCtx->PyCtx, NUdf::TBoxedValueAccessor::GetListIterator(*list->Value.Get()));
    listIter->Length = 0;
    listIter->CastCtx = list->CastCtx;
    return reinterpret_cast<PyObject*>(listIter);
}

PyObject* TPyLazyListIterator::Next(PyObject* self)
{
    PY_TRY {
        TPyLazyListIterator* iter = Cast(self);
        TPyLazyList* list = iter->List.Get();

        NUdf::TUnboxedValue item;
        if (iter->Iterator.Get().Next(item)) {
            ++iter->Length;

            for (auto skip = list->Step; --skip && iter->Iterator.Get().Skip(); ++iter->Length)
                continue;

            return ToPyObject(list->CastCtx, list->ItemType, item).Release();
        }

        // store calculated list length after traverse over whole list
        if (list->CachedLength == -1) {
            list->CachedLength = iter->Length;
        }

        return nullptr;
    } PY_CATCH(nullptr)
}

//////////////////////////////////////////////////////////////////////////////
// TPyThinList interface
//////////////////////////////////////////////////////////////////////////////
struct TPyThinList
{
    using TPtr = NUdf::TRefCountedPtr<TPyThinList, TPyPtrOps<TPyThinList>>;

    PyObject_HEAD;
    TPyCastContext::TPtr CastCtx;
    const NUdf::TType* ItemType;
    TPyCleanupListItem<NUdf::IBoxedValuePtr> Value;
    const NUdf::TUnboxedValue* Elements;
    Py_ssize_t Length;
    Py_ssize_t Step;

    inline static TPyThinList* Cast(PyObject* o) {
        return reinterpret_cast<TPyThinList*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    static PyObject* New(
            const TPyCastContext::TPtr& castCtx,
            const NUdf::TType* itemType,
            NUdf::IBoxedValuePtr value = NUdf::IBoxedValuePtr(),
            const NUdf::TUnboxedValue* elements = nullptr,
            Py_ssize_t length = 0,
            Py_ssize_t step = 1);

    static int Bool(PyObject* self);
    static PyObject* Repr(PyObject* self);
    static PyObject* Iter(PyObject* self);
    static Py_ssize_t Len(PyObject* self);
    static PyObject* Subscript(PyObject* self, PyObject* slice);
    static PyObject* ToIndexDict(PyObject* self, PyObject* /* arg */);
    static PyObject* Reversed(PyObject* self, PyObject* /* arg */);
    static PyObject* Take(PyObject* self, PyObject* arg);
    static PyObject* Skip(PyObject* self, PyObject* arg);
    static PyObject* HasFastLen(PyObject* self, PyObject* /* arg */);
    static PyObject* HasItems(PyObject* self, PyObject* /* arg */);
};

PyMappingMethods ThinListMapping = {
    INIT_MEMBER(mp_length, TPyThinList::Len),
    INIT_MEMBER(mp_subscript, TPyThinList::Subscript),
    INIT_MEMBER(mp_ass_subscript, nullptr),
};

PyNumberMethods ThinListNumbering = {
     INIT_MEMBER(nb_add, nullptr),
     INIT_MEMBER(nb_subtract, nullptr),
     INIT_MEMBER(nb_multiply, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_divide, nullptr),
#endif
     INIT_MEMBER(nb_remainder, nullptr),
     INIT_MEMBER(nb_divmod, nullptr),
     INIT_MEMBER(nb_power, nullptr),
     INIT_MEMBER(nb_negative, nullptr),
     INIT_MEMBER(nb_positive, nullptr),
     INIT_MEMBER(nb_absolute, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_bool, TPyThinList::Bool),
#else
     INIT_MEMBER(nb_nonzero, TPyThinList::Bool),
#endif
     INIT_MEMBER(nb_invert, nullptr),
     INIT_MEMBER(nb_lshift, nullptr),
     INIT_MEMBER(nb_rshift, nullptr),
     INIT_MEMBER(nb_and, nullptr),
     INIT_MEMBER(nb_xor, nullptr),
     INIT_MEMBER(nb_or, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_coerce, nullptr),
#endif
     INIT_MEMBER(nb_int, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_reserved, nullptr),
#else
     INIT_MEMBER(nb_long, nullptr),
#endif
     INIT_MEMBER(nb_float, nullptr),
#if PY_MAJOR_VERSION < 3
     INIT_MEMBER(nb_oct, nullptr),
     INIT_MEMBER(nb_hex, nullptr),
#endif

     INIT_MEMBER(nb_inplace_add, nullptr),
     INIT_MEMBER(nb_inplace_subtract, nullptr),
     INIT_MEMBER(nb_inplace_multiply, nullptr),
     INIT_MEMBER(nb_inplace_remainder, nullptr),
     INIT_MEMBER(nb_inplace_power, nullptr),
     INIT_MEMBER(nb_inplace_lshift, nullptr),
     INIT_MEMBER(nb_inplace_rshift, nullptr),
     INIT_MEMBER(nb_inplace_and, nullptr),
     INIT_MEMBER(nb_inplace_xor, nullptr),
     INIT_MEMBER(nb_inplace_or, nullptr),

     INIT_MEMBER(nb_floor_divide, nullptr),
     INIT_MEMBER(nb_true_divide, nullptr),
     INIT_MEMBER(nb_inplace_floor_divide, nullptr),
     INIT_MEMBER(nb_inplace_true_divide, nullptr),

     INIT_MEMBER(nb_index, nullptr),
#if PY_MAJOR_VERSION >= 3
     INIT_MEMBER(nb_matrix_multiply, nullptr),
     INIT_MEMBER(nb_inplace_matrix_multiply, nullptr),
#endif
};

static PyMethodDef TPyThinListMethods[] = {
    { "__reversed__",  TPyThinList::Reversed, METH_NOARGS, nullptr },
    { "to_index_dict",  TPyThinList::ToIndexDict, METH_NOARGS, to_index_dict__doc__ },
    { "reversed",  TPyThinList::Reversed, METH_NOARGS, reversed__doc__ },
    { "take",  TPyThinList::Take, METH_O, take__doc__ },
    { "skip",  TPyThinList::Skip, METH_O, skip__doc__ },
    { "has_fast_len",  TPyThinList::HasFastLen, METH_NOARGS, has_fast_len__doc__ },
    { "has_items",  TPyThinList::HasItems, METH_NOARGS, has_items__doc__ },
    { nullptr, nullptr, 0, nullptr }    /* sentinel */
};

#if PY_MAJOR_VERSION >= 3
#define Py_TPFLAGS_HAVE_ITER 0
#endif

PyTypeObject PyThinListType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TList"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyThinList)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyThinList::Dealloc),
#if PY_VERSION_HEX < 0x030800b4
    INIT_MEMBER(tp_print          , nullptr),
#else
    INIT_MEMBER(tp_vectorcall_offset, 0),
#endif
    INIT_MEMBER(tp_getattr        , nullptr),
    INIT_MEMBER(tp_setattr        , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_as_async       , nullptr),
#else
    INIT_MEMBER(tp_compare        , nullptr),
#endif
    INIT_MEMBER(tp_repr           , TPyThinList::Repr),
    INIT_MEMBER(tp_as_number      , &ThinListNumbering),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , &ThinListMapping),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , Py_TPFLAGS_HAVE_ITER),
    INIT_MEMBER(tp_doc            , "yql.TList object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , TPyThinList::Iter),
    INIT_MEMBER(tp_iternext       , nullptr),
    INIT_MEMBER(tp_methods        , TPyThinListMethods),
    INIT_MEMBER(tp_members        , nullptr),
    INIT_MEMBER(tp_getset         , nullptr),
    INIT_MEMBER(tp_base           , nullptr),
    INIT_MEMBER(tp_dict           , nullptr),
    INIT_MEMBER(tp_descr_get      , nullptr),
    INIT_MEMBER(tp_descr_set      , nullptr),
    INIT_MEMBER(tp_dictoffset     , 0),
    INIT_MEMBER(tp_init           , nullptr),
    INIT_MEMBER(tp_alloc          , nullptr),
    INIT_MEMBER(tp_new            , nullptr),
    INIT_MEMBER(tp_free           , nullptr),
    INIT_MEMBER(tp_is_gc          , nullptr),
    INIT_MEMBER(tp_bases          , nullptr),
    INIT_MEMBER(tp_mro            , nullptr),
    INIT_MEMBER(tp_cache          , nullptr),
    INIT_MEMBER(tp_subclasses     , nullptr),
    INIT_MEMBER(tp_weaklist       , nullptr),
    INIT_MEMBER(tp_del            , nullptr),
    INIT_MEMBER(tp_version_tag    , 0),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_finalize       , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b1
    INIT_MEMBER(tp_vectorcall     , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
    INIT_MEMBER(tp_print          , nullptr),
#endif
};

//////////////////////////////////////////////////////////////////////////////
// TPyThinListIterator interface
//////////////////////////////////////////////////////////////////////////////
struct TPyThinListIterator
{
    PyObject_HEAD;
    TPyThinList::TPtr List;
    const NUdf::TUnboxedValue* Elements;
    Py_ssize_t Count;

    inline static TPyThinListIterator* Cast(PyObject* o) {
        return reinterpret_cast<TPyThinListIterator*>(o);
    }

    inline static void Dealloc(PyObject* self) {
        delete Cast(self);
    }

    inline static PyObject* Repr(PyObject* self) {
        Y_UNUSED(self);
        return PyRepr("<yql.TListIterator>").Release();
    }

    static PyObject* New(TPyThinList* list);
    static PyObject* Next(PyObject* self);
};

PyTypeObject PyThinListIteratorType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    INIT_MEMBER(tp_name           , "yql.TListIterator"),
    INIT_MEMBER(tp_basicsize      , sizeof(TPyThinListIterator)),
    INIT_MEMBER(tp_itemsize       , 0),
    INIT_MEMBER(tp_dealloc        , TPyThinListIterator::Dealloc),
#if PY_VERSION_HEX < 0x030800b4
    INIT_MEMBER(tp_print          , nullptr),
#else
    INIT_MEMBER(tp_vectorcall_offset, 0),
#endif
    INIT_MEMBER(tp_getattr        , nullptr),
    INIT_MEMBER(tp_setattr        , nullptr),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_as_async       , nullptr),
#else
    INIT_MEMBER(tp_compare        , nullptr),
#endif
    INIT_MEMBER(tp_repr           , TPyThinListIterator::Repr),
    INIT_MEMBER(tp_as_number      , nullptr),
    INIT_MEMBER(tp_as_sequence    , nullptr),
    INIT_MEMBER(tp_as_mapping     , nullptr),
    INIT_MEMBER(tp_hash           , nullptr),
    INIT_MEMBER(tp_call           , nullptr),
    INIT_MEMBER(tp_str            , nullptr),
    INIT_MEMBER(tp_getattro       , nullptr),
    INIT_MEMBER(tp_setattro       , nullptr),
    INIT_MEMBER(tp_as_buffer      , nullptr),
    INIT_MEMBER(tp_flags          , Py_TPFLAGS_HAVE_ITER),
    INIT_MEMBER(tp_doc            , "yql.ListIterator object"),
    INIT_MEMBER(tp_traverse       , nullptr),
    INIT_MEMBER(tp_clear          , nullptr),
    INIT_MEMBER(tp_richcompare    , nullptr),
    INIT_MEMBER(tp_weaklistoffset , 0),
    INIT_MEMBER(tp_iter           , PyObject_SelfIter),
    INIT_MEMBER(tp_iternext       , TPyThinListIterator::Next),
    INIT_MEMBER(tp_methods        , nullptr),
    INIT_MEMBER(tp_members        , nullptr),
    INIT_MEMBER(tp_getset         , nullptr),
    INIT_MEMBER(tp_base           , nullptr),
    INIT_MEMBER(tp_dict           , nullptr),
    INIT_MEMBER(tp_descr_get      , nullptr),
    INIT_MEMBER(tp_descr_set      , nullptr),
    INIT_MEMBER(tp_dictoffset     , 0),
    INIT_MEMBER(tp_init           , nullptr),
    INIT_MEMBER(tp_alloc          , nullptr),
    INIT_MEMBER(tp_new            , nullptr),
    INIT_MEMBER(tp_free           , nullptr),
    INIT_MEMBER(tp_is_gc          , nullptr),
    INIT_MEMBER(tp_bases          , nullptr),
    INIT_MEMBER(tp_mro            , nullptr),
    INIT_MEMBER(tp_cache          , nullptr),
    INIT_MEMBER(tp_subclasses     , nullptr),
    INIT_MEMBER(tp_weaklist       , nullptr),
    INIT_MEMBER(tp_del            , nullptr),
    INIT_MEMBER(tp_version_tag    , 0),
#if PY_MAJOR_VERSION >= 3
    INIT_MEMBER(tp_finalize       , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b1
    INIT_MEMBER(tp_vectorcall     , nullptr),
#endif
#if PY_VERSION_HEX >= 0x030800b4 && PY_VERSION_HEX < 0x03090000
    INIT_MEMBER(tp_print          , nullptr),
#endif
};

//////////////////////////////////////////////////////////////////////////////
// TPyThinList implementation
//////////////////////////////////////////////////////////////////////////////
PyObject* TPyThinList::New(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        NUdf::IBoxedValuePtr value,
        const NUdf::TUnboxedValue* elements,
        Py_ssize_t length,
        Py_ssize_t step)
{
    TPyThinList* list = new TPyThinList;
    PyObject_INIT(list, &PyThinListType);

    list->CastCtx = castCtx;
    list->ItemType = itemType;
    list->Value.Set(castCtx->PyCtx, value);
    list->Elements = elements;
    list->Length = length;
    list->Step = step;

    return reinterpret_cast<PyObject*>(list);
}

PyObject* TPyThinList::Repr(PyObject*)
{
    return PyRepr("<yql.TList>").Release();
}

PyObject* TPyThinList::Iter(PyObject* self)
{
    PY_TRY {
        TPyThinList* list = Cast(self);
        return TPyThinListIterator::New(list);
    } PY_CATCH(nullptr)
}

Py_ssize_t TPyThinList::Len(PyObject* self)
{
    return Cast(self)->Length;
}

PyObject* TPyThinList::Subscript(PyObject* self, PyObject* slice)
{
    PY_TRY {
        TPyThinList* list = Cast(self);
        const auto vb = list->CastCtx->ValueBuilder;

        if (PyIndex_Check(slice)) {
            Py_ssize_t index = PyNumber_AsSsize_t(slice, PyExc_IndexError);

            if (index < 0) {
                index += list->Length;
            }

            if (index < 0 || index >= list->Length) {
                const TPyObjectPtr error = PyUnicode_FromFormat("index %zd out of bounds, list size: %zd", index, list->Length);
                PyErr_SetObject(PyExc_IndexError, error.Get());
                return nullptr;
            }

            if (list->Step > 0) {
                index *= list->Step;
            } else {
                index = list->Length - ++index;
                index *= -list->Step;
            }

            return ToPyObject(list->CastCtx, list->ItemType, list->Elements[index]).Release();
        }

        if (PySlice_Check(slice)) {
            Py_ssize_t start, stop, step, size;

            if (PySlice_GetIndicesEx(SLICEOBJ(slice), list->Length, &start, &stop, &step, &size) < 0) {
                return nullptr;
            }

            if (!step) {
                PyErr_SetString(PyExc_ValueError, "slice step cannot be zero");
                return nullptr;
            }

            if (size > 0) {
                const Py_ssize_t skip = list->Step * (list->Step > 0 ?
                    (step > 0 ? start : start + step * (size - 1)):
                    (step > 0 ? stop  : start + 1) - list->Length);

                return New(list->CastCtx, list->ItemType, list->Value.Get(), list->Elements + skip, size, step * list->Step);
            } else {
                return New(list->CastCtx, list->ItemType, list->Value.Get());
            }
        }

        const TPyObjectPtr type = PyObject_Type(slice);
        const TPyObjectPtr repr = PyObject_Repr(type.Get());
        const TPyObjectPtr error = PyUnicode_FromFormat("Unsupported slice object type: %R", repr.Get());
        PyErr_SetObject(PyExc_TypeError, error.Get());
        return nullptr;
    } PY_CATCH(nullptr)
}

#undef SLICEOBJ

PyObject* TPyThinList::ToIndexDict(PyObject* self, PyObject* /* arg */)
{
    PY_TRY {
        TPyThinList* list = Cast(self);
        const auto dict = list->CastCtx->ValueBuilder->ToIndexDict(NUdf::TUnboxedValuePod(list->Value.Get().Get()));
        return ToPyLazyDict(list->CastCtx, nullptr, list->ItemType, dict).Release();
    } PY_CATCH(nullptr)
}

PyObject* TPyThinList::Reversed(PyObject* self, PyObject* /* arg */)
{
    PY_TRY {
        TPyThinList* list = Cast(self);
        return New(list->CastCtx, list->ItemType, list->Value.Get(), list->Elements, list->Length, -list->Step);
    } PY_CATCH(nullptr)
}

PyObject* TPyThinList::Take(PyObject* self, PyObject* arg)
{
    PY_TRY {
        TPyThinList* list = Cast(self);
        const Py_ssize_t count = CastIndex(arg, "take");
        if (count < 0) {
            return nullptr;
        }

        if (const auto size = std::min(count, list->Length)) {
            return New(list->CastCtx, list->ItemType, list->Value.Get(), list->Step > 0 ? list->Elements : list->Elements + list->Length + size * list->Step, size, list->Step);
        } else {
            return New(list->CastCtx, list->ItemType, list->Value.Get());
        }
    } PY_CATCH(nullptr)
}

PyObject* TPyThinList::Skip(PyObject* self, PyObject* arg)
{
    PY_TRY {
        TPyThinList* list = Cast(self);
        const Py_ssize_t count = CastIndex(arg, "skip");
        if (count < 0) {
            return nullptr;
        }

        if (const auto size = std::max(list->Length - count, Py_ssize_t(0))) {
            return New(list->CastCtx, list->ItemType, list->Value.Get(), list->Step > 0 ? list->Elements + count * list->Step : list->Elements, size, list->Step);
        } else {
            return New(list->CastCtx, list->ItemType);
        }
    } PY_CATCH(nullptr)
}

PyObject* TPyThinList::HasFastLen(PyObject* self, PyObject* /* arg */)
{
    Py_RETURN_TRUE;
}

PyObject* TPyThinList::HasItems(PyObject* self, PyObject* /* arg */)
{
    if (Cast(self)->Length > 0)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}

int TPyThinList::Bool(PyObject* self)
{
    return Cast(self)->Length > 0 ? 1 : 0;
}

//////////////////////////////////////////////////////////////////////////////
// TPyThinListIterator implementation
//////////////////////////////////////////////////////////////////////////////
PyObject* TPyThinListIterator::New(TPyThinList* list)
{
    TPyThinListIterator* listIter = new TPyThinListIterator;
    PyObject_INIT(listIter, &PyThinListIteratorType);
    listIter->List.Reset(list);
    listIter->Elements = list->Step > 0 ? list->Elements - list->Step : list->Elements - list->Length * list->Step;
    listIter->Count = list->Length;
    return reinterpret_cast<PyObject*>(listIter);
}

PyObject* TPyThinListIterator::Next(PyObject* self)
{
    PY_TRY {
        TPyThinListIterator* iter = Cast(self);

        if (iter->Count) {
            --iter->Count;
            TPyThinList* list = iter->List.Get();
            return ToPyObject(list->CastCtx, list->ItemType, *(iter->Elements += list->Step)).Release();
        }

        return nullptr;
    } PY_CATCH(nullptr)
}

TPyObjectPtr ToPyLazyList(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        const NUdf::TUnboxedValuePod& value)
{
    if (const auto elements = value.GetElements()) {
        return TPyThinList::New(castCtx, itemType, value.AsBoxed(), elements, value.GetListLength());
    } else {
        return TPyLazyList::New(castCtx, itemType, value.AsBoxed());
    }
}

} // namspace NPython
