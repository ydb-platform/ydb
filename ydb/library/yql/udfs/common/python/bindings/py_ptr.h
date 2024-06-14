#pragma once

#include <Python.h> // PyObject

#include <ydb/library/yql/public/udf/udf_ptr.h>

namespace NPython {

template <typename T>
class TPyPtrOps
{
public:
    static inline void Ref(T* t) {
        Y_ASSERT(t);
        Py_INCREF(t);
    }

    static inline void UnRef(T* t) {
        Y_ASSERT(t);
        Py_DECREF(t);
    }

    static inline ui32 RefCount(const T* t) {
        Y_ASSERT(t);
        return t->ob_refcnt;
    }
};

class TPyObjectPtr:
        public NYql::NUdf::TRefCountedPtr<PyObject, TPyPtrOps<PyObject>>
{
    using TSelf = NYql::NUdf::TRefCountedPtr<PyObject, TPyPtrOps<PyObject>>;

public:
    inline TPyObjectPtr()
    {
    }

    inline TPyObjectPtr(PyObject* p)
        : TSelf(p, STEAL_REF)   // do not increment refcounter by default
    {
    }

    inline TPyObjectPtr(PyObject* p, AddRef)
        : TSelf(p)
    {
    }

    inline void ResetSteal(PyObject* p) {
        TSelf::Reset(p, STEAL_REF);
    }

    inline void ResetAddRef(PyObject* p) {
        TSelf::Reset(p);
    }

    inline void Reset() {
        TSelf::Reset();
    }

    template <class T>
    inline T* GetAs() const {
        return reinterpret_cast<T*>(Get());
    }

    void Reset(PyObject* p) = delete;
};

} // namspace NPython
