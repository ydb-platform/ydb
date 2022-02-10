#pragma once

#include "accessors_impl.h"

namespace NAccessors {
    /*
 * Adds API compatibility between different types representing memory regions.
 *
 * i.e. this will work:
 *
 * TString t;
 * const char* beg = NAccessors::Begin(t); // t.begin()
 * const char* end = NAccessors::End(t); // t.end()
 * size_t sz = NAccessors::Size(t); // t.size()
 *
 * as well as this:
 *
 * ui64 t;
 * const ui64* beg = NAccessors::Begin(t); // &t
 * const ui64* end = NAccessors::End(t); // &t + 1
 * size_t sz = NAccessors::Size(t); // 1
 *
 * Both will give you begin, end and size of the underlying memory region.
 */

    template <typename T>
    inline const typename TMemoryTraits<T>::TElementType* Begin(const T& t) {
        return NPrivate::TBegin<T>::Get(t);
    }

    template <typename T>
    inline const typename TMemoryTraits<T>::TElementType* End(const T& t) {
        return NPrivate::TEnd<T>::Get(t);
    }

    template <typename T>
    inline size_t Size(const T& t) {
        return End(t) - Begin(t);
    }

    /**
 * This gives some unification in terms of memory manipulation.
 */

    template <typename T>
    inline void Reserve(T& t, size_t sz) {
        NPrivate::TReserve<T>::Do(t, sz);
    }

    template <typename T>
    inline void Resize(T& t, size_t sz) {
        NPrivate::TResize<T>::Do(t, sz);
    }

    template <typename T>
    inline void Clear(T& t) {
        NPrivate::TClear<T, false>::Do(t);
    }

    template <typename T>
    inline void Init(T& t) {
        NPrivate::TClear<T, true>::Do(t);
    }

    template <typename T>
    inline void Append(T& t, const typename TMemoryTraits<T>::TElementType& v) {
        NPrivate::TAppend<T>::Do(t, v);
    }

    template <typename T>
    inline void Append(T& t,
                       const typename TMemoryTraits<T>::TElementType* beg,
                       const typename TMemoryTraits<T>::TElementType* end) {
        NPrivate::TAppendRegion<T>::Do(t, beg, end);
    }

    template <typename T>
    inline void Assign(T& t,
                       const typename TMemoryTraits<T>::TElementType* beg,
                       const typename TMemoryTraits<T>::TElementType* end) {
        NPrivate::TAssign<T>::Do(t, beg, end);
    }
}
