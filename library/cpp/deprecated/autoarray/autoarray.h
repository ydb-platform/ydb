#pragma once

#include <util/system/compat.h>
#include <util/system/yassert.h>
#include <util/system/defaults.h>
#include <util/system/sys_alloc.h>

#include <util/generic/typetraits.h>
#include <utility>

#include <new>
#include <util/generic/noncopyable.h>

struct autoarray_getindex {
    autoarray_getindex() = default;
};

struct aarr_b0 {
    aarr_b0() = default;
};

struct aarr_nofill {
    aarr_nofill() = default;
};

template <typename T>
struct ynd_type_traits {
    enum {
        empty_destructor = TTypeTraits<T>::IsPod,
    };
};

template <class T>
class autoarray : TNonCopyable {
protected:
    T* arr;
    size_t _size;

private:
    void AllocBuf(size_t siz) {
        arr = nullptr;
        _size = 0;
        if (siz) {
            arr = (T*)y_allocate(sizeof(T) * siz);
            _size = siz;
        }
    }

public:
    using value_type = T;
    using iterator = T*;
    using const_iterator = const T*;

    autoarray()
        : arr(nullptr)
        , _size(0)
    {
    }
    autoarray(size_t siz) {
        AllocBuf(siz);
        T* curr = arr;
        try {
            for (T* end = arr + _size; curr != end; ++curr)
                new (curr) T();
        } catch (...) {
            for (--curr; curr >= arr; --curr)
                curr->~T();
            y_deallocate(arr);
            throw;
        }
    }
    template <class A>
    explicit autoarray(size_t siz, A& fill) {
        AllocBuf(siz);
        T* curr = arr;
        try {
            for (T* end = arr + _size; curr != end; ++curr)
                new (curr) T(fill);
        } catch (...) {
            for (--curr; curr >= arr; --curr)
                curr->~T();
            y_deallocate(arr);
            throw;
        }
    }
    explicit autoarray(size_t siz, autoarray_getindex) {
        AllocBuf(siz);
        size_t nCurrent = 0;
        try {
            for (nCurrent = 0; nCurrent < _size; ++nCurrent)
                new (&arr[nCurrent]) T(nCurrent);
        } catch (...) {
            for (size_t n = 0; n < nCurrent; ++n)
                arr[n].~T();
            y_deallocate(arr);
            throw;
        }
    }
    explicit autoarray(size_t siz, aarr_b0) {
        AllocBuf(siz);
        memset(arr, 0, _size * sizeof(T));
    }
    explicit autoarray(size_t siz, aarr_nofill) {
        AllocBuf(siz);
    }
    template <class A>
    explicit autoarray(const A* fill, size_t siz) {
        AllocBuf(siz);
        size_t nCurrent = 0;
        try {
            for (nCurrent = 0; nCurrent < _size; ++nCurrent)
                new (&arr[nCurrent]) T(fill[nCurrent]);
        } catch (...) {
            for (size_t n = 0; n < nCurrent; ++n)
                arr[n].~T();
            y_deallocate(arr);
            throw;
        }
    }
    template <class A, class B>
    explicit autoarray(const A* fill, const B* cfill, size_t siz) {
        AllocBuf(siz);
        size_t nCurrent = 0;
        try {
            for (nCurrent = 0; nCurrent < _size; ++nCurrent)
                new (&arr[nCurrent]) T(fill[nCurrent], cfill);
        } catch (...) {
            for (size_t n = 0; n < nCurrent; ++n)
                arr[n].~T();
            y_deallocate(arr);
            throw;
        }
    }
    template <class A>
    explicit autoarray(const A* fill, size_t initsiz, size_t fullsiz) {
        AllocBuf(fullsiz);
        size_t nCurrent = 0;
        try {
            for (nCurrent = 0; nCurrent < ((initsiz < _size) ? initsiz : _size); ++nCurrent)
                new (&arr[nCurrent]) T(fill[nCurrent]);
            for (; nCurrent < _size; ++nCurrent)
                new (&arr[nCurrent]) T();
        } catch (...) {
            for (size_t n = 0; n < nCurrent; ++n)
                arr[n].~T();
            y_deallocate(arr);
            throw;
        }
    }
    template <class A>
    explicit autoarray(const A* fill, size_t initsiz, size_t fullsiz, const T& dummy) {
        AllocBuf(fullsiz);
        size_t nCurrent = 0;
        try {
            for (nCurrent = 0; nCurrent < ((initsiz < _size) ? initsiz : _size); ++nCurrent)
                new (&arr[nCurrent]) T(fill[nCurrent]);
            for (; nCurrent < _size; ++nCurrent)
                new (&arr[nCurrent]) T(dummy);
        } catch (...) {
            for (size_t n = 0; n < nCurrent; ++n)
                arr[n].~T();
            y_deallocate(arr);
            throw;
        }
    }

    template <class... R>
    explicit autoarray(size_t siz, R&&... fill) {
        AllocBuf(siz);
        T* curr = arr;
        try {
            for (T* end = arr + _size; curr != end; ++curr)
                new (curr) T(std::forward<R>(fill)...);
        } catch (...) {
            for (--curr; curr >= arr; --curr)
                curr->~T();
            y_deallocate(arr);
            throw;
        }
    }
    ~autoarray() {
        if (_size) {
            if (!ynd_type_traits<T>::empty_destructor)
                for (T *curr = arr, *end = arr + _size; curr != end; ++curr)
                    curr->~T();
            y_deallocate(arr);
        }
    }
    T& operator[](size_t pos) {
        Y_ASSERT(pos < _size);
        return arr[pos];
    }
    const T& operator[](size_t pos) const {
        Y_ASSERT(pos < _size);
        return arr[pos];
    }
    size_t size() const {
        return _size;
    }
    void swap(autoarray& with) {
        T* tmp_arr = arr;
        size_t tmp_size = _size;
        arr = with.arr;
        _size = with._size;
        with.arr = tmp_arr;
        with._size = tmp_size;
    }
    void resize(size_t siz) {
        autoarray<T> tmp(arr, _size, siz);
        swap(tmp);
    }
    void resize(size_t siz, const T& dummy) {
        autoarray<T> tmp(arr, _size, siz, dummy);
        swap(tmp);
    }
    T* rawpointer() {
        return arr;
    }
    const T* operator~() const {
        return arr;
    }
    T* begin() {
        return arr;
    }
    T* end() {
        return arr + _size;
    }
    T& back() {
        Y_ASSERT(_size);
        return arr[_size - 1];
    }
    bool empty() const {
        return !_size;
    }
    bool operator!() const {
        return !_size;
    }
    size_t operator+() const {
        return _size;
    }
    const T* begin() const {
        return arr;
    }
    const T* end() const {
        return arr + _size;
    }
    const T& back() const {
        Y_ASSERT(_size);
        return arr[_size - 1];
    }
    //operator T*() { return arr; }
};

template <class T>
inline bool operator==(const autoarray<T>& a, const autoarray<T>& b) {
    size_t count = a.size();
    if (count != b.size())
        return false;
    for (size_t i = 0; i < count; ++i) {
        if (a[i] != b[i])
            return false;
    }
    return true;
}
