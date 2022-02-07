#pragma once

#include <util/system/types.h>
#include <util/system/yassert.h>

namespace NKikimr {

class TAny64 {
public:
    TAny64(ui64 raw = 0)
        : Raw(raw)
    { }

    template <typename T>
    TAny64(T* pointer)
        : Pointer(static_cast<void*>(pointer))
    { /* no-op */ }

    template <typename T>
    TAny64(const T& value)
        : Raw(value)
    { /* no-op */ }

    void Reset() {
        Raw = 0;
    }

    template <typename T>
    T* Release() {
        T* result = static_cast<T*>(Pointer);
        Raw = 0;
        return result;
    }

    template <typename T>
    void SetPtr(T* pointer) {
        Pointer = static_cast<void*>(pointer);
    }

    template <typename T>
    void SetValue(const T& value) {
        Raw = value;
    }

    template <typename T>
    T* Ptr() {
        return static_cast<T*>(Pointer);
    }

    template <typename T>
    const T* Ptr() const {
        return static_cast<const T*>(Pointer);
    }

    template <typename T>
    T* MutablePtr() const {
        return const_cast<T*>(Ptr<T>());
    }

    template <typename T>
    T Value() const {
        return T(Raw);
    }

    operator bool() const { return Pointer; }

private:
    union {
        void* Pointer;
        ui64 Raw;
    };
};

static_assert(sizeof(TAny64) == sizeof(ui64), "Expected TAny64 size violated.");

} // namespace NKikimr

