#pragma once

#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NMiniKQL {

template <typename T, bool IsConst = std::is_const_v<T>>
class TStateWrapper;

template <typename T>
class TStateWrapper<T, true> {
public:
    TStateWrapper(const void* ptr)
        : State_(ReadUnaligned<typename std::remove_const<T>::type>(ptr))
    { }

    T* Get() {
        return &State_;
    }

    T* operator->() {
        return Get();
    }

private:
    T State_;
};

template <typename T>
class TStateWrapper<T, false> {
public:
    TStateWrapper(void* ptr)
        : State_(ReadUnaligned<T>(ptr))
        , Ptr_(ptr)
    { }

    ~TStateWrapper() {
        WriteUnaligned<T>(Ptr_, State_);
    }

    T* Get() {
        return &State_;
    }

    T* operator->() {
        return Get();
    }

private:
    T State_;
    void* Ptr_;
};

template <typename T>
inline TStateWrapper<T> MakeStateWrapper(void* ptr) {
    return TStateWrapper<T>(ptr);
}

template <typename T>
inline TStateWrapper<const T> MakeStateWrapper(const void* ptr) {
    return TStateWrapper<const T>(ptr);
}

template<typename T>
inline T Cast(T t) {
    return t;
}

inline NYql::NDecimal::TDecimal Cast(const std::shared_ptr<arrow::Buffer>& buffer) {
    NYql::NDecimal::TDecimal t;
    memcpy((void*)&t, buffer->data(), buffer->size());
    return t;
}

}
}
