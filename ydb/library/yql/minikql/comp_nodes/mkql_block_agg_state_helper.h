#pragma once

namespace NKikimr {
namespace NMiniKQL {

template <typename T, bool IsAligned>
class TStateWrapper;

template <typename T>
class TStateWrapper<T, true> {
public:
    TStateWrapper(void* ptr)
        : Ptr_(ptr) 
    {
        memcpy((void*)&State_, ptr, sizeof(State_));
    }

    TStateWrapper(const void* ptr)
        : Ptr_(nullptr) 
    {
        memcpy((void*)&State_, ptr, sizeof(State_));
    }

    ~TStateWrapper() {
        if (Ptr_) {
            memcpy(Ptr_, (void*)&State_, sizeof(State_));
        }
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
class TStateWrapper<T, false> {
public:
    TStateWrapper(void* ptr) : Ptr_(reinterpret_cast<T*>(ptr)) {}

    TStateWrapper(const void* ptr) : Ptr_(reinterpret_cast<const T*>(ptr)) {}

    T* Get() {
        return Ptr_;
    }

    T* operator->() {
        return Get();
    }

private:
    T* Ptr_;
};

template <typename T>
inline TStateWrapper<T, alignof(T) >= 16> MakeStateWrapper(void* ptr) {
    return TStateWrapper<T, alignof(T) >= 16>(ptr);
}

template <typename T>
inline TStateWrapper<const T, alignof(T) < 16> MakeStateWrapper(const void* ptr) {
    return TStateWrapper<const T, alignof(T) < 16>(ptr);
}

template<typename T>
inline T Cast(T t) {
    return t;
}

inline NYql::NDecimal::TInt128 Cast(const std::shared_ptr<arrow::Buffer>& buffer) {
    NYql::NDecimal::TInt128 t;
    memcpy((void*)&t, buffer->data(), buffer->size());
    return t;
}

}
}
