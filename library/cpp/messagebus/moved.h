#pragma once

#include <util/generic/utility.h>

template <typename T>
class TMoved {
private:
    mutable T Value;

public:
    TMoved() {
    }
    TMoved(const TMoved<T>& that) {
        DoSwap(Value, that.Value);
    }
    TMoved(const T& that) {
        DoSwap(Value, const_cast<T&>(that));
    }

    void swap(TMoved& that) {
        DoSwap(Value, that.Value);
    }

    T& operator*() {
        return Value;
    }

    const T& operator*() const {
        return Value;
    }

    T* operator->() {
        return &Value;
    }

    const T* operator->() const {
        return &Value;
    }
};
