#pragma once
#include "defs.h"

namespace NKikimr {
//
// Intusive stack template
// WARNING: It does not take the ownership of the items
// T - item type
// P - next item pointer
//

template <class T, T* T::*P>
class TIntrusiveStack {
    T *Root = nullptr;
    ui64 Count = 0;
public:
    TIntrusiveStack() {}

    T* Pop() {
        if (Root) {
            T *res = Root;
            Root = Root->*P;
            res->*P = nullptr;
            Count--;
            return res;
        } else {
            return nullptr;
        }
    }

    void Push(T *value) {
        Y_ABORT_UNLESS(value);
        Y_ABORT_UNLESS(value->*P == nullptr);
        value->*P = Root;
        Root = value;
        Count++;
    }

    ui64 Size() const {
        return Count;
    }
};

} // NKikimr
