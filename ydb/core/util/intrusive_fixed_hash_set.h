#pragma once
#include "defs.h"

#include <util/generic/vector.h>

namespace NKikimr {

//
// Intusive fixed hash set template
// WARNING: It does not take the ownership of the items
// T - item type
// N - next item pointer in case of collisions
// ui64 H() - hash function
// bool E(const T&) - equals function (returns true iff *this == T)
//

template <class T, T* T::*N, ui64 (*H)(const T&), bool (*E)(const T&, const T&)>
class TIntrusiveFixedHashSet {
    TVector<T*> Table;
public:
    TIntrusiveFixedHashSet(ui64 size) {
        Table.resize(size, nullptr);
    }

    void Push(T *value) {
        Y_ABORT_UNLESS(value->*N == nullptr);
        ui64 hash = H(*value);
        T*& entry = Table[hash % Table.size()];
        value->*N = entry;
        entry = value;
    }

    T* Find(T *value) const {
        ui64 hash = H(*value);
        for (T* entry = Table[hash % Table.size()]; entry; entry = entry->*N) {
            if (E(*entry, *value)) {
                return entry;
            }
        }
        return nullptr;
    }

    void Clear() {
        memset(&Table[0], 0, sizeof(T*) * Table.size());
    }
};

} // NKikimr
