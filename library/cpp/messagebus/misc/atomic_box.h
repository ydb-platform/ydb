#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

// TAtomic with human interface
template <typename T>
class TAtomicBox {
private:
    union {
        TAtomic Value;
        // when T is enum, it is convenient to inspect its content in gdb
        T ValueForDebugger;
    };

    static_assert(sizeof(T) <= sizeof(TAtomic), "expect sizeof(T) <= sizeof(TAtomic)");

public:
    TAtomicBox(T value = T())
        : Value(value)
    {
    }

    void Set(T value) {
        AtomicSet(Value, (TAtomic)value);
    }

    T Get() const {
        return (T)AtomicGet(Value);
    }

    bool CompareAndSet(T expected, T set) {
        return AtomicCas(&Value, (TAtomicBase)set, (TAtomicBase)expected);
    }
};
