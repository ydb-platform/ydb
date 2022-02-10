#pragma once

#include "defs.h"
#include "cpumask.h"

// Platform-specific class to set or get thread affinity
class TAffinity: public TThrRefBase, TNonCopyable {
    class TImpl;
    THolder<TImpl> Impl;

public:
    TAffinity();
    TAffinity(const ui8* cpus, ui32 size);
    explicit TAffinity(const TCpuMask& mask);
    ~TAffinity();

    void Current();
    void Set() const;
    bool Empty() const;

    operator TCpuMask() const;
};

// Scoped affinity setter
class TAffinityGuard : TNonCopyable {
    bool Stacked;
    TAffinity OldAffinity;

public:
    TAffinityGuard(const TAffinity* affinity) {
        Stacked = false; 
        if (affinity && !affinity->Empty()) {
            OldAffinity.Current();
            affinity->Set();
            Stacked = true;
        }
    }

    ~TAffinityGuard() {
        Release();
    }

    void Release() {
        if (Stacked) {
            OldAffinity.Set();
            Stacked = false;
        }
    }
};
