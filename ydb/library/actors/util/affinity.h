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

    void Current(size_t pid = 0);
    void Set(size_t pid = 0) const;
    bool Empty() const;

    operator TCpuMask() const;
};

// Scoped affinity setter
class TAffinityGuard : TNonCopyable {
    bool Stacked = false;
    TAffinity OldAffinity;
    size_t PId;

public:
    TAffinityGuard(const TAffinity* affinity, ui64 pid = 0)
        : PId(pid)
    {
        SetAffinity(affinity);
    }

    TAffinityGuard(ui64 pid = 0)
        : PId(pid)
    {
    }

    bool SetAffinity(const TAffinity* affinity) {
        if (affinity && !affinity->Empty()) {
            if (!Stacked) {
                OldAffinity.Current(PId);
            }
            affinity->Set(PId);
            Stacked = true;
            return true;
        }
        return false;
    }

    bool SetCpuMask(const TCpuMask &cpuMask) {
        TAffinity affinity(cpuMask);
        return SetAffinity(&affinity);
    }

    ~TAffinityGuard() {
        Release();
    }

    void Release() {
        if (Stacked) {
            OldAffinity.Set(PId);
            Stacked = false;
        }
    }
};
