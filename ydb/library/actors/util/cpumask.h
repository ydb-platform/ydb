#pragma once

#include "defs.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/string/split.h>
#include <util/generic/yexception.h>

using TCpuId = ui32;

// Simple data structure to operate with set of cpus
struct TCpuMask {
    TStackVec<bool, 1024> Cpus;

    // Creates empty mask
    TCpuMask() {}

    // Creates mask with single cpu set
    explicit TCpuMask(TCpuId cpuId) {
        Set(cpuId);
    }

    // Initialize mask from raw boolean array
    template <class T>
    TCpuMask(const T* cpus, TCpuId size) {
        Cpus.reserve(size);
        for (TCpuId i = 0; i != size; ++i) {
            Cpus.emplace_back(bool(cpus[i]));
        }
    }

    // Parse a numerical list of processors. The numbers are separated by commas and may include ranges. For example: 0,5,7,9-11
    explicit TCpuMask(const TString& cpuList) {
        try {
            for (TStringBuf s : StringSplitter(cpuList).Split(',')) {
                TCpuId l, r;
                if (s.find('-') != TString::npos) {
                    StringSplitter(s).Split('-').CollectInto(&l, &r);
                } else {
                    l = r = FromString<TCpuId>(s);
                }
                if (r >= Cpus.size()) {
                    Cpus.resize(r + 1, false);
                }
                for (TCpuId cpu = l; cpu <= r; cpu++) {
                    Cpus[cpu] = true;
                }
            }
        } catch (...) {
            ythrow TWithBackTrace<yexception>() << "Exception occured while parsing cpu list '" << cpuList << "': " << CurrentExceptionMessage();
        }
    }

    // Returns size of underlying vector
    TCpuId Size() const {
        return Cpus.size();
    }

    // Returns number of set bits in mask
    TCpuId CpuCount() const {
        TCpuId result = 0;
        for (bool value : Cpus) {
            result += value;
        }
        return result;
    }

    bool IsEmpty() const {
        for (bool value : Cpus) {
            if (value) {
                return false;
            }
        }
        return true;
    }

    bool IsSet(TCpuId cpu) const {
        return cpu < Cpus.size() && Cpus[cpu];
    }

    void Set(TCpuId cpu) {
        if (cpu >= Cpus.size()) {
            Cpus.resize(cpu + 1, false);
        }
        Cpus[cpu] = true;
    }

    void Reset(TCpuId cpu) {
        if (cpu < Cpus.size()) {
            Cpus[cpu] = false;
        }
    }

    void ResetAll() {
        for (bool &value : Cpus) {
            value = false;
        }
    }

    void RemoveTrailingZeros() {
        while (!Cpus.empty() && !Cpus.back()) {
            Cpus.pop_back();
        }
    }

    explicit operator bool() const {
        return !IsEmpty();
    }

    TCpuMask operator &(const TCpuMask& rhs) const {
        TCpuMask result;
        TCpuId size = Max(Size(), rhs.Size());
        result.Cpus.reserve(size);
        for (TCpuId cpu = 0; cpu < size; cpu++) {
            result.Cpus.emplace_back(IsSet(cpu) && rhs.IsSet(cpu));
        }
        return result;
    }

    TCpuMask operator |(const TCpuMask& rhs) const {
        TCpuMask result;
        TCpuId size = Max(Size(), rhs.Size());
        result.Cpus.reserve(size);
        for (TCpuId cpu = 0; cpu < size; cpu++) {
            result.Cpus.emplace_back(IsSet(cpu) || rhs.IsSet(cpu));
        }
        return result;
    }

    TCpuMask operator -(const TCpuMask& rhs) const {
        TCpuMask result;
        result.Cpus.reserve(Size());
        for (TCpuId cpu = 0; cpu < Size(); cpu++) {
            result.Cpus.emplace_back(IsSet(cpu) && !rhs.IsSet(cpu));
        }
        return result;
    }
};
