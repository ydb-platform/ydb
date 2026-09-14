#pragma once

#include <util/system/types.h>
#include <util/system/compiler.h>

namespace NShop {

using TMachineIdx = ui8;

///////////////////////////////////////////////////////////////////////////////

struct TMachineMask {
    using TIdx = TMachineIdx;
    using TMask = ui64;
    constexpr static TMask One = TMask(1);

    TMask Mask;

    TMachineMask(TMask mask = 0)
        : Mask(mask)
    {}

    operator TMask() const
    {
        return Mask;
    }

    void Set(TIdx idx)
    {
        Mask = Mask | (One << idx);
    }

    void SetAll(TMask mask)
    {
        Mask = Mask | mask;
    }

    void Reset(TIdx idx)
    {
        Mask = Mask & ~(One << idx) ;
    }

    void ResetAll(TMask mask)
    {
        Mask = Mask & ~mask;
    }

    bool Get(TIdx idx) const
    {
        return Mask & (One << idx);
    }

    bool IsZero() const
    {
        return Mask == 0;
    }

    TString ToString(TIdx size) const
    {
        TStringStream ss;
        for (TIdx idx = 0; idx < size; idx++) {
            if (idx % 8 == 0 && idx) {
                ss << '-'; // bytes separator
            }
            ss << (Get(idx)? '1': '0');
        }
        return ss.Str();
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TShopState {
    TMachineMask Warm; // machineIdx -> (1=warm | 0=frozen)
    TMachineIdx MachineCount;
    static constexpr TMachineIdx MaxMachines = 64; // we have only 64 bits, it must be enough for now

    TShopState(TMachineIdx machineCount = 1)
        : Warm((TMachineMask::One << machineCount) - 1) // all machines are warm
        , MachineCount(machineCount)
    {}

    void Freeze(TMachineIdx idx)
    {
        Y_ASSERT(idx < MachineCount);
        Warm.Reset(idx);
    }

    void Unfreeze(TMachineIdx idx)
    {
        Y_ASSERT(idx < MachineCount);
        Warm.Set(idx);
    }

    void AddMachine()
    {
        Warm.Set(MachineCount); // New machine is warm at start
        MachineCount++;
    }

    bool AllFrozen() const
    {
        return Warm.Mask == 0;
    }

    bool HasFrozen() const
    {
        if (Y_LIKELY(MachineCount < 8*sizeof(TMachineMask::TMask))) {
            return Warm.Mask != ((TMachineMask::One << MachineCount) - 1);
        } else {
            return Warm.Mask != TMachineMask::TMask(-1);
        }
    }

    void Print(IOutputStream& out) const
    {
        out << "MachineCount = " << int(MachineCount) << Endl
            << "Warm = [" << Warm.ToString(MachineCount) << "]" << Endl;
    }
};

}
