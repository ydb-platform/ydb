#pragma once

#include "defs.h"

namespace NActors {
    // wait-free one writer multi reader hash-tree for service mapping purposes
    // on fast updates on same key - could lead to false-negatives, we don't care as such cases are broken from service-map app logic

    template <typename TKey, typename TValue, typename THash, ui64 BaseSize = 256 * 1024, ui64 ExtCount = 4, ui64 ExtBranching = 4>
    class TServiceMap : TNonCopyable {
        struct TEntry : TNonCopyable {
            ui32 CounterIn;
            ui32 CounterOut;
            TKey Key;
            TValue Value;

            TEntry()
                : CounterIn(0)
                , CounterOut(0)
                , Key()
                , Value()
            {
            }
        };

        struct TBranch : TNonCopyable {
            TEntry Entries[ExtCount];
            TBranch* Branches[ExtBranching];

            TBranch() {
                Fill(Branches, Branches + ExtBranching, (TBranch*)nullptr);
            }
        };

        ui32 Counter;
        TBranch* Line[BaseSize];

        bool ScanBranch(TBranch* branch, const TKey& key, ui64 hash, TValue& ret) {
            for (ui32 i = 0; i != ExtCount; ++i) {
                const TEntry& entry = branch->Entries[i];
                const ui32 counterIn = AtomicLoad(&entry.CounterIn);
                if (counterIn != 0 && entry.Key == key) {
                    ret = entry.Value;
                    const ui32 counterOut = AtomicLoad(&entry.CounterOut);
                    if (counterOut == counterIn)
                        return true;
                }
            }

            const ui64 hash0 = hash % ExtBranching;
            if (TBranch* next = AtomicLoad(branch->Branches + hash0))
                return ScanBranch(next, key, hash / ExtBranching, ret);

            return false;
        }

        void ScanZeroOld(TBranch* branch, const TKey& key, ui64 hash, TEntry** zeroEntry, TEntry*& oldEntry) {
            for (ui32 i = 0; i != ExtCount; ++i) {
                TEntry& entry = branch->Entries[i];
                if (entry.CounterIn == 0) {
                    if (zeroEntry && !*zeroEntry) {
                        *zeroEntry = &entry;
                        if (oldEntry != nullptr)
                            return;
                    }
                } else {
                    if (entry.Key == key) {
                        oldEntry = &entry;
                        if (!zeroEntry || *zeroEntry)
                            return;
                    }
                }
            }

            const ui64 hash0 = hash % ExtBranching;
            if (TBranch* next = branch->Branches[hash0]) {
                ScanZeroOld(next, key, hash / ExtBranching, zeroEntry, oldEntry);
            } else { // found tail, if zeroEntry requested, but not yet found - insert one
                if (zeroEntry && !*zeroEntry) {
                    TBranch* next = new TBranch();
                    *zeroEntry = next->Entries;
                    AtomicStore(branch->Branches + hash0, next);
                }
            }
        }

    public:
        TServiceMap()
            : Counter(0)
        {
            Fill(Line, Line + BaseSize, (TBranch*)nullptr);
        }

        ~TServiceMap() {
            for (ui64 i = 0; i < BaseSize; ++i) {
                delete Line[i];
            }
        }

        TValue Find(const TKey& key) {
            THash hashOp;
            const ui64 hash = hashOp(key);
            const ui64 hash0 = hash % BaseSize;

            if (TBranch* branch = AtomicLoad(Line + hash0)) {
                TValue ret;
                if (ScanBranch(branch, key, hash / BaseSize, ret))
                    return ret;
            }

            return TValue();
        }

        // returns true on update, false on insert
        TValue Update(const TKey& key, const TValue& value) {
            THash hashOp;
            const ui64 hash = hashOp(key);
            const ui64 hash0 = hash % BaseSize;

            TEntry* zeroEntry = nullptr;
            TEntry* oldEntry = nullptr;

            if (TBranch* branch = Line[hash0]) {
                ScanZeroOld(branch, key, hash / BaseSize, &zeroEntry, oldEntry);
            } else {
                TBranch* next = new TBranch();
                zeroEntry = next->Entries;
                AtomicStore(Line + hash0, next);
            }

            // now we got both entries, first - push new one
            const ui32 counter = AtomicUi32Increment(&Counter);
            AtomicStore(&zeroEntry->CounterOut, counter);
            zeroEntry->Key = key;
            zeroEntry->Value = value;
            AtomicStore(&zeroEntry->CounterIn, counter);

            if (oldEntry != nullptr) {
                const TValue ret = oldEntry->Value;
                AtomicStore<ui32>(&oldEntry->CounterOut, 0);
                AtomicStore<ui32>(&oldEntry->CounterIn, 0);
                return ret;
            } else {
                return TValue();
            }
        }

        bool Erase(const TKey& key) {
            THash hashOp;
            const ui64 hash = hashOp(key);
            const ui64 hash0 = hash % BaseSize;

            TEntry* oldEntry = 0;

            if (TBranch* branch = Line[hash0]) {
                ScanZeroOld(branch, key, hash / BaseSize, 0, oldEntry);
            }

            if (oldEntry != 0) {
                AtomicStore<ui32>(&oldEntry->CounterOut, 0);
                AtomicStore<ui32>(&oldEntry->CounterIn, 0);
                return true;
            } else {
                return false;
            }
        }
    };
}
