#include "allocation_registry.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/env.h>
#include <util/system/spinlock.h>

namespace NYdb::NWasm {

namespace {

bool IsWasmStringDebugEnabled() {
    static const bool enabled = [] {
        const TString v = GetEnv("YDB_WASM_STRING_DEBUG");
        return v == "1" || v == "true" || v == "yes";
    }();
    return enabled;
}

void WasmStringDebug(const TString& message) {
    if (IsWasmStringDebugEnabled()) {
        Cerr << "[WasmString] " << message << Endl;
    }
}

struct TAllocationRecord {
    IWebAssemblyCompartment* Compartment = nullptr;
    uintptr_t Offset = 0;
    size_t Size = 0;
    ui64 Generation = 0;
};

struct TGenerationRecord {
    std::shared_ptr<void> Owner;
    size_t LiveCount = 0;
    //! Query scope is gone: the only thing left to do is outlive the values.
    bool OwnerReleased = false;
};

class TWasmAllocationRegistryImpl {
public:
    static TWasmAllocationRegistryImpl& Instance() {
        static TWasmAllocationRegistryImpl registry;
        return registry;
    }

    void Register(
        void* hostPtr,
        IWebAssemblyCompartment* compartment,
        uintptr_t offset,
        size_t size,
        ui64 generation,
        std::shared_ptr<void> owner)
    {
        if (!hostPtr || !compartment || offset == 0) {
            return;
        }
        with_lock (Lock_) {
            WasmStringDebug(TStringBuilder()
                << "Register: host=" << hostPtr << " offset=" << offset
                << " size=" << size << " generation=" << generation);
            OrphanedHosts_.erase(hostPtr);
            Allocations_[hostPtr] = TAllocationRecord{
                .Compartment = compartment,
                .Offset = offset,
                .Size = size,
                .Generation = generation,
            };
            if (generation != 0) {
                auto& generationRecord = Generations_[generation];
                ++generationRecord.LiveCount;
                if (owner && !generationRecord.Owner) {
                    generationRecord.Owner = std::move(owner);
                }
            }
        }
    }

    bool TryFree(void* hostPtr) {
        if (!hostPtr) {
            return false;
        }

        TAllocationRecord record;
        bool doFree = false;
        // Dropped after the lock: releasing the last reference destroys the
        // compartment, whose destructor calls back into the registry.
        std::shared_ptr<void> releasedOwner;
        {
            with_lock (Lock_) {
                // Compartment already torn down for this ptr — swallow free.
                if (OrphanedHosts_.erase(hostPtr)) {
                    WasmStringDebug("TryFree: destination=orphaned (no FreeBytes)");
                    return true;
                }
                auto it = Allocations_.find(hostPtr);
                if (it == Allocations_.end()) {
                    // Ordinary host string: the caller frees it itself.
                    return false;
                }
                record = it->second;
                Allocations_.erase(it);

                auto generationIt = Generations_.find(record.Generation);
                if (generationIt != Generations_.end()) {
                    auto& generationRecord = generationIt->second;
                    if (generationRecord.LiveCount > 0) {
                        --generationRecord.LiveCount;
                    }
                    // No point returning bytes to a guest allocator that is
                    // about to be destroyed along with its linear memory.
                    doFree = !generationRecord.OwnerReleased;
                    // The keep-alive is only needed while something points into
                    // linear memory; the query scope holds its own reference.
                    if (generationRecord.LiveCount == 0) {
                        releasedOwner = std::move(generationRecord.Owner);
                        Generations_.erase(generationIt);
                    }
                } else {
                    doFree = true;
                }
            }
        }

        if (!doFree) {
            WasmStringDebug(TStringBuilder()
                << "TryFree: destination=swallowed (owner released)"
                << " offset=" << record.Offset
                << " generation=" << record.Generation);
        } else {
            WasmStringDebug(TStringBuilder()
                << "TryFree: destination=FreeBytes"
                << " offset=" << record.Offset
                << " size=" << record.Size
                << " generation=" << record.Generation);
            try {
                record.Compartment->FreeBytes(record.Offset);
            } catch (...) {
                // Never propagate from free paths used by UnRef/dtors.
                WasmStringDebug(TStringBuilder()
                    << "TryFree: FreeBytes failed"
                    << " offset=" << record.Offset
                    << " size=" << record.Size
                    << " generation=" << record.Generation);
            }
        }
        return true;
    }

    void ReleaseOwner(ui64 generation) {
        if (generation == 0) {
            return;
        }
        // Dropped after the lock: see ForgetGeneration.
        std::shared_ptr<void> releasedOwner;
        with_lock (Lock_) {
            auto it = Generations_.find(generation);
            if (it == Generations_.end()) {
                return;
            }
            if (it->second.LiveCount == 0) {
                releasedOwner = std::move(it->second.Owner);
                Generations_.erase(it);
            } else {
                it->second.OwnerReleased = true;
                WasmStringDebug(TStringBuilder()
                    << "ReleaseOwner: generation=" << generation
                    << " outliving values=" << it->second.LiveCount);
            }
        }
    }

    void ForgetGeneration(ui64 generation) {
        if (generation == 0) {
            return;
        }
        // Dropping a keep-alive destroys a compartment handle, which forgets its
        // own generation: that must not happen under the lock.
        std::shared_ptr<void> releasedOwner;
        with_lock (Lock_) {
            TVector<void*> hostPtrs;
            for (const auto& [hostPtr, record] : Allocations_) {
                if (record.Generation == generation) {
                    hostPtrs.push_back(hostPtr);
                }
            }
            for (void* hostPtr : hostPtrs) {
                Allocations_.erase(hostPtr);
                OrphanedHosts_.insert(hostPtr);
            }
            auto it = Generations_.find(generation);
            if (it != Generations_.end()) {
                releasedOwner = std::move(it->second.Owner);
                Generations_.erase(it);
            }
            if (!hostPtrs.empty()) {
                WasmStringDebug(TStringBuilder()
                    << "ForgetGeneration: generation=" << generation
                    << " orphaned=" << hostPtrs.size());
            }
        }
    }

    size_t CountGeneration(ui64 generation) const {
        with_lock (Lock_) {
            size_t count = 0;
            for (const auto& [_, record] : Allocations_) {
                if (record.Generation == generation) {
                    ++count;
                }
            }
            return count;
        }
    }

private:
    mutable TAdaptiveLock Lock_;
    THashMap<void*, TAllocationRecord> Allocations_;
    THashMap<ui64, TGenerationRecord> Generations_;
    //! Host pointers left over when a compartment was destroyed. TryFree returns
    //! true without FreeBytes so UnRef does not call UdfFreeWithSize on a WASM
    //! address.
    THashSet<void*> OrphanedHosts_;
};

} // namespace

TWasmAllocationRegistry& TWasmAllocationRegistry::Instance() {
    static TWasmAllocationRegistry facade;
    return facade;
}

void TWasmAllocationRegistry::Register(
    void* hostPtr,
    IWebAssemblyCompartment* compartment,
    uintptr_t offset,
    size_t size,
    ui64 generation,
    std::shared_ptr<void> owner)
{
    TWasmAllocationRegistryImpl::Instance().Register(
        hostPtr, compartment, offset, size, generation, std::move(owner));
}

bool TWasmAllocationRegistry::TryFree(void* hostPtr) {
    return TWasmAllocationRegistryImpl::Instance().TryFree(hostPtr);
}

void TWasmAllocationRegistry::ReleaseOwner(ui64 generation) {
    TWasmAllocationRegistryImpl::Instance().ReleaseOwner(generation);
}

void TWasmAllocationRegistry::ForgetGeneration(ui64 generation) {
    TWasmAllocationRegistryImpl::Instance().ForgetGeneration(generation);
}

size_t TWasmAllocationRegistry::CountGeneration(ui64 generation) const {
    return TWasmAllocationRegistryImpl::Instance().CountGeneration(generation);
}

} // namespace NYdb::NWasm
