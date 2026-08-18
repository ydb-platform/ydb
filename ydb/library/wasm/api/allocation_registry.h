#pragma once

#include "compartment.h"

#include <util/system/types.h>

#include <memory>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

//! Process-wide map of host pointers into WASM linear memory to their
//! (compartment, offset, generation). Clients Register on AllocateBytes;
//! TryFree calls FreeBytes when the owner drops the last reference.
//!
//! A TStringValue built over linear memory keeps its refcount header there, so
//! even destroying such a value reads guest memory. The registry therefore holds
//! the compartment owner alive while any allocation of its generation is live:
//! ReleaseOwner (the query scope is gone) only drops that reference once the
//! last value has been freed.
class TWasmAllocationRegistry {
public:
    static TWasmAllocationRegistry& Instance();

    //! |owner| keeps the compartment alive; pass the query compartment handle.
    //! Only the first registration of a generation stores it.
    void Register(
        void* hostPtr,
        IWebAssemblyCompartment* compartment,
        uintptr_t offset,
        size_t size,
        ui64 generation,
        std::shared_ptr<void> owner = nullptr);

    //! If |hostPtr| is registered, FreeBytes and erase; returns true.
    //! Unknown pointer → false (caller may use UdfFreeWithSize).
    //! For a generation whose owner is released, erase without FreeBytes (the
    //! whole compartment is about to go) and drop the owner on the last one.
    bool TryFree(void* hostPtr);

    //! The query scope no longer needs the compartment. Live allocations keep it
    //! alive until their last TryFree; with none left the owner is dropped here.
    void ReleaseOwner(ui64 generation);

    //! Compartment is being destroyed: drop any bookkeeping left for it. Records
    //! are orphaned rather than freed, so a stray late UnRef does not call
    //! UdfFreeWithSize on a WASM address.
    void ForgetGeneration(ui64 generation);

    //! Number of live (not yet TryFree'd) registrations for |generation|.
    //! Used by tests to assert mid-lifetime UnRef frees.
    size_t CountGeneration(ui64 generation) const;

private:
    TWasmAllocationRegistry() = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
