#pragma once

#include "bytecode.h"
#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/noncopyable.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

struct IWebAssemblyCompartment
    : public TNonCopyable
{
    virtual ~IWebAssemblyCompartment() = default;

    //! Adds new module in WebAssembly bytecode format.
    virtual void AddModule(TRef bytecode, TStringBuf name = "") = 0;
    //! Adds new module in WebAssembly WAST format.
    virtual void AddModule(TStringBuf wast, TStringBuf name = "") = 0;

    //! Adds precompiled module. ObjectCode must be present.
    virtual void AddPrecompiledModule(const TModuleBytecode& bytecode, TStringBuf name = "") = 0;

    //! Adds sdk.
    virtual void AddSdk(const TModuleBytecode& bytecode) = 0;

    //! Strips compartment internal data structures.
    //! Stripped compartments can not be used for linking, but are faster to clone.
    virtual void Strip() = 0;

    //! Returns an opaque pointer to the function with name |name|.
    virtual void* GetFunction(const std::string& functionName) = 0;
    //! Returns an opaque pointer to the function with index |index|.
    virtual void* GetFunction(size_t index) = 0;
    //! Returns an opaque pointer to the execution context.
    virtual void* GetContext() = 0;

    //! Converts compartment offset to host pointer and checks boundaries.
    virtual void* GetHostPointer(uintptr_t offset, size_t length) = 0;
    //! Converts host pointer to compartment offset.
    //! Throws if the pointer is outside linear memory.
    virtual uintptr_t GetCompartmentOffset(void* hostAddress) = 0;
    //! Returns the current linear memory size in bytes.
    virtual size_t GetLinearMemorySize() const = 0;

    //! Clones compartment.
    //! NB: Cloning is slow and should not be called too often.
    virtual std::unique_ptr<IWebAssemblyCompartment> Clone() const = 0;

    //! Allocates |length| bytes in compartment linear memory via wasm export
    //! "malloc" with signature (i64)->(i64) on the runtime library instance
    //! (installed by AddSdk, debug name typically "env").
    //!
    //! Outcomes:
    //! - Success: non-zero compartment offset (host pointer via GetHostPointer).
    //! - Soft OOM: returns 0 when malloc itself returns null; no throw.
    //! - Hard failure: throws NYT::TErrorException if there is no runtime
    //!   library, no matching "malloc" export, or invoke traps (e.g.
    //!   memory.grow / OOB). WAVM::Runtime::Exception* is not propagated.
    virtual uintptr_t AllocateBytes(size_t length) = 0;
    //! Deallocates an offset previously returned by AllocateBytes via wasm
    //! "free" (i64)->(). Throws NYT::TErrorException on missing runtime /
    //! export or trap (same conversion rules as AllocateBytes).
    virtual void FreeBytes(uintptr_t offset) = 0;

    //! Sets execution timeout for sandboxed code.
    virtual void SetTimeout(std::optional<TDuration> timeout) = 0;
    virtual void SetDeadline(std::optional<TInstant> deadline) = 0;
    virtual void StartDeadlineTimer() = 0;
};

std::unique_ptr<IWebAssemblyCompartment> CreateImageFromSdk(const TModuleBytecode& bytecode);

std::unique_ptr<IWebAssemblyCompartment> CreateEmptyImage();
std::unique_ptr<IWebAssemblyCompartment> CreateMinimalRuntimeImage();
std::unique_ptr<IWebAssemblyCompartment> CreateStandardRuntimeImage();
std::unique_ptr<IWebAssemblyCompartment> CreateQueryLanguageImage();

////////////////////////////////////////////////////////////////////////////////

IWebAssemblyCompartment* GetCurrentCompartment();
void SetCurrentCompartment(IWebAssemblyCompartment* compartment);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
