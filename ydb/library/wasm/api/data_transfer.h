#pragma once

#include "compartment.h"

#include <util/generic/yexception.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

class TCopyGuard
    : public TNonCopyable
{
public:
    TCopyGuard() = default;
    TCopyGuard(IWebAssemblyCompartment* compartment, uintptr_t offset);

    Y_WEAK ~TCopyGuard();
    TCopyGuard(TCopyGuard&& other) noexcept;
    TCopyGuard& operator=(TCopyGuard&& other) noexcept;

    uintptr_t GetCopiedOffset() const;

protected:
    IWebAssemblyCompartment* Compartment_ = nullptr;
    uintptr_t CopiedOffset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Writable view into freshly allocated guest linear memory.
//! Host writes via HostData(); guest sees Offset() after the call.
class TGuestBuffer
    : public TNonCopyable
{
public:
    TGuestBuffer() = default;

    static TGuestBuffer Allocate(IWebAssemblyCompartment* compartment, size_t size);

    Y_WEAK ~TGuestBuffer();
    TGuestBuffer(TGuestBuffer&& other) noexcept;
    TGuestBuffer& operator=(TGuestBuffer&& other) noexcept;

    uintptr_t Offset() const;
    size_t Size() const;
    char* HostData() const;

    //! Relinquish ownership; caller must FreeBytes (or transfer to another owner).
    uintptr_t Release() noexcept;

private:
    TGuestBuffer(IWebAssemblyCompartment* compartment, uintptr_t offset, size_t size, char* hostData);

    IWebAssemblyCompartment* Compartment_ = nullptr;
    uintptr_t Offset_ = 0;
    size_t Size_ = 0;
    char* HostData_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgument>
TCopyGuard CopyIntoCompartment(TArgument data, IWebAssemblyCompartment* compartment);

template <>
TCopyGuard CopyIntoCompartment(TStringBuf data, IWebAssemblyCompartment* compartment);

template <>
TCopyGuard CopyIntoCompartment(const std::vector<i64>& data, IWebAssemblyCompartment* compartment);

template <>
TCopyGuard CopyIntoCompartment(TRange<uintptr_t> data, IWebAssemblyCompartment* compartment);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
