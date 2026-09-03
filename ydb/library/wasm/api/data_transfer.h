#pragma once

#include "compartment.h"

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
