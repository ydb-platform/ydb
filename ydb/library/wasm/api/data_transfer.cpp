#include "data_transfer.h"

#include "pointer.h"

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

TCopyGuard::TCopyGuard(IWebAssemblyCompartment* compartment, uintptr_t offset)
    : Compartment_(compartment)
    , CopiedOffset_(offset)
{ }

Y_WEAK TCopyGuard::~TCopyGuard()
{
    // NB(dtorilov): This is a stub, the correct implementation resides in ydb/library/wasm/engine/data_transfer.cpp.
    if (Compartment_ != nullptr && CopiedOffset_ != 0) {
        Compartment_->FreeBytes(CopiedOffset_);
    }
}

TCopyGuard::TCopyGuard(TCopyGuard&& other) noexcept
{
    std::swap(Compartment_, other.Compartment_);
    std::swap(CopiedOffset_, other.CopiedOffset_);
}

TCopyGuard& TCopyGuard::operator=(TCopyGuard&& other) noexcept
{
    std::swap(Compartment_, other.Compartment_);
    std::swap(CopiedOffset_, other.CopiedOffset_);
    return *this;
}

uintptr_t TCopyGuard::GetCopiedOffset() const
{
    return CopiedOffset_;
}

////////////////////////////////////////////////////////////////////////////////

template <>
TCopyGuard CopyIntoCompartment(TStringBuf data, IWebAssemblyCompartment* compartment)
{
    uintptr_t offset = compartment->AllocateBytes(data.size());
    auto* destination = PtrFromVM(compartment, std::bit_cast<char*>(offset), data.size());
    ::memcpy(destination, data.data(), data.size());
    return {compartment, offset};
}

template <>
TCopyGuard CopyIntoCompartment(const std::vector<i64>& data, IWebAssemblyCompartment* compartment)
{
    i64 byteLength = std::ssize(data) * sizeof(i64);
    uintptr_t offset = compartment->AllocateBytes(byteLength);
    auto* destination = PtrFromVM(compartment, std::bit_cast<i64*>(offset), byteLength);
    ::memcpy(destination, data.data(), byteLength);
    return {compartment, offset};
}

template <>
TCopyGuard CopyIntoCompartment(TRange<uintptr_t> data, IWebAssemblyCompartment* compartment)
{
    i64 byteLength = std::ssize(data) * sizeof(uintptr_t);
    uintptr_t offset = compartment->AllocateBytes(byteLength);
    auto* destination = PtrFromVM(compartment, std::bit_cast<uintptr_t*>(offset), std::ssize(data));
    ::memcpy(destination, data.begin(), byteLength);
    return {compartment, offset};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
