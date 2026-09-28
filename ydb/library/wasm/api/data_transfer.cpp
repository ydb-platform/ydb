#include "data_transfer.h"

#include "pointer.h"

#include <library/cpp/yt/error/error.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

namespace {

uintptr_t AllocateOrThrow(IWebAssemblyCompartment* compartment, size_t byteLength)
{
    if (byteLength == 0) {
        return 0;
    }
    const auto offset = compartment->AllocateBytes(byteLength);
    // Soft OOM: malloc returns null → offset 0. Must not PtrFromVM/memcpy into linear memory base.
    THROW_ERROR_EXCEPTION_IF(
        offset == 0,
        "WebAssembly CopyIntoCompartment failed: AllocateBytes returned 0 (soft OOM), size %v",
        byteLength);
    return offset;
}

} // namespace

TCopyGuard::TCopyGuard(IWebAssemblyCompartment* compartment, uintptr_t offset)
    : Compartment_(compartment)
    , CopiedOffset_(offset)
{ }

// Y_WEAK is a no-op on Windows (COFF); omitting the stub avoids duplicate
// symbols with the strong override in library/wasm/engine.
#if defined(__GNUC__)
Y_WEAK TCopyGuard::~TCopyGuard()
{
    if (Compartment_ != nullptr && CopiedOffset_ != 0) {
        try {
            Compartment_->FreeBytes(CopiedOffset_);
        } catch (...) {
            // FreeBytes may throw; never throw from dtor.
        }
    }
}
#endif

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
    const uintptr_t offset = AllocateOrThrow(compartment, data.size());
    if (offset == 0) {
        return {compartment, 0};
    }
    auto* destination = PtrFromVM(compartment, std::bit_cast<char*>(offset), data.size());
    ::memcpy(destination, data.data(), data.size());
    return {compartment, offset};
}

template <>
TCopyGuard CopyIntoCompartment(const std::vector<i64>& data, IWebAssemblyCompartment* compartment)
{
    const size_t byteLength = data.size() * sizeof(i64);
    const uintptr_t offset = AllocateOrThrow(compartment, byteLength);
    if (offset == 0) {
        return {compartment, 0};
    }
    auto* destination = PtrFromVM(compartment, std::bit_cast<i64*>(offset), data.size());
    ::memcpy(destination, data.data(), byteLength);
    return {compartment, offset};
}

template <>
TCopyGuard CopyIntoCompartment(TRange<uintptr_t> data, IWebAssemblyCompartment* compartment)
{
    const size_t byteLength = data.Size() * sizeof(uintptr_t);
    const uintptr_t offset = AllocateOrThrow(compartment, byteLength);
    if (offset == 0) {
        return {compartment, 0};
    }
    auto* destination = PtrFromVM(compartment, std::bit_cast<uintptr_t*>(offset), data.Size());
    ::memcpy(destination, data.Begin(), byteLength);
    return {compartment, offset};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
