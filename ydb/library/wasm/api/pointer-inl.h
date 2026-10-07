#ifndef POINTER_INL_H_
#error "Direct inclusion of this file is not allowed, include pointer.h"
// For the sake of sane code completion.
#include "pointer.h"
#endif

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/yexception.h>
#include <util/system/compiler.h>

#include <limits>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

Y_FORCE_INLINE size_t CheckedElementByteLength(size_t elementSize, size_t length)
{
    if (Y_UNLIKELY(length != 0 && elementSize > std::numeric_limits<size_t>::max() / length)) {
        ythrow yexception() << "WebAssembly pointer length overflow";
    }
    return elementSize * length;
}

} // namespace NDetail

template <typename T>
T* PtrFromVM(IWebAssemblyCompartment* compartment, T* data, size_t length)
{
    if (compartment) {
        const size_t byteLength = NDetail::CheckedElementByteLength(sizeof(T), length);
        return std::bit_cast<T*>(std::bit_cast<uintptr_t>(
            compartment->GetHostPointer(std::bit_cast<uintptr_t>(data), byteLength)));
    }

    return data;
}

template <typename T>
T* PtrToVM(IWebAssemblyCompartment* compartment, T* data, size_t length)
{
    if (compartment) {
        const size_t byteLength = NDetail::CheckedElementByteLength(sizeof(T), length);
        const auto offset = compartment->GetCompartmentOffset(
            std::bit_cast<void*>(std::bit_cast<uintptr_t>(data)));
        // Validate that [offset, offset + byteLength) lies within linear memory.
        compartment->GetHostPointer(offset, byteLength);
        return std::bit_cast<T*>(offset);
    }

    return data;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE T* ConvertPointer(
    T* offset,
    EAddressSpace sourceAddressSpace,
    EAddressSpace destinationAddressSpace,
    size_t length)
{
    auto* compartment = GetCurrentCompartment();
    if (!compartment) {
        return offset;
    }

    if (sourceAddressSpace == EAddressSpace::WebAssembly && destinationAddressSpace == EAddressSpace::Host) {
        return PtrFromVM(compartment, offset, length);
    }

    if (sourceAddressSpace == EAddressSpace::Host && destinationAddressSpace == EAddressSpace::WebAssembly) {
        return PtrToVM(compartment, offset, length);
    }

    if (sourceAddressSpace == EAddressSpace::Host && destinationAddressSpace == EAddressSpace::Host) {
        return offset;
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
