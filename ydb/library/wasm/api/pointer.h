#pragma once

#include "compartment.h"

#include <library/cpp/yt/misc/enum.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAddressSpace,
    (Host)
    (WebAssembly)
);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE T* PtrFromVM(IWebAssemblyCompartment* compartment, T* data, size_t length = 1);

template <typename T>
Y_FORCE_INLINE T* PtrToVM(IWebAssemblyCompartment* compartment, T* data, size_t length = 1);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE T* ConvertPointer(
    T* offset,
    EAddressSpace sourceAddressSpace,
    EAddressSpace destinationAddressSpace,
    size_t length = 1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm

#define POINTER_INL_H_
#include "pointer-inl.h"
#undef POINTER_INL_H_
