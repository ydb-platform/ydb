#pragma once

#include "udf_type_size_check.h"

#include <util/system/types.h>

namespace NYql::NUdf {

class IRegistrator;

using TBackTraceCallback = void (*)();
using TRegisterFunctionPtr = void (*)(IRegistrator& registrator, ui32 flags);
using TSetBackTraceCallbackPtr = void (*)(TBackTraceCallback callback);

// Descriptors registered with the host must have static storage duration.
// Extend at the end; StructSize lets the host check which fields are available.
struct TYqlUdfStaticLibrary {
    ui32 StructSize = 0;
    ui32 AbiVersion = 0;
    TRegisterFunctionPtr Register = nullptr;
    // May be null when the module uses the host's backtrace handling.
    TSetBackTraceCallbackPtr SetBackTraceCallback = nullptr;
};

UDF_ASSERT_TYPE_SIZE(TYqlUdfStaticLibrary, 24);

} // namespace NYql::NUdf
