#pragma once

#include <stdint.h>
#include <stddef.h>

struct TExpressionContext;
struct TUnversionedValue;
struct TFunctionContext;

namespace NYdb::NUdfStore::NAbi {

////////////////////////////////////////////////////////////////////////////////

enum class EValueType : uint8_t
{
    Min = 0x00,
    TheBottom = 0x01,
    Null = 0x02,
    Int64 = 0x03,
    Uint64 = 0x04,
    Double = 0x05,
    Boolean = 0x06,
    String = 0x10,
    Any = 0x11,
    Composite = 0x12,
    Max = 0xef
};

union TUnversionedValueData
{
    int64_t Int64;
    uint64_t Uint64;
    double Double;
    int8_t Boolean;
    char* String;
};

enum class EValueFlags : uint8_t
{
    None = 0x00,
    Aggregate = 0x01
};

//! Fully initializes #value but leaves it in invalid state.
void ClearValue(TUnversionedValue* value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NUdfStore::NAbi

struct TUnversionedValue
{
    uint16_t Id;
    NYdb::NUdfStore::NAbi::EValueType Type;
    NYdb::NUdfStore::NAbi::EValueFlags Flags;
    uint32_t Length;
    NYdb::NUdfStore::NAbi::TUnversionedValueData Data;
};

//! Allocates #size bytes within #context.
extern "C" char* AllocateBytes(TExpressionContext* context, size_t size);

//! Throws exception with #error message.
extern "C" void ThrowException(const char* error);

#define UDF_CPP_ABI_INL_H_
#include "udf_cpp_abi-inl.h"
#undef UDF_CPP_ABI_INL_H_
