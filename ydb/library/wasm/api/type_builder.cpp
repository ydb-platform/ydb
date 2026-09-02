#include "type_builder.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

#define XX(signature, type) \
    template <> \
    EWebAssemblyValueType InferType< signature >() \
    { \
        return type; \
    }

    XX(bool, EWebAssemblyValueType::Int32)
    XX(char, EWebAssemblyValueType::Int32)
    XX(int, EWebAssemblyValueType::Int32)
    XX(unsigned int, EWebAssemblyValueType::Int32)
    XX(unsigned char, EWebAssemblyValueType::Int32)

    // LLP64 (Windows): long is 32-bit; LP64 (Linux/macOS): long is 64-bit.
#if defined(_win_)
    XX(long, EWebAssemblyValueType::Int32)
    XX(unsigned long, EWebAssemblyValueType::Int32)
    XX(long long, EWebAssemblyValueType::Int64)
    XX(unsigned long long, EWebAssemblyValueType::Int64)
#else
    XX(long, EWebAssemblyValueType::Int64)
    XX(unsigned long, EWebAssemblyValueType::Int64)
#   if defined(__APPLE__)
        XX(unsigned long long, EWebAssemblyValueType::Int64)
#   endif
#endif

    XX(float, EWebAssemblyValueType::Float32)
    XX(double, EWebAssemblyValueType::Float64)

    XX(char*, EWebAssemblyValueType::UintPtr)
    XX(const char*, EWebAssemblyValueType::UintPtr)
    XX(long*, EWebAssemblyValueType::UintPtr)
    XX(char**, EWebAssemblyValueType::UintPtr)
    XX(const uint8_t*, EWebAssemblyValueType::UintPtr)
    XX(const uint8_t**, EWebAssemblyValueType::UintPtr)
    XX(int*, EWebAssemblyValueType::UintPtr)
    XX(unsigned long*, EWebAssemblyValueType::UintPtr)
#if defined(_win_)
    XX(long long*, EWebAssemblyValueType::UintPtr)
    XX(unsigned long long*, EWebAssemblyValueType::UintPtr)
#endif
    XX(void*, EWebAssemblyValueType::UintPtr)
    XX(void**, EWebAssemblyValueType::UintPtr)
    XX(void* const*, EWebAssemblyValueType::UintPtr)
    XX(const void*, EWebAssemblyValueType::UintPtr)

    XX(void, EWebAssemblyValueType::Void)

#undef XX

////////////////////////////////////////////////////////////////////////////////

// Y_WEAK is a no-op on Windows (COFF); omitting the stub avoids duplicate
// symbols with the strong override in library/wasm/engine.
#if defined(__GNUC__)
Y_WEAK TWebAssemblyRuntimeType GetTypeId(
    bool /*intrinsic*/,
    EWebAssemblyValueType /*returnType*/,
    TRange<EWebAssemblyValueType> /*argumentTypes*/)
{
    YT_UNIMPLEMENTED();
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
