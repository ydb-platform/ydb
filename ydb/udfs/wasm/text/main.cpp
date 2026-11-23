#include <ydb/services/udf_store/wasm/abi/bridge.h>
#include <ydb/services/udf_store/wasm/abi/bridge_abi.h>
#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <stdint.h>

using namespace NYdb::NUdfStore::NAbi;

namespace {

bool IsLetter(unsigned char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

bool IsDigit(unsigned char c) {
    return c >= '0' && c <= '9';
}

bool IsUpper(unsigned char c) {
    return c >= 'A' && c <= 'Z';
}

//! Pin `handle` into compartment LM (once per distinct value) and walk bytes.
template <typename TPred>
int64_t CountIf(uint64_t handle, TPred pred) {
    const uint64_t offset = BridgeEnsureString(handle);
    const int64_t length = BridgeGetStringLen(handle);
    const auto* data = reinterpret_cast<const unsigned char*>(
        static_cast<uintptr_t>(offset));
    int64_t n = 0;
    for (int64_t i = 0; i < length; ++i) {
        if (pred(data[i])) {
            ++n;
        }
    }
    return n;
}

} // namespace

extern "C" {

//! Text::count_letters(txt: String) -> Int64
//! Bridge CC: BridgeEnsureString pins the cell once per distinct value;
//! later calls on the same handle reuse the offset (no CopyIntoCompartment).
__attribute__((visibility("default"))) void count_letters(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t arg0)
{
    if (BridgeIsNull(arg0)) {
        *result = MakeNull().Release();
        return;
    }
    *result = MakeInt64(CountIf(arg0, IsLetter)).Release();
}

//! Text::count_digits(txt: String) -> Int64
__attribute__((visibility("default"))) void count_digits(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t arg0)
{
    if (BridgeIsNull(arg0)) {
        *result = MakeNull().Release();
        return;
    }
    *result = MakeInt64(CountIf(arg0, IsDigit)).Release();
}

//! Text::count_upper(txt: String) -> Int64
__attribute__((visibility("default"))) void count_upper(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t arg0)
{
    if (BridgeIsNull(arg0)) {
        *result = MakeNull().Release();
        return;
    }
    *result = MakeInt64(CountIf(arg0, IsUpper)).Release();
}

//! Text::text_length(txt: String) -> Int64
//! O(1) body: only Len is read. Pairs with the O(n) counters so the pin cost
//! is visible on its own when the body does almost nothing.
__attribute__((visibility("default"))) void text_length(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t arg0)
{
    if (BridgeIsNull(arg0)) {
        *result = MakeNull().Release();
        return;
    }
    *result = MakeInt64(BridgeGetStringLen(arg0)).Release();
}

//! Text::byte_at(txt: String, pos: Int64) -> Int64
//! One byte, O(1) in the payload. Distinct `pos` literals keep YQL from
//! collapsing the calls, so BridgeEnsureString reuses the pin across probes
//! on the same value.
__attribute__((visibility("default"))) void byte_at(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t arg0,
    uint64_t arg1)
{
    if (BridgeIsNull(arg0) || BridgeIsNull(arg1)) {
        *result = MakeNull().Release();
        return;
    }
    const int64_t pos = BridgeGetInt64(arg1);
    const int64_t length = BridgeGetStringLen(arg0);
    if (pos < 0 || pos >= length) {
        *result = MakeInt64(0).Release();
        return;
    }
    const uint64_t offset = BridgeEnsureString(arg0);
    const auto* data = reinterpret_cast<const unsigned char*>(
        static_cast<uintptr_t>(offset));
    *result = MakeInt64(static_cast<int64_t>(data[pos])).Release();
}

} // extern "C"
