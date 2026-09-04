#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

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

void SetNull(TUnversionedValue* result) {
    result->Type = EValueType::Null;
}

void SetInt64(TUnversionedValue* result, int64_t value) {
    result->Type = EValueType::Int64;
    result->Data.Int64 = value;
}

const TUnversionedValue* RequireString(TUnversionedValue* arg, const char* where) {
    if (!arg || arg->Type == EValueType::Null) {
        return nullptr;
    }
    if (arg->Type != EValueType::String) {
        ThrowException(where);
    }
    return arg;
}

template <typename TPred>
int64_t CountIf(const TUnversionedValue* arg, TPred pred) {
    int64_t n = 0;
    const char* data = arg->Data.String;
    const uint32_t length = arg->Length;
    for (uint32_t i = 0; i < length; ++i) {
        if (pred(static_cast<unsigned char>(data[i]))) {
            ++n;
        }
    }
    return n;
}

} // namespace

extern "C" {

//! Text::count_letters(txt: String) -> Int64
//! ASCII [A-Za-z] over a physical column: PreferWasm writes the cell into
//! linear memory once; this export walks those bytes in place.
__attribute__((visibility("default"))) void count_letters(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* arg0)
{
    const TUnversionedValue* arg = RequireString(arg0, "count_letters: expected String argument");
    if (!arg) {
        SetNull(result);
        return;
    }
    SetInt64(result, CountIf(arg, IsLetter));
}

//! Text::count_digits(txt: String) -> Int64
__attribute__((visibility("default"))) void count_digits(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* arg0)
{
    const TUnversionedValue* arg = RequireString(arg0, "count_digits: expected String argument");
    if (!arg) {
        SetNull(result);
        return;
    }
    SetInt64(result, CountIf(arg, IsDigit));
}

//! Text::count_upper(txt: String) -> Int64
__attribute__((visibility("default"))) void count_upper(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* arg0)
{
    const TUnversionedValue* arg = RequireString(arg0, "count_upper: expected String argument");
    if (!arg) {
        SetNull(result);
        return;
    }
    SetInt64(result, CountIf(arg, IsUpper));
}

//! Text::text_length(txt: String) -> Int64
//! Body is O(1) in the payload: only Length is read. Pairs with the O(n)
//! counters so the host→guest copy is visible on its own.
__attribute__((visibility("default"))) void text_length(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* arg0)
{
    const TUnversionedValue* arg = RequireString(arg0, "text_length: expected String argument");
    if (!arg) {
        SetNull(result);
        return;
    }
    SetInt64(result, static_cast<int64_t>(arg->Length));
}

//! Text::byte_at(txt: String, pos: Int64) -> Int64
//! One byte, O(1) in the payload. Distinct `pos` literals keep YQL from
//! collapsing the calls, so the host path copies the blob once per call
//! while PreferWasm reuses the scan's resident buffer.
__attribute__((visibility("default"))) void byte_at(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* arg0,
    TUnversionedValue* arg1)
{
    const TUnversionedValue* arg = RequireString(arg0, "byte_at: expected String argument");
    if (!arg) {
        SetNull(result);
        return;
    }
    if (!arg1 || arg1->Type == EValueType::Null) {
        SetNull(result);
        return;
    }
    if (arg1->Type != EValueType::Int64) {
        ThrowException("byte_at: expected Int64 position");
    }
    const int64_t pos = arg1->Data.Int64;
    if (pos < 0 || static_cast<uint64_t>(pos) >= arg->Length) {
        SetInt64(result, 0);
        return;
    }
    SetInt64(result, static_cast<unsigned char>(arg->Data.String[pos]));
}

} // extern "C"
