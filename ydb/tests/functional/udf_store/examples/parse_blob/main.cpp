#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <cstdio>
#include <cstring>

using namespace NYdb::NUdfStore::NAbi;

namespace {

//! Scan the guest-resident blob and return a compact summary.
//! When the query stage has a WASM UDF, large string columns are materialized
//! directly into linear memory; arg0->Data.String is already a WASM offset.
void FormatSummary(const char* data, uint32_t length, char* out, size_t outCap)
{
    uint8_t xorSum = 0;
    for (uint32_t i = 0; i < length; ++i) {
        xorSum ^= static_cast<uint8_t>(data[i]);
    }

    char head[9] = {};
    const uint32_t headLen = length < 4 ? length : 4;
    for (uint32_t i = 0; i < headLen; ++i) {
        std::snprintf(head + i * 2, 3, "%02x", static_cast<uint8_t>(data[i]));
    }

    std::snprintf(
        out,
        outCap,
        "len=%u;xor=%02x;head=%s",
        static_cast<unsigned>(length),
        static_cast<unsigned>(xorSum),
        head);
}

} // namespace

extern "C" {

//! ParseBlob::parse_blob(blob: String) -> String
//!
//! Demonstrates Host→Guest zero-copy for heavy blob args:
//! scan writes the column into WASM linear memory once; this export receives
//! only (offset, length) and reads the bytes in place — no second host→guest copy.
__attribute__((visibility("default"))) void parse_blob(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* arg0)
{
    if (!arg0 || arg0->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }
    if (arg0->Type != EValueType::String) {
        ThrowException("parse_blob: expected String argument");
    }

    char summary[96];
    FormatSummary(arg0->Data.String, arg0->Length, summary, sizeof(summary));
    const size_t summaryLen = std::strlen(summary);

    result->Type = EValueType::String;
    result->Length = static_cast<uint32_t>(summaryLen);
    result->Data.String = AllocateBytes(context, summaryLen);
    if (summaryLen > 0) {
        std::memcpy(result->Data.String, summary, summaryLen);
    }
}

//! ParseBlob::blob_head(blob: String) -> String
//!
//! Same argument, but the body is O(1) in the blob size: only the length and the
//! first bytes are read. Pairs with parse_blob to show what the host→guest copy
//! costs on its own — with a body that scans the whole blob, the copy is lost in
//! the noise of the scan.
__attribute__((visibility("default"))) void blob_head(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* arg0)
{
    if (!arg0 || arg0->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }
    if (arg0->Type != EValueType::String) {
        ThrowException("blob_head: expected String argument");
    }

    char head[9] = {};
    const uint32_t headLen = arg0->Length < 4 ? arg0->Length : 4;
    for (uint32_t i = 0; i < headLen; ++i) {
        std::snprintf(head + i * 2, 3, "%02x", static_cast<uint8_t>(arg0->Data.String[i]));
    }

    char summary[64];
    std::snprintf(
        summary,
        sizeof(summary),
        "len=%u;head=%s",
        static_cast<unsigned>(arg0->Length),
        head);
    const size_t summaryLen = std::strlen(summary);

    result->Type = EValueType::String;
    result->Length = static_cast<uint32_t>(summaryLen);
    result->Data.String = AllocateBytes(context, summaryLen);
    if (summaryLen > 0) {
        std::memcpy(result->Data.String, summary, summaryLen);
    }
}

//! ParseBlob::blob_offset(blob: String) -> Int64
//!
//! Diagnostic: returns where the argument bytes live in linear memory. With
//! resident columns the scan materializes every row separately, so a batch shows
//! many different offsets; on the host path each call copies into a buffer that
//! is freed right after, so the same offset comes back over and over.
__attribute__((visibility("default"))) void blob_offset(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* arg0)
{
    if (!arg0 || arg0->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }
    if (arg0->Type != EValueType::String) {
        ThrowException("blob_offset: expected String argument");
    }

    result->Type = EValueType::Int64;
    result->Data.Int64 = static_cast<int64_t>(
        reinterpret_cast<uintptr_t>(arg0->Data.String));
}

} // extern "C"
