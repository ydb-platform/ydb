#include <ydb/services/udf_store/wasm/abi/bridge.h>
#include <ydb/services/udf_store/wasm/abi/bridge_abi.h>
#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <cstdlib>
#include <cstring>

using namespace NYdb::NUdfStore::NAbi;

namespace {

void SetNull(uint64_t* result) {
    *result = MakeNull().Release();
}

void SetInt64(uint64_t* result, int64_t value) {
    *result = MakeInt64(value).Release();
}

void SetUint64(uint64_t* result, uint64_t value) {
    *result = MakeUint64(value).Release();
}

void SetInt32(uint64_t* result, int32_t value) {
    *result = BridgeMakeInt32(value);
}

void SetUint32(uint64_t* result, uint32_t value) {
    *result = BridgeMakeUint32(value);
}

void SetFloat(uint64_t* result, float value) {
    *result = BridgeMakeFloat(value);
}

void SetDouble(uint64_t* result, double value) {
    *result = MakeDouble(value).Release();
}

void SetBool(uint64_t* result, bool value) {
    *result = MakeBool(value).Release();
}

int64_t DecimalChecksum(uint64_t handle) {
    alignas(16) unsigned char bytes[16];
    BridgeCopyDecimal(handle, reinterpret_cast<uint64_t>(bytes));
    int64_t sum = 0;
    for (unsigned char byte : bytes) {
        sum += byte;
    }
    return sum;
}

uint64_t* AllocHandles(size_t count) {
    auto* handles = static_cast<uint64_t*>(malloc(count * sizeof(uint64_t)));
    if (!handles) {
        ThrowException("BridgeTypes: malloc failed");
    }
    return handles;
}

} // namespace

extern "C" {

// ---- leaf scalars (echo where BridgeMake* exists) ----

__attribute__((visibility("default"))) void echo_bool(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetBool(result, BridgeGetBool(arg) != 0);
}

__attribute__((visibility("default"))) void echo_int32(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetInt32(result, BridgeGetInt32(arg));
}

__attribute__((visibility("default"))) void echo_uint32(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetUint32(result, BridgeGetUint32(arg));
}

__attribute__((visibility("default"))) void echo_int64(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetInt64(result, BridgeGetInt64(arg));
}

__attribute__((visibility("default"))) void echo_uint64(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetUint64(result, BridgeGetUint64(arg));
}

__attribute__((visibility("default"))) void echo_float(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetFloat(result, BridgeGetFloat(arg));
}

__attribute__((visibility("default"))) void echo_double(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetDouble(result, BridgeGetDouble(arg));
}

__attribute__((visibility("default"))) void echo_string(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    TBridgeString value(arg, /*owned*/ false);
    const int64_t len = value.Len();
    if (len == 0) {
        *result = MakeString("", 0).Release();
        return;
    }
    char* buf = static_cast<char*>(malloc(static_cast<size_t>(len)));
    if (!buf) {
        ThrowException("echo_string: malloc failed");
    }
    value.CopyTo(buf, len);
    *result = MakeString(buf, len).Release();
    free(buf);
}

//! Utf8 is read with string intrinsics; BridgeMakeString returns String bytes.
__attribute__((visibility("default"))) void echo_utf8(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    echo_string(/*ctx*/ nullptr, result, arg);
}

// ---- leaf scalars (read-only makers: widen to Uint64 / checksum) ----

__attribute__((visibility("default"))) void read_date_as_uint64(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetUint64(result, BridgeGetUint64(arg));
}

__attribute__((visibility("default"))) void read_datetime_as_uint32(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetUint32(result, BridgeGetUint32(arg));
}

__attribute__((visibility("default"))) void read_timestamp_as_uint64(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetUint64(result, BridgeGetUint64(arg));
}

__attribute__((visibility("default"))) void read_decimal_checksum(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t arg)
{
    if (BridgeIsNull(arg)) {
        SetNull(result);
        return;
    }
    SetInt64(result, DecimalChecksum(arg));
}

// ---- containers ----

__attribute__((visibility("default"))) void list_sum_int64(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t listH)
{
    TBridgeList list(listH, /*owned*/ false);
    int64_t sum = 0;
    for (auto item : list.Items()) {
        sum += BridgeGetInt64(item.Get());
        item.Reset();
    }
    SetInt64(result, sum);
}

__attribute__((visibility("default"))) void dict_get_int64(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t dictH, uint64_t keyH)
{
    TBridgeDict dict(dictH, /*owned*/ false);
    TBridgeString key(keyH, /*owned*/ false);
    auto payload = dict.Lookup(key);
    if (!payload) {
        SetNull(result);
        return;
    }
    *result = MakeOptional(MakeInt64(BridgeGetInt64(payload.Get()))).Release();
    payload.Reset();
}

__attribute__((visibility("default"))) void tuple_kind_sum(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t tupleH)
{
    int64_t sum = 0;
    const int32_t count = BridgeGetMemberCount(tupleH);
    for (int32_t i = 0; i < count; ++i) {
        TBridgeValue element(BridgeGetElement(tupleH, i), /*owned*/ true);
        sum += BridgeGetKind(element.Get());
        element.Reset();
    }
    SetInt64(result, sum);
}

__attribute__((visibility("default"))) void struct_get_score(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t structH)
{
    const int32_t index = BridgeGetMemberIndex(
        structH,
        reinterpret_cast<uint64_t>("score"),
        5);
    if (index < 0) {
        SetNull(result);
        return;
    }
    TBridgeValue score(BridgeGetElement(structH, index), /*owned*/ true);
    if (!score) {
        SetNull(result);
        return;
    }
    SetFloat(result, BridgeGetFloat(score.Get()));
    score.Reset();
}

__attribute__((visibility("default"))) void variant_index(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t variantH)
{
    SetInt64(result, BridgeGetVariantIndex(variantH));
}

__attribute__((visibility("default"))) void optional_list_present_count(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t listH)
{
    TBridgeList list(listH, /*owned*/ false);
    int64_t present = 0;
    for (auto item : list.Items()) {
        // Optional<Int64> is marker-represented: the item is the payload
        // itself, reported with the payload's kind, so there is no Optional
        // node to unwrap and a null check is the whole test.
        if (item) {
            ++present;
        }
        item.Reset();
    }
    SetInt64(result, present);
}

__attribute__((visibility("default"))) void resource_tag_len(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t resourceH)
{
    if (BridgeIsNull(resourceH)) {
        SetNull(result);
        return;
    }
    SetInt64(result, BridgeGetResourceTagLen(resourceH));
}

// ---- makers ----

__attribute__((visibility("default"))) void make_greeting_struct(
    TExpressionContext* /*ctx*/, uint64_t* result)
{
    static const char kMsg[] = "hello";
    uint64_t* members = AllocHandles(2);
    members[0] = MakeString(kMsg, static_cast<int64_t>(sizeof(kMsg) - 1)).Release();
    members[1] = MakeInt64(42).Release();
    *result = BridgeMakeStruct(reinterpret_cast<uint64_t>(members), 2);
    free(members);
}

__attribute__((visibility("default"))) void make_int_list(
    TExpressionContext* /*ctx*/, uint64_t* result)
{
    uint64_t* items = AllocHandles(3);
    items[0] = MakeInt64(1).Release();
    items[1] = MakeInt64(2).Release();
    items[2] = MakeInt64(3).Release();
    *result = BridgeMakeList(reinterpret_cast<uint64_t>(items), 3);
    free(items);
}

__attribute__((visibility("default"))) void make_name_dict(
    TExpressionContext* /*ctx*/, uint64_t* result)
{
    static const char kKey[] = "answer";
    uint64_t* pairs = AllocHandles(2);
    pairs[0] = MakeString(kKey, static_cast<int64_t>(sizeof(kKey) - 1)).Release();
    pairs[1] = MakeInt64(42).Release();
    *result = BridgeMakeDict(BridgeGetResultType(), reinterpret_cast<uint64_t>(pairs), 1);
    free(pairs);
}

__attribute__((visibility("default"))) void make_variant_uint32(
    TExpressionContext* /*ctx*/, uint64_t* result)
{
    *result = MakeVariant(0, TBridgeValue(BridgeMakeUint32(5))).Release();
}

// ---- callable ----

__attribute__((visibility("default"))) void run_callable_int64(
    TExpressionContext* /*ctx*/, uint64_t* result, uint64_t callableH, uint64_t argH)
{
    if (BridgeIsNull(callableH)) {
        SetNull(result);
        return;
    }
    uint64_t args[1] = {argH};
    const uint64_t out = BridgeRun(callableH, reinterpret_cast<uint64_t>(args), 1);
    if (BridgeIsNull(out)) {
        SetNull(result);
        return;
    }
    SetInt64(result, BridgeGetInt64(out));
    BridgeUnref(out);
}

} // extern "C"
