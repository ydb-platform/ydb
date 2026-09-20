//! Shared mutable context in one WASM module (object_framework).
//!
//! CountRow / CountPositive bump counters on the same ctx handle; Snapshot
//! returns a human-readable dump for SELECT, e.g. "rows_seen=3;positives=2".

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>
#include <ydb/services/udf_store/wasm/object_framework/object_framework.h>

#include <string.h>

using namespace NYdb::NUdfStore::NAbi;

namespace {

struct TCtx {
    long long RowsSeen = 0;
    long long Positives = 0;
};

void CtxInit(void* self, const void* /*blob*/, size_t /*blobLen*/) {
    auto* ctx = static_cast<TCtx*>(self);
    ctx->RowsSeen = 0;
    ctx->Positives = 0;
}

void CtxDestroy(void* self) {
    auto* ctx = static_cast<TCtx*>(self);
    ctx->RowsSeen = 0;
    ctx->Positives = 0;
}

const TObjectType CtxType = {
    "Ctx",
    sizeof(TCtx),
    &CtxInit,
    &CtxDestroy,
};

uint64_t AsHandle(const TUnversionedValue* value) {
    if (!value || value->Type == EValueType::Null) {
        return 0;
    }
    if (value->Type == EValueType::Uint64) {
        return value->Data.Uint64;
    }
    if (value->Type == EValueType::Int64) {
        return static_cast<uint64_t>(value->Data.Int64);
    }
    ThrowException("expected int64/uint64 handle");
    return 0;
}

TCtx* GetCtx(const TUnversionedValue* handleArg) {
    auto* ctx = static_cast<TCtx*>(ObjectFrameworkGet(AsHandle(handleArg), &CtxType));
    if (!ctx) {
        ThrowException("unknown ctx handle");
    }
    return ctx;
}

int AppendLiteral(char* buf, int bufLen, int n, const char* lit) {
    for (const char* p = lit; *p; ++p) {
        if (n >= bufLen) {
            return -1;
        }
        buf[n++] = *p;
    }
    return n;
}

int AppendI64(char* buf, int bufLen, int n, long long v) {
    if (n >= bufLen) {
        return -1;
    }
    if (v < 0) {
        buf[n++] = '-';
        v = -v;
    }
    char tmp[32];
    int t = 0;
    if (v == 0) {
        tmp[t++] = '0';
    } else {
        while (v > 0) {
            tmp[t++] = static_cast<char>('0' + (v % 10));
            v /= 10;
        }
    }
    while (t > 0) {
        if (n >= bufLen) {
            return -1;
        }
        buf[n++] = tmp[--t];
    }
    return n;
}

//! Formats "rows_seen=<n>;positives=<m>" into |buf|; returns length (excl. NUL).
int FormatCtx(const TCtx* ctx, char* buf, int bufLen) {
    if (!ctx || !buf || bufLen < 24) {
        return 0;
    }
    int n = 0;
    n = AppendLiteral(buf, bufLen, n, "rows_seen=");
    if (n < 0) {
        return 0;
    }
    n = AppendI64(buf, bufLen, n, ctx->RowsSeen);
    if (n < 0) {
        return 0;
    }
    n = AppendLiteral(buf, bufLen, n, ";positives=");
    if (n < 0) {
        return 0;
    }
    n = AppendI64(buf, bufLen, n, ctx->Positives);
    if (n < 0 || n >= bufLen) {
        return 0;
    }
    buf[n] = 0;
    return n;
}

void PassThrough(TUnversionedValue* result, TUnversionedValue* inputArg) {
    if (!inputArg || inputArg->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }
    *result = *inputArg;
}

} // namespace

extern "C" {

__attribute__((visibility("default"))) void ctx_create(
    TExpressionContext* /*context*/,
    TUnversionedValue* result)
{
    const TObjectHandle handle = ObjectFrameworkCreate(&CtxType, nullptr, 0);
    if (handle == 0) {
        ThrowException("ctx_create failed");
    }
    result->Type = EValueType::Uint64;
    result->Data.Uint64 = handle;
}

__attribute__((visibility("default"))) void ctx_destroy(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* handleArg)
{
    ObjectFrameworkDestroy(AsHandle(handleArg));
    result->Type = EValueType::Null;
}

//! Always increments rows_seen; returns |input| unchanged.
__attribute__((visibility("default"))) void count_row(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* handleArg,
    TUnversionedValue* inputArg)
{
    ++GetCtx(handleArg)->RowsSeen;
    PassThrough(result, inputArg);
}

//! Increments positives when |input| is a positive Int64; returns |input| unchanged.
__attribute__((visibility("default"))) void count_positive(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* handleArg,
    TUnversionedValue* inputArg)
{
    TCtx* ctx = GetCtx(handleArg);
    if (inputArg && inputArg->Type == EValueType::Int64 && inputArg->Data.Int64 > 0) {
        ++ctx->Positives;
    }
    PassThrough(result, inputArg);
}

__attribute__((visibility("default"))) void ctx_snapshot(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* handleArg)
{
    const TCtx* ctx = GetCtx(handleArg);
    char tmp[96];
    const int n = FormatCtx(ctx, tmp, static_cast<int>(sizeof(tmp)));
    if (n <= 0) {
        ThrowException("ctx_snapshot: format failed");
    }
    result->Type = EValueType::String;
    result->Length = static_cast<uint32_t>(n);
    result->Data.String = AllocateBytes(context, result->Length);
    memcpy(result->Data.String, tmp, static_cast<size_t>(n));
}

} // extern "C"
