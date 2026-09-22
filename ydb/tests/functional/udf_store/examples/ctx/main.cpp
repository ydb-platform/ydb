//! Shared mutable context in one WASM module (object_framework).
//!
//! CountRow / CountPositive bump counters on the same ctx handle; Snapshot
//! returns a human-readable dump for SELECT, e.g. "rows_seen=3;positives=2".

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>
#include <ydb/services/udf_store/wasm/abi/bridge.h>
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

TCtx* GetCtx(uint64_t handleArg) {
    auto* ctx = static_cast<TCtx*>(ObjectFrameworkGet(BridgeGetUint64(handleArg), &CtxType));
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

} // namespace

extern "C" {

__attribute__((visibility("default"))) void ctx_create(
    TExpressionContext* /*context*/,
    uint64_t* result,
    uint64_t /*config*/)
{
    const TObjectHandle handle = ObjectFrameworkCreate(&CtxType, nullptr, 0);
    if (handle == 0) {
        ThrowException("ctx_create failed");
    }
    *result = MakeUint64(handle).Release();
}

__attribute__((visibility("default"))) void ctx_destroy(
    TExpressionContext* /*context*/,
    uint64_t* result,
    uint64_t handleArg)
{
    ObjectFrameworkDestroy(BridgeGetUint64(handleArg));
    *result = MakeNull().Release();
}

//! Always increments rows_seen; returns |input| unchanged.
__attribute__((visibility("default"))) void count_row(
    TExpressionContext* /*context*/,
    uint64_t* result,
    uint64_t handleArg,
    uint64_t inputArg)
{
    ++GetCtx(handleArg)->RowsSeen;
    *result = inputArg;
}

//! Increments positives when |input| is a positive Int64; returns |input| unchanged.
__attribute__((visibility("default"))) void count_positive(
    TExpressionContext* /*context*/,
    uint64_t* result,
    uint64_t handleArg,
    uint64_t inputArg)
{
    TCtx* ctx = GetCtx(handleArg);
    if (!BridgeIsNull(inputArg) && BridgeGetInt64(inputArg) > 0) {
        ++ctx->Positives;
    }
    *result = inputArg;
}

__attribute__((visibility("default"))) void ctx_snapshot(
    TExpressionContext* /*context*/,
    uint64_t* result,
    uint64_t handleArg)
{
    const TCtx* ctx = GetCtx(handleArg);
    char tmp[96];
    const int n = FormatCtx(ctx, tmp, static_cast<int>(sizeof(tmp)));
    if (n <= 0) {
        ThrowException("ctx_snapshot: format failed");
    }
    *result = MakeString(tmp, n).Release();
}

} // extern "C"
