#include "mkql_udf_service.h"
#include "mkql_alloc.h"
#include "mkql_terminator.h"

#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/service/dispatch/udf_service_dispatch.h>

#include <util/generic/yexception.h>

namespace NKikimr::NMiniKQL {

namespace {

// The UDF ABI uses ui64; MiniKQL uses size_t, which can be a distinct type.
void* AllocateWithSize(ui64 size) {
    return TWithDefaultMiniKQLAlloc::AllocWithSize(size);
}

void FreeWithSize(const void* memory, ui64 size) {
    TWithDefaultMiniKQLAlloc::FreeWithSize(memory, size);
}

// Keep initialization in this ordinary archive member: force-linking it would
// pull the allocator and require a UDF policy even in non-runtime consumers.
struct TBindUdfServiceSymbols {
    TBindUdfServiceSymbols() {
        YqlUdfBindSymbols(&UdfServiceSymbols);
    }
} BindUdfServiceSymbols;

} // namespace

constinit NYql::NUdf::TUdfSymbolsExchange UdfServiceSymbolsExchange = nullptr;

constinit const NYql::NUdf::TStaticSymbols UdfServiceSymbols{
    .UdfTerminate = &MKQLTerminate,
    .UdfRegisterObject = &MKQLRegisterObject,
    .UdfUnregisterObject = &MKQLUnregisterObject,
    .UdfAllocateWithSizeFunc = &AllocateWithSize,
    .UdfFreeWithSizeFunc = &FreeWithSize,
    .UdfArrowAllocateFunc = &MKQLArrowAllocate,
    .UdfArrowReallocateFunc = &MKQLArrowReallocate,
    .UdfArrowFreeFunc = &MKQLArrowFree,
};

void SetUdfServiceSymbolsExchange(NYql::NUdf::TUdfSymbolsExchange exchange) {
    Y_ENSURE(exchange, "UDF services require a live symbols exchanger");
    Y_ENSURE(!UdfServiceSymbolsExchange || UdfServiceSymbolsExchange == exchange,
             "MiniKQL runtime is already configured with a different UDF symbols exchanger");
    if (!UdfServiceSymbolsExchange) {
        UdfServiceSymbolsExchange = exchange;
    }
}

} // namespace NKikimr::NMiniKQL
