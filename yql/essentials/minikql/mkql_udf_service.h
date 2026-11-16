#pragma once

#include <yql/essentials/public/udf/service/dispatch/udf_service_dispatch.h>

namespace NKikimr::NMiniKQL {

extern const NYql::NUdf::TStaticSymbols UdfServiceSymbols;

inline const NYql::NUdf::TStaticSymbols& GetUdfServiceSymbols() noexcept {
    return UdfServiceSymbols;
}

extern NYql::NUdf::TUdfSymbolsExchange UdfServiceSymbolsExchange;

// Register before starting runtime workers. Repeated registration must agree.
void SetUdfServiceSymbolsExchange(NYql::NUdf::TUdfSymbolsExchange exchange);

inline NYql::NUdf::TUdfSymbolsExchange GetUdfServiceSymbolsExchange() noexcept {
    return UdfServiceSymbolsExchange ? UdfServiceSymbolsExchange : &YqlUdfExchangeThreadSymbols;
}

} // namespace NKikimr::NMiniKQL
