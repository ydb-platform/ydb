#include "udf_service_dispatch.h"

namespace NYql::NUdf::NServiceDispatch {

constinit const TStaticSymbols* DefaultSymbols = nullptr;
thread_local constinit const TStaticSymbols* ThreadSymbols = nullptr;

} // namespace NYql::NUdf::NServiceDispatch

extern "C" void YqlUdfBindSymbols(const NYql::NUdf::TStaticSymbols* symbols) noexcept {
    NYql::NUdf::NServiceDispatch::DefaultSymbols = symbols;
}

extern "C" const NYql::NUdf::TStaticSymbols* YqlUdfExchangeThreadSymbols(const NYql::NUdf::TStaticSymbols* symbols) noexcept {
    const auto* previous = NYql::NUdf::NServiceDispatch::ThreadSymbols;
    NYql::NUdf::NServiceDispatch::ThreadSymbols = symbols;
    return previous;
}
