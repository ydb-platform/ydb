#pragma once

namespace NYql::NUdf {
struct TStaticSymbols;

using TUdfSymbolsExchange = const TStaticSymbols* (*)(const TStaticSymbols*) noexcept;

namespace NServiceDispatch {
extern constinit const TStaticSymbols* DefaultSymbols;
extern thread_local constinit const TStaticSymbols* ThreadSymbols;
} // namespace NServiceDispatch
} // namespace NYql::NUdf

// Tables have static storage duration. The thread override is borrowed until restored.
extern "C" void YqlUdfBindSymbols(const NYql::NUdf::TStaticSymbols* symbols) noexcept;
extern "C" const NYql::NUdf::TStaticSymbols* YqlUdfExchangeThreadSymbols(const NYql::NUdf::TStaticSymbols* symbols) noexcept;
extern "C" inline const NYql::NUdf::TStaticSymbols* YqlUdfGetSymbols() noexcept {
    const auto* symbols = NYql::NUdf::NServiceDispatch::ThreadSymbols;
    return symbols ? symbols : NYql::NUdf::NServiceDispatch::DefaultSymbols;
}

namespace NYql::NUdf {

class TScopedUdfServiceSymbols {
public:
    TScopedUdfServiceSymbols(TUdfSymbolsExchange exchange, const TStaticSymbols* symbols) noexcept
        : Exchange_(exchange)
        , Previous_(Exchange_ ? Exchange_(symbols) : nullptr)
    {
    }

    ~TScopedUdfServiceSymbols() {
        if (Exchange_) {
            Exchange_(Previous_);
        }
    }

    TScopedUdfServiceSymbols(const TScopedUdfServiceSymbols&) = delete;
    TScopedUdfServiceSymbols& operator=(const TScopedUdfServiceSymbols&) = delete;

private:
    const TUdfSymbolsExchange Exchange_;
    const TStaticSymbols* const Previous_;
};

} // namespace NYql::NUdf
