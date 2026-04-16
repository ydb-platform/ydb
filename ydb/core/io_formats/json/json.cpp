#include "json.h"

#include <limits>

namespace NKikimr::NFormats {

NJson::TJsonWriterConfig DefaultJsonConfig() {
    constexpr ui32 doubleNDigits = std::numeric_limits<double>::max_digits10;
    constexpr ui32 floatNDigits = std::numeric_limits<float>::max_digits10;
    constexpr EFloatToStringMode floatMode = EFloatToStringMode::PREC_NDIGITS;
    return NJson::TJsonWriterConfig {
        .DoubleNDigits = doubleNDigits,
        .FloatNDigits = floatNDigits,
        .FloatToStringMode = floatMode,
        .ValidateUtf8 = false,
        .WriteNanAsString = true,
    };
}

} // namespace NKikimr::NFormats
