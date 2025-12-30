#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

inline void ThrowOnError(const NYdb::TOperation& operation) {
    if (!operation.Ready()) {
        return;
    }
    NStatusHelpers::ThrowOnError(operation.Status());
}

inline TDuration ParseDuration(TStringBuf str) {
    StripInPlace(str);
    if (!str.empty() && !IsAsciiAlpha(str.back())) {
        throw TMisuseException() << "Duration must end with a unit name (ex. 'h' for hours, 's' for seconds)";
    }
    return TDuration::Parse(str);
}

// Parse duration with a default unit for backward compatibility
// If the input is a plain number, it's interpreted using the defaultUnit
// Otherwise, it's parsed as a duration string (e.g., "5s", "2m", "1h")
inline TDuration ParseDurationWithDefaultUnit(TStringBuf str, TDuration (*defaultUnit)(double)) {
    StripInPlace(str);
    if (str.empty()) {
        throw TMisuseException() << "Duration cannot be empty";
    }
    
    // Check if the string ends with a unit (alphabetic character)
    if (IsAsciiAlpha(str.back())) {
        // New format with explicit unit (e.g., "5s", "2m", "1h")
        return TDuration::Parse(str);
    }
    
    // Old format: plain number - interpret using default unit
    double value = 0;
    if (!TryFromString(str, value)) {
        throw TMisuseException() << "Invalid duration value '" << str << "'";
    }
    if (value < 0) {
        throw TMisuseException() << "Duration must be non-negative";
    }
    if (!std::isfinite(value)) {
        throw TMisuseException() << "Duration must be finite";
    }
    return defaultUnit(value);
}

// Parse duration in milliseconds with support for new format (e.g., "5s", "100ms")
// If input is a plain number, it's interpreted as milliseconds (for backward compatibility)
inline TDuration ParseDurationMilliseconds(TStringBuf str) {
    return ParseDurationWithDefaultUnit(str, [](double ms) {
        return TDuration::MilliSeconds(static_cast<ui64>(ms));
    });
}

} // namespace NYdb::NConsoleClient
