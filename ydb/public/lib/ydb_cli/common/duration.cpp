#include "duration.h"

namespace NYdb::NConsoleClient {

TDuration ParseDuration(TStringBuf str) {
    StripInPlace(str);
    if (str.empty()) {
        throw TMisuseException() << "Duration cannot be empty";
    }
    if (!IsAsciiAlpha(str.back())) {
        throw TMisuseException() << "Duration must end with a unit name (ex. 'h' for hours, 's' for seconds)";
    }
    return TDuration::Parse(str);
}

TDuration ParseDurationWithDefaultUnit(TStringBuf str, TDuration (*defaultUnit)(double)) {
    StripInPlace(str);
    if (str.empty()) {
        throw TMisuseException() << "Duration cannot be empty";
    }
    
    // Check if the string ends with a unit (alphabetic character)
    if (IsAsciiAlpha(str.back())) {
        // New format with explicit unit (e.g., "5s", "2m", "1h")
        // TDuration::Parse already supports fractional values
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

TDuration ParseDurationMicroseconds(TStringBuf str) {
    return ParseDurationWithDefaultUnit(str, [](double us) {
        return TDuration::MicroSeconds(static_cast<ui64>(std::round(us)));
    });
}

TDuration ParseDurationMilliseconds(TStringBuf str) {
    return ParseDurationWithDefaultUnit(str, [](double ms) {
        // Convert milliseconds to microseconds and round to preserve precision
        return TDuration::MicroSeconds(static_cast<ui64>(std::round(ms * 1000.0)));
    });
}

TDuration ParseDurationSeconds(TStringBuf str) {
    return ParseDurationWithDefaultUnit(str, [](double sec) {
        // Convert seconds to microseconds and round to preserve precision
        return TDuration::MicroSeconds(static_cast<ui64>(std::round(sec * 1000000.0)));
    });
}

TDuration ParseDurationHours(TStringBuf str) {
    return ParseDurationWithDefaultUnit(str, [](double hours) {
        // Convert hours to microseconds and round to preserve precision
        return TDuration::MicroSeconds(static_cast<ui64>(std::round(hours * 3600000000.0)));
    });
}

} // namespace NYdb::NConsoleClient
