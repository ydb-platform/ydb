#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <util/datetime/base.h>
#include <util/string/strip.h>

#include <cmath>

namespace NYdb::NConsoleClient {

// Parse duration with a default unit for backward compatibility
// If the input is a plain number, it's interpreted using the defaultUnit
// Otherwise, it's parsed as a duration string (e.g., "5s", "2m", "1h")
// Supports fractional values: 0.5s = 500ms, 0.5m = 30s, 0.5h = 30m
TDuration ParseDurationWithDefaultUnit(TStringBuf str, TDuration (*defaultUnit)(double));

// Parse duration in microseconds with support for new format (e.g., "5s", "100ms", "500us")
// If input is a plain number, it's interpreted as microseconds (for backward compatibility)
// Supports fractional values: 0.5ms = 500us
TDuration ParseDurationMicroseconds(TStringBuf str);

// Parse duration in milliseconds with support for new format (e.g., "5s", "100ms")
// If input is a plain number, it's interpreted as milliseconds (for backward compatibility)
// Supports fractional values: 0.5s = 500ms
TDuration ParseDurationMilliseconds(TStringBuf str);

// Parse duration in seconds with support for new format (e.g., "5s", "100ms", "2m")
// If input is a plain number, it's interpreted as seconds (for backward compatibility)
// Supports fractional values: 0.5m = 30s
TDuration ParseDurationSeconds(TStringBuf str);

// Parse duration in hours with support for new format (e.g., "2h", "30m", "1d")
// If input is a plain number, it's interpreted as hours (for backward compatibility)
// Supports fractional values: 0.5h = 30m, 0.5d = 12h
TDuration ParseDurationHours(TStringBuf str);

// Parse duration requiring explicit unit (e.g., "5s", "2m", "1h")
// Throws exception if no unit is specified
TDuration ParseDuration(TStringBuf str);

} // namespace NYdb::NConsoleClient
