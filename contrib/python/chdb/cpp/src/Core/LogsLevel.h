#pragma once

namespace DB_CHDB
{
enum class LogsLevel : uint8_t
{
    none = 0, /// Disable
    fatal,
    error,
    warning,
    information,
    debug,
    trace,
    test,
};
}
