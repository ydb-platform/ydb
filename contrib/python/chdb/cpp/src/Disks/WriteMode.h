#pragma once

namespace DB_CHDB
{

/// Mode of opening a file for write.
enum class WriteMode : uint8_t
{
    Rewrite,
    Append
};

}
