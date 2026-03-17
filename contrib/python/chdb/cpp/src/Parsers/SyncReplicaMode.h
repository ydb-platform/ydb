#pragma once
#include <cstdint>

namespace DB_CHDB
{
enum class SyncReplicaMode : uint8_t
{
    DEFAULT,
    STRICT,
    LIGHTWEIGHT,
    PULL,
};
}
