#pragma once


namespace DB_CHDB
{

enum class DataDestinationType : uint8_t
{
    DISK,
    VOLUME,
    TABLE,
    DELETE,
    SHARD,
};

}
