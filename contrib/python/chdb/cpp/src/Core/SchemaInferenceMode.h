#pragma once

#include <cstdint>

namespace DB_CHDB
{

enum class SchemaInferenceMode : uint8_t
{
    DEFAULT,
    UNION,
};

}
