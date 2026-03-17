//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "SnowflakeType.hpp"

namespace sf
{

std::unordered_map<std::string, SnowflakeType::Type>
    SnowflakeType::m_strEnumIndex = {
        {"ANY", SnowflakeType::Type::ANY},
        {"ARRAY", SnowflakeType::Type::ARRAY},
        {"BINARY", SnowflakeType::Type::BINARY},
        {"BOOLEAN", SnowflakeType::Type::BOOLEAN},
        {"CHAR", SnowflakeType::Type::CHAR},
        {"DATE", SnowflakeType::Type::DATE},
        {"FIXED", SnowflakeType::Type::FIXED},
        {"OBJECT", SnowflakeType::Type::OBJECT},
        {"REAL", SnowflakeType::Type::REAL},
        {"FLOAT", SnowflakeType::Type::REAL},
        {"DOUBLE", SnowflakeType::Type::REAL},
        {"DOUBLE PRECISION", SnowflakeType::Type::REAL},
        {"VARCHAR", SnowflakeType::Type::TEXT},
        {"STRING", SnowflakeType::Type::TEXT},
        {"TEXT", SnowflakeType::Type::TEXT},
        {"TIME", SnowflakeType::Type::TIME},
        {"TIMESTAMP", SnowflakeType::Type::TIMESTAMP},
        {"TIMESTAMP_LTZ", SnowflakeType::Type::TIMESTAMP_LTZ},
        {"TIMESTAMP_NTZ", SnowflakeType::Type::TIMESTAMP_NTZ},
        {"TIMESTAMP_TZ", SnowflakeType::Type::TIMESTAMP_TZ},
        {"VARIANT", SnowflakeType::Type::VARIANT}};

}  // namespace sf
