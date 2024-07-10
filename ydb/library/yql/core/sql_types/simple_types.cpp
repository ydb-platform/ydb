#include "simple_types.h"

#include <util/generic/string.h>
#include <util/string/cast.h>

#include <unordered_map>

namespace NYql {

std::optional<std::string_view> LookupSimpleTypeBySqlAlias(const std::string_view& alias, bool flexibleTypesEnabled) {
    static const std::unordered_map<std::string_view, std::string_view> simpleTypes = {
        {"void",      "Void"},
        {"unit",      "Unit"},
        {"generic",   "Generic"},
        {"emptylist", "EmptyList"},
        {"emptydict", "EmptyDict"},

        {"bool", "Bool"},

        {"int8",  "Int8"},
        {"int16", "Int16"},
        {"int32", "Int32"},
        {"int64", "Int64"},

        {"tinyint",  "Int8"},
        {"smallint", "Int16"},
        {"int",      "Int32"},
        {"integer",  "Int32"},
        {"bigint",   "Int64"},

        {"uint8",  "Uint8"},
        {"uint16", "Uint16"},
        {"uint32", "Uint32"},
        {"uint64", "Uint64"},

        {"float",  "Float"},
        {"double", "Double"},

        {"dynumber", "DyNumber"},

        {"string",  "String"},
        {"varchar", "String"},
        {"utf8",    "Utf8"},

        {"uuid",         "Uuid"},
        {"yson",         "Yson"},
        {"json",         "Json"},
        {"jsondocument", "JsonDocument"},
        {"json_document", "JsonDocument"},
        {"xml",          "Xml"},
        {"yaml",         "Yaml"},

        {"date",      "Date"},
        {"datetime",  "Datetime"},
        {"timestamp", "Timestamp"},
        {"interval",  "Interval"},

        {"tzdate",      "TzDate"},
        {"tzdatetime",  "TzDatetime"},
        {"tztimestamp", "TzTimestamp"},

        {"date32",    "Date32"},
        {"datetime64",  "Datetime64"},
        {"timestamp64", "Timestamp64"},
        {"interval64",  "Interval64"},
        {"tzdate32",    "TzDate32"},
        {"tzdatetime64",  "TzDatetime64"},
        {"tztimestamp64", "TzTimestamp64"},
    };

    // new types (or aliases) should be added here
    static const std::unordered_map<std::string_view, std::string_view> newSimpleTypes = {
        {"text",      "Utf8"},
        {"bytes",     "String"},
    };

    auto normalized = to_lower(ToString(alias));
    if (auto it = simpleTypes.find(normalized); it != simpleTypes.end()) {
        return it->second;
    }

    if (flexibleTypesEnabled) {
        if (auto it = newSimpleTypes.find(normalized); it != newSimpleTypes.end()) {
            return it->second;
        }
    }
    return {};
}


}
