#include "simple_types.h"

#include <util/generic/string.h>
#include <util/string/cast.h>

#include <unordered_map>
#include <map>

namespace NYql {

namespace {
struct TSimpleType {
    std::string_view CanonicalSqlName;
    std::string_view YqlName;
    std::string_view Kind;
};

static const std::unordered_map<std::string_view, TSimpleType> SimpleTypes = {
    {"void", {"Void", "Void", "Void"}},
    {"unit", {"Unit", "Unit", "Unit"}},
    {"generic", {"Generic", "Generic", "Generic"}},
    {"emptylist", {"EmptyList", "EmptyList", "EmptyList"}},
    {"emptydict", {"EmptyDict", "EmptyDict", "EmptyDict"}},

    {"bool", {"Bool", "Bool", "Data"}},

    {"int8", {"Int8", "Int8", "Data"}},
    {"int16", {"Int16", "Int16", "Data"}},
    {"int32", {"Int32", "Int32", "Data"}},
    {"int64", {"Int64", "Int64", "Data"}},

    {"tinyint", {"", "Int8", ""}},
    {"smallint", {"", "Int16", ""}},
    {"int", {"", "Int32", ""}},
    {"integer", {"", "Int32", ""}},
    {"bigint", {"", "Int64", ""}},

    {"uint8", {"Uint8", "Uint8", "Data"}},
    {"uint16", {"Uint16", "Uint16", "Data"}},
    {"uint32", {"Uint32", "Uint32", "Data"}},
    {"uint64", {"Uint64", "Uint64", "Data"}},

    {"float", {"Float", "Float", "Data"}},
    {"double", {"Double", "Double", "Data"}},

    {"dynumber", {"DyNumber", "DyNumber", "Data"}},

    {"string", {"String", "String", "Data"}},
    {"varchar", {"", "String", ""}},
    {"utf8", {"Utf8", "Utf8", "Data"}},

    {"uuid", {"Uuid", "Uuid", "Data"}},
    {"yson", {"Yson", "Yson", "Data"}},
    {"json", {"Json", "Json", "Data"}},
    {"jsondocument", {"JsonDocument", "JsonDocument", "Data"}},
    {"json_document", {"JsonDocument", "JsonDocument", "Data"}},
    {"xml", {"", "Xml", ""}},
    {"yaml", {"", "Yaml", ""}},

    {"date", {"Date", "Date", "Data"}},
    {"datetime", {"Datetime", "Datetime", "Data"}},
    {"timestamp", {"Timestamp", "Timestamp", "Data"}},
    {"interval", {"Interval", "Interval", "Data"}},

    {"tzdate", {"TzDate", "TzDate", "Data"}},
    {"tzdatetime", {"TzDatetime", "TzDatetime", "Data"}},
    {"tztimestamp", {"TzTimestamp", "TzTimestamp", "Data"}},

    {"date32", {"Date32", "Date32", "Data"}},
    {"datetime64", {"Datetime64", "Datetime64", "Data"}},
    {"timestamp64", {"Timestamp64", "Timestamp64", "Data"}},
    {"interval64", {"Interval64", "Interval64", "Data"}},
    {"tzdate32", {"TzDate32", "TzDate32", "Data"}},
    {"tzdatetime64", {"TzDatetime64", "TzDatetime64", "Data"}},
    {"tztimestamp64", {"TzTimestamp64", "TzTimestamp64", "Data"}}};

// new types (or aliases) should be added here
static const std::unordered_map<std::string_view, TSimpleType> NewSimpleTypes = {
    {"text", {"", "Utf8", ""}},
    {"bytes", {"", "String", ""}}};
} // namespace

std::optional<std::string_view> LookupSimpleTypeBySqlAlias(const std::string_view& alias, bool flexibleTypesEnabled) {
    auto normalized = to_lower(ToString(alias));
    if (auto it = SimpleTypes.find(normalized); it != SimpleTypes.end()) {
        return it->second.YqlName;
    }

    if (flexibleTypesEnabled) {
        if (auto it = NewSimpleTypes.find(normalized); it != NewSimpleTypes.end()) {
            return it->second.YqlName;
        }
    }
    return {};
}

void EnumerateSimpleTypes(const std::function<void(std::string_view name, std::string_view kind)>& callback, bool flexibleTypesEnabled) {
    std::map<std::string_view, std::string_view> map;
    for (const auto& x : SimpleTypes) {
        if (!x.second.CanonicalSqlName.empty()) {
            map.emplace(x.second.CanonicalSqlName, x.second.Kind);
        }
    }

    if (flexibleTypesEnabled) {
        for (const auto& x : NewSimpleTypes) {
            if (!x.second.CanonicalSqlName.empty()) {
                map.emplace(x.second.CanonicalSqlName, x.second.Kind);
            }
        }
    }

    for (const auto& [name, kind] : map) {
        callback(name, kind);
    }
}

} // namespace NYql
