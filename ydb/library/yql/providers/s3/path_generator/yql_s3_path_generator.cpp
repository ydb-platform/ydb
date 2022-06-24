#include "yql_s3_path_generator.h"

#include <library/cpp/scheme/scheme.h>
#include <regex>
#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>
#include <util/string/split.h>

namespace NYql::NPathGenerator {

namespace {

void ReplaceAll(TString& str, const TString& from, const TString& to) {
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != TString::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
}

TVector<TString> GetPath(const TStringBuf& key) {
    return StringSplitter(key).Split('.').ToList<TString>();
}

i64 GetIntOrThrow(const NSc::TValue& json, const TString& error) {
    if (json.IsNumber()) {
        return json.GetIntNumber();
    }

    if (TStringBuf str = json.GetString(TStringBuf())) {
        {
            i64 result = 0;
            if (TryFromString<i64>(str, result)) {
                return result;
            }
        }
        {
            ui64 result = 0;
            if (TryFromString<ui64>(str, result)) {
                return result;
            }
        }
        {
            double result = 0;
            if (TryFromString<double>(str, result)) {
                return result;
            }
        }
    }

    ythrow yexception() << error;
}

TString GetStringOrThrow(const NSc::TValue& json, const TString& error) {
    if (json.IsString() || json.IsIntNumber() || json.IsNumber()) {
        return json.ForceString();
    }
    ythrow yexception() << error;
}

i64 GetBoolOrThrow(const NSc::TValue& json, const TString& error) {
    if (json.IsNull()) {
        ythrow yexception() << error;
    }
    return json.IsTrue();
}

EIntervalUnit ToIntervalUnit(const TString& unit) {
    const auto names = GetEnumNames<EIntervalUnit>();
    for (const auto& name: names) {
        if (name.second == unit) {
            return name.first;
        }
    }
    ythrow yexception() << "Invalid projection scheme: unit " << unit << " must be one of " << GetEnumAllNames<EIntervalUnit>();
}

TMap<EType, TString> ToLowerType() {
    TMap<EType, TString> result;
    for (const auto& p: GetEnumNames<EType>()) {
        result[p.first] = to_lower(p.second);
    }
    return result;
};

EType ToType(const TString& type) {
    static TMap<EType, TString> enumNames{ToLowerType()};
    for (const auto& name: enumNames) {
        if (name.second == type) {
            return name.first;
        }
    }
    ythrow yexception() << "Invalid projection scheme: type " << type << " must be one of " << to_lower(GetEnumAllNames<EType>());
}

void DoParseEnumType(const TString& columnName, const TString& type, const TMap<TString, NSc::TValue>& projection, ExplicitPartitioningConfig& config) {
    if (!projection.contains("values")) {
        ythrow yexception() << "Invalid projection scheme: values are required field for " << columnName << " " << type;
    }
    if (!projection.contains("type")) {
        ythrow yexception() << "Invalid projection scheme: type are required field for " << columnName << " " << type;
    }
    for (const auto& p: projection) {
        if (p.first == "type") {
            // already processed
        } else if (p.first == "values") {
            TString values = GetStringOrThrow(p.second, "The values must be a string");
            config.Rules.push_back(ColumnPartitioningConfig{.Type=ToType(type), .Name=columnName, .Values=StringSplitter(values).Split(',').ToList<TString>()});
        } else {
            ythrow yexception() << "Invalid projection scheme: enum element must include only type or values (as string) but got " << p.first;
        }
    }
}

void DoParseIntegerType(const TString& columnName, const TString& type, const TMap<TString, NSc::TValue>& projection, ExplicitPartitioningConfig& config) {
    if (!projection.contains("type")) {
        ythrow yexception() << "Invalid projection scheme: type are required field for " << columnName << " " << type;
    }
    if (!projection.contains("min")) {
        ythrow yexception() << "Invalid projection scheme: min are required field for " << columnName << " " << type;
    }
    if (!projection.contains("max")) {
        ythrow yexception() << "Invalid projection scheme: max are required field for " << columnName << " " << type;
    }
    ColumnPartitioningConfig columnConfig;
    columnConfig.Name = columnName;
    columnConfig.Type = ToType(type);
    for (const auto& p: projection) {
        if (p.first == "type") {
            // already processed
        } else if (p.first == "min") {
            columnConfig.Min = GetIntOrThrow(p.second, "The min must be a number");
        } else if (p.first == "max") {
            columnConfig.Max = GetIntOrThrow(p.second, "The max must be a number");
        } else if (p.first == "interval") {
            columnConfig.Interval = GetIntOrThrow(p.second, "The interval must be a number");
        } else if (p.first == "digits") {
            columnConfig.Digits = GetIntOrThrow(p.second, "The digits must be a number");
        } else {
            ythrow yexception() << "Invalid projection scheme: integer element must include only type, min, max, interval, digits but got " << p.first;
        }
    }
    config.Rules.push_back(columnConfig);
}

void DoParseDateType(const TString& columnName, const TString& type, const TMap<TString, NSc::TValue>& projection, ExplicitPartitioningConfig& config) {
    if (!projection.contains("type")) {
        ythrow yexception() << "Invalid projection scheme: type are required field for " << columnName << " " << type;
    }
    if (!projection.contains("min")) {
        ythrow yexception() << "Invalid projection scheme: min are required field for " << columnName << " " << type;
    }
    if (!projection.contains("max")) {
        ythrow yexception() << "Invalid projection scheme: max are required field for " << columnName << " " << type;
    }
    if (!projection.contains("format")) {
        ythrow yexception() << "Invalid projection scheme: format are required field for " << columnName << " " << type;
    }
    ColumnPartitioningConfig columnConfig;
    columnConfig.Name = columnName;
    columnConfig.Type = ToType(type);
    for (const auto& p: projection) {
        if (p.first == "type") {
            // already processed
        } else if (p.first == "min") {
            columnConfig.From = GetStringOrThrow(p.second, "The min must be a string");
        } else if (p.first == "max") {
            columnConfig.To = GetStringOrThrow(p.second, "The max must be a string");
        } else if (p.first == "format") {
            columnConfig.Format = GetStringOrThrow(p.second, "The format must be a string");
        } else if (p.first == "interval") {
            columnConfig.Interval = GetIntOrThrow(p.second, "The interval must be a number");
        } else if (p.first == "unit") {
            columnConfig.IntervalUnit = ToIntervalUnit(GetStringOrThrow(p.second, "The unit must be a string"));
        } else {
            ythrow yexception() << "Invalid projection scheme: date element must include only type, min, max, format, interval, unit but got " << p.first;
        }
    }
    config.Rules.push_back(columnConfig);
}

void DoParseColumn(const TString& columnName, const TMap<TString, NSc::TValue>& projection, ExplicitPartitioningConfig& config) {
    auto it = projection.find("type");
    if (it == projection.end()) {
        ythrow yexception() << "Invalid projection scheme: type element must exist for the column" << columnName;
    }

    const TString type = GetStringOrThrow(it->second, "The type must be a string for column " + columnName);
    if (type == "enum") {
        DoParseEnumType(columnName, type, projection, config);
    } else if (type == "integer") {
        DoParseIntegerType(columnName, type, projection, config);
    } else if (type == "date") {
        DoParseDateType(columnName, type, projection, config);
    } else {
        ythrow yexception() << "Invalid projection scheme: type element should be one of enum, date, integer but got " << type;
    }
}

void DoParseProjection(TMap<TString, TMap<TString, NSc::TValue>> projection, ExplicitPartitioningConfig& config) {
    for (const auto& p: projection) {
        DoParseColumn(p.first, p.second, config);
    }
}

void DoGenerate(const std::vector<ColumnPartitioningConfig>& rules,
                TString locationTemplate,
                std::vector<ColumnWithValue>& columnsWithValue,
                TMap<TString, std::vector<ColumnWithValue>>& result,
                int pathsLimit,
                TInstant now,
                size_t p = 0);

void DoGenerateEnum(const std::vector<ColumnPartitioningConfig>& rules,
                    TString locationTemplate,
                    std::vector<ColumnWithValue>& columnsWithValue,
                    TMap<TString, std::vector<ColumnWithValue>>& result,
                    int pathsLimit,
                    TInstant now,
                    size_t p = 0) {
    const auto& rule = rules[p];
    for (const auto& value: rule.Values) {
        TString copyLocationTemplate = locationTemplate;
        ReplaceAll(copyLocationTemplate, "${" + rule.Name + "}", value);
        columnsWithValue.push_back(ColumnWithValue{.Name=rule.Name, .Type=NUdf::EDataSlot::String, .Value=value});
        DoGenerate(rules, copyLocationTemplate, columnsWithValue, result, pathsLimit, now, p + 1);
        columnsWithValue.pop_back();
    }
}

std::string fmtInteger(int32_t width, int64_t value)
{
    if (width > 64) {
         ythrow yexception() << "Digits cannot exceed 64, but received " << width;
    }
    if (width == 0) {
        return std::to_string(value);
    }
    char buf[65];
    snprintf(buf, 64, "%0*ld", width, value);
    return buf;
}

bool IsOverflow(int64_t a, int64_t b) {
    if (a ^ b < 0) {
        return false;
    }
    if (a > 0) {
        return b > std::numeric_limits<int64_t>::max() - a;
    }
    return b < std::numeric_limits<int64_t>::min() - a;
}

bool IsOverflow(uint64_t a, uint64_t b) {
    uint64_t diff = std::numeric_limits<int64_t>::max() - a;
    return b > diff;
}

void DoGenerateInteger(const std::vector<ColumnPartitioningConfig>& rules,
                       TString locationTemplate,
                       std::vector<ColumnWithValue>& columnsWithValue,
                       TMap<TString, std::vector<ColumnWithValue>>& result,
                       int pathsLimit,
                       TInstant now,
                       size_t p = 0) {
    const auto& rule = rules[p];
    for (int64_t i = rule.Min; i <= rule.Max; i += rule.Interval) {
        TString copyLocationTemplate = locationTemplate;
        ReplaceAll(copyLocationTemplate, "${" + rule.Name + "}", fmtInteger(rule.Digits, i));
        columnsWithValue.push_back(ColumnWithValue{.Name=rule.Name, .Type=NUdf::EDataSlot::Int64, .Value=ToString(i)});
        DoGenerate(rules, copyLocationTemplate, columnsWithValue, result, pathsLimit, now, p + 1);
        columnsWithValue.pop_back();

        if (IsOverflow(i, rule.Interval)) {
            return; // correct overflow handling
        }
    }
}

TDuration FromUnit(int64_t interval, EIntervalUnit unit) {
    switch (unit) {
    case EIntervalUnit::MILLISECONDS:
        return TDuration::MilliSeconds(interval);
    case EIntervalUnit::SECONDS:
        return TDuration::Seconds(interval);
    case EIntervalUnit::MINUTES:
        return TDuration::Minutes(interval);
    case EIntervalUnit::HOURS:
        return TDuration::Hours(interval);
    case EIntervalUnit::DAYS:
        return TDuration::Days(interval);
    case EIntervalUnit::WEEKS:
        return TDuration::Days(interval *  7);
    case EIntervalUnit::MONTHS:
        return TDuration::Days(interval *  30);
    case EIntervalUnit::YEARS:
        return TDuration::Days(interval * 365);
    default:
        ythrow yexception() << "Only the " << GetEnumAllNames<EIntervalUnit>() << " units are supported but got " << unit;
    }
}

TInstant ParseDate(const TString& dateStr, const TInstant& now) {
    try {
        size_t idx = 0;
        int64_t unixTime = std::stoll(dateStr, &idx);
        if (idx == dateStr.Size()) {
            return TInstant::FromValue(unixTime * 1000 * 1000);
        }
    } catch (const std::exception&) {
        // trying other options
    }

    const std::string pattern = "^\\s*(NOW)\\s*(([\\+\\-])\\s*([0-9]+)\\s*(YEARS?|MONTHS?|WEEKS?|DAYS?|HOURS?|MINUTES?|SECONDS?)\\s*)?$";
    std::regex word_regex(pattern);
    auto wordBegin = std::sregex_iterator(dateStr.begin(), dateStr.end(), word_regex);
    auto wordEnd = std::sregex_iterator();
    if (wordBegin != wordEnd) {
        auto it = wordBegin->begin();
        const TString sign = (it + 3)->str();
        const TString offset = (it + 4)->str();
        const TString unit = (it + 5)->str();
        if (sign) {
            return now + (sign.front() == '+' ? 1 : -1) * FromUnit(stoll(offset), ToIntervalUnit(unit));
        } else {
            return now;
        }
    }

    return TInstant::ParseIso8601(dateStr);
}

void DoGenerateDate(const std::vector<ColumnPartitioningConfig>& rules,
                    TString locationTemplate,
                    std::vector<ColumnWithValue>& columnsWithValue,
                    TMap<TString, std::vector<ColumnWithValue>>& result,
                    int pathsLimit,
                    TInstant now,
                    size_t p = 0) {
    const auto& rule = rules[p];
    const TInstant to = ParseDate(rule.To, now);
    const TDuration interval = FromUnit(rule.Interval, rule.IntervalUnit);
    for (TInstant current = ParseDate(rule.From, now); current <= to; current += interval) {
        TString copyLocationTemplate = locationTemplate;
        const TString time = current.FormatLocalTime(rule.Format.c_str());
        ReplaceAll(copyLocationTemplate, "${" + rule.Name + "}", time);
        columnsWithValue.push_back(ColumnWithValue{.Name=rule.Name, .Type=NUdf::EDataSlot::String, .Value=time});
        DoGenerate(rules, copyLocationTemplate, columnsWithValue, result, pathsLimit, now, p + 1);
        columnsWithValue.pop_back();

        if (IsOverflow(current.GetValue(), interval.GetValue())) {
            return; // correct overflow handling
        }
    }
}

void DoGenerate(const std::vector<ColumnPartitioningConfig>& rules,
                TString locationTemplate,
                std::vector<ColumnWithValue>& columnsWithValue,
                TMap<TString, std::vector<ColumnWithValue>>& result,
                int pathsLimit,
                TInstant now,
                size_t p) {
    if (rules.size() == p) {
        if (result.size() == static_cast<uint>(pathsLimit)) {
            ythrow yexception() << "The limit on the number of paths has been reached: " << result.size() << " of " << pathsLimit;
        }
        result[locationTemplate] = columnsWithValue;
        return;
    }

    const auto& rule = rules[p];
    switch (rule.Type) {
    case EType::ENUM:
        DoGenerateEnum(rules, locationTemplate, columnsWithValue, result, pathsLimit, now, p);
        break;
    case EType::INTEGER:
        DoGenerateInteger(rules, locationTemplate, columnsWithValue, result, pathsLimit, now, p);
        break;
    case EType::DATE:
        DoGenerateDate(rules, locationTemplate, columnsWithValue, result, pathsLimit, now, p);
        break;
    default:
        ythrow yexception() << "Only the enum, integer, date types are supported but got " << to_lower(ToString(rule.Type));
    }
}

void DoValidateTemplate(const ExplicitPartitioningConfig& config, const std::vector<TString>& partitionBy) {
    TSet<TString> vars;
    std::regex word_regex("\\$\\{(.*?)\\}");
    auto wordBegin = std::sregex_iterator(config.LocationTemplate.begin(), config.LocationTemplate.end(), word_regex);
    auto wordEnd = std::sregex_iterator();
    for (auto word = wordBegin; word != wordEnd; ++word) {
        vars.insert((word->begin() + 1)->str());
    }

    TSet<TString> columns;
    for (const auto& columnName: partitionBy) {
        columns.insert(columnName);
        if (!vars.contains(columnName)) {
            ythrow yexception() << "Template " << config.LocationTemplate << " must include ${" << columnName << "}";
        }
    }

    for (const auto& rule: config.Rules) {
        columns.insert(rule.Name);
        if (!vars.contains(rule.Name)) {
            ythrow yexception() << "Template " << config.LocationTemplate << " must include ${" << rule.Name << "}";
        }
    }

    for (const auto& var: vars) {
        if (!columns.contains(var)) {
            ythrow yexception() << "Colum named " << var << " does not exist for template " << config.LocationTemplate;
        }
    }
}

void AddProjection(const TStringBuf& key, const NSc::TValue& json, const TVector<TString>& path, TMap<TString, TMap<TString, NSc::TValue>>& projection, ExplicitPartitioningConfig& result) {
    if (path.size() != 3 && path.size() != 2) {
        ythrow yexception() << "The key must be three-component or two-component, but received " << key;
    }

    if (path.size() == 2 && path[1] != "enabled") {
        ythrow yexception() << "Unknown key " << key;
    }

    if (path.size() == 2) {
        result.Enabled = GetBoolOrThrow(json, "The projection.enabled must be a bool");
        return;
    }

    projection[path[1]][path[2]] = json;
}

void AddStorage(const TStringBuf& key, const NSc::TValue& json, const TVector<TString>& path, ExplicitPartitioningConfig& result) {
    if (path.size() != 3) {
        ythrow yexception() << "The key must be three-component, but received " << key;
    }

    if (path[1] != "location" || path[2] != "template") {
        ythrow yexception() << "The key must be storage.location.template, but received " << key;
    }

    result.LocationTemplate = GetStringOrThrow(json, "The storage.location.template must be a string");
}

}

ExplicitPartitioningConfig ParsePartitioningRules(const TString& config, const std::vector<TString>& partitionBy) {
    ExplicitPartitioningConfig result;
    if (partitionBy.empty()) {
        ythrow yexception() << "Partition by must always be specified";
    }

    if (!config) {
        for (const auto& columnName: partitionBy) {
            result.LocationTemplate += "/" + columnName + "=${" +  columnName + "}";
        }
        return result;
    }
    NSc::TValue json = NSc::TValue::FromJsonThrow(config, NSc::TJsonOpts::JO_PARSER_DISALLOW_DUPLICATE_KEYS | NSc::TJsonOpts::JO_SORT_KEYS);
    if (!json.IsDict()) {
        ythrow yexception() << "Invalid projection scheme: top-level element must be a dictionary";
    }

    TMap<TString, TMap<TString, NSc::TValue>> projection;

    for (const auto& p: json.GetDict()) {
        const auto path = GetPath(p.first);
        if (path.empty()) {
            ythrow yexception() << "Invalid key: key should start with storage or projection, but got an empty value";
        }
        const TString& kind = path.front();
        if (kind == "projection") {
            AddProjection(p.first, p.second, path, projection, result);
        } else if (kind == "storage") {
            AddStorage(p.first, p.second, path, result);
        } else {
            ythrow yexception() << "Invalid key: key should start with storage or projection, but got " << p.first;
        }
    }
    DoParseProjection(projection, result);
    DoValidateTemplate(result, partitionBy);
    return result;
}

std::vector<ExpandedPartitioningRule> ExpandPartitioningRules(const ExplicitPartitioningConfig& config, int pathsLimit) {
    if (!config.Enabled) {
        return config.LocationTemplate
                ? std::vector{ExpandedPartitioningRule{.Path=config.LocationTemplate}}
                : std::vector<ExpandedPartitioningRule>{};
    }
    auto now = TInstant::Now();
    TMap<TString, std::vector<ColumnWithValue>> result;
    std::vector<ColumnWithValue> columns;
    DoGenerate(config.Rules, config.LocationTemplate, columns, result, pathsLimit, now);
    std::vector<ExpandedPartitioningRule> rules;
    for (const auto& p: result) {
        rules.push_back(ExpandedPartitioningRule{.Path=p.first, .ColumnValues=p.second});
    }
    return rules;
}

}
