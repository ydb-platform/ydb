#include "yql_s3_path_generator.h"

#include <library/cpp/scheme/scheme.h>
#include <regex>
#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/datetime/datetime.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

namespace NYql::NPathGenerator {

namespace {

TInstant Strptime(const TString& input, const TString& format) {
    struct tm inputTm;
    memset(&inputTm, 0, sizeof(tm));
    inputTm.tm_mday = 1;
    if (strptime(input.data(), format.data(), &inputTm) != nullptr) {
        const time_t seconds = TimeGM(&inputTm);
        if (seconds != static_cast<time_t>(-1)) {
            return TInstant::Seconds(seconds);
        }
    }
    ythrow yexception() << "Can't parse date " << input << " in format " << format;
}

TString Strftime(const char* format, TInstant time) {
    struct tm tm{};
    time_t clock = time.Seconds();
    localtime_r(&clock, &tm);
    size_t size = Max<size_t>(strlen(format) * 2 + 1, 107);
    TTempBuf buf(size);
    int r = strftime(buf.Data(), buf.Size(), format, &tm);
    if (r != 0) {
        return TString(buf.Data(), r);
    }
    ythrow yexception() << "Can't format date " << time << " in format " << format;
}


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

IPathGenerator::EIntervalUnit ToIntervalUnit(const TString& unit) {
    const auto names = GetEnumNames<IPathGenerator::EIntervalUnit>();
    for (const auto& name: names) {
        if (name.second == unit) {
            return name.first;
        }
    }
    ythrow yexception() << "Invalid projection scheme: unit " << unit << " must be one of " << GetEnumAllNames<IPathGenerator::EIntervalUnit>();
}

TMap<IPathGenerator::EType, TString> ToLowerType() {
    TMap<IPathGenerator::EType, TString> result;
    for (const auto& p: GetEnumNames<IPathGenerator::EType>()) {
        result[p.first] = to_lower(p.second);
    }
    return result;
};

IPathGenerator::EType ToType(const TString& type) {
    static TMap<IPathGenerator::EType, TString> enumNames{ToLowerType()};
    for (const auto& name: enumNames) {
        if (name.second == type) {
            return name.first;
        }
    }
    ythrow yexception() << "Invalid projection scheme: type " << type << " must be one of " << to_lower(GetEnumAllNames<IPathGenerator::EType>());
}

std::string fmtInteger(int32_t width, i64 value)
{
    if (width > 64) {
         ythrow yexception() << "Digits cannot exceed 64, but received " << width;
    }
    if (width == 0) {
        return std::to_string(value);
    }
    char buf[65];
    snprintf(buf, sizeof(buf), "%0*lld", width, static_cast<long long>(value));
    return buf;
}

bool IsOverflow(i64 a, i64 b) {
    if ((a ^ b) < 0) {
        return false;
    }
    if (a > 0) {
        return b > std::numeric_limits<i64>::max() - a;
    }
    return b < std::numeric_limits<i64>::min() - a;
}

bool IsOverflow(ui64 a, ui64 b) {
    uint64_t diff = std::numeric_limits<ui64>::max() - a;
    return b > diff;
}

ui64 AbsToUi64(i64 value) {
    if (value >= 0) {
        return value;
    }
    if (value == std::numeric_limits<i64>::min()) {
        return (ui64)std::numeric_limits<i64>::max() + 1;
    }
    return -value;
}

bool IsOverflow(ui64 a, i64 b) {
    return b > 0 ? IsOverflow(a, (ui64)b) : a < AbsToUi64(b);
}

TDuration FromUnit(int64_t interval, IPathGenerator::EIntervalUnit unit) {
    switch (unit) {
    case IPathGenerator::EIntervalUnit::MILLISECONDS:
        return TDuration::MilliSeconds(interval);
    case IPathGenerator::EIntervalUnit::SECONDS:
        return TDuration::Seconds(interval);
    case IPathGenerator::EIntervalUnit::MINUTES:
        return TDuration::Minutes(interval);
    case IPathGenerator::EIntervalUnit::HOURS:
        return TDuration::Hours(interval);
    case IPathGenerator::EIntervalUnit::DAYS:
        return TDuration::Days(interval);
    case IPathGenerator::EIntervalUnit::WEEKS:
        return TDuration::Days(interval *  7);
    case IPathGenerator::EIntervalUnit::MONTHS: // external special handling
    case IPathGenerator::EIntervalUnit::YEARS:
    default:
        ythrow yexception() << "Only the " << GetEnumAllNames<IPathGenerator::EIntervalUnit>() << " units are supported but got " << unit;
    }
}

TInstant DoAddMonths(TInstant current, i64 months) {
    NKikimr::NMiniKQL::TScopedAlloc alloc{__LOCATION__};
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("MemoryAddMonths");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);
    return DateTime::DoAddMonths(current, months, builder.GetDateBuilder());
}

TInstant DoAddYears(TInstant current, i64 years) {
    NKikimr::NMiniKQL::TScopedAlloc alloc{__LOCATION__};
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("MemoryAddYears");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);
    return DateTime::DoAddYears(current, years, builder.GetDateBuilder());
}

TInstant AddUnit(TInstant current, int64_t interval, IPathGenerator::EIntervalUnit unit) {
    if (unit == IPathGenerator::EIntervalUnit::MONTHS) {
        return DoAddMonths(current, interval);
    }

    if (unit == IPathGenerator::EIntervalUnit::YEARS) {
        return DoAddYears(current, interval);
    }

    const TDuration delta = FromUnit(abs(interval), unit);
    if (delta.GetValue() > std::numeric_limits<i64>::max()) {
        ythrow yexception() << "Interval is overflowed";
    }

    const i64 deltaValue = (interval > 0 ? 1LL : -1LL) * delta.GetValue();
    if (IsOverflow(current.GetValue(), deltaValue)) {
        ythrow yexception() << "Timestamp is overflowed";
    }

    return interval > 0 ? current + delta : current - delta;
}

TInstant ParseDate(const TString& dateStr, const TInstant& now) {
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
            return AddUnit(now, (sign.front() == '+' ? 1 : -1) * stoll(offset), ToIntervalUnit(unit));
        } else {
            return now;
        }
    }

    return TInstant::ParseIso8601(dateStr);
}

TString FormatColumnValues(const std::vector<IPathGenerator::TColumnWithValue>& columnsWithValue) {
    std::vector<TString> parts;
    std::transform(columnsWithValue.cbegin(), columnsWithValue.cend(),
                   std::back_inserter(parts),
                   [](auto v) { return TStringBuilder{} << "${" << v.Name << "} = " << v.Value; });
    return TStringBuilder{} << "{ " << JoinSeq(", ", parts) << " }";
}

}

struct TPathGenerator: public IPathGenerator {
    TExplicitPartitioningConfig Config;
    TRules Rules;
    TMap<TString, TColumnPartitioningConfig> ColumnConfig; // column name -> config
    TMap<TString, NUdf::EDataSlot> Columns;

public:
    TPathGenerator(const TString& projection, const std::vector<TString>& partitionedBy, const TMap<TString, NUdf::EDataSlot>& columns, size_t pathsLimit)
        : Columns(columns)
    {
        try {
            ParsePartitioningRules(projection, partitionedBy);
            ExpandPartitioningRules(pathsLimit);
        } catch (const yexception& e) {
            throw yexception() << "projection error: " << e.what();
        } catch (...) {
            throw yexception() << "projection error: " << CurrentExceptionMessage();
        }
    }

    TString Format(const TStringBuf& columnName, const TStringBuf& dataValue) const override {
        auto it = ColumnConfig.find(columnName);
        if (it == ColumnConfig.end()) {
            ythrow yexception() << columnName << " column not found in config";
        }
        const auto& config = it->second;
        switch (config.Type) {
        case IPathGenerator::EType::UNDEFINED: {
            ythrow yexception() << columnName << " column has undefined type";
        }
        case IPathGenerator::EType::ENUM: {
            if (Find(config.Values, dataValue) == config.Values.end()) {
                ythrow yexception() << dataValue << " data not found as enum item";
            }
            return TString{dataValue};
        }
        case IPathGenerator::EType::INTEGER: {
            i64 value = 0;
            if (!TryFromString(dataValue.Data(), dataValue.size(), value)) {
                ythrow yexception() << dataValue << " data is not a int64";
            }
            return fmtInteger(config.Digits, value);
        }
        case IPathGenerator::EType::DATE: {
            TInstant time = TInstant::ParseIso8601(dataValue);
            return Strftime(config.Format.c_str(), time);
        }
        break;
        }
    }

    TString Parse(const TStringBuf& columnName, const TStringBuf& pathValue) const override {
        auto it = ColumnConfig.find(columnName);
        if (it == ColumnConfig.end()) {
            ythrow yexception() << columnName << " column not found in config";
        }
        const auto& config = it->second;
        switch (config.Type) {
        case IPathGenerator::EType::UNDEFINED: {
            ythrow yexception() << columnName << " column has undefined type";
        }
        case IPathGenerator::EType::ENUM: {
            if (Find(config.Values, pathValue) == config.Values.end()) {
                ythrow yexception() << pathValue << " value not found as enum item";
            }
            return TString{pathValue};
        }
        case IPathGenerator::EType::INTEGER: {
            int64_t value = 0;
            if (!TryFromString(pathValue.Data(), pathValue.size(), value)) {
                ythrow yexception() << pathValue << " value is not a int64";
            }
            return std::to_string(value);
        }
        case IPathGenerator::EType::DATE: {
            TInstant time = Strptime(TString{pathValue}, config.Format);
            return time.ToStringLocal();
        }
        break;
        }
    }

    const TRules& GetRules() const override {
        return Rules;
    }

    const TExplicitPartitioningConfig& GetConfig() const override {
        return Config;
    }

private:
    // Parse
    void ParsePartitioningRules(const TString& config, const std::vector<TString>& partitionedBy) {
        if (partitionedBy.empty()) {
            ythrow yexception() << "Partition by must always be specified";
        }

        if (!config) {
            TStringBuilder builder;
            for (const auto& columnName: partitionedBy) {
                builder << columnName << "=${" <<  columnName << "}/";
            }
            Config.LocationTemplate = builder;
            return;
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
                AddProjection(p.first, p.second, path, projection);
            } else if (kind == "storage") {
                AddStorage(p.first, p.second, path);
            } else {
                ythrow yexception() << "Invalid key: key should start with storage or projection, but got " << p.first;
            }
        }
        DoParseProjection(projection);
        DoValidateTemplate(partitionedBy);
        for (const auto& config: Config.Rules) {
            ColumnConfig[config.Name] = config;
        }
    }

    void AddProjection(const TStringBuf& key, const NSc::TValue& json, const TVector<TString>& path, TMap<TString, TMap<TString, NSc::TValue>>& projection) {
        if (path.size() != 3 && path.size() != 2) {
            ythrow yexception() << "The key must be three-component or two-component, but received " << key;
        }

        if (path.size() == 2 && path[1] != "enabled") {
            ythrow yexception() << "Unknown key " << key;
        }

        if (path.size() == 2) {
            Config.Enabled = GetBoolOrThrow(json, "The projection.enabled must be a bool");
            return;
        }

        projection[path[1]][path[2]] = json;
    }

    void AddStorage(const TStringBuf& key, const NSc::TValue& json, const TVector<TString>& path) {
        if (path.size() != 3) {
            ythrow yexception() << "The key must be three-component, but received " << key;
        }

        if (path[1] != "location" || path[2] != "template") {
            ythrow yexception() << "The key must be storage.location.template, but received " << key;
        }

        TString locationTemplate = GetStringOrThrow(json, "The storage.location.template must be a string");
        // normalize: no slash at the begginging, single slash at the end
        locationTemplate = StripString(locationTemplate, EqualsStripAdapter('/'));
        Config.LocationTemplate = TStringBuilder() << locationTemplate << '/';
    }


    void DoParseEnumType(const TString& columnName, const TString& type, const TMap<TString, NSc::TValue>& projection) {
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
                TString valuesStr = GetStringOrThrow(p.second, "The values must be a string");
                auto values = StringSplitter(valuesStr).Split(',').SkipEmpty().ToList<TString>();
                for (auto& v : values) {
                    v = StripString(v);
                }
                Config.Rules.push_back(IPathGenerator::TColumnPartitioningConfig{.Type=ToType(type), .Name=columnName, .Values=std::move(values)});
            } else {
                ythrow yexception() << "Invalid projection scheme: enum element must include only type or values (as string) but got " << p.first;
            }
        }
    }

    void DoParseIntegerType(const TString& columnName, const TString& type, const TMap<TString, NSc::TValue>& projection) {
        if (!projection.contains("type")) {
            ythrow yexception() << "Invalid projection scheme: type are required field for " << columnName << " " << type;
        }
        if (!projection.contains("min")) {
            ythrow yexception() << "Invalid projection scheme: min are required field for " << columnName << " " << type;
        }
        if (!projection.contains("max")) {
            ythrow yexception() << "Invalid projection scheme: max are required field for " << columnName << " " << type;
        }
        IPathGenerator::TColumnPartitioningConfig columnConfig;
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
        Config.Rules.push_back(columnConfig);
    }

    void DoParseDateType(const TString& columnName, const TString& type, const TMap<TString, NSc::TValue>& projection) {
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
        IPathGenerator::TColumnPartitioningConfig columnConfig;
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
        Config.Rules.push_back(columnConfig);
    }

    void DoParseColumn(const TString& columnName, const TMap<TString, NSc::TValue>& projection) {
        auto it = projection.find("type");
        if (it == projection.end()) {
            ythrow yexception() << "Invalid projection scheme: type element must exist for the column" << columnName;
        }

        const TString type = GetStringOrThrow(it->second, "The type must be a string for column " + columnName);
        if (type == "enum") {
            DoParseEnumType(columnName, type, projection);
        } else if (type == "integer") {
            DoParseIntegerType(columnName, type, projection);
        } else if (type == "date") {
            DoParseDateType(columnName, type, projection);
        } else {
            ythrow yexception() << "Invalid projection scheme: type element should be one of enum, date, integer but got " << type;
        }
    }

    void DoParseProjection(TMap<TString, TMap<TString, NSc::TValue>> projection) {
        for (const auto& p: projection) {
            DoParseColumn(p.first, p.second);
        }
    }

    void DoValidateTemplate(const std::vector<TString>& partitionedBy) {
        TSet<TString> vars;
        std::regex word_regex("\\$\\{(.*?)\\}");
        auto wordBegin = std::sregex_iterator(Config.LocationTemplate.begin(), Config.LocationTemplate.end(), word_regex);
        auto wordEnd = std::sregex_iterator();
        for (auto word = wordBegin; word != wordEnd; ++word) {
            vars.insert((word->begin() + 1)->str());
        }

        TSet<TString> partitionedByColumns;
        for (const auto& columnName: partitionedBy) {
            partitionedByColumns.insert(columnName);
            if (!vars.contains(columnName)) {
                ythrow yexception() << "Template " << Config.LocationTemplate << " must include ${" << columnName << "}";
            }
        }

        TSet<TString> rulesColumns;
        for (const auto& rule: Config.Rules) {
            rulesColumns.insert(rule.Name);
            if (!vars.contains(rule.Name)) {
                ythrow yexception() << "Template " << Config.LocationTemplate << " must include ${" << rule.Name << "}";
            }
        }

        for (const auto& var: vars) {
            if (!partitionedByColumns.contains(var)) {
                ythrow yexception() << "Partitioned by column named " << var << " does not exist for template " << Config.LocationTemplate;
            }
            if (!rulesColumns.contains(var)) {
                ythrow yexception() << "Projection column named " << var << " does not exist for template " << Config.LocationTemplate;
            }
        }
    }

    // Generate
    void ExpandPartitioningRules(size_t pathsLimit) {
        if (!Config.Enabled) {
            Rules = Config.LocationTemplate
                    ? TRules{TExpandedPartitioningRule{.Path=Config.LocationTemplate}}
                    : TRules{};
            return;
        }
        auto now = TInstant::Now();
        TMap<TString, std::vector<TColumnWithValue>> result;
        std::vector<TColumnWithValue> columns;
        DoGenerate(Config.Rules, Config.LocationTemplate, columns, result, pathsLimit, now);
        for (const auto& p: result) {
            Rules.push_back(TExpandedPartitioningRule{.Path=p.first, .ColumnValues=p.second});
        }

        if (Rules.empty()) {
            ythrow yexception() << "The projection contains an empty set of paths";
        }
    }

    void DoGenerateDate(const std::vector<TColumnPartitioningConfig>& rules,
                    TString locationTemplate,
                    std::vector<TColumnWithValue>& columnsWithValue,
                    TMap<TString, std::vector<TColumnWithValue>>& result,
                    size_t pathsLimit,
                    TInstant now,
                    size_t p = 0) {
        const auto& rule = rules[p];
        const TInstant to = ParseDate(rule.To, now);
        for (TInstant current = ParseDate(rule.From, now); current <= to; ) {
            TString copyLocationTemplate = locationTemplate;
            const TString time = Strftime(rule.Format.c_str(), current);
            ReplaceAll(copyLocationTemplate, "${" + rule.Name + "}", time);
            columnsWithValue.push_back(CreateDateColumnWithValue(rule.Name, current));
            DoGenerate(rules, copyLocationTemplate, columnsWithValue, result, pathsLimit, now, p + 1);
            columnsWithValue.pop_back();

            try {
                current = AddUnit(current, rule.Interval, rule.IntervalUnit);
            } catch (...) {
                // correct overflow handling
                break;
            }
        }
    }

    void DoGenerate(const std::vector<TColumnPartitioningConfig>& rules,
                    TString locationTemplate,
                    std::vector<TColumnWithValue>& columnsWithValue,
                    TMap<TString, std::vector<TColumnWithValue>>& result,
                    size_t pathsLimit,
                    TInstant now,
                    size_t p = 0) {
        if (rules.size() == p) {
            if (result.size() == pathsLimit) {
                ythrow yexception() << "The limit on the number of paths has been reached: " << result.size() << " of " << pathsLimit;
            }

            auto pib = result.emplace(locationTemplate, columnsWithValue);
            if (!pib.second) {
                ythrow yexception() << "Location path " << locationTemplate << " is composed by different projection value sets " << FormatColumnValues(pib.first->second) << " and " << FormatColumnValues(columnsWithValue);
            }
            return;
        }

        const auto& rule = rules[p];
        switch (rule.Type) {
        case IPathGenerator::EType::ENUM:
            DoGenerateEnum(rules, locationTemplate, columnsWithValue, result, pathsLimit, now, p);
            break;
        case IPathGenerator::EType::INTEGER:
            DoGenerateInteger(rules, locationTemplate, columnsWithValue, result, pathsLimit, now, p);
            break;
        case IPathGenerator::EType::DATE:
            DoGenerateDate(rules, locationTemplate, columnsWithValue, result, pathsLimit, now, p);
            break;
        default:
            ythrow yexception() << "Only the enum, integer, date types are supported but got " << to_lower(ToString(rule.Type));
        }
    }

    void DoGenerateEnum(const std::vector<IPathGenerator::TColumnPartitioningConfig>& rules,
                    TString locationTemplate,
                    std::vector<IPathGenerator::TColumnWithValue>& columnsWithValue,
                    TMap<TString, std::vector<IPathGenerator::TColumnWithValue>>& result,
                    size_t pathsLimit,
                    TInstant now,
                    size_t p = 0) {
        const auto& rule = rules[p];
        for (const auto& value: rule.Values) {
            TString copyLocationTemplate = locationTemplate;
            ReplaceAll(copyLocationTemplate, "${" + rule.Name + "}", value);
            columnsWithValue.push_back(IPathGenerator::TColumnWithValue{.Name=rule.Name, .Type=NUdf::EDataSlot::String, .Value=value});
            DoGenerate(rules, copyLocationTemplate, columnsWithValue, result, pathsLimit, now, p + 1);
            columnsWithValue.pop_back();
        }
    }

    void DoGenerateInteger(const std::vector<IPathGenerator::TColumnPartitioningConfig>& rules,
                       TString locationTemplate,
                       std::vector<IPathGenerator::TColumnWithValue>& columnsWithValue,
                       TMap<TString, std::vector<IPathGenerator::TColumnWithValue>>& result,
                       size_t pathsLimit,
                       TInstant now,
                       size_t p = 0) {
        const auto& rule = rules[p];
        for (i64 i = rule.Min; i <= rule.Max; i += rule.Interval) {
            TString copyLocationTemplate = locationTemplate;
            ReplaceAll(copyLocationTemplate, "${" + rule.Name + "}", fmtInteger(rule.Digits, i));
            columnsWithValue.push_back(CreateIntegerColumnWithValue(rule.Name, i));
            DoGenerate(rules, copyLocationTemplate, columnsWithValue, result, pathsLimit, now, p + 1);
            columnsWithValue.pop_back();

            if (IsOverflow(i, rule.Interval)) {
                return; // correct overflow handling
            }
        }
    }

    IPathGenerator::TColumnWithValue CreateDateColumnWithValue(const TString& name, const TInstant& current) {
        auto it = Columns.find(name);
        auto slot = it == Columns.end() ? NUdf::EDataSlot::Date : it->second;
        switch (slot) {
            case NUdf::EDataSlot::Datetime:
                return {.Name=name, .Type=NUdf::EDataSlot::Datetime, .Value=Strftime("%FT%TZ", current)};
            default:
            break;
        }

        return {.Name=name, .Type=NUdf::EDataSlot::Date, .Value=Strftime("%F", current)};
    }

    IPathGenerator::TColumnWithValue CreateIntegerColumnWithValue(const TString& name, int64_t value) {
        auto it = Columns.find(name);
        auto slot = it == Columns.end() ? NUdf::EDataSlot::Int64 : it->second;
        switch (slot) {
            case NUdf::EDataSlot::Int32:
                CheckCastInt32(value, name);
                return {.Name=name, .Type=NUdf::EDataSlot::Int32, .Value=ToString(value)};
            case NUdf::EDataSlot::Uint32:
                CheckCastUint32(value, name);
                return {.Name=name, .Type=NUdf::EDataSlot::Uint32, .Value=ToString(value)};
            case NUdf::EDataSlot::Uint64:
                CheckCastUint64(value, name);
                return {.Name=name, .Type=NUdf::EDataSlot::Uint64, .Value=ToString(value)};
            default:
            break;
        }
        return {.Name=name, .Type=NUdf::EDataSlot::Int64, .Value=ToString(value)};
    }

    static void CheckCastInt32(int64_t value, const TString& column) {
        if (std::numeric_limits<int32_t>::min() <= value && value <= std::numeric_limits<int32_t>::max()) {
            return;
        }
        ythrow yexception() << "The value " << value << " is not representable as an int32 type for column " << column;
    }

    static void CheckCastUint32(int64_t value, const TString& column) {
        if (value >= 0 && value <= std::numeric_limits<uint32_t>::max()) {
            return;
        }
        ythrow yexception() << "The value " << value << " is not representable as an uint32 type for column " << column;
    }

    static void CheckCastUint64(int64_t value, const TString& column) {
        if (value >= 0) {
            return;
        }
        ythrow yexception() << "The value " << value << " is not representable as an uint64 type for column " << column;
    }
};

TPathGeneratorPtr CreatePathGenerator(const TString& projection, const std::vector<TString>& partitionedBy, const TMap<TString, NUdf::EDataSlot>& columns, size_t pathsLimit) {
    return std::make_shared<TPathGenerator>(projection, partitionedBy, columns, pathsLimit);
}

}
