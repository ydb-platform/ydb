#pragma once

#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <util/generic/map.h>
#include <util/generic/string.h>

#include <vector>


namespace NYql::NPathGenerator {

struct IPathGenerator {
    enum class EType {
        UNDEFINED = 0,
        ENUM = 1,
        INTEGER = 2,
        DATE = 3
    };

    enum class EIntervalUnit {
        UNDEFINED = 0,
        MILLISECONDS = 1,
        SECONDS = 2,
        MINUTES = 3,
        HOURS = 4,
        DAYS = 5,
        WEEKS = 6,
        MONTHS = 7,
        YEARS = 8
    };

    struct TColumnPartitioningConfig {
        EType Type = EType::UNDEFINED;
        TString Name;
        TString Format;
        TString From;
        TString To;
        i64 Min = 0;
        i64 Max = 0;
        EIntervalUnit IntervalUnit = EIntervalUnit::DAYS;
        i64 Interval = 1;
        i32 Digits = 0;
        std::vector<TString> Values;
    };

    struct TExplicitPartitioningConfig {
        bool Enabled = false;
        TString LocationTemplate;
        std::vector<TColumnPartitioningConfig> Rules;
    };

    struct TColumnWithValue {
        TString Name;
        NUdf::EDataSlot Type;
        TString Value;
    };

    struct TExpandedPartitioningRule {
        TString Path;
        std::vector<TColumnWithValue> ColumnValues;
    };

    using TRules = std::vector<TExpandedPartitioningRule>;

    virtual TString Format(const TStringBuf& columnName, const TStringBuf& dataValue) const = 0;
    virtual TString Parse(const TStringBuf& columnName, const TStringBuf& pathValue) const = 0;
    virtual const TRules& GetRules() const = 0;
    virtual const TExplicitPartitioningConfig& GetConfig() const = 0;
    virtual ~IPathGenerator() = default;
};

using TPathGeneratorPtr = std::shared_ptr<const IPathGenerator>;

TPathGeneratorPtr CreatePathGenerator(
    const TString& projection,
    const std::vector<TString>& partitionedBy,
    const TMap<TString, NUdf::EDataSlot>& columns = {},
    size_t pathsLimit = 50000);

}
