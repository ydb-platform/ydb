#pragma once

#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <util/generic/string.h>
#include <vector>

namespace NYql::NPathGenerator {

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

struct ColumnPartitioningConfig {
    EType Type = EType::UNDEFINED;
    TString Name;
    TString Format;
    TString From;
    TString To;
    int64_t Min = 0;
    int64_t Max = 0;
    EIntervalUnit IntervalUnit = EIntervalUnit::DAYS;
    int64_t Interval = 1;
    int32_t Digits = 0;
    std::vector<TString> Values;
};

struct ExplicitPartitioningConfig {
    bool Enabled = false;
    TString LocationTemplate;
    std::vector<ColumnPartitioningConfig> Rules;
};

ExplicitPartitioningConfig ParsePartitioningRules(const TString& config, const std::vector<TString>& partitionBy);

struct ColumnWithValue {
    TString Name;
    NUdf::EDataSlot Type;
    TString Value;
};

struct ExpandedPartitioningRule {
    TString Path;
    std::vector<ColumnWithValue> ColumnValues;
};

std::vector<ExpandedPartitioningRule> ExpandPartitioningRules(const ExplicitPartitioningConfig& config, int pathsLimit);

}
