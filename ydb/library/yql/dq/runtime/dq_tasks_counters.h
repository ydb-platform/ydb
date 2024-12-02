#pragma once

#include <yql/essentials/minikql/comp_nodes/mkql_counters.h>

#include <util/generic/hash.h>

namespace NYql::NDq {

// all values are signed to be NYql::NUdf::TCounter-compatible
// negative values are not used and discarded before export (to proto)

enum TOperatorType {
    Unknown,
    Join,
    Filter,
    Aggregation
};

struct TSpillingStat {
    i64 WriteRows = 0;
    i64 WriteBytes = 0;
    i64 ReadRows = 0;
    i64 ReadBytes = 0;
};

struct TFilterStat {
    i64 Positive = 0;
    i64 Negative = 0;
};

struct TJoinInputStat {
    TFilterStat Bloom;
    i64 BloomSkipped = 0;
};

struct TJoinStats {
    TJoinInputStat Left;
    TJoinInputStat Right;
    TSpillingStat Spilling;
};

struct TOperatorStat {
    TString OperatorId;
    TOperatorType OperatorType;
    // Operator Output
    i64 Rows = 0;
    i64 Bytes = 0;
    // Operator specific stats
    TJoinStats Join;
    TFilterStat Filter;
};

class TDqTaskCountersProvider : public NYql::NUdf::ICountersProvider {

public:
    NYql::NUdf::TCounter GetCounter(const NUdf::TStringRef& module, const NUdf::TStringRef& name, bool deriv) override;

    NYql::NUdf::TScopedProbe GetScopedProbe(const NUdf::TStringRef& module, const NUdf::TStringRef& name) override;

    THashMap<TString, TOperatorStat> OperatorStat;
};

} // namespace NYql::NDq
