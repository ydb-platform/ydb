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

struct TOperatorStat {
    TString OperatorId;
    TOperatorType OperatorType;
    // Operator Output
    i64 Rows = 0;
    i64 Bytes = 0;
    // Operator specific stats
};

class TDqTaskCountersProvider : public NYql::NUdf::ICountersProvider {

public:
    NYql::NUdf::TCounter GetCounter(const NUdf::TStringRef& module, const NUdf::TStringRef& name, bool deriv) override;

    NYql::NUdf::TScopedProbe GetScopedProbe(const NUdf::TStringRef& module, const NUdf::TStringRef& name) override;

    THashMap<TString, TOperatorStat> OperatorStat;
};

} // namespace NYql::NDq
