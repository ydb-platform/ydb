#pragma once

#include <yql/essentials/core/yql_statistics.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <map>

namespace NYql {

struct TS3ProviderStatistics : public IProviderStatistics {
    ui64 RawByteSize = 0;
    bool OverrideApplied = false;
    double FullRawRowAvgSize = 0.0;
    double FullDecodedRowAvgSize = 0.0;
    double PrunedRawRowAvgSize = 0.0;
    double PrunedDecodedRowAvgSize = 0.0;
    TString Format;
    TString Compression;
    std::unordered_map<ui64, double> Costs;
};

double GetDqSourceWrapBaseCost(const NNodes::TDqSourceWrapBase& wrapBase, TTypeAnnotationContext& typeCtx);

} // namespace NYql
