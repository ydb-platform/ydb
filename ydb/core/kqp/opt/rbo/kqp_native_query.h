#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/common/kqp_query_metadata.h>

namespace NKikimr::NKqp {

struct TNativeQuery {
    struct TResult {
        TVector<TString> ColumnHints;
        ui64 RowsLimit = 0;
    };

    TVector<TIntrusivePtr<TOpRoot>> Roots;
    TVector<TResult> Results;
    TQueryExecutionMetadata Execution;
};

} // namespace NKikimr::NKqp
