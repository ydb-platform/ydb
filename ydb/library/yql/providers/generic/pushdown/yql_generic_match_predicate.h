#pragma once

#include <ydb/library/yql/providers/generic/pushdown/yql_generic_column_statistics.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

#include <util/generic/map.h>

namespace NYql::NGenericPushDown {

    bool MatchPredicate(const TMap<TString, TColumnStatistics>& columns, const NYql::NConnector::NApi::TPredicate& predicate);

}
