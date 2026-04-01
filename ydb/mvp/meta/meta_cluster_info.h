#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/hash.h>

namespace NMVP {

TString BuildClusterInfoQuery(TStringBuf rootDomain);
NYdb::TParams BuildClusterInfoQueryParams(TStringBuf clusterName);
bool TryExtractClusterInfo(const NYdb::TResultSet& resultSet, THashMap<TString, TString>& clusterInfo);

} // namespace NMVP
