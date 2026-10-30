#pragma once

#include "cluster_tracker.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NPQ::NClusterTracker {

TString NormalizeDiscoveryHost(TStringBuf authority);

// Last path component of an x-ydb-database value. `/Root` and an empty path have no account.
TString AccountNameFromDatabase(TStringBuf database);

TVector<TString> ParseFnxClusterCsv(TStringBuf csv);

// `tableName` next to the Cluster table: `.../Cluster` -> `.../<tableName>`.
TString SiblingTablePath(TStringBuf clusterTablePath, TStringBuf tableName);

TString MakeListClustersQuery(TStringBuf clusterTablePath, TStringBuf versionTablePath);
TString MakeListBalancersQuery(TStringBuf balancerTablePath, TStringBuf versionTablePath);
TString MakeListAccountsQuery(TStringBuf accountTablePath);
TString MakeCreateClusterQuery(TStringBuf clusterTablePath);
TString MakeCreateBalancerQuery(TStringBuf balancerTablePath);
TString MakeCreateVersionsQuery(TStringBuf versionTablePath);

bool IssuesLookLikeAlreadyExists(TStringBuf issues);
bool IssuesLookLikeMissingTable(TStringBuf issues);
bool IssuesLookLikeMissingColumn(TStringBuf issues);
bool IssuesLookLikeMissingVersionsTable(TStringBuf issues);
bool IssuesLookLikeClusterSchemaGone(TStringBuf issues);

} // namespace NKikimr::NPQ::NClusterTracker
