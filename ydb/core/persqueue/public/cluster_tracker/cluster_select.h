#pragma once

#include "cluster_tracker.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NPQ::NClusterTracker {

TString NormalizeDiscoveryHost(TStringBuf authority);

TVector<TString> ParseFnxClusterCsv(TStringBuf csv);

TString BalancerTablePathFromClusterTable(TStringBuf clusterTablePath);

TString MakeListClustersQuery(TStringBuf clusterTablePath, TStringBuf versionTablePath);
TString MakeListBalancersQuery(TStringBuf balancerTablePath, TStringBuf versionTablePath);
TString MakeCreateClusterQuery(TStringBuf clusterTablePath);
TString MakeAlterAddFnxQuery(TStringBuf clusterTablePath);
TString MakeCreateBalancerQuery(TStringBuf balancerTablePath);
TString MakeCreateVersionsQuery(TStringBuf versionTablePath);
TString MakeBackfillFnxQuery(TStringBuf clusterTablePath);

bool IssuesLookLikeAlreadyExists(TStringBuf issues);
bool IssuesLookLikeMissingTable(TStringBuf issues);
bool IssuesLookLikeMissingColumn(TStringBuf issues);
bool IssuesLookLikeMissingVersionsTable(TStringBuf issues);
bool IssuesLookLikeClusterSchemaGone(TStringBuf issues);

} // namespace NKikimr::NPQ::NClusterTracker
