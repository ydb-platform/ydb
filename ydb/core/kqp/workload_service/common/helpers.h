#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NKqp::NWorkload {

TString CreateDatabaseId(const TString& database, bool serverless, TPathId pathId);
TString DatabaseIdToDatabase(TStringBuf databaseId);

TString CreatePoolKey(const TString& databaseId, const TString& poolId);
std::pair<TString, TString> ParsePoolKey(const TString& poolKey, TString& error);

NYql::TIssues GroupIssues(const NYql::TIssues& issues, const TString& message);

void ParsePoolSettings(const NKikimrSchemeOp::TResourcePoolDescription& description, NResourcePool::TPoolSettings& poolConfig);

ui64 SaturationSub(ui64 x, ui64 y);

}  // NKikimr::NKqp::NWorkload
