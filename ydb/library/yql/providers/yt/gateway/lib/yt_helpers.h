#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_op_settings.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/map.h>

#include <functional>
#include <utility>

namespace NYql {

class TMkqlIOCache;
class IExecuteResOrPull;
class TTableLimiter;
struct TYqlOperationOptions;

namespace NCommon {

class TOperationResult;

}

TMaybe<ui64> GetUsedRows(const NYT::TRichYPath& table, ui64 tableRowCount);
TMaybe<ui64> GetUncompressedFileSize(NYT::ITransactionPtr tx, const TString& path);

TString TransformPath(TStringBuf tmpFolder, TStringBuf name, bool isTempTable, TStringBuf userName);

NYT::TNode GetUserAttributes(NYT::ITransactionPtr tx, TString path, TMaybe<bool> includeYqlAttrs = Nothing());
void TransferTableAttributes(const NYT::TNode& attributes, const std::function<void(const TString&,const TString&)>& receiver);
NYT::TNode FilterYqlAttributes(const NYT::TNode& attributes);

bool IterateYamredRows(NYT::ITransactionPtr tx, const NYT::TRichYPath& table, ui32 tableIndex, TMkqlIOCache& specsCache,
    IExecuteResOrPull& exec, const TTableLimiter& limiter, const TMaybe<TSampleParams>& sampling = {});
bool IterateYsonRows(NYT::ITransactionPtr tx, const NYT::TRichYPath& table, ui32 tableIndex, TMkqlIOCache& specsCache,
    IExecuteResOrPull& exec, const TTableLimiter& limiter, const TMaybe<TSampleParams>& sampling = {});
bool SelectRows(NYT::IClientPtr client, const TString& table, ui32 tableIndex, TMkqlIOCache& specsCache,
    IExecuteResOrPull& exec, TTableLimiter& limiter);

NYT::TNode YqlOpOptionsToSpec(const TYqlOperationOptions& opOpts, const TString& userName, const TVector<std::pair<TString, TString>>& code = {});
NYT::TNode YqlOpOptionsToAttrs(const TYqlOperationOptions& opOpts);

void CreateParents(const TVector<TString>& tables, NYT::IClientBasePtr tx);

// must be used inside 'catch' because it rethrows current exception to analyze it's type
void FillResultFromCurrentException(NCommon::TOperationResult& result, TPosition pos = {}, bool shortErrors = false);

// must be used inside 'catch' because it rethrows current exception to analyze it's type
template<typename TResult>
static TResult ResultFromCurrentException(TPosition pos = {}, bool shortErrors = false) {
    TResult result;
    FillResultFromCurrentException(result, pos, shortErrors);
    return result;
}

TMaybe<TString> SerializeRichYPathAttrs(const NYT::TRichYPath& richPath);
void DeserializeRichYPathAttrs(const TString& serializedAttrs, NYT::TRichYPath& richPath);

IYtGateway::TCanonizedPath CanonizedPath(const TString& path);

void EnsureSpecDoesntUseNativeYtTypes(const NYT::TNode& spec, TStringBuf tableName, bool read);

TIssue MakeIssueFromYtError(const NYT::TYtError& e, TStringBuf what, TPosition pos = {}, bool shortErrors = false);

} // NYql
