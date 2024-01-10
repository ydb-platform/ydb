#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_exec_ctx.h>
#include <ydb/library/yql/providers/yt/gateway/lib/transaction_cache.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>

namespace NYql::NNative {

TString GetTypeFromAttributes(const NYT::TNode& attr, bool getDocumentType);

TString GetTypeFromNode(const NYT::TNode& node, bool getDocumentType);

TMaybe<TVector<IYtGateway::TBatchFolderResult::TFolderItem>> MaybeGetFolderFromCache(TTransactionCache::TEntry::TPtr entry, TStringBuf cacheKey);

TMaybe<TFileLinkPtr> MaybeGetFilePtrFromCache(TTransactionCache::TEntry::TPtr entry, const IYtGateway::TBatchFolderOptions::TFolderPrefixAttrs& folder);

NYT::TAttributeFilter MakeAttrFilter(const TSet<TString>& attributes, bool isResolvingLink);

IYtGateway::TBatchFolderResult::TFolderItem MakeFolderItem(const NYT::TNode& node, const TString& prefix, const TString& name, const TVector<TString>& reqAttrKeys);

const TTransactionCache::TEntry::TFolderCache::value_type& StoreResInCache(const TTransactionCache::TEntry::TPtr& entry, TVector<IYtGateway::TBatchFolderResult::TFolderItem>&& items, const TString& cacheKey);

TFileLinkPtr SaveItemsToTempFile(const TExecContext<IYtGateway::TBatchFolderOptions>::TPtr& execCtx, const TVector<IYtGateway::TFolderResult::TFolderItem>& folderItems);

IYtGateway::TBatchFolderResult ExecResolveLinks(const TExecContext<IYtGateway::TResolveOptions>::TPtr& execCtx);

IYtGateway::TBatchFolderResult ExecGetFolder(const TExecContext<IYtGateway::TBatchFolderOptions>::TPtr& execCtx);

} // NYql::NNative
