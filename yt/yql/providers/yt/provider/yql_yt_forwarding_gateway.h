#include "yql_yt_provider.h"

using namespace NThreading;

namespace NYql {

class TYtForwardingGatewayBase: public IYtGateway {
public:
    TYtForwardingGatewayBase(IYtGateway::TPtr&& slave);

    void OpenSession(TOpenSessionOptions&& options) override;

    TFuture<void> CloseSession(TCloseSessionOptions&& options) override;

    TFuture<void> CleanupSession(TCleanupSessionOptions&& options) override;

    TFuture<TFinalizeResult> Finalize(TFinalizeOptions&& options) override;

    TFuture<TCanonizePathsResult> CanonizePaths(TCanonizePathsOptions&& options) override;

    TFuture<TTableInfoResult> GetTableInfo(TGetTableInfoOptions&& options) override;
    TFuture<TTableRangeResult> GetTableRange(TTableRangeOptions&& options) override;

    TFuture<TFolderResult> GetFolder(TFolderOptions&& options) override;

    TFuture<TBatchFolderResult> ResolveLinks(TResolveOptions&& options) override;

    TFuture<TBatchFolderResult> GetFolders(TBatchFolderOptions&& options) override;

    TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) override;

    TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) override;

    TFuture<TRunResult> Prepare(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) const override;

    TFuture<TCalcResult> Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) override;

    TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) override;

    TFuture<TCommitResult> Commit(TCommitOptions&& options) override;

    TFuture<TDropTrackablesResult> DropTrackables(TDropTrackablesOptions&& options) override;

    TFuture<TPathStatResult> PathStat(TPathStatOptions&& options) override;

    TPathStatResult TryPathStat(TPathStatOptions&& options) override;

    bool TryParseYtUrl(const TString& url, TString* cluster, TString* path) const override;

    TString GetDefaultClusterName() const override;

    TString GetClusterServer(const TString& cluster) const override;

    NYT::TRichYPath GetRealTable(const TString& sessionId, const TString& cluster, const TString& table, ui32 epoch, const TString& tmpFolder, bool temp, bool anonymous) const override;

    NYT::TRichYPath GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const override;

    TFuture<TDownloadTablesResult> DownloadTables(TDownloadTablesOptions&& options) override;

    TFuture<TUploadTableResult> UploadTable(TUploadTableOptions&& options) override;

    NThreading::TFuture<TRunResult> GetTableStat(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) override;

    TFullResultTableResult PrepareFullResultTable(TFullResultTableOptions&& options) override;

    void SetStatUploader(IStatUploader::TPtr statUploader) override;

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override;

    TGetTablePartitionsResult GetTablePartitions(TGetTablePartitionsOptions&& options) override;

    void AddCluster(const TYtClusterConfig& config) override;

protected:
    IYtGateway::TPtr Slave_;
};

} // namspace NYql
