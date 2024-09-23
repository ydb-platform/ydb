#include "yql_yt_profiling.h"

#include <ydb/library/yql/utils/log/profile.h>

#include <util/generic/ptr.h>

using namespace NThreading;

namespace NYql {
namespace {

class TProfilingYtGateway final: public IYtGateway {
public:
    TProfilingYtGateway(IYtGateway::TPtr&& slave)
        : Slave_(std::move(slave))
    {
    }

    void OpenSession(TOpenSessionOptions&& options) final {
        YQL_PROFILE_FUNC(TRACE);
        Slave_->OpenSession(std::move(options));
    }

    TFuture<void> CloseSession(TCloseSessionOptions&& options) final {
        YQL_PROFILE_FUNC(TRACE);
        return Slave_->CloseSession(std::move(options));
    }

    TFuture<void> CleanupSession(TCleanupSessionOptions&& options) final {
        YQL_PROFILE_FUNC(TRACE);
        return Slave_->CleanupSession(std::move(options));
    }

    TFuture<TFinalizeResult> Finalize(TFinalizeOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->Finalize(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TCanonizePathsResult> CanonizePaths(TCanonizePathsOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->CanonizePaths(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TTableInfoResult> GetTableInfo(TGetTableInfoOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->GetTableInfo(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TTableRangeResult> GetTableRange(TTableRangeOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->GetTableRange(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TFolderResult> GetFolder(TFolderOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->GetFolder(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TBatchFolderResult> ResolveLinks(TResolveOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->ResolveLinks(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TBatchFolderResult> GetFolders(TBatchFolderOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->GetFolders(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->ResOrPull(node, ctx, std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->Run(node, ctx, std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TRunResult> Prepare(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) const final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->Prepare(node, ctx, std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TCalcResult> Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->Calc(nodes, ctx, std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->Publish(node, ctx, std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TCommitResult> Commit(TCommitOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->Commit(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TDropTrackablesResult> DropTrackables(TDropTrackablesOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->DropTrackables(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TFuture<TPathStatResult> PathStat(TPathStatOptions&& options) final {
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        auto future = Slave_->PathStat(std::move(options));
        return YQL_PROFILE_BIND_VAL(future, profileScope);
    }

    TPathStatResult TryPathStat(TPathStatOptions&& options) final {
        YQL_PROFILE_FUNC(TRACE);
        return Slave_->TryPathStat(std::move(options));
    }

    bool TryParseYtUrl(const TString& url, TString* cluster, TString* path) const final {
        YQL_PROFILE_FUNC(TRACE);
        return Slave_->TryParseYtUrl(url, cluster, path);
    }

    TString GetDefaultClusterName() const final {
        YQL_PROFILE_FUNC(TRACE);
        return Slave_->GetDefaultClusterName();
    }

    TString GetClusterServer(const TString& cluster) const final {
        return Slave_->GetClusterServer(cluster);
    }

    NYT::TRichYPath GetRealTable(const TString& sessionId, const TString& cluster, const TString& table, ui32 epoch, const TString& tmpFolder) const final {
        return Slave_->GetRealTable(sessionId, cluster, table, epoch, tmpFolder);
    }

    NYT::TRichYPath GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const final {
        return Slave_->GetWriteTable(sessionId, cluster, table, tmpFolder);
    }

    NThreading::TFuture<TRunResult> GetTableStat(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) final {
        return Slave_->GetTableStat(node, ctx, std::move(options));
    }

    TFullResultTableResult PrepareFullResultTable(TFullResultTableOptions&& options) final {
        YQL_PROFILE_FUNC(TRACE);
        return Slave_->PrepareFullResultTable(std::move(options));
    }

    void SetStatUploader(IStatUploader::TPtr statUploader) final {
        YQL_PROFILE_FUNC(TRACE);
        Slave_->SetStatUploader(statUploader);
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) final {
        Y_UNUSED(compiler);
    }

    TGetTablePartitionsResult GetTablePartitions(TGetTablePartitionsOptions&& options) override {
        YQL_PROFILE_FUNC(TRACE);
        return Slave_->GetTablePartitions(std::move(options));
    }

    void AddCluster(const TYtClusterConfig&) override {
    }

private:
    IYtGateway::TPtr Slave_;
};

} // namespace

IYtGateway::TPtr CreateYtProfilingGateway(IYtGateway::TPtr slave) {
    return MakeIntrusive<TProfilingYtGateway>(std::move(slave));
}

} // namspace NYql
