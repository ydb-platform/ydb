#include "yql_yt_forwarding_gateway.h"

using namespace NThreading;

namespace NYql {

TYtForwardingGatewayBase::TYtForwardingGatewayBase(IYtGateway::TPtr&& slave)
    : Slave_(std::move(slave))
{
}

void TYtForwardingGatewayBase::OpenSession(TOpenSessionOptions&& options) {
    Slave_->OpenSession(std::move(options));
}

TFuture<void> TYtForwardingGatewayBase::CloseSession(TCloseSessionOptions&& options) {
    return Slave_->CloseSession(std::move(options));
}

TFuture<void> TYtForwardingGatewayBase::CleanupSession(TCleanupSessionOptions&& options) {
    return Slave_->CleanupSession(std::move(options));
}

TFuture<IYtGateway::TFinalizeResult> TYtForwardingGatewayBase::Finalize(TFinalizeOptions&& options) {
    return Slave_->Finalize(std::move(options));
}

TFuture<IYtGateway::TCanonizePathsResult> TYtForwardingGatewayBase::CanonizePaths(TCanonizePathsOptions&& options) {
    return Slave_->CanonizePaths(std::move(options));
}

TFuture<IYtGateway::TTableInfoResult> TYtForwardingGatewayBase::GetTableInfo(TGetTableInfoOptions&& options) {
    return Slave_->GetTableInfo(std::move(options));
}

TFuture<IYtGateway::TTableRangeResult> TYtForwardingGatewayBase::GetTableRange(TTableRangeOptions&& options) {
    return Slave_->GetTableRange(std::move(options));
}

TFuture<IYtGateway::TFolderResult> TYtForwardingGatewayBase::GetFolder(TFolderOptions&& options) {
    return Slave_->GetFolder(std::move(options));
}

TFuture<IYtGateway::TBatchFolderResult> TYtForwardingGatewayBase::ResolveLinks(TResolveOptions&& options) {
    return Slave_->ResolveLinks(std::move(options));
}

TFuture<IYtGateway::TBatchFolderResult> TYtForwardingGatewayBase::GetFolders(TBatchFolderOptions&& options) {
    return Slave_->GetFolders(std::move(options));
}

TFuture<IYtGateway::TResOrPullResult> TYtForwardingGatewayBase::ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) {
    return Slave_->ResOrPull(node, ctx, std::move(options));
}

TFuture<IYtGateway::TRunResult> TYtForwardingGatewayBase::Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) {
    return Slave_->Run(node, ctx, std::move(options));
}

TFuture<IYtGateway::TRunResult> TYtForwardingGatewayBase::Prepare(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) const {
    return Slave_->Prepare(node, ctx, std::move(options));
}

TFuture<IYtGateway::TCalcResult> TYtForwardingGatewayBase::Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) {
    return Slave_->Calc(nodes, ctx, std::move(options));
}

TFuture<IYtGateway::TPublishResult> TYtForwardingGatewayBase::Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) {
    return Slave_->Publish(node, ctx, std::move(options));
}

TFuture<IYtGateway::TCommitResult> TYtForwardingGatewayBase::Commit(TCommitOptions&& options) {
    return Slave_->Commit(std::move(options));
}

TFuture<IYtGateway::TDropTrackablesResult> TYtForwardingGatewayBase::DropTrackables(TDropTrackablesOptions&& options) {
    return Slave_->DropTrackables(std::move(options));
}

TFuture<IYtGateway::TPathStatResult> TYtForwardingGatewayBase::PathStat(TPathStatOptions&& options) {
    return Slave_->PathStat(std::move(options));
}

IYtGateway::TPathStatResult TYtForwardingGatewayBase::TryPathStat(TPathStatOptions&& options) {
    return Slave_->TryPathStat(std::move(options));
}

bool TYtForwardingGatewayBase::TryParseYtUrl(const TString& url, TString* cluster, TString* path) const {
    return Slave_->TryParseYtUrl(url, cluster, path);
}

TString TYtForwardingGatewayBase::GetDefaultClusterName() const {
    return Slave_->GetDefaultClusterName();
}

TString TYtForwardingGatewayBase::GetClusterServer(const TString& cluster) const {
    return Slave_->GetClusterServer(cluster);
}

NYT::TRichYPath TYtForwardingGatewayBase::GetRealTable(const TString& sessionId, const TString& cluster, const TString& table, ui32 epoch, const TString& tmpFolder, bool temp, bool anonymous) const {
    return Slave_->GetRealTable(sessionId, cluster, table, epoch, tmpFolder, temp, anonymous);
}

NYT::TRichYPath TYtForwardingGatewayBase::GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const {
    return Slave_->GetWriteTable(sessionId, cluster, table, tmpFolder);
}

TFuture<IYtGateway::TDownloadTablesResult> TYtForwardingGatewayBase::DownloadTables(TDownloadTablesOptions&& options) {
    return Slave_->DownloadTables(std::move(options));
}

TFuture<IYtGateway::TUploadTableResult> TYtForwardingGatewayBase::UploadTable(TUploadTableOptions&& options) {
    return Slave_->UploadTable(std::move(options));
}

NThreading::TFuture<IYtGateway::TRunResult> TYtForwardingGatewayBase::GetTableStat(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) {
    return Slave_->GetTableStat(node, ctx, std::move(options));
}

IYtGateway::TFullResultTableResult TYtForwardingGatewayBase::PrepareFullResultTable(TFullResultTableOptions&& options) {
    return Slave_->PrepareFullResultTable(std::move(options));
}

void TYtForwardingGatewayBase::SetStatUploader(IStatUploader::TPtr statUploader) {
    Slave_->SetStatUploader(statUploader);
}

void TYtForwardingGatewayBase::RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) {
    Slave_->RegisterMkqlCompiler(compiler);
}

IYtGateway::TGetTablePartitionsResult TYtForwardingGatewayBase::GetTablePartitions(TGetTablePartitionsOptions&& options) {
    return Slave_->GetTablePartitions(std::move(options));
}

void TYtForwardingGatewayBase::AddCluster(const TYtClusterConfig& config) {
    Slave_->AddCluster(config);
}

} // namspace NYql
