#include "yql_dq_integration_impl.h"

namespace NYql {

ui64 TDqIntegrationBase::Partition(const TDqSettings& config, size_t maxPartitions, const TExprNode& node,
    TVector<TString>& partitions, TString* clusterName, TExprContext& ctx, bool canFallback) {
    Y_UNUSED(config);
    Y_UNUSED(maxPartitions);
    Y_UNUSED(node);
    Y_UNUSED(partitions);
    Y_UNUSED(clusterName);
    Y_UNUSED(ctx);
    Y_UNUSED(canFallback);
    return 0;
}

TMaybe<ui64> TDqIntegrationBase::CanRead(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TExprNode& read, TExprContext& ctx, bool skipIssues) {
    Y_UNUSED(read);
    Y_UNUSED(ctx);
    Y_UNUSED(skipIssues);
    return Nothing();
}

TExprNode::TPtr TDqIntegrationBase::WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) {
    Y_UNUSED(read);
    Y_UNUSED(ctx);

    return read;
}

TMaybe<bool> TDqIntegrationBase::CanWrite(const TDqSettings&, const TExprNode& write, TExprContext& ctx) {
    Y_UNUSED(write);
    Y_UNUSED(ctx);
    return Nothing();
}

void TDqIntegrationBase::RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler)  {
    Y_UNUSED(compiler);
}

bool TDqIntegrationBase::CanFallback() {
    return false;
}

void TDqIntegrationBase::FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sourceType) {
    Y_UNUSED(node);
    Y_UNUSED(settings);
    Y_UNUSED(sourceType);
}

void TDqIntegrationBase::FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sinkType) {
    Y_UNUSED(node);
    Y_UNUSED(settings);
    Y_UNUSED(sinkType);
}

void TDqIntegrationBase::FillTransformSettings(const TExprNode& node, ::google::protobuf::Any& settings) {
    Y_UNUSED(node);
    Y_UNUSED(settings);
}

void TDqIntegrationBase::Annotate(const TExprNode& node, THashMap<TString, TString>& params) {
    Y_UNUSED(node);
    Y_UNUSED(params);
}

bool TDqIntegrationBase::PrepareFullResultTableParams(const TExprNode& root, TExprContext& ctx, THashMap<TString, TString>& params, THashMap<TString, TString>& secureParams) {
    Y_UNUSED(root);
    Y_UNUSED(ctx);
    Y_UNUSED(params);
    Y_UNUSED(secureParams);
    return false;
}

void TDqIntegrationBase::WriteFullResultTableRef(NYson::TYsonWriter& writer, const TVector<TString>& columns, const THashMap<TString, TString>& graphParams) {
    Y_UNUSED(writer);
    Y_UNUSED(columns);
    Y_UNUSED(graphParams);
}

} // namespace NYql
