#pragma once

#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NYql {

class TDqIntegrationBase: public IDqIntegration {
public:
    ui64 Partition(const TExprNode& node, TVector<TString>& partitions, TString* clusterName, TExprContext& ctx, const TPartitionSettings& settings) override;
    bool CheckPragmas(const TExprNode& node, TExprContext& ctx, bool skipIssues) override;
    bool CanRead(const TExprNode& read, TExprContext& ctx, bool skipIssues) override;
    TMaybe<ui64> EstimateReadSize(ui64 dataSizePerJob, ui32 maxTasksPerStage, const TVector<const TExprNode*>& nodes, TExprContext& ctx) override;
    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx, const TWrapReadSettings& settings) override;
    TMaybe<TOptimizerStatistics> ReadStatistics(const TExprNode::TPtr& readWrap, TExprContext& ctx) override;
    TExprNode::TPtr RecaptureWrite(const TExprNode::TPtr& write, TExprContext& ctx) override;
    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override;
    TMaybe<bool> CanWrite(const TExprNode& write, TExprContext& ctx) override;
    bool CanBlockRead(const NNodes::TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) override;
    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) override;
    bool CanFallback() override;
    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sourceType, size_t, TExprContext&) override;
    void FillLookupSourceSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sourceType) override;
    void FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sinkType) override;
    void FillTransformSettings(const TExprNode& node, ::google::protobuf::Any& settings) override;
    void Annotate(const TExprNode& node, THashMap<TString, TString>& params) override;
    bool PrepareFullResultTableParams(const TExprNode& root, TExprContext& ctx, THashMap<TString, TString>& params, THashMap<TString, TString>& secureParams, const TMaybe<TColumnOrder>& order = {}) override;
    void WriteFullResultTableRef(NYson::TYsonWriter& writer, const TVector<TString>& columns, const THashMap<TString, TString>& graphParams) override;
    bool FillSourcePlanProperties(const NNodes::TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override;
    bool FillSinkPlanProperties(const NNodes::TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override;
    void ConfigurePeepholePipeline(bool beforeDqTransforms, const THashMap<TString, TString>& params, TTransformationPipeline* pipeline) override;
    void NotifyDqTimeout() override;

protected:
    bool CanBlockReadTypes(const TStructExprType* node);
};

} // namespace NYql
