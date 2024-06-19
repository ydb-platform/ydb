#pragma once

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>

#include <util/generic/ptr.h>

namespace NYql {

class TPlanFormatterBase : public IPlanFormatter {
public:
    TPlanFormatterBase() = default;
    ~TPlanFormatterBase() = default;

    void WriteDetails(const TExprNode& node, NYson::TYsonWriter& writer) override;
    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override;
    void GetResultDependencies(const TExprNode::TPtr& node, TExprNode::TListType& children, bool compact) override;
    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override;
    ui32 GetOutputs(const TExprNode& node, TVector<TPinInfo>& outputs, bool withLimits) override;
    TString GetProviderPath(const TExprNode& node) override;
    void WritePlanDetails(const TExprNode& node, NYson::TYsonWriter& writer, bool withLimits) override;
    void WritePullDetails(const TExprNode& node, NYson::TYsonWriter& writer) override;
    void WritePinDetails(const TExprNode& node, NYson::TYsonWriter& writer) override;
    TString GetOperationDisplayName(const TExprNode& node) override;
    bool WriteSchemaHeader(NYson::TYsonWriter& writer) override;
    void WriteTypeDetails(NYson::TYsonWriter& writer, const TTypeAnnotationNode& type) override;
};

class TTrackableNodeProcessorBase : public ITrackableNodeProcessor {
public:
    TTrackableNodeProcessorBase() = default;

    void GetUsedNodes(const TExprNode& node, TVector<TString>& usedNodeIds) override;
    void GetCreatedNodes(const TExprNode& node, TVector<TExprNodeAndId>& createdNodes, TExprContext& ctx) override;
    IGraphTransformer& GetCleanupTransformer() override;

protected:
    TNullTransformer NullTransformer_;
};

class TDataProviderBase : public IDataProvider, public TPlanFormatterBase {
public:
    TDataProviderBase() = default;
    ~TDataProviderBase() = default;

    bool Initialize(TExprContext& ctx) override;
    IGraphTransformer& GetConfigurationTransformer() override;
    TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) override;
    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override;
    const THashMap<TString, TString>* GetClusterTokens() override;
    IGraphTransformer& GetIODiscoveryTransformer() override;
    IGraphTransformer& GetEpochsTransformer() override;
    IGraphTransformer& GetIntentDeterminationTransformer() override;
    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override;
    bool CanParse(const TExprNode& node) override;
    void FillModifyCallables(THashSet<TStringBuf>& callables) override;
    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override;
    IGraphTransformer& GetConstraintTransformer(bool instantOnly, bool subGraph) override;
    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override;
    void PostRewriteIO() override;
    void Reset() override;
    IGraphTransformer& GetRecaptureOptProposalTransformer() override;
    IGraphTransformer& GetStatisticsProposalTransformer() override;
    IGraphTransformer& GetLogicalOptProposalTransformer() override;
    IGraphTransformer& GetPhysicalOptProposalTransformer() override;
    IGraphTransformer& GetPhysicalFinalizingTransformer() override;
    IGraphTransformer& GetLoadTableMetadataTransformer() override;
    bool IsPersistent(const TExprNode& node) override;
    bool IsRead(const TExprNode& node) override;
    bool IsWrite(const TExprNode& node) override;
    bool CanBuildResult(const TExprNode& node, TSyncMap& syncList) override;
    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override;
    bool GetExecWorld(const TExprNode::TPtr& node, TExprNode::TPtr& root) override;
    bool CanEvaluate(const TExprNode& node) override;
    void EnterEvaluation(ui64 id) override;
    void LeaveEvaluation(ui64 id) override;
    TExprNode::TPtr CleanupWorld(const TExprNode::TPtr& node, TExprContext& ctx) override;
    TExprNode::TPtr OptimizePull(const TExprNode::TPtr& source, const TFillSettings& fillSettings, TExprContext& ctx,
        IOptimizationContext& optCtx) override;
    bool CanExecute(const TExprNode& node) override;
    bool ValidateExecution(const TExprNode& node, TExprContext& ctx) override;
    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override;
    IGraphTransformer& GetCallableExecutionTransformer() override;
    IGraphTransformer& GetFinalizingTransformer() override;
    bool CollectDiagnostics(NYson::TYsonWriter& writer) override;
    bool GetTasksInfo(NYson::TYsonWriter& writer) override;
    bool CollectStatistics(NYson::TYsonWriter& writer, bool totalOnly) override;
    bool CollectDiscoveredData(NYson::TYsonWriter& writer) override;
    IPlanFormatter& GetPlanFormatter() override;
    ITrackableNodeProcessor& GetTrackableNodeProcessor() override;
    IGraphTransformer& GetPlanInfoTransformer() override;
    IDqIntegration* GetDqIntegration() override;
    IDqOptimization* GetDqOptimization() override;

protected:
    THolder<IGraphTransformer> DefConstraintTransformer_;
    TNullTransformer NullTransformer_;
    TTrackableNodeProcessorBase NullTrackableNodeProcessor_;
};

TExprNode::TPtr DefaultCleanupWorld(const TExprNode::TPtr& node, TExprContext& ctx);

} // namespace NYql
