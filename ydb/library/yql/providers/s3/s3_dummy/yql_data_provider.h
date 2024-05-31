#pragma once
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>


static constexpr TStringBuf ProviderName = "dummy-s3";

namespace NYql {

class TDefaultPlanFormatter : public IPlanFormatter {
public:
    void WriteDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        Y_UNUSED(node);
        Y_UNUSED(writer);
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(node);
        Y_UNUSED(children);
        Y_UNUSED(compact);
        return false;
    }

    void GetResultDependencies(const TExprNode::TPtr& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(node);
        Y_UNUSED(children);
        Y_UNUSED(compact);
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override {
        Y_UNUSED(node);
        Y_UNUSED(inputs);
        Y_UNUSED(withLimits);
        return 0;
    }

    ui32 GetOutputs(const TExprNode& node, TVector<TPinInfo>& outputs, bool withLimits) override {
        Y_UNUSED(node);
        Y_UNUSED(outputs);
        Y_UNUSED(withLimits);
        return 0;
    }

    TString GetProviderPath(const TExprNode& node) override {
        Y_UNUSED(node);
        return "";
    }

    void WritePlanDetails(const TExprNode& node, NYson::TYsonWriter& writer, bool withLimits) override {
        Y_UNUSED(node);
        Y_UNUSED(writer);
        Y_UNUSED(withLimits);
    }

    void WritePullDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        Y_UNUSED(node);
        Y_UNUSED(writer);
    }

    void WritePinDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        Y_UNUSED(node);
        Y_UNUSED(writer);
    }
    
    TString GetOperationDisplayName(const TExprNode& node) override {
        Y_UNUSED(node);
        return "";
    }

    bool WriteSchemaHeader(NYson::TYsonWriter& writer) override {
        Y_UNUSED(writer);
        return false;
    }

    void WriteTypeDetails(NYson::TYsonWriter& writer, const TTypeAnnotationNode& type) override {
        Y_UNUSED(writer);
        Y_UNUSED(type);
    }
};

class TDefaultConstraintTransformer : public TSyncTransformerBase {
public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        Y_UNUSED(input);
        Y_UNUSED(output);
        Y_UNUSED(ctx);
        return IGraphTransformer::TStatus::Ok;
    }

    void Rewind() override {}
};

class TDefaultTrackableNodeProcessor : public ITrackableNodeProcessor {
public:
    void GetUsedNodes(const TExprNode& node, TVector<TString>& usedNodeIds) override {
        Y_UNUSED(node);
        Y_UNUSED(usedNodeIds);
    }

    void GetCreatedNodes(const TExprNode& node, TVector<TExprNodeAndId>& createdNodes, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(createdNodes);
        Y_UNUSED(ctx);
    }

    IGraphTransformer& GetCleanupTransformer() override {
        return NullTransformer_;
    }

protected:
    TNullTransformer NullTransformer_;
};

class TDefaultDataProvider : public IDataProvider {
public:

    TStringBuf GetName() const override { 
        return ProviderName;
    }

    bool Initialize(TExprContext& ctx) override {
        Y_UNUSED(ctx);
        return true;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return NullTransformer_;
    }

    TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) override {
        Y_UNUSED(cluster);
        Y_UNUSED(ctx);
        return {};
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return nullptr;
    }

    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        Y_UNUSED(name);
        Y_UNUSED(properties);
    }

    IGraphTransformer& GetIODiscoveryTransformer() override {
        return NullTransformer_;
    }

    IGraphTransformer& GetEpochsTransformer() override {
        return NullTransformer_;
    }

    IGraphTransformer& GetIntentDeterminationTransformer() override {
        return NullTransformer_;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);

        cluster = Nothing();
        return true;
    }

    bool CanParse(const TExprNode& node) override {
        Y_UNUSED(node);
        return false;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return NullTransformer_;
    }

    IGraphTransformer& GetConstraintTransformer(bool instantOnly, bool subGraph) override {
        Y_UNUSED(instantOnly);
        Y_UNUSED(subGraph);
        return DefaultConstraintTransformer_;
    }

    void FillModifyCallables(THashSet<TStringBuf>& callables) override {
        Y_UNUSED(callables);
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        return node;
    }

    IGraphTransformer& GetRecaptureOptProposalTransformer() override {
        return NullTransformer_;
    }

    IGraphTransformer& GetStatisticsProposalTransformer() override {
        return NullTransformer_;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return NullTransformer_;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return NullTransformer_;
    }

    IGraphTransformer& GetPhysicalFinalizingTransformer() override {
        return NullTransformer_;
    }

    void PostRewriteIO() override {}

    void Reset() override {}

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return NullTransformer_;
    }

    bool IsPersistent(const TExprNode& node) override {
        Y_UNUSED(node);
        return false;
    }

    bool IsRead(const TExprNode& node) override {
        Y_UNUSED(node);
        return false;
    }

    bool IsWrite(const TExprNode& node) override {
        Y_UNUSED(node);
        return false;
    }

    bool CanBuildResult(const TExprNode& node, TSyncMap& syncList) override {
        Y_UNUSED(node);
        Y_UNUSED(syncList);
        return false;
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        Y_UNUSED(node);
        Y_UNUSED(syncList);
        Y_UNUSED(canRef);
        return false;
    }

    bool GetExecWorld(const TExprNode::TPtr& node, TExprNode::TPtr& root) override {
        root = nullptr;
        Y_UNUSED(node);
        return false;
    }

    bool CanEvaluate(const TExprNode& node) override {
        Y_UNUSED(node);
        return false;
    }

    void EnterEvaluation(ui64 id) override {
        Y_UNUSED(id);
    }

    void LeaveEvaluation(ui64 id) override {
        Y_UNUSED(id);
    }

    TExprNode::TPtr CleanupWorld(const TExprNode::TPtr& node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        return node;
    }

    TExprNode::TPtr OptimizePull(const TExprNode::TPtr& source, const TFillSettings& fillSettings, TExprContext& ctx,
        IOptimizationContext& optCtx) override {
        Y_UNUSED(fillSettings);
        Y_UNUSED(ctx);
        Y_UNUSED(optCtx);
        return source;
    }

    bool CanExecute(const TExprNode& node) override {
        Y_UNUSED(node);
        return false;
    }

    bool ValidateExecution(const TExprNode& node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);
        return true;
    }

    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
        Y_UNUSED(node);
        Y_UNUSED(children);
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return NullTransformer_;
    }

    IGraphTransformer& GetFinalizingTransformer() override {
        return NullTransformer_;
    }

    bool CollectDiagnostics(NYson::TYsonWriter& writer) override {
        Y_UNUSED(writer);
        return false;
    }

    bool GetTasksInfo(NYson::TYsonWriter& writer) override {
        Y_UNUSED(writer);
        return false;
    }

    bool CollectStatistics(NYson::TYsonWriter& writer, bool totalOnly) override {
        Y_UNUSED(writer);
        Y_UNUSED(totalOnly);
        return false;
    }

    bool CollectDiscoveredData(NYson::TYsonWriter& writer) override {
        Y_UNUSED(writer);
        return false;
    }

    IGraphTransformer& GetPlanInfoTransformer() override {
        return NullTransformer_;
    }

    IPlanFormatter& GetPlanFormatter() override {
        return DefaultPlanFormatter_;
    }

    ITrackableNodeProcessor& GetTrackableNodeProcessor() override {
        return DefaultTrackableNodeProcessor_;
    }

    IDqIntegration* GetDqIntegration() override {
        return nullptr;
    }

    IDqOptimization* GetDqOptimization() override {
        return nullptr;
    }

private:
    TNullTransformer NullTransformer_;
    TDefaultConstraintTransformer DefaultConstraintTransformer_;
    TDefaultTrackableNodeProcessor DefaultTrackableNodeProcessor_;
    TDefaultPlanFormatter DefaultPlanFormatter_;
};

}