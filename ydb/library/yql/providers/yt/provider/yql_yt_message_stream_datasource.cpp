#include "yql_yt_message_stream.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NYql {
namespace {
using namespace NNodes;

// Provider phases may run again after KQP resolves an external data source,
// without rewinding the enclosing compilation pipeline.
class TYtMessageStreamPipeline final : public TGraphTransformerBase {
public:
    explicit TYtMessageStreamPipeline(TAutoPtr<IGraphTransformer> pipeline)
        : Pipeline_(std::move(pipeline))
    {}

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        if (Finished_) {
            Rewind();
        }
        const auto status = Pipeline_->Transform(input, output, ctx);
        Finished_ = status.Level == TStatus::Ok;
        return status;
    }
    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        return Pipeline_->GetAsyncFuture(input);
    }
    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        const auto status = Pipeline_->ApplyAsyncChanges(input, output, ctx);
        Finished_ = status.Level == TStatus::Ok;
        return status;
    }
    void Rewind() override {
        Pipeline_->Rewind();
        Finished_ = false;
    }
    TAutoPtr<IGraphTransformer> Pipeline_;
    bool Finished_ = false;
};

class TYtMessageStreamTransformer final : public TGraphTransformerBase {
public:
    TYtMessageStreamTransformer(std::shared_ptr<IYtMessageStreamIntegration> streams, TTransformStage stream, TTransformStage table)
        : Streams_(std::move(streams)), Stream_(std::move(stream)), Table_(std::move(table))
    {}

private:
    IGraphTransformer& Select(const TExprNode& node) {
        return Streams_->CanParse(node)
            ? Stream_.GetTransformer() : Table_.GetTransformer();
    }
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        return Select(*input).Transform(input, output, ctx);
    }
    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        return Select(input).GetAsyncFuture(input);
    }
    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        return Select(*input).ApplyAsyncChanges(input, output, ctx);
    }
    void Rewind() override {
        Stream_.GetTransformer().Rewind();
        Table_.GetTransformer().Rewind();
    }
    std::shared_ptr<IYtMessageStreamIntegration> Streams_;
    TTransformStage Stream_;
    TTransformStage Table_;
};

class TYtMessageStreamDqIntegrationWrapper final : public IDqIntegration {
public:
    TYtMessageStreamDqIntegrationWrapper(IDqIntegration& tables, std::shared_ptr<IYtMessageStreamIntegration> streams)
        : Tables_(tables)
        , Streams_(std::move(streams))
    {}

    ui64 Partition(const TExprNode& node, TVector<TString>& partitions, TString* clusterName,
        TExprContext& ctx, const TPartitionSettings& settings) override {
        return Select(node).Partition(node, partitions, clusterName, ctx, settings);
    }
    bool CanRead(const TExprNode& read, TExprContext& ctx, bool skipIssues) override {
        return Select(read).CanRead(read, ctx, skipIssues);
    }
    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx, const TWrapReadSettings& settings) override {
        return Select(*read).WrapRead(read, ctx, settings);
    }
    TMaybe<TOptimizerStatistics> ReadStatistics(const TExprNode::TPtr& read, TExprContext& ctx) override {
        return Select(*read).ReadStatistics(read, ctx);
    }
    bool CanBlockRead(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& types) override {
        return IsStream(node.Ref()) ? false : Tables_.CanBlockRead(node, ctx, types);
    }
    TMaybe<ui64> EstimateReadSize(ui64 dataSizePerJob, ui32 maxTasksPerStage,
        const TVector<const TExprNode*>& nodes, TExprContext& ctx) override {
        TVector<const TExprNode*> tables;
        TVector<const TExprNode*> streams;
        for (const auto* node : nodes) {
            (IsStream(*node) ? streams : tables).push_back(node);
        }
        if (streams.empty()) {
            return Tables_.EstimateReadSize(dataSizePerJob, maxTasksPerStage, nodes, ctx);
        }
        const auto streamSize = Streams_->GetDqIntegration().EstimateReadSize(dataSizePerJob, maxTasksPerStage, streams, ctx);
        if (!streamSize || tables.empty()) {
            return streamSize;
        }
        const auto tableSize = Tables_.EstimateReadSize(dataSizePerJob, maxTasksPerStage, tables, ctx);
        return tableSize ? TMaybe<ui64>(*tableSize + *streamSize) : Nothing();
    }
    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        // Native YT table callables are registered by the YT sink integration.
        // This wrapper has a distinct IDqIntegration pointer, so registering
        // Tables_ here would add the same callables twice when QYT is enabled.
        Streams_->GetDqIntegration().RegisterMkqlCompiler(compiler);
    }
    void FillSourceSettings(const TExprNode& node, google::protobuf::Any& settings, TString& sourceType,
        size_t maxPartitions, TExprContext& ctx) override {
        Select(node).FillSourceSettings(node, settings, sourceType, maxPartitions, ctx);
    }
    bool CheckPragmas(const TExprNode& node, TExprContext& ctx, bool skipIssues) override {
        return Tables_.CheckPragmas(node, ctx, skipIssues);
    }

    TExprNode::TPtr RecaptureWrite(const TExprNode::TPtr& write, TExprContext& ctx) override {
        return Tables_.RecaptureWrite(write, ctx);
    }

    TMaybe<bool> CanWrite(const TExprNode& write, TExprContext& ctx) override {
        return Tables_.CanWrite(write, ctx);
    }

    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) override {
        return Tables_.WrapWrite(write, ctx);
    }

    bool CanFallback() override {
        return Tables_.CanFallback();
    }

    TMaybe<TSourceWatermarksSettings> ExtractSourceWatermarksSettings(const TExprNode& node, const ::google::protobuf::Any& settings, const TString& sourceType) override {
        return Tables_.ExtractSourceWatermarksSettings(node, settings, sourceType);
    }

    void FillLookupSourceSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sourceType) override {
        return Tables_.FillLookupSourceSettings(node, settings, sourceType);
    }

    void FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sinkType) override {
        return Tables_.FillSinkSettings(node, settings, sinkType);
    }

    void FillTransformSettings(const TExprNode& node, ::google::protobuf::Any& settings) override {
        return Tables_.FillTransformSettings(node, settings);
    }

    void Annotate(const TExprNode& node, THashMap<TString, TString>& params) override {
        return Tables_.Annotate(node, params);
    }

    bool PrepareFullResultTableParams(const TExprNode& root, TExprContext& ctx, THashMap<TString, TString>& params, THashMap<TString, TString>& secureParams, const TMaybe<TColumnOrder>& columnOrder) override {
        return Tables_.PrepareFullResultTableParams(root, ctx, params, secureParams, columnOrder);
    }

    void WriteFullResultTableRef(NYson::TYsonWriter& writer, const TVector<TString>& columns, const THashMap<TString, TString>& graphParams) override {
        return Tables_.WriteFullResultTableRef(writer, columns, graphParams);
    }

    bool FillSourcePlanProperties(const NNodes::TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override {
        return Tables_.FillSourcePlanProperties(node, properties);
    }

    bool FillSinkPlanProperties(const NNodes::TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override {
        return Tables_.FillSinkPlanProperties(node, properties);
    }

    void ConfigurePeepholePipeline(bool beforeDqTransforms, const THashMap<TString, TString>& params, TTransformationPipeline* pipeline) override {
        return Tables_.ConfigurePeepholePipeline(beforeDqTransforms, params, pipeline);
    }

    void NotifyDqTimeout() override {
        return Tables_.NotifyDqTimeout();
    }
private:
    bool IsStream(const TExprNode& node) const {
        if (Streams_->CanParse(node)) {
            return true;
        }
        if (const auto source = TMaybeNode<TDqSource>(&node)) {
            return TYtMessageStreamDataSource::Match(&source.Cast().DataSource().Ref());
        }
        if (const auto source = TMaybeNode<TDqSourceWideWrap>(&node)) {
            return TYtMessageStreamDataSource::Match(&source.Cast().DataSource().Ref());
        }
        if (const auto source = TMaybeNode<TDqSourceWrap>(&node)) {
            return TYtMessageStreamDataSource::Match(&source.Cast().DataSource().Ref());
        }
        return false;
    }
    IDqIntegration& Select(const TExprNode& node) {
        return IsStream(node) ? Streams_->GetDqIntegration() : Tables_;
    }
    IDqIntegration& Tables_;
    std::shared_ptr<IYtMessageStreamIntegration> Streams_;
};

// Keep the native YT provider unchanged. Only MessageStream operations are
// handled here; the remaining provider and plan interfaces are delegated.
class TYtMessageStreamDataSourceWrapper final : public IDataProvider, public IPlanFormatter {
public:
    TYtMessageStreamDataSourceWrapper(TIntrusivePtr<IDataProvider> tables, std::shared_ptr<IYtMessageStreamIntegration> streams)
        : Tables_(std::move(tables))
        , Streams_(std::move(streams))
        , Dq_(*Tables_->GetDqIntegration(), Streams_)
    {}

    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        Streams_->AddCluster(name, properties);
        Tables_->AddCluster(name, properties);
    }
    const THashMap<TString, TString>* GetClusterTokens() override {
        Tokens_.clear();
        if (const auto* tokens = Tables_->GetClusterTokens()) {
            Tokens_ = *tokens;
        }
        for (const auto& [cluster, token] : Streams_->GetClusterTokens()) {
            Tokens_[cluster] = token;
        }
        return &Tokens_;
    }
    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        return TYtMessageStreamDataSource::Match(&node)
            ? Streams_->ValidateParameters(node, ctx, cluster)
            : Tables_->ValidateParameters(node, ctx, cluster);
    }
    bool CanParse(const TExprNode& node) override {
        return Streams_->CanParse(node) || Tables_->CanParse(node);
    }
    bool IsRead(const TExprNode& node) override {
        return Streams_->IsRead(node) || Tables_->IsRead(node);
    }
    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        if (node->ChildrenSize() && Streams_->CanParse(node->Head())) {
            return Streams_->RewriteIO(node, ctx);
        }
        return Tables_->RewriteIO(node, ctx);
    }
    IGraphTransformer& GetIODiscoveryTransformer() override {
        if (!Discovery_) {
            // Lower queue reads before native discovery can interpret them as tables.
            auto lower = CreateFunctorTransformer([streams = Streams_](TExprNode::TPtr input,
                TExprNode::TPtr& output, TExprContext& ctx) {
                return OptimizeExpr(input, output, [streams](const TExprNode::TPtr& node, TExprContext& ctx) {
                    if ((node->IsCallable("Right!") || node->IsCallable("Left!"))
                        && node->ChildrenSize() && streams->CanParse(node->Head())) {
                        return streams->RewriteIO(node, ctx);
                    }
                    return node;
                }, ctx, TOptimizeExprSettings(nullptr));
            });
            Discovery_ = new TYtMessageStreamPipeline(CreateCompositeGraphTransformer({
                {std::move(lower), "YtMessageStreamDiscovery", TIssuesIds::DEFAULT_ERROR},
                {Tables_->GetIODiscoveryTransformer(), "YtTableDiscovery", TIssuesIds::DEFAULT_ERROR},
            }, false));
        }
        return *Discovery_;
    }
    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        if (!Annotation_) {
            Annotation_ = new TYtMessageStreamTransformer(Streams_,
                {Streams_->GetTypeAnnotationTransformer(), "YtMessageStreamTypes", TIssuesIds::DEFAULT_ERROR},
                {Tables_->GetTypeAnnotationTransformer(instantOnly), "YtTableTypes", TIssuesIds::DEFAULT_ERROR});
        }
        return *Annotation_;
    }
    IGraphTransformer& GetConstraintTransformer(bool instantOnly, bool subGraph) override {
        if (!Constraints_) {
            Constraints_ = new TYtMessageStreamTransformer(Streams_,
                {CreateDefCallableConstraintTransformer(), "YtMessageStreamConstraints", TIssuesIds::DEFAULT_ERROR},
                {Tables_->GetConstraintTransformer(instantOnly, subGraph), "YtTableConstraints", TIssuesIds::DEFAULT_ERROR});
        }
        return *Constraints_;
    }
    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        if (!Metadata_) {
            Metadata_ = new TYtMessageStreamPipeline(CreateCompositeGraphTransformer({
                {Tables_->GetLoadTableMetadataTransformer(), "YtTableMetadata", TIssuesIds::DEFAULT_ERROR},
                {Streams_->GetLoadTableMetadataTransformer(), "YtMessageStreamMetadata", TIssuesIds::DEFAULT_ERROR},
            }, false));
        }
        return *Metadata_;
    }
    IDqIntegration* GetDqIntegration() override { return &Dq_; }
    IPlanFormatter& GetPlanFormatter() override { return *this; }
    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        if (Streams_->IsRead(node)) {
            children.push_back(node.Child(0));
            return true;
        }
        return Tables_->GetPlanFormatter().GetDependencies(node, children, compact);
    }
    TStringBuf GetName() const override {
        return Tables_->GetName();
    }

    bool Initialize(TExprContext& ctx) override {
        return Tables_->Initialize(ctx);
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return Tables_->GetConfigurationTransformer();
    }

    TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) override {
        return Tables_->GetClusterInfo(cluster, ctx);
    }

    TMaybe<TString> ResolveClusterToken(const TString& cluster) override {
        if (const auto* token = Streams_->GetClusterTokens().FindPtr(cluster)) {
            return *token;
        }
        return Tables_->ResolveClusterToken(cluster);
    }

    const THashSet<TString>& GetValidClusters() override {
        return Tables_->GetValidClusters();
    }

    IGraphTransformer& GetEpochsTransformer() override {
        return Tables_->GetEpochsTransformer();
    }

    IGraphTransformer& GetIntentDeterminationTransformer() override {
        if (!Intent_) {
            // Queue reads do not acquire native YT table intents. Their metadata
            // is loaded by the MessageStream integration after IO discovery.
            Intent_ = new TYtMessageStreamTransformer(Streams_,
                {CreateFunctorTransformer([](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) {
                    output = input;
                    return IGraphTransformer::TStatus::Ok;
                }), "YtMessageStreamIntent", TIssuesIds::DEFAULT_ERROR},
                {Tables_->GetIntentDeterminationTransformer(), "YtTableIntent", TIssuesIds::DEFAULT_ERROR});
        }
        return *Intent_;
    }

    void FillModifyCallables(THashSet<TStringBuf>& callables) override {
        return Tables_->FillModifyCallables(callables);
    }

    IGraphTransformer& GetRecaptureOptProposalTransformer() override {
        return Tables_->GetRecaptureOptProposalTransformer();
    }

    IGraphTransformer& GetStatisticsProposalTransformer() override {
        return Tables_->GetStatisticsProposalTransformer();
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return Tables_->GetLogicalOptProposalTransformer();
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return Tables_->GetPhysicalOptProposalTransformer();
    }

    IGraphTransformer& GetPhysicalFinalizingTransformer() override {
        return Tables_->GetPhysicalFinalizingTransformer();
    }

    void PostRewriteIO() override {
        return Tables_->PostRewriteIO();
    }

    void Reset() override {
        return Tables_->Reset();
    }

    bool IsPersistent(const TExprNode& node) override {
        return Tables_->IsPersistent(node);
    }

    bool IsWrite(const TExprNode& node) override {
        return Tables_->IsWrite(node);
    }

    bool CanBuildResult(const TExprNode& node, TSyncMap& syncList) override {
        return Tables_->CanBuildResult(node, syncList);
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        return Tables_->CanPullResult(node, syncList, canRef);
    }

    bool GetExecWorld(const TExprNode::TPtr& node, TExprNode::TPtr& root) override {
        return Tables_->GetExecWorld(node, root);
    }

    bool CanEvaluate(const TExprNode& node) override {
        return Tables_->CanEvaluate(node);
    }

    void EnterEvaluation(ui64 id) override {
        return Tables_->EnterEvaluation(id);
    }

    void LeaveEvaluation(ui64 id) override {
        return Tables_->LeaveEvaluation(id);
    }

    TExprNode::TPtr CleanupWorld(const TExprNode::TPtr& node, TExprContext& ctx) override {
        return Tables_->CleanupWorld(node, ctx);
    }

    TExprNode::TPtr OptimizePull(const TExprNode::TPtr& source, const TFillSettings& fillSettings, TExprContext& ctx, IOptimizationContext& optCtx) override {
        return Tables_->OptimizePull(source, fillSettings, ctx, optCtx);
    }

    void RegisterWorldArg(const TExprNode::TPtr& arg, const TExprNode::TPtr& world) override {
        return Tables_->RegisterWorldArg(arg, world);
    }

    bool CanExecute(const TExprNode& node) override {
        return Tables_->CanExecute(node);
    }

    bool ValidateExecution(const TExprNode& node, TExprContext& ctx) override {
        return Tables_->ValidateExecution(node, ctx);
    }

    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
        return Tables_->GetRequiredChildren(node, children);
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return Tables_->GetCallableExecutionTransformer();
    }

    IGraphTransformer& GetFinalizingTransformer() override {
        return Tables_->GetFinalizingTransformer();
    }

    bool CollectDiagnostics(NYson::TYsonWriter& writer) override {
        return Tables_->CollectDiagnostics(writer);
    }

    bool GetTasksInfo(NYson::TYsonWriter& writer) override {
        return Tables_->GetTasksInfo(writer);
    }

    bool CollectStatistics(NYson::TYsonWriter& writer, bool totalOnly) override {
        return Tables_->CollectStatistics(writer, totalOnly);
    }

    bool CollectDiscoveredData(NYson::TYsonWriter& writer) override {
        return Tables_->CollectDiscoveredData(writer);
    }

    IGraphTransformer& GetPlanInfoTransformer() override {
        return Tables_->GetPlanInfoTransformer();
    }

    ITrackableNodeProcessor& GetTrackableNodeProcessor() override {
        return Tables_->GetTrackableNodeProcessor();
    }

    IDqOptimization* GetDqOptimization() override {
        return Tables_->GetDqOptimization();
    }

    IYtflowIntegration* GetYtflowIntegration() override {
        return Tables_->GetYtflowIntegration();
    }

    IYtflowOptimization* GetYtflowOptimization() override {
        return Tables_->GetYtflowOptimization();
    }

    NLayers::ILayersIntegrationPtr GetLayersIntegration() const override {
        return Tables_->GetLayersIntegration();
    }

    bool IsFullCaptureReady() override {
        return Tables_->IsFullCaptureReady();
    }
    bool HasCustomPlan(const TExprNode& node) override {
        return Tables_->GetPlanFormatter().HasCustomPlan(node);
    }

    void WriteDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        return Tables_->GetPlanFormatter().WriteDetails(node, writer);
    }

    void GetResultDependencies(const TExprNode::TPtr& node, TExprNode::TListType& children, bool compact) override {
        return Tables_->GetPlanFormatter().GetResultDependencies(node, children, compact);
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override {
        return Tables_->GetPlanFormatter().GetInputs(node, inputs, withLimits);
    }

    ui32 GetOutputs(const TExprNode& node, TVector<TPinInfo>& outputs, bool withLimits) override {
        return Tables_->GetPlanFormatter().GetOutputs(node, outputs, withLimits);
    }

    TString GetProviderPath(const TExprNode& node) override {
        return Tables_->GetPlanFormatter().GetProviderPath(node);
    }

    void WritePlanDetails(const TExprNode& node, NYson::TYsonWriter& writer, bool withLimits) override {
        return Tables_->GetPlanFormatter().WritePlanDetails(node, writer, withLimits);
    }

    void WritePullDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        return Tables_->GetPlanFormatter().WritePullDetails(node, writer);
    }

    void WritePinDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        return Tables_->GetPlanFormatter().WritePinDetails(node, writer);
    }

    TString GetOperationDisplayName(const TExprNode& node) override {
        return Tables_->GetPlanFormatter().GetOperationDisplayName(node);
    }

    TString GetLinkDisplayName(const TExprNode& source, const TExprNode& dest) override {
        return Tables_->GetPlanFormatter().GetLinkDisplayName(source, dest);
    }

    bool WriteSchemaHeader(NYson::TYsonWriter& writer) override {
        return Tables_->GetPlanFormatter().WriteSchemaHeader(writer);
    }

    void WriteTypeDetails(NYson::TYsonWriter& writer, const TTypeAnnotationNode& type) override {
        return Tables_->GetPlanFormatter().WriteTypeDetails(writer, type);
    }
private:
    TIntrusivePtr<IDataProvider> Tables_;
    std::shared_ptr<IYtMessageStreamIntegration> Streams_;
    TYtMessageStreamDqIntegrationWrapper Dq_;
    TAutoPtr<IGraphTransformer> Intent_;
    TAutoPtr<IGraphTransformer> Discovery_;
    TAutoPtr<IGraphTransformer> Annotation_;
    TAutoPtr<IGraphTransformer> Constraints_;
    TAutoPtr<IGraphTransformer> Metadata_;
    THashMap<TString, TString> Tokens_;
};

} // namespace

TIntrusivePtr<IDataProvider> WrapYtDataSourceWithMessageStreams(
    TIntrusivePtr<IDataProvider> tableSource, std::shared_ptr<IYtMessageStreamIntegration> streams)
{
    Y_ENSURE(tableSource && tableSource->GetDqIntegration());
    Y_ENSURE(streams);
    return MakeIntrusive<TYtMessageStreamDataSourceWrapper>(std::move(tableSource), std::move(streams));
}

} // namespace NYql
