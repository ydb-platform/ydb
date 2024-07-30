#pragma once

#include "yql_graph_transformer.h"
#include "yql_callable_names.h"

#include <ydb/library/yql/public/udf/udf_validate.h>

#include <library/cpp/yson/writer.h>

#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>

#include <functional>

class IRandomProvider;
class ITimeProvider;

namespace NKikimr  {
    namespace NMiniKQL {
        class IFunctionRegistry;
    }
}

namespace NYql {

struct TPinInfo {
    const TExprNode* DataSource;
    const TExprNode* DataSink;
    const TExprNode* Key;
    TString DisplayName;
    bool HideInBasicPlan;

    TPinInfo(const TExprNode* dataSource, const TExprNode* dataSink,
        const TExprNode* key, const TString& displayName, bool hideInBasicPlan)
        : DataSource(dataSource)
        , DataSink(dataSink)
        , Key(key)
        , DisplayName(displayName)
        , HideInBasicPlan(hideInBasicPlan)
    {}
};

class IPlanFormatter {
public:
    virtual ~IPlanFormatter() {}

    virtual void WriteDetails(const TExprNode& node, NYson::TYsonWriter& writer) = 0;
    // returns visibility of node
    virtual bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) = 0;
    virtual void GetResultDependencies(const TExprNode::TPtr& node, TExprNode::TListType& children, bool compact) = 0;
    // returns full number of inputs
    virtual ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) = 0;
    // returns full number of outputs
    virtual ui32 GetOutputs(const TExprNode& node, TVector<TPinInfo>& outputs, bool withLimits) = 0;
    virtual TString GetProviderPath(const TExprNode& node) = 0;
    virtual void WritePlanDetails(const TExprNode& node, NYson::TYsonWriter& writer, bool withLimits) = 0;
    virtual void WritePullDetails(const TExprNode& node, NYson::TYsonWriter& writer) = 0;
    virtual void WritePinDetails(const TExprNode& node, NYson::TYsonWriter& writer) = 0;
    virtual TString GetOperationDisplayName(const TExprNode& node) = 0;
    // returns false if provider schemas aren't supported
    virtual bool WriteSchemaHeader(NYson::TYsonWriter& writer) = 0;
    virtual void WriteTypeDetails(NYson::TYsonWriter& writer, const TTypeAnnotationNode& type) = 0;
};

class ITrackableNodeProcessor {
public:
    virtual ~ITrackableNodeProcessor() = default;

    struct TExprNodeAndId
    {
        TExprNode::TPtr Node;
        TString Id;
    };

    virtual void GetUsedNodes(const TExprNode& node, TVector<TString>& usedNodeIds) = 0;
    virtual void GetCreatedNodes(const TExprNode& node, TVector<TExprNodeAndId>& createdNodes, TExprContext& ctx) = 0;
    virtual IGraphTransformer& GetCleanupTransformer() = 0;
};

class IDqIntegration;
class IDqOptimization;

class IOptimizationContext;

class IDataProvider : public TThrRefBase {
public:
    virtual ~IDataProvider() {}

    virtual TStringBuf GetName() const = 0;

    enum class EResultFormat {
        Yson,
        Custom,
        Skiff
    };

    // settings for result data provider
    struct TFillSettings {
        TMaybe<ui64> AllResultsBytesLimit = 100000;
        TMaybe<ui64> RowsLimitPerWrite = 1000; // only if list is written
        EResultFormat Format;
        TString FormatDetails;
        bool Discard = false;
    };

    virtual bool Initialize(TExprContext& ctx) = 0;

    //-- configuration
    virtual IGraphTransformer& GetConfigurationTransformer() = 0;
    virtual TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) = 0;
    virtual const THashMap<TString, TString>* GetClusterTokens() = 0;
    virtual void AddCluster(const TString& name, const THashMap<TString, TString>& properties) = 0;

    //-- discovery & rewrite
    virtual IGraphTransformer& GetIODiscoveryTransformer() = 0;

    //-- assign epochs
    virtual IGraphTransformer& GetEpochsTransformer() = 0;

    //-- intent determination
    virtual IGraphTransformer& GetIntentDeterminationTransformer() = 0;

    //-- type check
    virtual bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) = 0;
    virtual bool CanParse(const TExprNode& node) = 0;
    virtual IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) = 0;
    virtual IGraphTransformer& GetConstraintTransformer(bool instantOnly, bool subGraph) = 0;
    // Fill set of callables, which have world as first child and should be trimmed in evaluation
    virtual void FillModifyCallables(THashSet<TStringBuf>& callables) = 0;

    //-- optimizations
    virtual TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) = 0;
    virtual IGraphTransformer& GetRecaptureOptProposalTransformer() = 0;
    virtual IGraphTransformer& GetStatisticsProposalTransformer() = 0;
    virtual IGraphTransformer& GetLogicalOptProposalTransformer() = 0;
    virtual IGraphTransformer& GetPhysicalOptProposalTransformer() = 0;
    virtual IGraphTransformer& GetPhysicalFinalizingTransformer() = 0;
    virtual void PostRewriteIO() = 0;
    virtual void Reset() = 0;

    //-- metadata loading
    virtual IGraphTransformer& GetLoadTableMetadataTransformer() = 0;

    // This function is used in core optimizers to check either the node can be used as input multiple times or not
    virtual bool IsPersistent(const TExprNode& node) = 0;
    virtual bool IsRead(const TExprNode& node) = 0;
    virtual bool IsWrite(const TExprNode& node) = 0;

    // Right! or worlds are written to syncList
    virtual bool CanBuildResult(const TExprNode& node, TSyncMap& syncList) = 0;
    virtual bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) = 0;
    virtual bool GetExecWorld(const TExprNode::TPtr& node, TExprNode::TPtr& root) = 0;
    virtual bool CanEvaluate(const TExprNode& node) = 0;
    virtual void EnterEvaluation(ui64 id) = 0;
    virtual void LeaveEvaluation(ui64 id) = 0;
    virtual TExprNode::TPtr CleanupWorld(const TExprNode::TPtr& node, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr OptimizePull(const TExprNode::TPtr& source, const TFillSettings& fillSettings, TExprContext& ctx,
        IOptimizationContext& optCtx) = 0;

    //-- execution
    virtual bool CanExecute(const TExprNode& node) = 0;
    virtual bool ValidateExecution(const TExprNode& node, TExprContext& ctx) = 0;
    virtual void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) = 0;
    virtual IGraphTransformer& GetCallableExecutionTransformer() = 0;

    //-- finalizing
    virtual IGraphTransformer& GetFinalizingTransformer() = 0;
    virtual bool CollectDiagnostics(NYson::TYsonWriter& writer) = 0;
    virtual bool GetTasksInfo(NYson::TYsonWriter& writer) = 0;
    virtual bool CollectStatistics(NYson::TYsonWriter& writer, bool totalOnly) = 0;
    virtual bool CollectDiscoveredData(NYson::TYsonWriter& writer) = 0;

    //-- plan
    virtual IGraphTransformer& GetPlanInfoTransformer() = 0;
    virtual IPlanFormatter& GetPlanFormatter() = 0;

    //-- garbage collection
    virtual ITrackableNodeProcessor& GetTrackableNodeProcessor() = 0;

    // DQ
    virtual IDqIntegration* GetDqIntegration() = 0;
    virtual IDqOptimization* GetDqOptimization() = 0;
};

struct IPipelineConfigurator;
struct TTypeAnnotationContext;
struct TResultProviderConfig;
struct TYqlOperationOptions;
struct TOperationProgress;
class TGatewaysConfig;


using TOperationProgressWriter = std::function<void(const TOperationProgress&)>;

enum class ESourceSyntax {
    Unknown,
    Sql,
    Yql
};

struct TDataProviderInfo {
    using TFutureStatus = NThreading::TFuture<IGraphTransformer::TStatus>;

    THashSet<TString> Names;
    TIntrusivePtr<IDataProvider> Source;
    TIntrusivePtr<IDataProvider> Sink;
    bool SupportFullResultDataSink = false;
    bool WaitForActiveProcesses = true;
    bool SupportsHidden = false;

    std::function<NThreading::TFuture<void>(const TString& sessionId, const TString& username,
        const TOperationProgressWriter& progressWriter, const TYqlOperationOptions& operationOptions,
        TIntrusivePtr<IRandomProvider> randomProvider, TIntrusivePtr<ITimeProvider> timeProvider)> OpenSession;

    std::function<bool()> HasActiveProcesses;

    // COMPAT(gritukan): Remove it after Arcadia migration.
    std::function<void(const TString& sessionId)> CloseSession;
    std::function<void(const TString& sessionId)> CleanupSession;

    std::function<NThreading::TFuture<void>(const TString& sessionId)> CloseSessionAsync;

    std::function<NThreading::TFuture<void>(const TString& sessionId)> CleanupSessionAsync;

    std::function<TString(const TString& url, const TString& alias)> TokenResolver;
};

using THiddenQueryAborter = std::function<void()>; // aborts hidden query, which is running within a separate TProgram
class TQContext;
using TDataProviderInitializer = std::function<TDataProviderInfo(
    const TString& userName,
    const TString& sessionId,
    const TGatewaysConfig* gatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    TIntrusivePtr<IRandomProvider> randomProvider,
    TIntrusivePtr<TTypeAnnotationContext> typeCtx,
    const TOperationProgressWriter& progressWriter,
    const TYqlOperationOptions& operationOptions,
    THiddenQueryAborter hiddenAborter,
    const TQContext& qContext)>;

} // namespace NYql
