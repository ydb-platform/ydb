#pragma once

#include <ydb/library/yql/core/credentials/yql_credentials.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/core/services/yql_plan.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/url_lister/interface/url_lister_manager.h>
#include <ydb/library/yql/core/url_preprocessing/interface/url_preprocessing.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_user_data.h>
#include <ydb/library/yql/core/qplayer/storage/interface/yql_qstorage.h>
#include <ydb/library/yql/providers/config/yql_config_provider.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/sql/sql.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/threading/future/future.h>

#include <util/system/file.h>
#include <util/generic/ptr.h>

#include <functional>

namespace NKikimr {
namespace NMiniKQL {
    class IFunctionRegistry;
}
}

namespace NYql {

class TProgram;
using TProgramPtr = TIntrusivePtr<TProgram>;
class TProgramFactory;
using TProgramFactoryPtr = TIntrusivePtr<TProgramFactory>;

///////////////////////////////////////////////////////////////////////////////
// TProgramFactory
///////////////////////////////////////////////////////////////////////////////
class TProgramFactory: public TThrRefBase, private TMoveOnly
{
public:
    TProgramFactory(
        bool useRepeatableRandomAndTimeProviders,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        ui64 nextUniqueId,
        const TVector<TDataProviderInitializer>& dataProvidersInit,
        const TString& runner);

    void AddUserDataTable(const TUserDataTable& userDataTable);
    void SetCredentials(TCredentials::TPtr credentials);
    void SetGatewaysConfig(const TGatewaysConfig* gatewaysConfig);
    void SetModules(IModuleResolver::TPtr modules);
    void SetUrlListerManager(IUrlListerManagerPtr urlListerManager);
    void SetUdfResolver(IUdfResolver::TPtr udfResolver);
    void SetUdfIndex(TUdfIndex::TPtr udfIndex, TUdfIndexPackageSet::TPtr udfIndexPackageSet);
    void SetFileStorage(TFileStoragePtr fileStorage);
    void SetUrlPreprocessing(IUrlPreprocessing::TPtr urlPreprocessing);
    void EnableRangeComputeFor();
    void SetArrowResolver(IArrowResolver::TPtr arrowResolver);

    TProgramPtr Create(
            const TFile& file,
            const TString& sessionId = TString(),
            const TQContext& qContext = {});

    TProgramPtr Create(
            const TString& filename,
            const TString& sourceCode,
            const TString& sessionId = TString(),
            EHiddenMode hiddenMode = EHiddenMode::Disable,
            const TQContext& qContext = {});

    void UnrepeatableRandom();
private:
    const bool UseRepeatableRandomAndTimeProviders_;
    bool UseUnrepeatableRandom = false;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_;
    const ui64 NextUniqueId_;
    TVector<TDataProviderInitializer> DataProvidersInit_;
    TUserDataTable UserDataTable_;
    TCredentials::TPtr Credentials_;
    const TGatewaysConfig* GatewaysConfig_;
    IModuleResolver::TPtr Modules_;
    IUrlListerManagerPtr UrlListerManager_;
    IUdfResolver::TPtr UdfResolver_;
    TUdfIndex::TPtr UdfIndex_;
    TUdfIndexPackageSet::TPtr UdfIndexPackageSet_;
    TFileStoragePtr FileStorage_;
    IUrlPreprocessing::TPtr UrlPreprocessing_;
    TString Runner_;
    bool EnableRangeComputeFor_ = false;
    IArrowResolver::TPtr ArrowResolver_;
};

///////////////////////////////////////////////////////////////////////////////
// TProgram
///////////////////////////////////////////////////////////////////////////////
class TProgram: public TThrRefBase, private TNonCopyable
{
public:
    friend TProgramFactory;
    using TStatus = IGraphTransformer::TStatus;
    using TFutureStatus = NThreading::TFuture<TStatus>;

public:
    ~TProgram();

    void AddCredentials(const TVector<std::pair<TString, TCredential>>& credentials);
    void ClearCredentials();

    void AddUserDataTable(const TUserDataTable& userDataTable);

    bool ParseYql();
    bool ParseSql();
    bool ParseSql(const NSQLTranslation::TTranslationSettings& settings);

    bool Compile(const TString& username, bool skipLibraries = false);

    TStatus Discover(const TString& username);

    TFutureStatus DiscoverAsync(const TString& username);

    TStatus Lineage(const TString& username, IOutputStream* traceOut = nullptr, IOutputStream* exprOut = nullptr, bool withTypes = false);

    TFutureStatus LineageAsync(const TString& username, IOutputStream* traceOut = nullptr, IOutputStream* exprOut = nullptr, bool withTypes = false);

    TStatus Validate(const TString& username, IOutputStream* exprOut = nullptr, bool withTypes = false);

    TFutureStatus ValidateAsync(const TString& username, IOutputStream* exprOut = nullptr, bool withTypes = false);

    TStatus Optimize(
            const TString& username,
            IOutputStream* traceOut = nullptr,
            IOutputStream* tracePlan = nullptr,
            IOutputStream* exprOut = nullptr,
            bool withTypes = false);

    TFutureStatus OptimizeAsync(
            const TString& username,
            IOutputStream* traceOut = nullptr,
            IOutputStream* tracePlan = nullptr,
            IOutputStream* exprOut = nullptr,
            bool withTypes = false);

    TStatus Run(
            const TString& username,
            IOutputStream* traceOut = nullptr,
            IOutputStream* tracePlan = nullptr,
            IOutputStream* exprOut = nullptr,
            bool withTypes = false);

    TFutureStatus RunAsync(
            const TString& username,
            IOutputStream* traceOut = nullptr,
            IOutputStream* tracePlan = nullptr,
            IOutputStream* exprOut = nullptr,
            bool withTypes = false);

    TStatus LineageWithConfig(
            const TString& username,
            const IPipelineConfigurator& pipelineConf);

    TFutureStatus LineageAsyncWithConfig(
            const TString& username,
            const IPipelineConfigurator& pipelineConf);

    TStatus OptimizeWithConfig(
            const TString& username,
            const IPipelineConfigurator& pipelineConf);

    TFutureStatus OptimizeAsyncWithConfig(
            const TString& username,
            const IPipelineConfigurator& pipelineConf);

    TStatus RunWithConfig(
            const TString& username,
            const IPipelineConfigurator& pipelineConf);

    TFutureStatus RunAsyncWithConfig(
            const TString& username,
            const IPipelineConfigurator& pipelineConf);

    TFutureStatus ContinueAsync();

    bool HasActiveProcesses();
    bool NeedWaitForActiveProcesses();

    [[nodiscard]]
    NThreading::TFuture<void> Abort();

    TIssues Issues() const;
    TIssues CompletedIssues() const;
    void FinalizeIssues();

    void Print(IOutputStream* exprOut, IOutputStream* planOut, bool cleanPlan = false);

    inline void PrintErrorsTo(IOutputStream& out) const {
        Issues().PrintWithProgramTo(out, Filename_, SourceCode_);
    }

    inline const TAstNode* AstRoot() const {
        return AstRoot_;
    }

    inline const TExprNode::TPtr& ExprRoot() const {
        return ExprRoot_;
    }

    inline TExprContext& ExprCtx() const {
        return *ExprCtx_;
    }

    inline bool HasResults() const {
        return ResultProviderConfig_ &&
                !ResultProviderConfig_->CommittedResults.empty();
    }

    inline const TVector<TString>& Results() const {
        return ResultProviderConfig_->CommittedResults;
    }

    TMaybe<TString> GetQueryAst(TMaybe<size_t> memoryLimit = {});
    TMaybe<TString> GetQueryPlan(const TPlanSettings& settings = {});

    void SetDiagnosticFormat(NYson::EYsonFormat format) {
        DiagnosticFormat_ = format;
    }

    void SetResultType(IDataProvider::EResultFormat type) {
        ResultType_ = type;
    }

    TMaybe<TString> GetDiagnostics();
    IGraphTransformer::TStatistics GetRawDiagnostics();

    TMaybe<TString> GetTasksInfo();

    TMaybe<TString> GetStatistics(bool totalOnly = false, THashMap<TString, TStringBuf> extraYsons = {});

    TMaybe<TString> GetDiscoveredData();

    TMaybe<TString> GetLineage();

    TString ResultsAsString() const;
    void ConfigureYsonResultFormat(NYson::EYsonFormat format);

    inline IOutputStream* ExprStream() const { return ExprStream_; }
    inline IOutputStream* PlanStream() const { return PlanStream_; }

    NYson::EYsonFormat GetResultFormat() const { return ResultFormat_; }
    NYson::EYsonFormat GetOutputFormat() const { return OutputFormat_; }

    void SetValidateOptions(NUdf::EValidateMode validateMode);
    void SetDisableNativeUdfSupport(bool disable);
    void SetUseTableMetaFromGraph(bool use);

    void SetProgressWriter(TOperationProgressWriter writer) {
        Y_ENSURE(!TypeCtx_, "TypeCtx_ already created");
        ProgressWriter_ = ThreadSafeProgressWriter(writer);
    }

    void SetAuthenticatedUser(const TString& user) {
        Y_ENSURE(!TypeCtx_, "TypeCtx_ already created");
        OperationOptions_.AuthenticatedUser = user;
    }

    void SetOperationId(const TString& id) {
        Y_ENSURE(!TypeCtx_, "TypeCtx_ already created");
        OperationOptions_.Id = id;
    }

    void SetSharedOperationId(const TString& id) {
        Y_ENSURE(!TypeCtx_, "TypeCtx_ already created");
        OperationOptions_.SharedId = id;
    }

    void SetOperationTitle(const TString& title) {
        Y_ENSURE(!TypeCtx_, "TypeCtx_ already created");
        if (!title.Contains("YQL")) {
            ythrow yexception() << "Please mention YQL in the title '" << title << "'";
        }

        OperationOptions_.Title = title;
    }

    void SetOperationUrl(const TString& url) {
        Y_ENSURE(!TypeCtx_, "TypeCtx_ already created");
        OperationOptions_.Url = url;
    }

    void SetQueryName(const TString& name) {
        Y_ENSURE(!TypeCtx_, "TypeCtx_ already created");
        OperationOptions_.QueryName = name;
    }

    void SetOperationAttrsYson(const TString& attrs) {
        Y_ENSURE(!TypeCtx_, "TypeCtx_ already created");
        OperationOptions_.AttrsYson = attrs;
    }

    void SetParametersYson(const TString& parameters);
    // should be used after Compile phase
    bool ExtractQueryParametersMetadata();

    const TString& GetExtractedQueryParametersMetadataYson() const {
        return ExtractedQueryParametersMetadataYson_;
    }

    void EnableResultPosition() {
        SupportsResultPosition_ = true;
    }

    IPlanBuilder& GetPlanBuilder();

    void SetAbortHidden(std::function<void()>&& func) {
        AbortHidden_ = std::move(func);
    }

    TMaybe<TSet<TString>> GetUsedClusters() {
        CollectUsedClusters();
        return UsedClusters_;
    }

private:
    TProgram(
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TIntrusivePtr<IRandomProvider> randomProvider,
        const TIntrusivePtr<ITimeProvider> timeProvider,
        ui64 nextUniqueId,
        const TVector<TDataProviderInitializer>& dataProvidersInit,
        const TUserDataTable& userDataTable,
        const TCredentials::TPtr& credentials,
        const IModuleResolver::TPtr& modules,
        const IUrlListerManagerPtr& urlListerManager,
        const IUdfResolver::TPtr& udfResolver,
        const TUdfIndex::TPtr& udfIndex,
        const TUdfIndexPackageSet::TPtr& udfIndexPackageSet,
        const TFileStoragePtr& fileStorage,
        const IUrlPreprocessing::TPtr& urlPreprocessing,
        const TGatewaysConfig* gatewaysConfig,
        const TString& filename,
        const TString& sourceCode,
        const TString& sessionId,
        const TString& runner,
        bool enableRangeComputeFor,
        const IArrowResolver::TPtr& arrowResolver,
        EHiddenMode hiddenMode,
        const TQContext& qContext);

    TTypeAnnotationContextPtr BuildTypeAnnotationContext(const TString& username);
    TTypeAnnotationContextPtr GetAnnotationContext() const;
    TTypeAnnotationContextPtr ProvideAnnotationContext(const TString& username);
    bool CollectUsedClusters();

    NThreading::TFuture<void> OpenSession(const TString& username);

    [[nodiscard]]
    NThreading::TFuture<void> CleanupLastSession();
    [[nodiscard]]
    NThreading::TFuture<void> CloseLastSession();

    TFutureStatus RemoteKikimrValidate(const TString& cluster);
    TFutureStatus RemoteKikimrOptimize(const TString& cluster, const IPipelineConfigurator* pipelineConf);
    TFutureStatus RemoteKikimrRun(const TString& cluster, const IPipelineConfigurator* pipelineConf);

    bool FillParseResult(NYql::TAstParseResult&& astRes, NYql::TWarningRules* warningRules = nullptr);
    TString GetSessionId() const;

    NThreading::TFuture<IGraphTransformer::TStatus> AsyncTransformWithFallback(bool applyAsyncChanges);
    void SaveExprRoot();

private:
    std::optional<bool> CheckFallbackIssues(const TIssues& issues);
    void HandleSourceCode(TString& sourceCode);
    void HandleTranslationSettings(NSQLTranslation::TTranslationSettings& loadedSettings,
        const NSQLTranslation::TTranslationSettings*& currentSettings);

    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const TIntrusivePtr<ITimeProvider> TimeProvider_;
    const ui64 NextUniqueId_;

    TAstNode* AstRoot_;
    std::unique_ptr<TMemoryPool> AstPool_;
    const IModuleResolver::TPtr Modules_;
    TAutoPtr<TExprContext> ExprCtx_;
    TTypeAnnotationContextPtr TypeCtx_;

    TVector<TDataProviderInitializer> DataProvidersInit_;
    TAdaptiveLock DataProvidersLock_;
    TVector<TDataProviderInfo> DataProviders_;
    TYqlOperationOptions OperationOptions_;
    TCredentials::TPtr Credentials_;
    const IUrlListerManagerPtr UrlListerManager_;
    IUdfResolver::TPtr UdfResolver_;
    const TUdfIndex::TPtr UdfIndex_;
    const TUdfIndexPackageSet::TPtr UdfIndexPackageSet_;
    const TFileStoragePtr FileStorage_;
    TUserDataTable SavedUserDataTable_;
    TUserDataStorage::TPtr UserDataStorage_;
    const TGatewaysConfig* GatewaysConfig_;
    TGatewaysConfig LoadedGatewaysConfig_;
    TString Filename_;
    TString SourceCode_;
    ESourceSyntax SourceSyntax_;
    ui16 SyntaxVersion_;

    TExprNode::TPtr ExprRoot_;
    TExprNode::TPtr SavedExprRoot_;
    mutable TAdaptiveLock SessionIdLock_;
    TString SessionId_;
    NThreading::TFuture<void> CloseLastSessionFuture_;
    TAutoPtr<IPlanBuilder> PlanBuilder_;
    TAutoPtr<IGraphTransformer> Transformer_;
    TIntrusivePtr<TResultProviderConfig> ResultProviderConfig_;
    bool SupportsResultPosition_ = false;
    IDataProvider::EResultFormat ResultType_;
    NYson::EYsonFormat ResultFormat_;
    NYson::EYsonFormat OutputFormat_;
    TMaybe<NYson::EYsonFormat> DiagnosticFormat_;
    NUdf::EValidateMode ValidateMode_ = NUdf::EValidateMode::None;
    bool DisableNativeUdfSupport_ = false;
    bool UseTableMetaFromGraph_ = false;
    TMaybe<TSet<TString>> UsedClusters_;
    TMaybe<TSet<TString>> UsedProviders_;
    TMaybe<TString> ExternalQueryAst_;
    TMaybe<TString> ExternalQueryPlan_;
    TMaybe<TString> ExternalDiagnostics_;

    IOutputStream* ExprStream_ = nullptr;
    IOutputStream* PlanStream_ = nullptr;
    TOperationProgressWriter ProgressWriter_ = [](const TOperationProgress&) {};
    TString ExtractedQueryParametersMetadataYson_;
    const bool EnableRangeComputeFor_;
    const IArrowResolver::TPtr ArrowResolver_;
    i64 FallbackCounter_ = 0;
    const EHiddenMode HiddenMode_ = EHiddenMode::Disable;
    THiddenQueryAborter AbortHidden_ = [](){};
    TMaybe<TString> LineageStr_;

    TQContext QContext_;
    TIssues FinalIssues_;
};

} // namspace NYql
