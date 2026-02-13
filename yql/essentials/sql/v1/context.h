#pragma once

#include "source.h"
#include "sql.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/issue/protos/issue_id.pb.h>
#include <yql/essentials/public/issue/yql_warning.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/cluster_mapping.h>

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>

namespace NSQLTranslationV1 {
inline bool IsAnonymousName(const TString& name) {
    return name == "$_";
}

inline bool IsStreamingService(const TString& service) {
    return service == NYql::RtmrProviderName || service == NYql::PqProviderName;
}

struct TNodeWithUsageInfo: public TThrRefBase {
    explicit TNodeWithUsageInfo(const TNodePtr& node, TPosition namePos, int level)
        : Node(node)
        , NamePos(namePos)
        , Level(level)
    {
    }

    TNodePtr Node;
    TPosition NamePos;
    int Level = 0;
    bool IsUsed = false;
};

using TNodeWithUsageInfoPtr = TIntrusivePtr<TNodeWithUsageInfo>;
using TNamedNodesMap = THashMap<TString, TDeque<TNodeWithUsageInfoPtr>>;
using TBlocks = TVector<TNodePtr>;

struct TScopedState: public TThrRefBase {
    TString CurrService;
    TDeferredAtom CurrCluster;
    bool PragmaClassicDivision = true;
    bool PragmaCheckedOps = false;
    bool StrictJoinKeyTypes = false;
    bool UnicodeLiterals = false;
    bool WarnUntypedStringLiterals = false;
    bool SimplePgByDefault = false;
    TNamedNodesMap NamedNodes;

    struct TLocal {
        TVector<std::pair<TString, TDeferredAtom>> UsedClusters;
        THashSet<TString> UsedPlainClusters;
        THashSet<INode*> UsedExprClusters;
        THashMap<INode*, std::pair<TString, TNodePtr>> ExprClustersMap;
        TVector<TNodePtr> ExprClusters;
    };

    TLocal Local;

    void UseCluster(const TString& service, const TDeferredAtom& cluster);
    const TVector<std::pair<TString, TDeferredAtom>>& GetUsedClusters();
    TNodePtr WrapCluster(const TDeferredAtom& cluster, TContext& ctx);
    void AddExprCluster(TNodePtr expr, TContext& ctx);
    void Clear();
    TNodePtr LookupNode(const TString& name);
};

using TScopedStatePtr = TIntrusivePtr<TScopedState>;

class TColumnRefScope;
enum class EColumnRefState {
    Deny,
    Allow,
    AsStringLiteral,
    AsPgType,
    MatchRecognizeMeasures,
    MatchRecognizeDefine,
    MatchRecognizeDefineAggregate,
};

enum class EYqlSelectMode {
    Disable,
    Auto,
    Force,
};

class TContext {
public:
    TContext(const TLexers& lexers,
             const TParsers& parsers,
             const NSQLTranslation::TTranslationSettings& settings,
             const NSQLTranslation::TSQLHints& hints,
             NYql::TIssues& issues,
             const TString& query = {});

    virtual ~TContext();

    const NYql::TPosition& Pos() const;

    void PushCurrentBlocks(TBlocks* blocks);
    void PopCurrentBlocks();
    TBlocks& GetCurrentBlocks() const;

    TString MakeName(const TString& name);

    IOutputStream& Fatal();
    IOutputStream& Error(NYql::TIssueCode code = NYql::TIssuesIds::DEFAULT_ERROR);
    IOutputStream& Error(NYql::TPosition pos, NYql::TIssueCode code = NYql::TIssuesIds::DEFAULT_ERROR);
    bool Warning(NYql::TPosition pos, NYql::TIssueCode code, std::function<void(IOutputStream&)> message,
                 bool forceError = false);
    IOutputStream& Info(NYql::TPosition pos);

    void SetWarningPolicyFor(NYql::TIssueCode code, NYql::EWarningAction action);

    const TString& Token(const NSQLv1Generated::TToken& token) {
        Position_.Row = token.GetLine();
        Position_.Column = token.GetColumn() + 1;
        return token.GetValue();
    }

    TPosition TokenPosition(const NSQLv1Generated::TToken& token) {
        TPosition pos = Position_;
        pos.Row = token.GetLine();
        pos.Column = token.GetColumn() + 1;
        return pos;
    }

    inline void IncrementMonCounter(const TString& name, const TString& value) {
        if (IncrementMonCounterFunction) {
            IncrementMonCounterFunction(name, value);
        }
    }

    bool HasCluster(const TString& cluster) const {
        return GetClusterProvider(cluster).Defined();
    }

    TMaybe<TString> GetClusterProvider(const TString& cluster) const {
        TString unusedNormalizedClusterName;
        return GetClusterProvider(cluster, unusedNormalizedClusterName);
    }

    TMaybe<TString> GetClusterProvider(const TString& cluster, TString& normalizedClusterName) const {
        auto provider = ClusterMapping_.GetClusterProvider(cluster, normalizedClusterName);
        if (!provider) {
            if (Settings.AssumeYdbOnClusterWithSlash && cluster.StartsWith('/')) {
                normalizedClusterName = cluster;
                return TString(NYql::KikimrProviderName);
            }
            if (Settings.DynamicClusterProvider) {
                normalizedClusterName = cluster.StartsWith('/') ? cluster : Settings.PathPrefix + "/" + cluster;
                return Settings.DynamicClusterProvider;
            }
            return Nothing();
        }

        return provider;
    }

    bool IsDynamicCluster(const TDeferredAtom& cluster) const;
    bool HasNonYtProvider(const ISource& source) const;
    bool UseUnordered(const ISource& source) const;
    bool UseUnordered(const TTableRef& table) const;

    bool SetPathPrefix(const TString& value, TMaybe<TString> arg = TMaybe<TString>());

    TNodePtr GetPrefixedPath(const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& path);
    TStringBuf GetPrefixPath(const TString& service, const TDeferredAtom& cluster) const;

    TNodePtr UniversalAlias(const TString& baseName, TNodePtr&& node);

    void BodyPart() {
        IntoHeading_ = false;
    }

    bool IsParseHeading() const {
        return IntoHeading_;
    }

    bool IsAlreadyDeclared(const TString& varName) const;
    void DeclareVariable(const TString& varName, const TPosition& pos, const TNodePtr& typeNode, bool isWeak = false);

    bool AddExport(TPosition symbolPos, const TString& symbolName);
    TString AddImport(const TVector<TString>& modulePath);
    TString AddSimpleUdf(const TString& udf);
    void SetPackageVersion(const TString& packageName, ui32 version);

    bool IsStreamingService(const TStringBuf service) const;

    bool CheckColumnReference(TPosition pos, const TString& name) {
        const bool allowed = GetColumnReferenceState() != EColumnRefState::Deny;
        if (!allowed) {
            Error(pos) << "Column reference \"" << name << "\" is not allowed " << NoColumnErrorContext_;
            IncrementMonCounter("sql_errors", "ColumnReferenceInScopeIsNotAllowed");
        }
        return allowed;
    }

    EColumnRefState GetColumnReferenceState() const {
        return ColumnReferenceState_;
    }

    EColumnRefState GetTopLevelColumnReferenceState() const {
        return TopLevelColumnReferenceState_;
    }

    [[nodiscard]] TString GetMatchRecognizeDefineVar() const {
        YQL_ENSURE(EColumnRefState::MatchRecognizeMeasures == ColumnReferenceState_ ||
                       EColumnRefState::MatchRecognizeDefine == ColumnReferenceState_ ||
                       EColumnRefState::MatchRecognizeDefineAggregate == ColumnReferenceState_,
                   "MATCH_RECOGNIZE Var can only be accessed within processing of MATCH_RECOGNIZE lambdas");
        return MatchRecognizeDefineVar_;
    }

    TString ExtractMatchRecognizeAggrVar() {
        YQL_ENSURE(EColumnRefState::MatchRecognizeMeasures == ColumnReferenceState_ ||
                       EColumnRefState::MatchRecognizeDefine == ColumnReferenceState_ ||
                       EColumnRefState::MatchRecognizeDefineAggregate == ColumnReferenceState_,
                   "MATCH_RECOGNIZE Var can only be accessed within processing of MATCH_RECOGNIZE lambdas");
        return std::exchange(MatchRecognizeAggrVar_, "");
    }

    [[nodiscard]] bool SetMatchRecognizeAggrVar(TString var) {
        YQL_ENSURE(EColumnRefState::MatchRecognizeMeasures == ColumnReferenceState_ ||
                       EColumnRefState::MatchRecognizeDefine == ColumnReferenceState_ ||
                       EColumnRefState::MatchRecognizeDefineAggregate == ColumnReferenceState_,
                   "MATCH_RECOGNIZE Var can only be accessed within processing of MATCH_RECOGNIZE lambdas");
        if (MatchRecognizeAggrVar_.empty()) {
            MatchRecognizeAggrVar_ = std::move(var);
        } else if (MatchRecognizeAggrVar_ != var) {
            Error() << "Illegal use of aggregates or navigation operators in MATCH_RECOGNIZE";
            return false;
        }
        return true;
    }

    [[nodiscard]] auto& GetMatchRecognizeAggregations() {
        YQL_ENSURE(EColumnRefState::MatchRecognizeMeasures == ColumnReferenceState_ ||
                       EColumnRefState::MatchRecognizeDefine == ColumnReferenceState_ ||
                       EColumnRefState::MatchRecognizeDefineAggregate == ColumnReferenceState_,
                   "MATCH_RECOGNIZE Var can only be accessed within processing of MATCH_RECOGNIZE lambdas");
        return MatchRecognizeAggregations_;
    }

    TVector<NSQLTranslation::TSQLHint> PullHintForToken(NYql::TPosition tokenPos);
    bool WarnUnusedHints();

    TScopedStatePtr CreateScopedState() const;

    EYqlSelectMode GetYqlSelectMode() const {
        return YqlSelectMode_;
    }

    void SetYqlSelectMode(EYqlSelectMode mode) {
        YqlSelectMode_ = mode;
        if (YqlSelectMode_ != EYqlSelectMode::Disable) {
            DeriveColumnOrder = true;
        }
    }

    bool EnsureBackwardCompatibleFeatureAvailable(
        TPosition position,
        TStringBuf feature,
        NYql::TLangVersion version);

private:
    bool IsBackwardCompatibleFeatureAvailable(NYql::TLangVersion featureVer) const;

    IOutputStream& MakeIssue(
        NYql::ESeverity severity,
        NYql::TIssueCode code,
        NYql::TPosition pos,
        bool forceError,
        bool& isError);

public:
    const TLexers Lexers;
    const TParsers Parsers;

private:
    NYql::TPosition Position_;
    THolder<TStringOutput> IssueMsgHolder_;
    NSQLTranslation::TClusterMapping ClusterMapping_;
    TString PathPrefix_;
    THashMap<TString, TString> ProviderPathPrefixes_;
    THashMap<TString, TString> ClusterPathPrefixes_;
    bool IntoHeading_ = true;
    NSQLTranslation::TSQLHints SqlHints_;

    friend class TColumnRefScope;

    EColumnRefState ColumnReferenceState_ = EColumnRefState::Deny;
    EColumnRefState TopLevelColumnReferenceState_ = EColumnRefState::Deny;
    TString MatchRecognizeDefineVar_;
    TString MatchRecognizeAggrVar_;
    struct TMatchRecognizeAggregation {
        TString Var;
        TAggregationPtr Aggr;
    };
    TVector<TMatchRecognizeAggregation> MatchRecognizeAggregations_;
    TString NoColumnErrorContext_ = "in current scope";
    TVector<TBlocks*> CurrentBlocks_;
    EYqlSelectMode YqlSelectMode_ = EYqlSelectMode::Disable;

public:
    THashMap<TString, std::pair<TPosition, TNodePtr>> Variables;
    THashSet<TString> WeakVariables;
    NSQLTranslation::TTranslationSettings Settings;
    const TString Query;
    std::unique_ptr<TMemoryPool> Pool;
    NYql::TIssues& Issues;
    TMap<TString, TNodePtr> UniversalAliases;
    THashSet<TString> Exports;
    THashMap<TString, TString> ImportModuleAliases;
    THashMap<TString, TString> RequiredModules;
    TMap<TString, TString> SimpleUdfs;
    NSQLTranslation::TIncrementMonCounterFunction IncrementMonCounterFunction;
    TScopedStatePtr Scoped;
    int ScopeLevel = 0;
    size_t AnonymousNameIndex = 0;
    TDeque<TScopedStatePtr> AllScopes;
    bool HasPendingErrors;
    THashMap<TString, ui32> GenIndexes;
    using TWinSpecsRef = std::reference_wrapper<TWinSpecs>;
    TDeque<TWinSpecsRef> WinSpecsScopes;
    bool PragmaRefSelect = false;
    bool PragmaSampleSelect = false;
    bool PragmaAllowDotInAlias = false;
    bool PragmaInferSchema = false;
    bool PragmaAutoCommit = false;
    bool PragmaUseTablePrefixForEach = false;
    bool SimpleColumns = true;
    bool CoalesceJoinKeysOnQualifiedAll = false;
    bool PragmaDirectRead = false;
    bool PragmaYsonFast = true;
    bool PragmaYsonAutoConvert = false;
    bool PragmaYsonStrict = true;
    bool PragmaRegexUseRe2 = true;
    bool PragmaPullUpFlatMapOverJoin = true;
    bool FilterPushdownOverJoinOptionalSide = false;
    bool RotateJoinTree = true;
    bool WarnUnnamedColumns = false;
    bool DiscoveryMode = false;
    bool ExceptIntersectBefore202503 = true; // TODO(YQL-20367)
    bool EnableSystemColumns = true;
    bool DqEngineEnable = false;
    bool DqEngineForce = false;
    TString CostBasedOptimizer;
    TMaybe<ui32> CostBasedOptimizerVersion;
    TMaybe<bool> JsonQueryReturnsJsonDocument;
    TMaybe<bool> AnsiInForEmptyOrNullableItemsCollections;
    TMaybe<bool> AnsiRankForNullableKeys = true;
    const bool AnsiQuotedIdentifiers;
    bool AnsiOptionalAs = true;
    bool OrderedColumns = false;
    bool DeriveColumnOrder = false;
    bool PositionalUnionAll = false;
    bool BogousStarInGroupByOverJoin = false;
    bool UnorderedSubqueries = true;
    bool PragmaDataWatermarks = true;
    bool WarnOnAnsiAliasShadowing = true;
    ui32 ResultRowsLimit = 0;
    ui64 ResultSizeLimit = 0;
    TString RuntimeLogLevel;
    ui32 PragmaGroupByLimit = 1 << 6;
    ui32 PragmaGroupByCubeLimit = 5;
    // if FlexibleTypes=true, emit TypeOrMember callable and resolve Type/Column uncertainty on type annotation stage, otherwise always emit Type
    bool FlexibleTypes = true;
    // see YQL-10265
    bool AnsiCurrentRow = false;
    TMaybe<bool> YsonCastToString;
    using TLiteralWithPosition = std::pair<TString, TPosition>;
    using TLibraryStuff = std::tuple<TPosition, std::optional<TLiteralWithPosition>, std::optional<TLiteralWithPosition>>;
    std::unordered_map<TString, TLibraryStuff> Libraries; // alias -> optional file with token
    using TPackageStuff = std::tuple<
        TPosition, TLiteralWithPosition,
        std::optional<TLiteralWithPosition>>;

    std::unordered_map<TString, TPackageStuff> Packages; // alias -> url with optional token

    using TOverrideLibraryStuff = std::tuple<TPosition>;
    std::unordered_map<TString, TOverrideLibraryStuff> OverrideLibraries; // alias -> position

    THashMap<TString, ui32> PackageVersions;
    NYql::TWarningPolicy WarningPolicy;
    TString PqReadByRtmrCluster;
    bool EmitStartsWith = true;
    TMaybe<bool> EmitAggApply;
    bool UseBlocks = false;
    bool EmitTableSource = false;
    bool AnsiLike = false;
    bool FeatureR010 = false; // Row pattern recognition: FROM clause
    TMaybe<bool> CompactGroupBy;
    bool BlockEngineEnable = false;
    bool BlockEngineForce = false;
    bool UnorderedResult = false;
    ui64 ParallelModeCount = 0;
    bool CompactNamedExprs = true;
    bool ValidateUnusedExprs = false;
    bool AnsiImplicitCrossJoin = false; // select * from A,B
    bool DistinctOverWindow = false;
    bool SeqMode = false;
    bool DistinctOverKeys = false;
    bool GroupByExprAfterWhere = false;
    bool FailOnGroupByExprOverride = false;
    bool EmitUnionMerge = false;
    bool OptimizeSimpleIlike = false;
    bool PersistableFlattenAndAggrExprs = false;
    bool DisableLegacyNotNull = false;
    bool DebugPositions = false;
    bool StrictWarningAsError = false;
    bool WindowNewPipeline = false;
    TMaybe<bool> DirectRowDependsOn;
    TVector<size_t> ForAllStatementsParts;
    TMaybe<TString> Engine;
};

class TColumnRefScope {
public:
    TColumnRefScope(TContext& ctx, EColumnRefState state, bool isTopLevelExpr = true, const TString& defineVar = "")
        : PrevTop_(ctx.TopLevelColumnReferenceState_)
        , Prev_(ctx.ColumnReferenceState_)
        , PrevErr_(ctx.NoColumnErrorContext_)
        , PrevDefineVar_(ctx.MatchRecognizeDefineVar_)
        , Ctx_(ctx)
    {
        if (isTopLevelExpr) {
            Ctx_.ColumnReferenceState_ = Ctx_.TopLevelColumnReferenceState_ = state;
        } else {
            Ctx_.ColumnReferenceState_ = state;
        }
        YQL_ENSURE(
            defineVar.empty() ||
                EColumnRefState::MatchRecognizeMeasures == state ||
                EColumnRefState::MatchRecognizeDefine == state ||
                EColumnRefState::MatchRecognizeDefineAggregate == state,
            "Internal logic error");
        ctx.MatchRecognizeDefineVar_ = defineVar;
    }

    void SetNoColumnErrContext(const TString& msg) {
        Ctx_.NoColumnErrorContext_ = msg;
    }

    ~TColumnRefScope() {
        Ctx_.TopLevelColumnReferenceState_ = PrevTop_;
        Ctx_.ColumnReferenceState_ = Prev_;
        std::swap(Ctx_.NoColumnErrorContext_, PrevErr_);
        std::swap(Ctx_.MatchRecognizeDefineVar_, PrevDefineVar_);
    }

private:
    const EColumnRefState PrevTop_;
    const EColumnRefState Prev_;
    TString PrevErr_;
    TString PrevDefineVar_;
    TContext& Ctx_;
};

TMaybe<EColumnRefState> GetFunctionArgColumnStatus(TContext& ctx, const TString& module, const TString& func, size_t argIndex);

class TTranslation {
protected:
    typedef TSet<ui32> TSetType;

protected:
    explicit TTranslation(TContext& ctx);

public:
    TContext& Context();
    IOutputStream& Error();

    const TString& Token(const NSQLv1Generated::TToken& token) {
        return Ctx_.Token(token);
    }

    TString Identifier(const NSQLv1Generated::TToken& token) {
        return IdContent(Ctx_, Token(token));
    }

    TString Identifier(const TString& str) const {
        return IdContent(Ctx_, str);
    }

    TNodePtr GetNamedNode(const TString& name);

    using TNodeBuilderByName = std::function<TNodePtr(const TString& effectiveName)>;
    TString PushNamedNode(TPosition namePos, const TString& name, const TNodeBuilderByName& builder);
    TString PushNamedNode(TPosition namePos, const TString& name, TNodePtr node);
    TString PushNamedAtom(TPosition namePos, const TString& name);
    bool PopNamedNode(const TString& name);
    bool WarnUnusedNodes() const;

    template <typename TNode>
    void AltNotImplemented(const TString& ruleName, const TNode& node) {
        AltNotImplemented(ruleName, node.Alt_case(), node, TNode::descriptor());
    }

    template <typename TNode>
    TString AltDescription(const TNode& node) const {
        return AltDescription(node, node.Alt_case(), TNode::descriptor());
    }

protected:
    void AltNotImplemented(const TString& ruleName, ui32 altCase, const google::protobuf::Message& node, const google::protobuf::Descriptor* descr);
    TString AltDescription(const google::protobuf::Message& node, ui32 altCase, const google::protobuf::Descriptor* descr) const;

protected:
    TContext& Ctx_;
};

void EnumerateSqlFlags(std::function<void(std::string_view)> callback);
} // namespace NSQLTranslationV1
