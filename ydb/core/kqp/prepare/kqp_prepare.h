#pragma once

#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_transform.h>

namespace NKikimr {
namespace NKqp {

enum class ECommitSafety {
    Full,
    Safe,
    Moderate
};

struct TExprScope {
    NYql::NNodes::TCallable Callable;
    NYql::NNodes::TCoLambda Lambda;
    ui32 Depth;

    TExprScope(NYql::NNodes::TCallable callable, NYql::NNodes::TCoLambda Lambda, ui32 depth)
        : Callable(callable)
        , Lambda(Lambda)
        , Depth(depth) {}
};

struct TNodeInfo {
    TMaybe<TExprScope> Scope;
    bool IsImmediate;
    bool RequireImmediate;
    bool IsExecutable;
    bool AreInputsExecutable;

    TNodeInfo()
        : IsImmediate(false)
        , RequireImmediate(false)
        , IsExecutable(false)
        , AreInputsExecutable(false) {}
};

using TExprToExprListMap = THashMap<const NYql::TExprNode*, TVector<NYql::NNodes::TExprBase>>;
using TExprToNodeInfoMap = THashMap<const NYql::TExprNode*, TNodeInfo>;

struct TScopedNode {
    NYql::NNodes::TExprBase Node;
    TMaybe<TExprScope> Scope;

    TScopedNode(NYql::NNodes::TExprBase node, TMaybe<TExprScope> scope = {})
        : Node(node)
        , Scope(scope) {}
};

struct TKqpAnalyzeResults {
    bool CanExecute;
    TVector<TScopedNode> ExecutionRoots;
    TExprToNodeInfoMap ExprToNodeInfoMap;
    TExprToExprListMap LambdaToExecRootsMap;
    TExprToExprListMap CallableToExecRootsMap;
};

using TMkqlResult = NKikimrMiniKQL::TResult;

struct TMkqlExecuteResult {
    TString Program;
    NThreading::TFuture<IKqpGateway::TMkqlResult> Future;

    TMkqlExecuteResult(const TString& program, const NThreading::TFuture<IKqpGateway::TMkqlResult>& future)
        : Program(program)
        , Future(future) {}

    TMkqlExecuteResult(const NThreading::TFuture<IKqpGateway::TMkqlResult>& future)
        : Program()
        , Future(future) {}

    TMkqlExecuteResult() {}
};

struct TKqlTransformContext : TThrRefBase {
    TKqlTransformContext(NYql::TKikimrConfiguration::TPtr& config, TIntrusivePtr<NYql::TKikimrQueryContext> queryCtx,
        TIntrusivePtr<NYql::TKikimrTablesData> tables)
        : Config(config)
        , QueryCtx(queryCtx)
        , Tables(tables) {}

    NYql::TKikimrConfiguration::TPtr Config;
    TIntrusivePtr<NYql::TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<NYql::TKikimrTablesData> Tables;
    NKikimrKqp::TKqlSettings Settings;
    NActors::TActorId ReplyTarget;

    TKqpAnalyzeResults AnalyzeResults;
    NKikimrKqp::TPreparedKql* PreparingKql = nullptr;
    const NKikimrKqp::TPreparedKql* PreparedKql;
    NKqpProto::TKqpStatsQuery QueryStats;
    const NKqpProto::TKqpPhyQuery* PhysicalQuery;

    TVector<TSimpleSharedPtr<NKikimrMiniKQL::TResult>> MkqlResults;
    TVector<NKikimrMiniKQL::TResult> PhysicalQueryResults;

    ECommitSafety CommitSafety() const {
        auto safetyValue = Config->CommitSafety.Get().GetRef();
        if (safetyValue == "Full") {
            return ECommitSafety::Full;
        } else if (safetyValue == "Safe") {
            return ECommitSafety::Safe;
        } else if (safetyValue == "Moderate") {
            return ECommitSafety::Moderate;
        }

        YQL_ENSURE(false, "Unexpected value for CommitSafety.");
    }

    NKikimrKqp::TPreparedKql& GetPreparingKql() {
        YQL_ENSURE(PreparingKql);
        return *PreparingKql;
    }

    const NKikimrKqp::TPreparedKql& GetPreparedKql() {
        YQL_ENSURE(PreparedKql);
        return *PreparedKql;
    }

    IKqpGateway::TMkqlSettings GetMkqlSettings(bool hasDataEffects, TInstant now) const;
    void AddMkqlStats(const TString& program, NKikimrQueryStats::TTxStats&& txStats);

    void Reset() {
        Settings = {};
        ReplyTarget = {};
        AnalyzeResults = {};
        MkqlResults.clear();
        QueryStats = {};
        PreparingKql = nullptr;
        PreparedKql = nullptr;
        PhysicalQuery = nullptr;
        PhysicalQueryResults.clear();
    }
};

class TKqpTransactionState : public TThrRefBase {
public:
    TKqpTransactionState(TIntrusivePtr<NYql::TKikimrSessionContext> sessionCtx)
        : SessionCtx(sessionCtx) {}

    TKqpTransactionContext& Tx() { return static_cast<TKqpTransactionContext&>(SessionCtx->Tx()); }
    const TKqpTransactionContext& Tx() const { return static_cast<TKqpTransactionContext&>(SessionCtx->Tx()); }

private:
    TIntrusivePtr<NYql::TKikimrSessionContext> SessionCtx;
};

bool AddDeferredEffect(NYql::NNodes::TExprBase effect, const TVector<NKikimrKqp::TParameterBinding>& bindings,
    NYql::TExprContext& ctx, TKqpTransactionState& txState, TKqlTransformContext& transformCtx,
    bool preserveParamValues);

bool AddDeferredEffect(NYql::NNodes::TExprBase effect, NYql::TExprContext& ctx, TKqpTransactionState& txState,
    TKqlTransformContext& transformCtx, bool preserveParamValues);

NYql::TIssue GetLocksInvalidatedIssue(const TKqpTransactionContext& txCtx, const TMaybe<TKqpTxLock>& invalidatedLock);

bool MergeLocks(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value, TKqpTransactionContext& txCtx,
        NYql::TExprContext& ctx);

std::pair<bool, std::vector<NYql::TIssue>> MergeLocks(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
        TKqpTransactionContext& txCtx);

bool UnpackMergeLocks(const NKikimrMiniKQL::TResult& result, TKqpTransactionContext& txCtx, NYql::TExprContext& ctx);

TKqpParamsMap BuildParamsMap(const TVector<NKikimrKqp::TParameterBinding>& bindings,
    TIntrusivePtr<TKqpTransactionState> txState, TIntrusivePtr<TKqlTransformContext> transformCtx, bool acquireLocks);

TVector<NKikimrKqp::TParameterBinding> CollectParams(NYql::NNodes::TExprBase query);

NKikimrMiniKQL::TParams GetLocksParamValue(const TKqpTxLocks& locks);

TAutoPtr<NYql::IGraphTransformer> CreateKqpSimplifyTransformer();
TAutoPtr<NYql::IGraphTransformer> CreateKqpAnalyzeTransformer(TIntrusivePtr<TKqlTransformContext> transformCtx);
TAutoPtr<NYql::IGraphTransformer> CreateKqpRewriteTransformer(TIntrusivePtr<TKqlTransformContext> transformCtx);
TAutoPtr<NYql::IGraphTransformer> CreateKqpExecTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx);
TAutoPtr<NYql::IGraphTransformer> CreateKqpSubstituteTransformer(TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx);
TAutoPtr<NYql::IGraphTransformer> CreateKqpFinalizeTransformer(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpTypeAnnotationTransformer(const TString& cluster,
    TIntrusivePtr<NYql::TKikimrTablesData> tablesData, NYql::TTypeAnnotationContext& typesCtx,
    NYql::TKikimrConfiguration::TPtr config);
TAutoPtr<NYql::IGraphTransformer> CreateKqpCheckKiProgramTransformer();
TAutoPtr<NYql::IGraphTransformer> CreateKqpCheckQueryTransformer();

} // namespace NKqp
} // namespace NKikimr
