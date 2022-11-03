#pragma once

#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_transform.h>

namespace NKikimr {
namespace NKqp {

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

    NKqpProto::TKqpStatsQuery QueryStats;
    std::shared_ptr<const NKqpProto::TKqpPhyQuery> PhysicalQuery;

    TVector<TSimpleSharedPtr<NKikimrMiniKQL::TResult>> MkqlResults;
    TVector<NKikimrMiniKQL::TResult> PhysicalQueryResults;

    void Reset() {
        Settings = {};
        ReplyTarget = {};
        MkqlResults.clear();
        QueryStats = {};
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


bool MergeLocks(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value, TKqpTransactionContext& txCtx,
        NYql::TExprContext& ctx);

std::pair<bool, std::vector<NYql::TIssue>> MergeLocks(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
        TKqpTransactionContext& txCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpTypeAnnotationTransformer(const TString& cluster,
    TIntrusivePtr<NYql::TKikimrTablesData> tablesData, NYql::TTypeAnnotationContext& typesCtx,
    NYql::TKikimrConfiguration::TPtr config);

TAutoPtr<NYql::IGraphTransformer> CreateKqpCheckQueryTransformer();

} // namespace NKqp
} // namespace NKikimr
