#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/ptr.h>

namespace NYql {

struct TKikimrConfiguration;
struct TKikimrQueryContext;
class TKikimrTablesData;
class TKikimrTableDescription;

} // namespace NYql

namespace NKikimr::NKqp {

struct TUserRequestContext;
struct TOptimizerHints;

namespace NOpt {

struct TKqpOptimizeContext : public TSimpleRefCount<TKqpOptimizeContext> {
    TKqpOptimizeContext(const TString& cluster, const TIntrusivePtr<NYql::TKikimrConfiguration>& config,
        const TIntrusivePtr<NYql::TKikimrQueryContext> queryCtx, const TIntrusivePtr<NYql::TKikimrTablesData>& tables,
        const TIntrusivePtr<NKikimr::NKqp::TUserRequestContext>& userRequestContext,
        bool usePessimisticLocks = false);

    TString Cluster;
    const TIntrusivePtr<NYql::TKikimrConfiguration> Config;
    const TIntrusivePtr<NYql::TKikimrQueryContext> QueryCtx;
    const TIntrusivePtr<NYql::TKikimrTablesData> Tables;
    const TIntrusivePtr<NKikimr::NKqp::TUserRequestContext> UserRequestContext;
    bool UsePessimisticLocks;
    int JoinsCount{};
    int EquiJoinsCount{};
    std::shared_ptr<NJson::TJsonValue> OverrideStatistics{};
    std::shared_ptr<NKikimr::NKqp::TOptimizerHints> Hints{};
    NKikimr::NKqp::TShufflingOrderingsByJoinLabels ShufflingOrderingsByJoinLabels;
    NKikimr::NKqp::TKqpStatsStore KqpStats;
    NKikimr::NKqp::TCBOOptimizerStats CBOStats;

    std::shared_ptr<NJson::TJsonValue> GetOverrideStatistics();

    NKikimr::NKqp::TOptimizerHints GetOptimizerHints();

    bool IsDataQuery() const;

    bool IsScanQuery() const;

    bool IsGenericQuery() const;
};

struct TKqpBuildQueryContext : TThrRefBase {
    TKqpBuildQueryContext() {}

    TVector<NYql::NNodes::TKqpPhysicalTx> PhysicalTxs;
    TVector<NYql::NNodes::TExprBase> QueryResults;

    void Reset() {
        PhysicalTxs.clear();
        QueryResults.clear();
    }
};

bool IsKqpEffectsStage(const NYql::NNodes::TDqStageBase& stage);
bool NeedSinks(const NYql::TKikimrTableDescription& table, const TKqpOptimizeContext& kqpCtx);
bool CanEnableStreamWrite(const NYql::TKikimrTableDescription& table, const TKqpOptimizeContext& kqpCtx);
bool HasReadTable(const TStringBuf table, const NYql::TExprNode::TPtr& root);

TMaybe<NYql::NNodes::TKqlQueryList> BuildKqlQuery(NYql::NNodes::TKiDataQueryBlocks queryBlocks,
    const NYql::TKikimrTablesData& tablesData, NYql::TExprContext& ctx, bool withSystemColumns,
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, NYql::TTypeAnnotationContext& typesCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpFinalizingOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx);
TAutoPtr<NYql::IGraphTransformer> CreateKqpQueryPhasesTransformer();
TAutoPtr<NYql::IGraphTransformer> CreateKqpQueryEffectsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx);
TAutoPtr<NYql::IGraphTransformer> CreateKqpCheckPhysicalQueryTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpBuildTxsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx, NYql::TTypeAnnotationContext& typesCtx, TIntrusivePtr<NYql::TKikimrConfiguration>& config);

TAutoPtr<NYql::IGraphTransformer> CreateKqpSinkPrecomputeTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpBuildPhysicalQueryTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx);

TAutoPtr<NYql::IGraphTransformer> CreateKqpQueryBlocksTransformer(TAutoPtr<NYql::IGraphTransformer> queryBlockTransformer);

THolder<NYql::TVisitorTransformerBase> CreateKqpTypeAnnotationTransformer(const TString& cluster,
    TIntrusivePtr<NYql::TKikimrTablesData> tablesData, TIntrusivePtr<NYql::TKikimrConfiguration> config);

TAutoPtr<NYql::IGraphTransformer> CreateKqpCheckQueryTransformer();

} // namespace NOpt

} // namespace NKikimr::NKqp
