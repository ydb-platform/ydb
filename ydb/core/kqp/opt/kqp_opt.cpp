#include "kqp_opt_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

template<typename T>
TExprBase ProjectColumnsInternal(const TExprBase& input, const T& columnNames, TExprContext& ctx) {
    const auto rowArgument = Build<TCoArgument>(ctx, input.Pos())
        .Name("row")
        .Done();

    TVector<TExprBase> columnGetters;
    columnGetters.reserve(columnNames.size());
    for (const auto& column : columnNames) {
        const auto tuple = Build<TCoNameValueTuple>(ctx, input.Pos())
            .Name().Build(column)
            .template Value<TCoMember>()
                .Struct(rowArgument)
                .Name().Build(column)
                .Build()
            .Done();

        columnGetters.emplace_back(std::move(tuple));
    }

    return Build<TCoMap>(ctx, input.Pos())
        .Input(input)
        .Lambda()
            .Args({rowArgument})
            .Body<TCoAsStruct>()
                .Add(columnGetters)
                .Build()
            .Build()
        .Done();
}

} // namespace

bool IsKqpPureLambda(const TCoLambda& lambda) {
    return !FindNode(lambda.Body().Ptr(), [](const TExprNode::TPtr& node) {
        if (TMaybeNode<TKqlReadTableBase>(node)) {
            return true;
        }

        if (TMaybeNode<TKqlReadTableRangesBase>(node)) {
            return true;
        }

        if (TMaybeNode<TKqlLookupTableBase>(node)) {
            return true;
        }

        if (TMaybeNode<TKqlTableEffect>(node)) {
            return true;
        }

        return false;
    });
}

bool IsKqpPureInputs(const TExprList& inputs) {
    return !FindNode(inputs.Ptr(), [](const TExprNode::TPtr& node) {
        if (TMaybeNode<TKqpCnStreamLookup>(node)) {
            return true;
        }

        if (auto source = TExprBase(node).Maybe<TDqSource>()) {
            if (source.Cast().Settings().Maybe<TKqpReadRangesSourceSettings>()) {
                return true;
            }
        }

        return false;
    });
}

bool IsKqpEffectsStage(const TDqStageBase& stage) {
    return stage.Program().Body().Maybe<TKqpEffects>().IsValid();
}

bool NeedSinks(const TKikimrTableDescription& table, const TKqpOptimizeContext& kqpCtx) {
    return kqpCtx.IsGenericQuery()
        && (table.Metadata->Kind != EKikimrTableKind::Olap || kqpCtx.Config->EnableOlapSink)
        && (table.Metadata->Kind != EKikimrTableKind::Datashard || kqpCtx.Config->EnableOltpSink);
}

TExprBase ProjectColumns(const TExprBase& input, const TVector<TString>& columnNames, TExprContext& ctx) {
    return ProjectColumnsInternal(input, columnNames, ctx);
}

TExprBase ProjectColumns(const TExprBase& input, const THashSet<TStringBuf>& columnNames, TExprContext& ctx) {
    return ProjectColumnsInternal(input, columnNames, ctx);
}

TKqpTable BuildTableMeta(const TKikimrTableMetadata& meta, const TPositionHandle& pos, TExprContext& ctx) {
    return Build<TKqpTable>(ctx, pos)
        .Path().Build(meta.Name)
        .PathId().Build(meta.PathId.ToString())
        .SysView().Build(meta.SysView)
        .Version().Build(meta.SchemaVersion)
        .Done();
}

TKqpTable BuildTableMeta(const TKikimrTableDescription& tableDesc, const TPositionHandle& pos, TExprContext& ctx) {
    YQL_ENSURE(tableDesc.Metadata);
    return BuildTableMeta(*tableDesc.Metadata, pos, ctx);
}

bool IsBuiltEffect(const TExprBase& effect) {
    // Stage with effect output
    if (effect.Maybe<TDqOutput>()) {
        return true;
    }

    // Stage with sink effect
    if (effect.Maybe<TKqpSinkEffect>()) {
        return true;
    }

    return false;
}

} // namespace NKikimr::NKqp::NOpt
