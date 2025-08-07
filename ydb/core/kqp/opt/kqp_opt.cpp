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
    return (kqpCtx.IsGenericQuery()
            || (kqpCtx.IsDataQuery() && (table.Metadata->Kind != EKikimrTableKind::Olap || kqpCtx.Config->AllowOlapDataQuery)))
        && (table.Metadata->Kind != EKikimrTableKind::Olap || kqpCtx.Config->EnableOlapSink)
        && (table.Metadata->Kind != EKikimrTableKind::Datashard || kqpCtx.Config->EnableOltpSink);
}

bool CanEnableStreamWrite(const NYql::TKikimrTableDescription& table, const TKqpOptimizeContext& kqpCtx) {
    return table.Metadata->Kind == EKikimrTableKind::Olap
            || (table.Metadata->Kind == EKikimrTableKind::Datashard && kqpCtx.Config->EnableStreamWrite);
}

bool HasReadTable(const TStringBuf table, const TExprNode::TPtr& root) {
    bool result = false;
    VisitExpr(root, [&](const NYql::TExprNode::TPtr& node) {
        if (NYql::NNodes::TKqpCnStreamLookup::Match(node.Get())) {
            const auto& streamLookup = TExprBase(node).Cast<NYql::NNodes::TKqpCnStreamLookup>();
            const TStringBuf tablePathId = streamLookup.Table().PathId().Value();
            if (table == tablePathId) {
                result = true;
            }
        } else if (NYql::NNodes::TDqSource::Match(node.Get())) {
            const auto& source = TExprBase(node).Cast<NYql::NNodes::TDqSource>();
            const auto sourceSettings = source.Settings().Maybe<TKqpReadRangesSourceSettings>();
            if (sourceSettings) {
                const TStringBuf tablePathId = sourceSettings.Cast().Table().PathId().Value();
                if (table == tablePathId) {
                    result = true;
                }
            }
        }
        return !result;
    });
    return result;
}

TExprBase ProjectColumns(const TExprBase& input, const TVector<TString>& columnNames, TExprContext& ctx) {
    return ProjectColumnsInternal(input, columnNames, ctx);
}

TExprBase ProjectColumns(const TExprBase& input, const TVector<TStringBuf>& columnNames, TExprContext& ctx) {
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

bool IsSortKeyPrimary(
    const NYql::NNodes::TCoLambda& keySelector,
    const NYql::TKikimrTableDescription& tableDesc,
    const TMaybe<THashSet<TStringBuf>>& passthroughFields
) {
    auto checkKey = [keySelector, &tableDesc, &passthroughFields] (NYql::NNodes::TExprBase key, ui32 index) {
        if (!key.Maybe<TCoMember>()) {
            return false;
        }

        auto member = key.Cast<TCoMember>();
        if (member.Struct().Raw() != keySelector.Args().Arg(0).Raw()) {
            return false;
        }

        auto column = TString(member.Name().Value());
        auto columnIndex = tableDesc.GetKeyColumnIndex(column);
        if (!columnIndex || *columnIndex != index) {
            return false;
        }

        if (passthroughFields && !passthroughFields->contains(column)) {
            return false;
        }

        return true;
    };

    auto lambdaBody = keySelector.Body();
    if (auto maybeTuple = lambdaBody.Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (size_t i = 0; i < tuple.Size(); ++i) {
            if (!checkKey(tuple.Item(i), i)) {
                return false;
            }
        }
    } else {
        if (!checkKey(lambdaBody, 0)) {
            return false;
        }
    }

    return true;
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

void DumpAppliedRule(const TString& name, const NYql::TExprNode::TPtr& input,
    const NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx)
{
// #define KQP_ENABLE_DUMP_APPLIED_RULE
#ifdef KQP_ENABLE_DUMP_APPLIED_RULE
    if (input != output) {
        auto builder = TStringBuilder() << "Rule applied: " << name << Endl;
        builder << "Expression before rule application: " << Endl;
        builder << KqpExprToPrettyString(*input, ctx) << Endl;
        builder << "Expression after rule application: " << Endl;
        builder << KqpExprToPrettyString(*output, ctx);
        YQL_CLOG(TRACE, ProviderKqp) << builder;
    }
#else
    Y_UNUSED(ctx);
    if (input != output) {
        YQL_CLOG(TRACE, ProviderKqp) << name;
    }
#endif
}

} // namespace NKikimr::NKqp::NOpt
