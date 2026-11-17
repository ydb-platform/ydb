#include "kqp_opt_cbo.h"
#include "kqp_opt_log_join_projection.h"
#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_join.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_join_cost_based.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_olap_filter.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_rules.h>
#include <ydb/core/kqp/opt/physical/predicate_collector.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_hopping.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

#include <yql/essentials/core/yql_aggregate_expander.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/core/yql_opt_match_recognize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/utils/log/log.h>

#include <algorithm>
#include <optional>

namespace NKikimr::NKqp::NOpt {

namespace NPrivate {

using namespace NYql;
using namespace NYql::NNodes;

TJoinOutputColumns BuildJoinOutputColumns(const TCoEquiJoin& join) {
    TJoinOutputColumns columns;
    const size_t inputsCount = join.ArgCount() - 2;
    const auto renames = LoadJoinRenameMap(join.Arg(inputsCount + 1).Ref());

    for (size_t i = 0; i < inputsCount; ++i) {
        const auto input = join.Arg(i).Cast<TCoEquiJoinInput>();
        if (!input.Scope().Ref().IsAtom()) {
            continue;
        }

        const TString scope(input.Scope().Ref().Content());
        const auto* inputType = input.List().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        for (const auto* item : inputType->GetItems()) {
            const TString originalName = FullColumnName(scope, item->GetName());
            const TJoinOutputColumn column{ i, TString(item->GetName()), originalName };
            if (const auto rename = renames.find(originalName); rename != renames.end()) {
                for (const auto outputName : rename->second) {
                    columns.emplace(TString(outputName), column);
                }
            } else {
                columns.emplace(originalName, column);
            }
        }
    }

    return columns;
}

TJoinColumnUsage CollectJoinColumnUsage(const TExprNode::TPtr& body, const TExprNode& joinedRow) {
    TJoinColumnUsage usage;
    const auto collectUsed = [&](const auto& self, const TExprNode::TPtr& current) -> void {
        if (TCoJsonValue::Match(current.Get())) {
            return;
        }
        if (const auto member = TMaybeNode<TCoMember>(current); member && member.Cast().Struct().Raw() == &joinedRow) {
            usage.OutputFieldNames.insert(member.Cast().Name().StringValue());
        }
        for (const auto& child : current->Children()) {
            if (child.Get() == &joinedRow && !TCoMember::Match(current.Get())) {
                usage.WholeRow = true;
            }
            self(self, child);
        }
    };
    collectUsed(collectUsed, body);
    return usage;
}

THashSet<TString> CollectRequiredJoinSources(const TJoinColumnUsage& usage, const TJoinOutputColumns& outputColumns) {
    THashSet<TString> sources;
    for (const auto& member : usage.OutputFieldNames) {
        if (const auto output = outputColumns.find(member); output != outputColumns.end()) {
            sources.insert(output->second.OriginalName);
        }
    }
    return sources;
}

bool HasMultipleParents(const TExprNode& node, const TParentsMap& parents) {
    const auto it = parents.find(&node);
    return it != parents.end() && it->second.size() > 1;
}

} // namespace NPrivate

namespace {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

struct TJoinJsonValueProjection {
    TCoJsonValue JsonValue;
    TString SourceColumnName;
    TString ProjectionColumnName;
};

struct TJoinJsonValueProjectionRewrite {
    // Indexed by the corresponding EquiJoin input argument.
    TVector<TVector<TJoinJsonValueProjection>> ProjectionsByInput;
    TNodeOnNodeOwnedMap Replacements;
};

// Returns nothing if any JSON_VALUE call cannot be moved to a join input.
std::optional<TJoinJsonValueProjectionRewrite> CollectJoinJsonValueProjections(const TCoFlatMapBase& node, const TCoEquiJoin& join,
    const TExprNode::TListType& jsonValues, const NPrivate::TJoinOutputColumns& outputColumns, TExprContext& ctx) {
    TJoinJsonValueProjectionRewrite rewrite;
    rewrite.ProjectionsByInput.resize(join.ArgCount() - 2);
    // Type annotation guarantees List<Struct> for EquiJoin and one row argument for FlatMap.
    const auto* joinedType = join.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto& joinedRow = node.Lambda().Args().Arg(0).Ref();
    size_t nextAlias = 0;

    for (const auto& jsonNode : jsonValues) {
        const auto jsonValue = TCoJsonValue(jsonNode);
        if (!CanBePushedAsOlapJsonValue(jsonValue, &joinedRow)) {
            return std::nullopt;
        }

        const TString fullName = jsonValue.Json().Cast<TCoMember>().Name().StringValue();
        const auto output = outputColumns.find(fullName);
        if (output == outputColumns.end() || !joinedType->FindItem(fullName)) {
            return std::nullopt;
        }
        const auto inputIndex = output->second.InputIndex;
        const auto& source = output->second.SourceColumnName;
        const auto input = join.Arg(inputIndex).Cast<TCoEquiJoinInput>();
        const TString scope(input.Scope().Ref().Content());
        const auto* inputType = input.List().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

        TString alias;
        do {
            alias = TStringBuilder() << KqpJoinJsonValueColumnName << nextAlias++;
        } while (inputType->FindItem(alias) || joinedType->FindItem(FullColumnName(scope, alias)));
        rewrite.ProjectionsByInput[inputIndex].push_back({ jsonValue, source, alias });
        rewrite.Replacements.emplace(
            jsonNode.Get(), ctx.NewCallable(jsonValue.Pos(), "Member",
                                { node.Lambda().Args().Arg(0).Ptr(), ctx.NewAtom(jsonValue.Pos(), FullColumnName(scope, alias)) }));
    }

    return rewrite;
}

TExprNode::TPtr AddJsonValueProjectionsToJoinInput(const TCoEquiJoinInput& input, const TVector<TJoinJsonValueProjection>& projections,
    const THashSet<TStringBuf>* keys, const NPrivate::TJoinColumnUsage& usage, const THashSet<TString>& requiredSources, TPositionHandle pos,
    TExprContext& ctx) {
    const auto* inputType = input.List().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const TString scope(input.Scope().Ref().Content());
    const auto row = ctx.NewArgument(pos, "row");
    TExprNode::TListType items;
    for (const auto* item : inputType->GetItems()) {
        const auto nameView = item->GetName();
        const bool projectedSource = std::ranges::any_of(projections, [&](const TJoinJsonValueProjection& projection) {
            return projection.SourceColumnName == nameView;
        });
        // This is a JSON_VALUE and may be replaced
        if (projectedSource && !usage.WholeRow && !requiredSources.contains(FullColumnName(scope, nameView)) &&
            (!keys || !keys->contains(nameView))) {
            continue;
        }
        const auto name = ctx.NewAtom(pos, item->GetName());
        items.push_back(ctx.NewList(pos, { name, ctx.NewCallable(pos, "Member", { row, name }) }));
    }
    for (const auto& projection : projections) {
        TNodeOnNodeOwnedMap jsonSourceReplacer;
        jsonSourceReplacer.emplace(
            projection.JsonValue.Json().Raw(), ctx.NewCallable(pos, "Member", { row, ctx.NewAtom(pos, projection.SourceColumnName) }));
        items.push_back(ctx.NewList(
            pos, { ctx.NewAtom(pos, projection.ProjectionColumnName), ctx.ReplaceNodes(projection.JsonValue.Ptr(), jsonSourceReplacer) }));
    }
    auto value = ctx.NewCallable(pos, "Just", { ctx.NewCallable(pos, "AsStruct", std::move(items)) });
    auto flatMap = ctx.NewCallable(pos, "FlatMap", { input.List().Ptr(), ctx.NewLambda(pos, ctx.NewArguments(pos, { row }), std::move(value)) });
    return ctx.ChangeChild(input.Ref(), 0, std::move(flatMap));
}

TExprBase PushJsonValueProjectionThroughEquiJoin(const TCoFlatMapBase& node, TExprContext& ctx) {
    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }
    const auto jsonValues = FindNodes(node.Lambda().Body().Ptr(), [](const TExprNode::TPtr& n) {
        return TCoJsonValue::Match(n.Get());
    });
    if (jsonValues.empty() || !node.Input().Maybe<TCoEquiJoin>()) {
        return node;
    }

    const auto join = node.Input().Cast<TCoEquiJoin>();
    // No-op in case a rewrite has already happened.
    if (FindNode(join.Ptr(), [](const TExprNode::TPtr& n) {
            return n->IsAtom() && n->Content().StartsWith(KqpJoinJsonValueColumnName);
        })) {
        return node;
    }
    // EquiJoin stores the join tree and settings after its input branches.
    const size_t inputsCount = join.ArgCount() - 2;
    const auto outputColumns = NPrivate::BuildJoinOutputColumns(join);
    auto rewrite = CollectJoinJsonValueProjections(node, join, jsonValues, outputColumns, ctx);
    if (!rewrite) {
        return node;
    }
    const auto& projections = rewrite->ProjectionsByInput;
    auto& replacements = rewrite->Replacements;

    const auto& joinedRow = node.Lambda().Args().Arg(0).Ref();
    const auto usage = NPrivate::CollectJoinColumnUsage(node.Lambda().Body().Ptr(), joinedRow);
    const auto requiredSources = NPrivate::CollectRequiredJoinSources(usage, outputColumns);
    const auto joinKeys = CollectEquiJoinKeyColumnsByLabel(join.Arg(inputsCount).Ref());
    auto joinArgs = join.Ref().ChildrenList();
    for (size_t i = 0; i < inputsCount; ++i) {
        if (projections[i].empty()) {
            continue;
        }
        const auto input = join.Arg(i).Cast<TCoEquiJoinInput>();
        const TString scope(input.Scope().Ref().Content());
        joinArgs[i] =
            AddJsonValueProjectionsToJoinInput(input, projections[i], joinKeys.FindPtr(scope), usage, requiredSources, node.Pos(), ctx);
    }
    replacements.emplace(join.Raw(), ctx.ChangeChildren(join.Ref(), std::move(joinArgs)));
    return TExprBase(ctx.ReplaceNodes(node.Ptr(), replacements));
}

class TKqpJoinProjectionTransformer: public TOptimizeTransformerBase {
public:
    explicit TKqpJoinProjectionTransformer(const TKqpOptimizeContext& kqpCtx)
        : TOptimizeTransformerBase(nullptr, NYql::NLog::EComponent::ProviderKqp, {})
        , KqpCtx(kqpCtx)
    {
        AddHandler(0, &TCoFlatMap::Match, "KqpJoinProjection", Hndl(&TKqpJoinProjectionTransformer::PushJsonValueProjectionThroughJoin));
    }

private:
    TMaybeNode<TExprBase> PushJsonValueProjectionThroughJoin(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        if (!KqpCtx.Config->HasOptEnableOlapPushdown() || !KqpCtx.Config->GetEnableOlapPushdownProjections() ||
            !KqpCtx.Config->HasOptEnableOlapJsonValueProjectionThroughJoin()) {
            return node;
        }
        const auto flatMap = node.Cast<TCoFlatMap>();
        // Other join types are not supported yet, but may be in the future.
        if (!flatMap.Input().Maybe<TCoEquiJoin>()) {
            return node;
        }
        if (NPrivate::HasMultipleParents(flatMap.Input().Ref(), *getParents())) {
            return node;
        }
        TExprBase output = PushJsonValueProjectionThroughEquiJoin(flatMap, ctx);
        DumpAppliedRule("PushJsonValueProjectionThroughJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    const TKqpOptimizeContext& KqpCtx;
};

class TKqpLogicalOptTransformer : public TOptimizeTransformerBase {
public:
    TKqpLogicalOptTransformer(TTypeAnnotationContext& typesCtx, TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
        const TKikimrConfiguration::TPtr& config)
        : TOptimizeTransformerBase(nullptr, NYql::NLog::EComponent::ProviderKqp, {})
        , TypesCtx(typesCtx)
        , KqpCtx(*kqpCtx)
        , Config(config)
    {
#define HNDL(name) "KqpLogical-"#name, Hndl(&TKqpLogicalOptTransformer::name)
        AddHandler(0, &TCoTopBase::Match, HNDL(RewriteHybridRankTopSort));
        AddHandler(0, &TCoTop::Match, HNDL(TopSortSelectIndex));
        AddHandler(0, &TCoTopSort::Match, HNDL(TopSortSelectIndex));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(PushExtractedPredicateToReadTable));
        AddHandler(0, &TCoAggregate::Match, HNDL(RewriteAggregate));
        AddHandler(0, &TCoAggregateCombine::Match, HNDL(PushdownOlapGroupByKeys));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeSortToTopSort));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInToEquiJoin));
        AddHandler(0, &TCoFlatMap::Match, HNDL(RewriteSqlInCompactToJoin));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteStreamEquiJoinWithLookup));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(OptimizeEquiJoinWithCosts));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteEquiJoin));
        AddHandler(0, &TDqJoin::Match, HNDL(JoinToIndexLookup));
        AddHandler(0, &TCoCalcOverWindowBase::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoCalcOverWindowGroup::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoTopSort::Match, HNDL(RewriteTopSortOverFlatMap));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(RewriteFlatMapOverExtend));
        AddHandler(0, &TKqlDeleteRows::Match, HNDL(DeleteOverLookup));
        AddHandler(0, &TKqlUpsertRowsBase::Match, HNDL(ExcessUpsertInputColumns));
        AddHandler(0, &TCoTake::Match, HNDL(DropTakeOverLookupTable));
        AddHandler(0, &TCoTopBase::Match, HNDL(PushLimitOverFullText));

        AddHandler(0, &TKqlReadTableBase::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TKqlReadTableRangesBase::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TKqpReadOlapTableRangesBase::Match, HNDL(ApplyExtractMembersToReadOlapTable<false>));
        AddHandler(0, &TKqlLookupTableBase::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TKqlReadTableFullTextIndex::Match, HNDL(ApplyExtractMembersToReadTable<false>));
        AddHandler(0, &TCoTop::Match, HNDL(TopSortOverExtend));
        AddHandler(0, &TCoTopSort::Match, HNDL(TopSortOverExtend));
        AddHandler(0, &TCoUnorderedBase::Match, HNDL(UnorderedOverDqReadWrap));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqReadWrap));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(PushExtractMembersToDqJoin));
        AddHandler(0, &TCoCountBase::Match, HNDL(TakeOrSkipOverDqReadWrap));
        AddHandler(0, &TCoExtendBase::Match, HNDL(ExtendOverDqReadWrap));
        AddHandler(0, &TCoNarrowMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoNarrowFlatMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoNarrowMultiMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoWideMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoMatchRecognize::Match, HNDL(MatchRecognize));

        AddHandler(1, &TCoFlatMapBase::Match, HNDL(SelectJsonIndex));
        AddHandler(1, &TCoFlatMapBase::Match, HNDL(RewriteFlatMapOverFullTextMatch));
        AddHandler(1, &TCoFlatMapBase::Match, HNDL(RewriteFlatMapOverJsonRead));
        AddHandler(1, &TCoTop::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(1, &TCoTopSort::Match, HNDL(RewriteTopSortOverIndexRead));
        AddHandler(1, &TCoTake::Match, HNDL(RewriteTakeOverIndexRead));
        AddHandler(1, &TDqReadWrapBase::Match, HNDL(DqReadWrapByProvider));

        AddHandler(2, &TCoFlatMap::Match, HNDL(RewriteFlatMapOverIndexRead));

        AddHandler(3, &TKqlReadTableIndex::Match, HNDL(RewriteIndexRead));
        AddHandler(3, &TKqlStreamLookupIndex::Match, HNDL(RewriteStreamLookupIndex));
        AddHandler(3, &TKqlReadTableIndexRanges::Match, HNDL(RewriteIndexRead));
        AddHandler(3, &TDqReadWrap::Match, HNDL(ExtractMembersOverDqReadWrapMultiUsage));
        AddHandler(3, &TDqReadWrapBase::Match, HNDL(UnorderedOverDqReadWrapMultiUsage));

        AddHandler(4, &TKqlLookupTableBase::Match, HNDL(RewriteLookupTable));

        AddHandler(5, &TKqlReadTableFullTextIndex::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(5, &TKqlReadTableBase::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(5, &TKqlReadTableRangesBase::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(5, &TKqpReadOlapTableRangesBase::Match, HNDL(ApplyExtractMembersToReadOlapTable<true>));
        AddHandler(5, &TKqlLookupTableBase::Match, HNDL(ApplyExtractMembersToReadTable<true>));
        AddHandler(6, TOptimizeTransformerBase::Any(), HNDL(InspectErroneousIndexAccess));

#undef HNDL

        SetGlobal(5u);
    }

public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        auto status = TOptimizeTransformerBase::DoTransform(input, output, ctx);

        if (status == TStatus::Ok) {
            for (const auto& hint: KqpCtx.GetOptimizerHints().GetUnappliedString()) {
                ctx.AddWarning(YqlIssue({}, TIssuesIds::YQL_UNUSED_HINT, "Unapplied hint: " + hint));
            }
        }

        return status;
    }

protected:
    TMaybeNode<TExprBase> TopSortSelectIndex(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpTopSortSelectIndex(node, ctx, KqpCtx);
        DumpAppliedRule("KqpTopSortSelectIndex", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushExtractedPredicateToReadTable(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = KqpPushExtractedPredicateToReadTable(node, ctx, KqpCtx, TypesCtx, *getParents());
        if (output) {
            DumpAppliedRule("PushExtractedPredicateToReadTable", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> PushdownOlapGroupByKeys(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpPushDownOlapGroupByKeys(node, ctx, KqpCtx);
        DumpAppliedRule("PushdownOlapGroupByKeys", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> InspectErroneousIndexAccess(TExprBase node, TExprContext& ctx) {
        if (IsIn({"FulltextScore", "FulltextMatch"}, node.Ref().Content())) {
            auto message = TStringBuilder{} << "Failed to rewrite " << node.Ref().Content() << " callable";
            TIssue baseIssue{ctx.GetPosition(node.Pos()), message};
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_BAD_REQUEST, baseIssue);

            TIssue subIssue{ctx.GetPosition(node.Pos()), "Fulltext index is not specified or unsupported predicate is used to access index"};
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, subIssue);
            baseIssue.AddSubIssue(MakeIntrusive<TIssue>(std::move(subIssue)));
            ctx.AddError(baseIssue);
            return {};
        }

        DumpAppliedRule("InspectErroneousIndexAccess", node.Ptr(), node.Ptr(), ctx);
        return node;
    }

    TMaybeNode<TExprBase> RewriteAggregate(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TMaybeNode<TExprBase> output;
        auto aggregate = node.Cast<TCoAggregateBase>();

        if (const auto& hopSetting = GetSetting(aggregate.Settings().Ref(), "hopping"); hopSetting) {
            auto input = aggregate.Input().Maybe<TDqConnection>();
            if (!input) {
                return node;
            }

            TMaybe<NHoppingWindow::EPolicy> defaultLatePolicy;
            if (KqpCtx.UserRequestContext && KqpCtx.UserRequestContext->WatermarkLateEventsPolicy) {
                const auto policy = to_lower(KqpCtx.UserRequestContext->WatermarkLateEventsPolicy);
                defaultLatePolicy = TryFromString<NHoppingWindow::EPolicy>(policy).GetOrElse(NHoppingWindow::EPolicy::Drop);
            }

            output = NHopping::RewriteAsHoppingWindow(
                node,
                ctx,
                getParents,
                input.Cast(),
                false,
                TDuration::MilliSeconds(TDqSettings::TDefault::WatermarksLateArrivalDelayMs),
                KqpCtx.Config->GetEnableWatermarks(),
                defaultLatePolicy,
                KqpCtx.Config->FeatureFlags.GetEnableHoppingWindowStartCheck()
            );
        } else if (KqpCtx.Config->FeatureFlags.GetEnableStreamingAggregation() && aggregate.Ref().GetConstraint<TStreamingConstraintNode>()) {
            output = RewriteAsStreamingAggregation(aggregate, ctx, getParents);
        } else {
            if (node.Ref().GetConstraint<TStreamingConstraintNode>() && Config->OptValidateStreamingConstraints.Get().GetOrElse(true)) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Ref().Pos()), "Aggregation of streaming input without windows is not supported"));
                return nullptr;
            }

            NDq::TSpillingSettings spillingSettings(KqpCtx.Config->GetEnabledSpillingNodes());
            output = DqRewriteAggregate(node, ctx, TypesCtx, false, KqpCtx.Config->HasOptEnableOlapPushdown() || KqpCtx.Config->HasOptUseFinalizeByKey(), KqpCtx.Config->HasOptUseFinalizeByKey(), spillingSettings.IsAggregationSpillingEnabled());
        }

        if (output) {
            DumpAppliedRule("RewriteAggregate", node.Ptr(), output.Cast().Ptr(), ctx);
        }

        return output;
    }

    TMaybeNode<TExprBase> RewriteAsStreamingAggregation(TCoAggregateBase aggregate, TExprContext& ctx, const TGetParents& getParents) {
        const auto& aggregateInput = aggregate.Input();
        const auto& maybeInputConnection = aggregateInput.Maybe<TDqConnection>();
        if (maybeInputConnection) {
            if (!IsSingleConsumerConnection(maybeInputConnection.Cast(), *getParents())) {
                return aggregate;
            }
        } else if (!IsKqpCompletePureExpr(aggregateInput)) {
            return aggregate;
        }

        const auto& settings = aggregate.Settings();
        if (const auto setting = GetSetting(settings.Ref(), "session")) {
            ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), "Session windows are not supported for streaming aggregation"));
            return {};
        }

        if (AnyOf(aggregate.Handlers().Ref().ChildrenList(), [](const auto& handler) {
            return handler->Child(TCoAggregateTuple::idx_Trait)->IsCallable("AggApply");
        })) {
            TAggregateExpander expander(/* usePartitionsByKeys */ true, /* useFinalizeByKeys */ false,
                aggregate.Ptr(), ctx, TypesCtx, /* forceCompact */ false, /* compactForDistinct */ false,
                /* usePhases */ false, /* useBlocks */ false);
            return TExprBase(expander.ExpandAggregate());
        }

        const auto& aggregatePos = aggregate.Ref().Pos();
        auto cleanedSettings = RemoveSetting(settings.Ref(), "compact", ctx); // Streaming aggregation already processes original rows with Init/Update, without a partial-state merge phase

        if (const auto stateTablePath = KqpCtx.Config->StreamingAggregationStateTablePath.Get()) {
            if (*stateTablePath && KqpCtx.Config->OptValidateStreamingCheckpoints.Get().GetOrElse(true) && !KqpCtx.Config->DisableCheckpoints.Get().GetOrElse(false)) {
                ctx.AddError(TIssue(ctx.GetPosition(aggregatePos), "Checkpoints are not supported for streaming aggregation with a state table"));
                return {};
            }

            cleanedSettings = AddSetting(*cleanedSettings, settings.Ref().Pos(), "state_table_path", ctx.NewAtom(settings.Ref().Pos(), *stateTablePath), ctx);
        }

        const auto buildAggregation = [&](TExprBase input) {
            return Build<TKqpStreamingAggregation>(ctx, aggregatePos)
                .Input(input)
                .Keys(aggregate.Keys())
                .Handlers(aggregate.Handlers())
                .Settings(cleanedSettings)
                .Done();
        };

        if (!maybeInputConnection) {
            return buildAggregation(aggregateInput);
        }

        const auto inputConnection = maybeInputConnection.Cast();
        const auto streamArg = Build<TCoArgument>(ctx, aggregatePos).Name("stream").Done();
        const auto streamingAggregation = buildAggregation(streamArg);

        if (aggregate.Keys().Empty()) {
            const auto input = Build<TDqCnUnionAll>(ctx, inputConnection.Ref().Pos())
                .Output(inputConnection.Output())
                .Done();

            return Build<TDqCnUnionAll>(ctx, aggregatePos)
                .Output()
                    .Stage<TDqStage>()
                        .Inputs()
                            .Add(input)
                            .Build()
                        .Program()
                            .Args(streamArg)
                            .Body(streamingAggregation)
                            .Build()
                        .Settings(TDqStageSettings().SetPartitionMode(TDqStageSettings::EPartitionMode::Aggregate).BuildNode(ctx, aggregatePos))
                        .Build()
                    .Index().Build("0")
                    .Build()
                .Done();
        }

        const auto hashShuffle = Build<TDqCnHashShuffle>(ctx, inputConnection.Ref().Pos())
            .Output(inputConnection.Output())
            .KeyColumns(aggregate.Keys())
            .Done();

        return Build<TDqCnUnionAll>(ctx, aggregatePos)
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(hashShuffle)
                        .Build()
                    .Program()
                        .Args(streamArg)
                        .Body(streamingAggregation)
                        .Build()
                    .Settings(TDqStageSettings().BuildNode(ctx, aggregatePos))
                    .Build()
                .Index().Build("0")
                .Build()
            .Done();
    }

    TMaybeNode<TExprBase> RewriteTakeSortToTopSort(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = DqRewriteTakeSortToTopSort(node, ctx, *getParents());
        DumpAppliedRule("RewriteTakeSortToTopSort", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteSqlInToEquiJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteSqlInToEquiJoin(node, ctx, KqpCtx, Config);
        DumpAppliedRule("RewriteSqlInToEquiJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteSqlInCompactToJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteSqlInCompactToJoin(node, ctx);
        DumpAppliedRule("KqpRewriteSqlInCompactToJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    static TExprNode::TPtr DqLookupSourceFromKqlReadTableRanges(const TExprBase& node, TExprContext& ctx) {
        auto maybeRanges = node.Maybe<TKqlReadTableRanges>();
        if (!maybeRanges) {
            return {};
        }
        auto ranges = maybeRanges.Cast();
        YQL_ENSURE(!TKqpReadTableSettings::Parse(ranges).Sampling, "Sampling is not supported for lookups");
        if (!ranges.Ranges().Maybe<TCoVoid>()) {
            ctx.AddWarning(TIssue(ctx.GetPosition(node.Pos()), "Right-side predicate pushdown blocks mandatory streamlookup rewrite"));
            return {};
        }
        const auto inputSeqType = node.Raw()->GetTypeAnn();
        return Build<TDqLookupSourceWrap>(ctx, node.Pos())
            .Input<TKqpReadRangesSourceSettings>()
                .Table(ranges.Table())
                .Columns(ranges.Columns())
                .Settings(ranges.Settings())
                .RangesExpr<TCoVoid>().Build()
            .Build()
            .DataSource<TCoDataSource>()
                .Category<TCoAtom>().Value(NYql::KikimrProviderName).Build()
                .Build()
            .RowType(ExpandType(ranges.Pos(), *GetSeqItemType(inputSeqType), ctx))
            .Settings<TCoAtomList>().Add(ranges.Table().Path()).Build()
        .Done().Ptr();
    }

    TMaybeNode<TExprBase> RewriteStreamEquiJoinWithLookup(TExprBase node, TExprContext& ctx) {
        // First step of stream lookup join with DQ external sources (not kqp tables)
        TExprBase output = DqRewriteStreamEquiJoinWithLookup(node, ctx, TypesCtx, DqLookupSourceFromKqlReadTableRanges);
        DumpAppliedRule("KqpRewriteStreamEquiJoinWithLookup", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> OptimizeEquiJoinWithCosts(TExprBase node, TExprContext& ctx) {
        TCBOSettings settings{
            .CBOTimeout = Config->CBOTimeout.Get().GetOrElse(NKikimr::NKqp::TCBOSettings{}.CBOTimeout),
            .CBOHardTimeout = Config->CBOHardTimeout.Get().GetOrElse(NKikimr::NKqp::TCBOSettings{}.CBOHardTimeout),
            .ShuffleEliminationJoinNumCutoff = Config->ShuffleEliminationJoinNumCutoff.Get().GetOrElse(TDqSettings::TDefault::ShuffleEliminationJoinNumCutoff)
        };

        auto optLevel = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel());
        bool useBlockJoin = Config->UseBlockHashJoin.Get().GetOrElse(false);
        bool enableShuffleElimination = KqpCtx.Config->OptShuffleElimination.Get().GetOrElse(KqpCtx.Config->GetDefaultEnableShuffleElimination());
        auto providerCtx = TKqpProviderContext(KqpCtx, optLevel, useBlockJoin, Config);
        auto stats = KqpCtx.KqpStats.GetStats(node.Raw());
        TTableAliasMap* tableAliases = stats? stats->TableAliases.Get(): nullptr;
        auto opt = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(providerCtx, settings, ctx, enableShuffleElimination, KqpCtx.KqpStats.ShufflingsFSM, tableAliases));
        TExprBase output = KqpOptimizeEquiJoinWithCosts(node, ctx, TypesCtx, KqpCtx.KqpStats, optLevel,
            *opt, [](auto& rels, auto label, auto node, auto stat) {
                rels.emplace_back(std::make_shared<TKqpRelOptimizerNode>(TString(label), *stat, node));
            },
            KqpCtx.EquiJoinsCount,
            KqpCtx.GetOptimizerHints(),
            enableShuffleElimination,
            &KqpCtx.ShufflingOrderingsByJoinLabels,
            &KqpCtx.CBOStats,
            [trace = KqpCtx.GetCompileTrace()](const std::function<void()>& search) {
                // Exponential in the number of joins at worst, so it is a phase.
                TCompileTraceScope scope(trace, TCompileTraceLevels::Phase, "Search join order");
                search();
            }
        );
        DumpAppliedRule("OptimizeEquiJoinWithCosts", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteEquiJoin(TExprBase node, TExprContext& ctx) {
        bool useCBO = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel()) >= 2;
        TMaybeNode<TExprBase> output = NKikimr::NKqp::KqpRewriteEquiJoin(node, KqpCtx.Config->GetHashJoinMode(), useCBO, ctx, TypesCtx, KqpCtx.KqpStats, KqpCtx.JoinsCount, KqpCtx.GetOptimizerHints());
        if (output) {
            DumpAppliedRule("RewriteEquiJoin", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> JoinToIndexLookup(TExprBase node, TExprContext& ctx) {
        bool useCBO = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel()) >= 2;
        TExprBase output = KqpJoinToIndexLookup(node, ctx, KqpCtx, useCBO, KqpCtx.GetOptimizerHints());
        DumpAppliedRule("JoinToIndexLookup", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ExpandWindowFunctions(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqExpandWindowFunctions(node, ctx, TypesCtx, !KqpCtx.Config->GetWindowFunctionsV2());
        DumpAppliedRule("ExpandWindowFunctions", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTopSortOverFlatMap(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteTopSortOverFlatMap(node, ctx);
        DumpAppliedRule("RewriteTopSortOverFlatMap", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> PushLimitOverFullText(TExprBase node, TExprContext& ctx) {
        auto output = KqpPushLimitOverFullText(node, ctx);
        if (!output.IsValid()) {
            return {};
        }

        DumpAppliedRule("PushLimitOverFullText", node.Ptr(), output.Cast().Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteFlatMapOverFullTextMatch(TExprBase node, TExprContext& ctx) {
        auto output = KqpRewriteFlatMapOverFullTextMatch(node, ctx, KqpCtx, TypesCtx);
        if (!output.IsValid()) {
            return {};
        }

        DumpAppliedRule("RewriteFlatMapOverFullTextMatch", node.Ptr(), output.Cast().Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> SelectJsonIndex(TExprBase node, TExprContext& ctx) {
        auto output = KqpSelectJsonIndex(node, ctx, KqpCtx);
        if (!output.IsValid()) {
            return {};
        }

        DumpAppliedRule("SelectJsonIndex", node.Ptr(), output.Cast().Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteFlatMapOverJsonRead(TExprBase node, TExprContext& ctx) {
        auto output = KqpRewriteFlatMapOverJsonRead(node, ctx, KqpCtx);
        if (!output.IsValid()) {
            return {};
        }

        DumpAppliedRule("RewriteFlatMapOverJsonRead", node.Ptr(), output.Cast().Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTopSortOverIndexRead(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = KqpRewriteTopSortOverIndexRead(node, ctx, TypesCtx, KqpCtx, *getParents());
        DumpAppliedRule("RewriteTopSortOverIndexRead", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteHybridRankTopSort(TExprBase node, TExprContext& ctx) {
        auto output = KqpRewriteHybridRankTopSort(node, ctx, KqpCtx);
        if (!output.IsValid()) {
            return {};
        }

        DumpAppliedRule("RewriteHybridRankTopSort", node.Ptr(), output.Cast().Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteFlatMapOverIndexRead(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = KqpRewriteFlatMapOverIndexRead(node, ctx, KqpCtx, *getParents());
        DumpAppliedRule("RewriteFlatMapOverIndexRead", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteTakeOverIndexRead(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = KqpRewriteTakeOverIndexRead(node, ctx, KqpCtx, *getParents());
        DumpAppliedRule("RewriteTakeOverIndexRead", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteFlatMapOverExtend(TExprBase node, TExprContext& ctx) {
        auto output = DqFlatMapOverExtend(node, ctx);
        DumpAppliedRule("RewriteFlatMapOverExtend", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteIndexRead(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteIndexRead(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteIndexRead", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteLookupIndex(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteLookupIndex(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteLookupIndex", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteStreamLookupIndex(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteStreamLookupIndex(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteStreamLookupIndex", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteLookupTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteLookupTable(node, ctx, KqpCtx);
        DumpAppliedRule("RewriteLookupTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> DeleteOverLookup(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        TExprBase output = KqpDeleteOverLookup(node, ctx, KqpCtx, *getParents());
        DumpAppliedRule("DeleteOverLookup", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> ExcessUpsertInputColumns(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpExcessUpsertInputColumns(node, ctx);
        DumpAppliedRule("ExcessUpsertInputColumns", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> DropTakeOverLookupTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpDropTakeOverLookupTable(node, ctx, KqpCtx);
        DumpAppliedRule("DropTakeOverLookupTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> TopSortOverExtend(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = KqpTopSortOverExtend(node, ctx, *getParents());
        DumpAppliedRule("TopSortOverExtend", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> UnorderedOverDqReadWrap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = NDq::UnorderedOverDqReadWrap(node, ctx, getParents, true, TypesCtx);
        if (output) {
            DumpAppliedRule("UnorderedOverDqReadWrap", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqReadWrap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = NDq::ExtractMembersOverDqReadWrap(node, ctx, getParents, true, TypesCtx);
        if (output) {
            DumpAppliedRule("ExtractMembersOverDqReadWrap", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> PushExtractMembersToDqJoin(TExprBase node, TExprContext& ctx) {
        auto output = NDq::DqPushExtractMembersToDqJoin(node, ctx);
        if (output) {
            DumpAppliedRule("PushExtractMembersToDqJoin", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> TakeOrSkipOverDqReadWrap(TExprBase node, TExprContext& ctx) {
        auto output = NDq::TakeOrSkipOverDqReadWrap(node, ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("TakeOrSkipOverDqReadWrap", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> ExtendOverDqReadWrap(TExprBase node, TExprContext& ctx) {
        auto output = NDq::ExtendOverDqReadWrap(node, ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("ExtendOverDqReadWrap", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> DqReadWideWrapFieldSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto output = NDq::DqReadWideWrapFieldSubset(node, ctx, getParents, TypesCtx);
        if (output) {
            DumpAppliedRule("DqReadWideWrapFieldSubset", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> MatchRecognize(TExprBase node, TExprContext& ctx) {
        auto output = ExpandMatchRecognize(node.Ptr(), ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("MatchRecognize", node.Ptr(), output, ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> DqReadWrapByProvider(TExprBase node, TExprContext& ctx) {
        auto output = NDq::DqReadWrapByProvider(node, ctx, TypesCtx);
        if (output) {
            DumpAppliedRule("DqReadWrapByProvider", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        auto output = NDq::ExtractMembersOverDqReadWrapMultiUsage(node, ctx, optCtx, getParents, TypesCtx);
        if (output) {
            DumpAppliedRule("ExtractMembersOverDqReadWrapMultiUsage", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    TMaybeNode<TExprBase> UnorderedOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        auto output = NDq::UnorderedOverDqReadWrapMultiUsage(node, ctx, optCtx, getParents, TypesCtx);
        if (output) {
            DumpAppliedRule("UnorderedOverDqReadWrapMultiUsage", node.Ptr(), output.Cast().Ptr(), ctx);
        }
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> ApplyExtractMembersToReadTable(TExprBase node, TExprContext& ctx,
        const TGetParents& getParents)
    {
        TExprBase output = KqpApplyExtractMembersToReadTable(node, ctx, *getParents(), IsGlobal);
        DumpAppliedRule("ApplyExtractMembersToReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    template <bool IsGlobal>
    TMaybeNode<TExprBase> ApplyExtractMembersToReadOlapTable(TExprBase node, TExprContext& ctx,
        const TGetParents& getParents)
    {
        TExprBase output = KqpApplyExtractMembersToReadOlapTable(node, ctx, *getParents(), IsGlobal);
        DumpAppliedRule("ApplyExtractMembersToReadOlapTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

private:
    TTypeAnnotationContext& TypesCtx;
    TKqpOptimizeContext& KqpCtx;
    const TKikimrConfiguration::TPtr& Config;
};

}   // anonymous namespace

TAutoPtr<IGraphTransformer> CreateKqpJoinProjectionTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx) {
    return THolder<IGraphTransformer>(new TKqpJoinProjectionTransformer(*kqpCtx));
}

TAutoPtr<IGraphTransformer> CreateKqpLogOptTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typesCtx, const TKikimrConfiguration::TPtr& config)
{
    return THolder<IGraphTransformer>(new TKqpLogicalOptTransformer(typesCtx, kqpCtx, config));
}

}   // namespace NKikimr::NKqp::NOpt
