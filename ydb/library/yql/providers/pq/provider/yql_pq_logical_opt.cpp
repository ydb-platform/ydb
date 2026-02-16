#include "yql_pq_provider_impl.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_helpers.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/library/yql/utils/plan/plan_utils.h>

#include <ydb/library/yql/providers/common/pushdown/collection.h>
#include <ydb/library/yql/providers/common/pushdown/physical_opt.h>
#include <ydb/library/yql/providers/common/pushdown/predicate_node.h>

#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_predicate_pushdown.h>

namespace NYql {

using namespace NNodes;

namespace {
    struct TPushdownSettings: public NPushdown::TSettings {
        TPushdownSettings()
            : NPushdown::TSettings(NLog::EComponent::ProviderGeneric)
        {
            using EFlag = NPushdown::TSettings::EFeatureFlag;
            Enable(
                // Operator features
                EFlag::ExpressionAsPredicate | EFlag::ArithmeticalExpressions | EFlag::ImplicitConversionToInt64 |
                EFlag::StringTypes | EFlag::LikeOperator | EFlag::DoNotCheckCompareArgumentsTypes | EFlag::InOperator |
                EFlag::IsDistinctOperator | EFlag::JustPassthroughOperators | EFlag::DivisionExpressions | EFlag::CastExpression |
                EFlag::ToBytesFromStringExpressions | EFlag::FlatMapOverOptionals |

                // Split features
                EFlag::SplitOrOperator
            );
            EnableFunction("Re2.Grep");  // For REGEXP pushdown
        }
    };

std::unordered_set<TString> GetUsedColumnNames(const TCoExtractMembers& extractMembers) {
    std::unordered_set<TString> usedColumnNames;
    for (const auto& member : extractMembers.Members()) {
        usedColumnNames.emplace(member.StringValue());
    }

    return usedColumnNames;
}

void GetUsedWatermarkColumnNames(const TExprBase& expr, std::unordered_set<TString>& result) {
    if (const auto maybeMember = expr.Maybe<TCoMember>()) {
        const auto member = maybeMember.Cast();
        result.insert(member.Name().StringValue());
        return;
    }

    for (const auto& child : expr.Raw()->Children()) {
        GetUsedWatermarkColumnNames(TExprBase(child), result);
    }
}

TVector<TCoNameValueTuple> DropUnusedMetadata(const TPqTopic& pqTopic, const std::unordered_set<TString>& usedColumnNames) {
    TVector<TCoNameValueTuple> newSourceMetadata;
    for (auto metadataItem : pqTopic.Metadata()) {
        auto metadataName = metadataItem.Cast<TCoNameValueTuple>().Value().Maybe<TCoAtom>().Cast().StringValue();
        if (FindPqMetaFieldDescriptorBySysColumn(metadataName) && usedColumnNames.contains(metadataName)) {
            newSourceMetadata.push_back(metadataItem);
        }
    }

    return newSourceMetadata;
}

TCoNameValueTupleList DropUnusedMetadataFromDqWrapSettings(
    const TDqSourceWrap& dqSourceWrap,
    const TVector<TCoNameValueTuple>& newSourceMetadata,
    TExprContext& ctx)
{
    TVector<TCoNameValueTuple> newSettings;
    for (const auto settingItem : dqSourceWrap.Settings().Maybe<TCoNameValueTupleList>().Cast()) {
        if (settingItem.Name() == "metadataColumns") {
            std::vector<TExprNode::TPtr> newMetadataColumns;
            newMetadataColumns.reserve(newSourceMetadata.size());

            for (auto metadataName : newSourceMetadata) {
                newMetadataColumns.push_back(ctx.NewAtom(
                    dqSourceWrap.Pos(),
                    metadataName.Value().Maybe<TCoAtom>().Cast().StringValue()));
            }

            if (!newMetadataColumns.empty()) {
                newSettings.push_back(Build<TCoNameValueTuple>(ctx, dqSourceWrap.Pos())
                    .Name().Build("metadataColumns")
                    .Value(ctx.NewList(dqSourceWrap.Pos(), std::move(newMetadataColumns)))
                    .Done());
            }

            continue;
        }

        newSettings.push_back(settingItem);
    }

    return Build<TCoNameValueTupleList>(ctx, dqSourceWrap.Pos())
        .Add(std::move(newSettings))
        .Done();
}

TExprNode::TPtr DropUnusedRowItems(
    TPositionHandle position,
    const TStructExprType* oldRowType,
    const std::unordered_set<TString>& usedColumnNames,
    TExprContext& ctx)
{
    TVector<const TItemExprType*> newFields;
    newFields.reserve(oldRowType->GetSize());

    for (auto itemExprType : oldRowType->GetItems()) {
        const auto columnName = TString(itemExprType->GetName());
        if (!usedColumnNames.contains(columnName)) {
            continue;
        }

        newFields.push_back(itemExprType);
    }

    return ExpandType(position, *ctx.MakeType<TStructExprType>(newFields), ctx);
}

TExprNode::TPtr DropUnusedColumns(
    TExprBase oldColumns,
    const std::unordered_set<TString>& usedColumnNames,
    TExprContext& ctx)
{
    TExprNode::TListType res;
    for (const auto& column : oldColumns.Cast<TCoAtomList>()) {
        if (!usedColumnNames.contains(column.StringValue())) {
            continue;
        }

        res.push_back(column.Ptr());
    }

    return ctx.NewList(oldColumns.Pos(), std::move(res));
}

class TPqLogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TPqLogicalOptProposalTransformer(TPqState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderPq, {})
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TPqLogicalOptProposalTransformer::name)
      //  AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembers));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqWrap));
        AddHandler(0, &TCoFlatMap::Match, HNDL(PushFilterToPqTopicSource));
        #undef HNDL
    }

    /*
    TMaybeNode<TExprBase> ExtractMembers(TExprBase node, TExprContext& ctx) const {
        const auto& extract = node.Cast<TCoExtractMembers>();
        const auto& input = extract.Input();
        const auto& read = input.Maybe<TCoRight>().Input().Maybe<TPqReadTopic>();
        if (!read) {
            return node;
        }

        const auto& cast = read.Cast();
        return Build<TCoRight>(ctx, extract.Pos())
            .Input<TPqReadTopic>()
            .World(cast.World())
            .DataSource(cast.DataSource())
            .Topic(cast.Topic())
            .Columns(extract.Members())
            .Build()
            .Done();
    }*/

    TMaybeNode<TExprBase> ExtractMembersOverDqWrap(TExprBase node, TExprContext& ctx) const {
        const auto& extractMembers = node.Cast<TCoExtractMembers>();
        const auto& extractMembersInput = extractMembers.Input();
        const auto& maybeDqSourceWrap = extractMembersInput.Maybe<TDqSourceWrap>();
        if (!maybeDqSourceWrap) {
            return node;
        }

        const auto& dqSourceWrap = maybeDqSourceWrap.Cast();
        if (dqSourceWrap.DataSource().Category() != PqProviderName) {
            return node;
        }

        const auto& maybeDqPqTopicSource = dqSourceWrap.Input().Maybe<TDqPqTopicSource>();
        if (!maybeDqPqTopicSource) {
            return node;
        }

        const auto& dqPqTopicSource = maybeDqPqTopicSource.Cast();
        const auto& pqTopic = dqPqTopicSource.Topic();

        auto usedColumnNames = GetUsedColumnNames(extractMembers);
        if (const auto maybeWatermark = dqPqTopicSource.Watermark()) {
            const auto watermark = maybeWatermark.Cast();
            GetUsedWatermarkColumnNames(watermark, usedColumnNames);
        }

        const TStructExprType* inputRowType = pqTopic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        const TStructExprType* outputRowType = node.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        if (outputRowType->GetSize() == 0 && inputRowType->GetSize() > 0) {
            auto item = GetLightColumn(*inputRowType);
            YQL_ENSURE(item);
            YQL_ENSURE(usedColumnNames.insert(TString(item->GetName())).second);
        }

        const auto oldRowType = pqTopic.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        if (oldRowType->GetSize() == usedColumnNames.size()) {
            return node;
        }

        const auto& newSourceMetadata = DropUnusedMetadata(pqTopic, usedColumnNames);

        const TExprNode::TPtr newPqTopicSource = Build<TDqPqTopicSource>(ctx, dqPqTopicSource.Pos())
            .InitFrom(dqPqTopicSource)
            .Topic<TPqTopic>()
                .InitFrom(pqTopic)
                .Metadata().Add(newSourceMetadata).Build()
                .RowSpec(DropUnusedRowItems(pqTopic.RowSpec().Pos(), inputRowType, usedColumnNames, ctx))
                .Build()
            .Columns(DropUnusedColumns(dqPqTopicSource.Columns(), usedColumnNames, ctx))
            .RowType(DropUnusedRowItems(dqPqTopicSource.RowType().Pos(), oldRowType, usedColumnNames, ctx))
            .Done()
            .Ptr();

        const TExprNode::TPtr newDqSourceWrap = Build<TDqSourceWrap>(ctx, dqSourceWrap.Pos())
            .InitFrom(dqSourceWrap)
            .Input(newPqTopicSource)
            .Settings(DropUnusedMetadataFromDqWrapSettings(dqSourceWrap, newSourceMetadata, ctx))
            .RowType(DropUnusedRowItems(dqSourceWrap.RowType().Pos(), oldRowType, usedColumnNames, ctx))
            .Done()
            .Ptr();

        if (outputRowType->GetSize() == usedColumnNames.size()) {
            return newDqSourceWrap;
        }

        return Build<TCoExtractMembers>(ctx, node.Pos())
            .InitFrom(extractMembers)
            .Input(ctx.ReplaceNode(extractMembersInput.Ptr(), dqSourceWrap.Ref(), newDqSourceWrap))
            .Done();
    }

    bool IsEmptyFilterPredicate(const TCoLambda& lambda) const {
        auto maybeBool = lambda.Body().Maybe<TCoBool>();
        if (!maybeBool) {
            return false;
        }
        return TStringBuf(maybeBool.Cast().Literal()) == "true"sv;
    }

    TMaybeNode<TExprBase> PushFilterToPqTopicSource(TExprBase node, TExprContext& ctx) const {
        auto flatmap = node.Cast<TCoFlatMap>();
        auto maybeExtractMembers = flatmap.Input().Maybe<TCoExtractMembers>();

        auto maybeDqSourceWrap =
            maybeExtractMembers
            ? maybeExtractMembers.Cast().Input().Maybe<TDqSourceWrap>()
            : flatmap.Input().Maybe<TDqSourceWrap>();
        ;
        if (!maybeDqSourceWrap) {
            return node;
        }
        TDqSourceWrap dqSourceWrap = maybeDqSourceWrap.Cast();
        auto maybeDqPqTopicSource = dqSourceWrap.Input().Maybe<TDqPqTopicSource>();
        if (!maybeDqPqTopicSource) {
            return node;
        }
        TDqPqTopicSource dqPqTopicSource = maybeDqPqTopicSource.Cast();
        if (!dqPqTopicSource.FilterPredicate().Ref().Content().empty()) {
            YQL_CLOG(TRACE, ProviderPq) << "Push filter. Lambda is already not empty";
            return node;
        }

        NPushdown::TPredicateNode predicate = MakePushdownNode(flatmap.Lambda(), ctx, node.Pos(), TPushdownSettings());
        if (predicate.IsEmpty()) {
            return node;
        }

        TStringBuilder err;
        NYql::NConnector::NApi::TPredicate predicateProto;
        if (!NYql::SerializeFilterPredicate(ctx, predicate.ExprNode.Cast(), flatmap.Lambda().Args().Arg(0), &predicateProto, err)) {
            ctx.AddWarning(TIssue(ctx.GetPosition(node.Pos()), "Failed to serialize filter predicate for source: " + err));
            return node;
        }

        TString serializedProto;
        YQL_ENSURE(predicateProto.SerializeToString(&serializedProto));
        YQL_CLOG(INFO, ProviderPq) << "Build new TCoFlatMap with predicate";

        if (maybeExtractMembers) {
            return Build<TCoFlatMap>(ctx, flatmap.Pos())
                .InitFrom(flatmap)
                .Input<TCoExtractMembers>()
                    .InitFrom(maybeExtractMembers.Cast())
                    .Input<TDqSourceWrap>()
                        .InitFrom(dqSourceWrap)
                        .Input<TDqPqTopicSource>()
                            .InitFrom(dqPqTopicSource)
                            .FilterPredicate().Value(serializedProto).Build()
                            .Build()
                        .Build()
                    .Build()
                .Done();
        }
        return Build<TCoFlatMap>(ctx, flatmap.Pos())
            .InitFrom(flatmap)
            .Input<TDqSourceWrap>()
                .InitFrom(dqSourceWrap)
                .Input<TDqPqTopicSource>()
                    .InitFrom(dqPqTopicSource)
                    .FilterPredicate().Value(serializedProto).Build()
                    .Build()
                .Build()
            .Done();
    }

private:
    TPqState::TPtr State_;
};

}

THolder<IGraphTransformer> CreatePqLogicalOptProposalTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqLogicalOptProposalTransformer>(state);
}

} // namespace NYql
