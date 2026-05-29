#include "yql_pq_provider_impl.h"

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/providers/common/pushdown/collection.h>
#include <ydb/library/yql/providers/common/pushdown/intervals_converter.h>
#include <ydb/library/yql/providers/common/pushdown/physical_opt.h>
#include <ydb/library/yql/providers/common/pushdown/predicate_node.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_predicate_pushdown.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/utils/plan/plan_utils.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_helpers.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/utils/log/log.h>

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
            EFlag::ToBytesFromStringExpressions | EFlag::FlatMapOverOptionals | EFlag::PredicateAsExpression |

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

TVector<TCoNameValueTuple> DropUnusedMetadata(
    const TPqTopic& pqTopic,
    const std::unordered_set<TString>& usedColumnNames,
    bool includeUserAttributes)
{
    TVector<TCoNameValueTuple> newSourceMetadata;
    for (auto metadataItem : pqTopic.Metadata()) {
        auto metadataName = metadataItem.Cast<TCoNameValueTuple>().Value().Maybe<TCoAtom>().Cast().StringValue();
        if (GetPqMetaFieldDescriptorBySysColumn(metadataName, includeUserAttributes) && usedColumnNames.contains(metadataName)) {
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
        SetGlobal(0); // Stage 0 of this optimizer is global => we can remap nodes.
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

    TMaybeNode<TExprBase> ExtractMembersOverDqWrap(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
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
        if (const auto maybeWatermarkExpr = dqPqTopicSource.WatermarkExpr()) {
            const auto watermarkExpr = maybeWatermarkExpr.Cast();
            GetUsedWatermarkColumnNames(watermarkExpr, usedColumnNames);
        }

        const auto oldRowType = pqTopic.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        const auto oldRowColumnsCount = oldRowType->GetSize();

        // If shared reading disabled we should check that this optimisation will not produce double topic reading
        std::vector<TCoExtractMembers> remapNodes;
        if (!UseSharedReadingForTopic(dqPqTopicSource)) {
            if (const auto& consumers = NDq::GetConsumers(dqSourceWrap, *getParents()); consumers.size() > 1) {
                for (const auto& consumer : consumers) {
                    const auto& maybeExtractMembers = TMaybeNode<TCoExtractMembers>(consumer);
                    if (!maybeExtractMembers) {
                        YQL_CLOG(TRACE, ProviderPq) << "PQ ExtractMembersOverDqWrap. Detected source multi usage, skip optimisation";
                        return node;
                    }

                    if (consumer == node.Raw()) {
                        continue;
                    }

                    const auto& otherExtractMembers = maybeExtractMembers.Cast();
                    remapNodes.emplace_back(otherExtractMembers);
                    for (const auto& member : otherExtractMembers.Members()) {
                        usedColumnNames.emplace(member.Value());
                    }

                    if (usedColumnNames.size() == oldRowColumnsCount) {
                        return node;
                    }
                }

                YQL_CLOG(TRACE, ProviderPq) << "PQ ExtractMembersOverDqWrap. Detected source multi usage, extract common columns: " << usedColumnNames.size();
            }
        }

        const TStructExprType* inputRowType = pqTopic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        if (usedColumnNames.size() == 0 && inputRowType->GetSize() > 0) {
            auto item = GetLightColumn(*inputRowType);
            YQL_ENSURE(item);
            YQL_ENSURE(usedColumnNames.insert(TString(item->GetName())).second);
        }

        if (oldRowColumnsCount == usedColumnNames.size()) {
            return node;
        }

        const auto& newSourceMetadata = DropUnusedMetadata(pqTopic, usedColumnNames, State_->EnableUserAttributesInTopicQuery);

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

        const auto makeNewExtractMembers = [&ctx, &usedColumnNames, &dqSourceWrap, &newDqSourceWrap](const TCoExtractMembers& initialExtractMembers) {
            const TStructExprType* outputRowType = initialExtractMembers.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            if (outputRowType->GetSize() == usedColumnNames.size()) {
                return newDqSourceWrap;
            }

            return Build<TCoExtractMembers>(ctx, initialExtractMembers.Pos())
                .InitFrom(initialExtractMembers)
                .Input(ctx.ReplaceNode(initialExtractMembers.Input().Ptr(), dqSourceWrap.Ref(), newDqSourceWrap))
                .Done()
                .Ptr();
        };

        // Replace Topic source for all consumers with common columns set
        for (const auto& otherExtractMembers : remapNodes) {
            optCtx.RemapNode(otherExtractMembers.Ref(), makeNewExtractMembers(otherExtractMembers));
        }

        return makeNewExtractMembers(extractMembers);
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

        if (!maybeDqSourceWrap) {
            return node;
        }
        TDqSourceWrap dqSourceWrap = maybeDqSourceWrap.Cast();
        auto maybeDqPqTopicSource = dqSourceWrap.Input().Maybe<TDqPqTopicSource>();
        if (!maybeDqPqTopicSource) {
            return node;
        }
        TDqPqTopicSource dqPqTopicSource = maybeDqPqTopicSource.Cast();
        const auto& topic = dqPqTopicSource.Topic();

        const auto* topicMeta = State_->FindTopicMeta(topic);
        if (!topicMeta || !topicMeta->FederatedTopic) {
            return node;
        }

        if (auto list = dqPqTopicSource.Partitions().Maybe<TCoList>()) {
            if (list.Cast().Args().Count() == 1) {  //  only ListType
                YQL_CLOG(INFO, ProviderPq) << "Empty list by partitions, replace node to List";
                return ctx.NewCallable(node.Pos(), "List", { ExpandType(node.Pos(), *node.Ref().GetTypeAnn(), ctx) });
            }
        }

        if (!dqPqTopicSource.FilterPredicate().Ref().Content().empty()
            || !dqPqTopicSource.WriteTimePredicate().Ref().Content().empty()
            || !dqPqTopicSource.OffsetPredicate().Ref().Content().empty()
            || !dqPqTopicSource.Partitions().Ref().IsCallable("Void")) {
            return node;
        }

        TString offsetPredicateSerializedProto;
        TString writeTimePredicateSerializedProto;

        if (State_->EnableTopicsPredicatePushdown) {
            std::unordered_set<TString> fields{"_yql_sys_offset", "_yql_sys_write_time", "_yql_sys_partition_id"};
            bool hasPredicate = !!FindNode(flatmap.Lambda().Body().Ptr(),
                [&](const TExprNode::TPtr& node) {
                    if (!TCoMember::Match(node.Get())) {
                        return false;
                    }
                    auto member = TExprBase(node).Cast<TCoMember>();
                    auto name = member.Name().StringValue();
                    return fields.contains(name);
                });
            if (hasPredicate) {
                auto hasEvaluateFilter = [&](const TExprNode::TPtr& node) -> bool {
                    return node->IsCallable({"EvaluateExpr"});
                };
                if (!!FindNode(flatmap.Lambda().Body().Ptr(), hasEvaluateFilter)) {
                    return node;
                }

                auto splitedPredicate = SplitPredicateByMember(flatmap.Lambda(), fields, ctx, true);
                if (splitedPredicate) {
                    auto modifiedPredicate = ReplaceCompareNodesToEvaluate(splitedPredicate.Cast().Ptr(), flatmap.Lambda().Body().Ptr(), ctx);
                    if (modifiedPredicate) {    // something replaced
                        auto originalLambda = flatmap.Lambda();
                        auto newLambda = Build<TCoLambda>(ctx, originalLambda.Pos())
                            .Args(originalLambda.Args())
                            .Body(TExprBase(modifiedPredicate))
                            .Done();

                        return Build<TCoFlatMap>(ctx, flatmap.Pos())
                            .InitFrom(flatmap)
                            .Lambda(newLambda)
                            .Done();
                    }
                    const auto& federatedTopics = *(topicMeta->FederatedTopic);
                    TInstant minWriteTime = TInstant::Max();
                    for (const auto& topic : federatedTopics) {
                        for (auto [p, time] : topic.MaxWriteTime) {
                            minWriteTime = std::min(minWriteTime, time);
                        }
                    }
                    auto [offsetProto, emptyRangeByOffsets] = SerializePredicate("_yql_sys_offset", flatmap.Lambda(), ctx);
                    auto [writeTimeProto, emptyRangeByWriteTime] = SerializePredicate("_yql_sys_write_time", flatmap.Lambda(), ctx, minWriteTime.MicroSeconds());
                    if (emptyRangeByOffsets || emptyRangeByWriteTime) {
                        YQL_CLOG(INFO, ProviderPq) << "Empty range by offsets or write time, replace node to List";
                        return ctx.NewCallable(node.Pos(), "List", { ExpandType(node.Pos(), *node.Ref().GetTypeAnn(), ctx) });
                    }
                    offsetPredicateSerializedProto = offsetProto;
                    writeTimePredicateSerializedProto = writeTimeProto;
                }
            }
        }
        
        TString sharedReadingPridicateSerializedProto;
        if (UseSharedReadingForTopic(dqPqTopicSource)) {
            // Push predicate only if enabled shared reading, because this optimisation may produce double topic reading
            NPushdown::TPredicateNode sharedReadingPredicate = MakePushdownNode(flatmap.Lambda(), ctx, node.Pos(), TPushdownSettings());
            if (!sharedReadingPredicate.IsEmpty()) {
                TStringBuilder err;
                NYql::NConnector::NApi::TPredicate predicateProto;
                if (!NYql::SerializeFilterPredicate(ctx, sharedReadingPredicate.ExprNode.Cast(), flatmap.Lambda().Args().Arg(0), &predicateProto, err)) {
                    ctx.AddWarning(TIssue(ctx.GetPosition(node.Pos()), "Failed to serialize filter predicate for source: " + err));
                    return node;
                }
                YQL_ENSURE(predicateProto.SerializeToString(&sharedReadingPridicateSerializedProto));
            }   
        }

        bool isPartitionListUpdated = false;
        TExprNode::TPtr partitionList = dqPqTopicSource.Partitions().Ptr();
        if (State_->EnableTopicsPredicatePushdown) {
            auto splitedPredicate = SplitPredicateByMember(flatmap.Lambda(), {"_yql_sys_partition_id"}, ctx, false);

            if (splitedPredicate) {
                auto newFilterArg = ctx.NewArgument(node.Pos(), "_yql_sys_partition_id");
                auto predicateBody = splitedPredicate.Cast().Ptr();

                auto lambdaArg = flatmap.Lambda().Args().Arg(0).Raw();
                TNodeOnNodeOwnedMap replaces;
                VisitExpr(predicateBody, [&](const TExprNode::TPtr& exprNode) {
                    if (TCoMember::Match(exprNode.Get())) {
                        auto member = TCoMember(exprNode.Get());
                        if (member.Struct().Raw() == lambdaArg
                            && member.Name().StringValue() == "_yql_sys_partition_id") {
                            replaces[exprNode.Get()] = newFilterArg;
                        }
                        return false;
                    }
                    return true;
                });
                auto replacedBody = ctx.ReplaceNodes(std::move(predicateBody), replaces);

                auto newFilterLambda = ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), {newFilterArg}),
                    std::move(replacedBody));

                const auto& federatedTopics = *(topicMeta->FederatedTopic);
                size_t topicPartitionsCount = 0;
                for (const auto& topic : federatedTopics) {
                    topicPartitionsCount = std::max(topicPartitionsCount, static_cast<size_t>(topic.PartitionsCount));
                }
                if (!topicPartitionsCount) {
                    return node;
                }
                isPartitionListUpdated = true;
                partitionList = ctx.Builder(node.Pos())
                    .Callable("EvaluateExpr")
                        .Callable(0, "Filter")
                            .Callable(0, "ListFromRange")
                                .Callable(0, "Uint64")
                                    .Atom(0, 0)
                                .Seal()
                                .Callable(1, "Uint64")
                                    .Atom(0, topicPartitionsCount)
                                .Seal()
                            .Seal()
                            .Add(1, newFilterLambda)
                        .Seal()
                    .Seal()
                    .Build();
            }
        }

        if (sharedReadingPridicateSerializedProto.empty()
            && !isPartitionListUpdated
            && offsetPredicateSerializedProto.empty()
            && writeTimePredicateSerializedProto.empty()) {
            return node;
        }

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
                            .FilterPredicate().Value(sharedReadingPridicateSerializedProto).Build()
                            .Partitions(partitionList)
                            .OffsetPredicate().Value(offsetPredicateSerializedProto).Build()
                            .WriteTimePredicate().Value(writeTimePredicateSerializedProto).Build()
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
                    .FilterPredicate().Value(sharedReadingPridicateSerializedProto).Build()
                    .Partitions(partitionList)
                    .OffsetPredicate().Value(offsetPredicateSerializedProto).Build()
                    .WriteTimePredicate().Value(writeTimePredicateSerializedProto).Build()
                    .Build()
                .Build()
            .Done();
    }

private:
    static std::optional<TStringBuf> GetTopicSourceSetting(const TDqPqTopicSource& topicSource, TStringBuf name) {
        const auto settingsCount = topicSource.Settings().Size();
        for (size_t i = 0; i < settingsCount; ++i) {
            const auto& setting = topicSource.Settings().Item(i);
            if (setting.Name().Value() != name) {
                continue;
            }
            if (const auto& maybeValue = setting.Value()) {
                const TExprNode& value = maybeValue.Cast().Ref();
                YQL_ENSURE(value.IsAtom());
                return value.Content();
            }
            break;
        }
        return std::nullopt;
    }

    bool UseSharedReadingForTopic(const TDqPqTopicSource& topicSource) const {
        const bool sharedReading = FromString(GetTopicSourceSetting(topicSource, SharedReading).value_or("false"));
        const bool streamingTopicRead = FromString(GetTopicSourceSetting(topicSource, StreamingTopicRead).value_or(State_->StreamingTopicsReadByDefault ? "true" : "false"));
        return sharedReading && streamingTopicRead;
    }

    TExprNode::TPtr ReplaceCompareNodesToEvaluate(const TExprNode::TPtr& lambda, TExprNode::TPtr&& originalLambdaBody, TExprContext& ctx) const {
        // Pre-compute containsMember for all nodes into a cache to avoid
        // quadratic complexity when the same large expression subtree appears
        // in many comparisons (e.g. 1k comparisons sharing a 1k-node expr).
        TNodeMap<bool> containsMemberCache;
        std::function<bool(const TExprNode::TPtr&)> containsMemberCached = [&](const TExprNode::TPtr& root) -> bool {
            auto [it, inserted] = containsMemberCache.emplace(root.Get(), false);
            if (!inserted) {
                return it->second;
            }
            if (TCoMember::Match(root.Get())) {
                it->second = true;
            } else {
                for (const auto& child : root->Children()) {
                    if (containsMemberCached(child)) {
                        it->second = true;
                        break;
                    }
                }
            }
            return it->second;
        };

        auto isConstant = [](const TExprNode::TPtr& node) -> bool {
            auto isLiteral = [](const TExprNode::TPtr& node) -> bool {
                return TCoIntegralCtor::Match(node.Get()) || TCoDataCtor::Match(node.Get()) || TCoDateBase::Match(node.Get());
            };
            if (isLiteral(node)) {
                return true;
            }
            if (TCoJust::Match(node.Get())) {
                auto just = TCoJust(node.Get());
                auto inner = just.Input().Ptr();
                return isLiteral(inner);
            }
            return false;
        };

        // Collect all replacements first, then apply them in a single pass.
        TNodeOnNodeOwnedMap replaces;

        VisitExpr(lambda, [&](const TExprNode::TPtr& exprNode) {
            if (!TCoCompare::Match(exprNode.Get())) {
                return true; // continue traversal
            }
            auto compare = TCoCompare(exprNode.Get());
            auto left = compare.Left().Ptr();
            auto right = compare.Right().Ptr();

            if (!containsMemberCached(right) && !isConstant(right) && !replaces.contains(right.Get())) {
                replaces[right.Get()] = ctx.Builder(exprNode->Pos())
                    .Callable("EvaluateExpr")
                        .Add(0, right)
                    .Seal()
                    .Build();
            }
            
            if (!containsMemberCached(left) && !isConstant(left) && !replaces.contains(left.Get())) {
                replaces[left.Get()] = ctx.Builder(exprNode->Pos())
                    .Callable("EvaluateExpr")
                        .Add(0, left)
                    .Seal()
                    .Build();
            }
            return false; // don't descend into compare children
        });
        if (replaces.empty()) {
            return {};
        }
        return ctx.ReplaceNodes(std::move(originalLambdaBody), replaces);
    }

    std::pair<TString, bool> SerializePredicate(
        const TString& memberName,
        const NNodes::TCoLambda& lambda,
        TExprContext& ctx,
        std::optional<ui64> min = std::nullopt
    ) const {
        if (!State_->EnableTopicsPredicatePushdown) {
            return {{}, false};
        }
        auto settings = NPushdown::TSettings(NLog::EComponent::ProviderPq);
        settings.EnableMember(memberName);

        using EFlag = NPushdown::TSettings::EFeatureFlag;
        settings.Enable(
            EFlag::ExpressionAsPredicate | EFlag::ImplicitConversionToInt64 |
            EFlag::DoNotCheckCompareArgumentsTypes | EFlag::InOperator |
            EFlag::JustPassthroughOperators | EFlag::PredicateAsExpression |
            EFlag::SplitOrOperator | EFlag::TimestampCtor
        );

        NPushdown::TPredicateNode predicate = MakePushdownNode(lambda, ctx, lambda.Pos(), settings);
        if (predicate.IsEmpty()) {
            return {{}, false};
        }
        TStringBuilder err;
        TDisjointIntervalTree<i64> tree;
        if (!NYql::NPushdown::ConvertPredicateToIntervals(predicate.ExprNode.Cast(), tree, err)) {
            ctx.AddWarning(TIssue(ctx.GetPosition(lambda.Pos()), "Failed to calculate filter predicate for source: " + err));
            return {{}, false};
        }

        NPq::NProto::TOffsetPredicate proto;
        if (tree.Empty()) {
            return {{}, true};
        }

        YQL_CLOG(TRACE, ProviderPq) << "ConvertPredicateToIntervals result over " << memberName << ": {" << tree.Min() << ", " << tree.Max() << "}" << Endl;;
        if (tree.Min() != Min<i64>() || tree.Max() != Max<i64>()) {
            auto* item = proto.AddItem();
            if (tree.Min() != Min<i64>()) {
                ui64 treeMin = std::max(tree.Min(), (i64)0);
                item->SetBegin(!min ? treeMin : std::min(treeMin, *min));
            }
            if (tree.Max() != Max<i64>()) {
                ui64 treeMax = std::max(tree.Max(), (i64)0);
                item->SetEnd(treeMax);
            }
        }
        TString result; 
        YQL_ENSURE(proto.SerializeToString(&result));
        return {result, false};
    }

    TMaybeNode<TExprBase> SplitPredicateByMember(
        const NNodes::TCoLambda& lambda,
        std::unordered_set<TString> fields,
        TExprContext& ctx,
        bool anyExpressions
    ) const {
        if (!State_->EnableTopicsPredicatePushdown) {
            return {};
        }
        auto settings = NPushdown::TSettings(NLog::EComponent::ProviderPq);
        for (const auto& member : fields) {
            settings.EnableMember(member);
        }

        using EFlag = NPushdown::TSettings::EFeatureFlag;
        settings.Enable(
            EFlag::ExpressionAsPredicate | EFlag::ImplicitConversionToInt64 |
            EFlag::DoNotCheckCompareArgumentsTypes | EFlag::InOperator |
            EFlag::JustPassthroughOperators | EFlag::PredicateAsExpression |
            EFlag::SplitOrOperator | EFlag::TimestampCtor | EFlag::IntervalCtor | EFlag::ArithmeticalExpressions
        );
        if (anyExpressions) {
            settings.Enable(EFlag::AnyExpressionExceptMember);
        }

        NPushdown::TPredicateNode predicate = MakePushdownNode(lambda, ctx, lambda.Pos(), settings);
        if (predicate.IsEmpty()) {
            return {};
        }
        return predicate.ExprNode;
    }

    TPqState::TPtr State_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreatePqLogicalOptProposalTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqLogicalOptProposalTransformer>(state);
}

} // namespace NYql
