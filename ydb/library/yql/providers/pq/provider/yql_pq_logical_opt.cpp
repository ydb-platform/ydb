#include "yql_pq_provider_impl.h"

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/providers/common/pushdown/collection.h>
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

        size_t topicPartitionsCount = 0;
        for (auto kv : topic.Props()) {
            auto key = kv.Name().Value();
            if (key == PartitionsCountProp) {
                topicPartitionsCount = FromString(kv.Value().Ref().Content());
            }
        }

        if (!dqPqTopicSource.FilterPredicate().Ref().Content().empty()) {
            YQL_CLOG(TRACE, ProviderPq) << "Push filter. Lambda is already not empty";
            return node;
        }

        if (!dqPqTopicSource.Partitions().Ref().IsCallable("List")) {
            YQL_CLOG(TRACE, ProviderPq) << "Push filter. Lambda is already not empty";
            return node;
        }

        if (!dqPqTopicSource.WriteTimePredicate().Ref().Content().empty()) {
            YQL_CLOG(TRACE, ProviderPq) << "Push filter. Lambda is already not empty";
            return node;
        }

        if (!dqPqTopicSource.OffsetPredicate().Ref().Content().empty()) {
            YQL_CLOG(TRACE, ProviderPq) << "OffsetPredicate is already not empty";
            return node;
        }

        if (dqPqTopicSource.CompareArgsEvaluate().Maybe<TCoVoid>()) {
            Cerr << "PushFilterToPqTopicSource TCoVoid  " << Endl;
            auto compareArgsEvaluate = GetEvaluteListFromCompareNodes(flatmap.Lambda(), ctx);
            if(compareArgsEvaluate) {
                Cerr << "PushFilterToPqTopicSource compareArgsEvaluate  " << Endl;


               auto maybeOptionalIf = flatmap.Lambda().Body().Maybe<TCoOptionalIf>();
               if (!maybeOptionalIf.IsValid()) { // Nothing to push
                    return node;    // TODO
               }
            
                TCoOptionalIf optionalIf = maybeOptionalIf.Cast();
                TNodeOnNodeOwnedMap deepClones;
                auto predicateLambdaCopy = ctx.DeepCopy(flatmap.Lambda().Ref(), ctx, deepClones, true, true);
                
                if (maybeExtractMembers) {
                    return Build<TCoFlatMap>(ctx, flatmap.Pos())
                        .InitFrom(flatmap)
                        .Input<TCoExtractMembers>()
                            .InitFrom(maybeExtractMembers.Cast())
                            .Input<TDqSourceWrap>()
                                .InitFrom(dqSourceWrap)
                                .Input<TDqPqTopicSource>()
                                    .InitFrom(dqPqTopicSource)
                                    .Predicate(predicateLambdaCopy)
                                    .CompareArgsEvaluate(compareArgsEvaluate)
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
                            .Predicate(predicateLambdaCopy)
                            .CompareArgsEvaluate(compareArgsEvaluate)
                            .Build()
                        .Build()
                    .Done();
            }
        } else if (dqPqTopicSource.CompareArgsEvaluate().Raw()->IsList()) {
            auto list = dqPqTopicSource.CompareArgsEvaluate().Ptr();
            for (ui32 j = 0; j < list->ChildrenSize(); ++j) {
                if (list->Child(j)->IsCallable("EvaluateExpr")) {
                    return node;    // wait EvaluateExpr is calculated
                }
            }
        }

        Cerr << "PushFilterToPqTopicSource maybeLambda  " << Endl;

        auto maybeLambda = dqPqTopicSource.Predicate().Maybe<TCoLambda>();
        if (!maybeLambda) {
            maybeLambda = flatmap.Lambda();
        }

        Cerr << "PushFilterToPqTopicSource eplaceCompareNodes  " << Endl;
        auto list = dqPqTopicSource.CompareArgsEvaluate().Ptr();
        ReplaceCompareNodes(maybeLambda.Cast(), ctx, list);

        TString sharedReadingPridicateSerializedProto;
        if (UseSharedReadingForTopic(dqPqTopicSource)) {
            // Push predicate only if enabled shared reading, because this optimisation may produce double topic reading
            auto settings = TPushdownSettings();

            
            NPushdown::TPredicateNode sharedReadingPredicate = MakePushdownNode(flatmap.Lambda(), ctx, node.Pos(), settings);
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
        {
            auto settings = TPushdownSettings();
            settings.EnableMember("_yql_sys_partition_id");
            NPushdown::TPredicateNode partitionIdPredicate = MakePushdownNode(flatmap.Lambda(), ctx, node.Pos(), settings);
            if (!partitionIdPredicate.IsEmpty()) {
                auto lambdaArg = flatmap.Lambda().Args().Arg(0).Ptr();
                auto newFilterLambda = Build<TCoLambda>(ctx, node.Pos())
                    .Args({"_yql_sys_partition_id"})
                    .Body<TExprApplier>()
                        .Apply(partitionIdPredicate.ExprNode.Cast())
                        .With(TExprBase(lambdaArg), "_yql_sys_partition_id")
                        .Build()
                    .Done();

                isPartitionListUpdated = true;
                partitionList = ctx.Builder(node.Pos())
                    .Callable("EvaluateExpr")
                        .Callable(0, "Map")
                            .Callable(0, "Filter")
                                .Callable(0, "Map")
                                    .Callable(0, "Enumerate")
                                        .Callable(0, "Replicate")
                                            .Callable(0, "Int32")
                                                .Atom(0, "1")
                                            .Seal()
                                            .Callable(1, "Int32")
                                                .Atom(0, ToString(topicPartitionsCount))
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                    .Lambda(1)
                                        .Param("item")
                                        .Callable("AsStruct")
                                            .List(0)
                                                .Atom(0, "_yql_sys_partition_id")
                                                .Callable(1, "Nth")
                                                    .Arg(0, "item")
                                                    .Atom(1, "0", TNodeFlags::Default)
                                                .Seal()
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                                .Add(1, newFilterLambda.Ptr())
                            .Seal()
                            .Lambda(1)
                                .Param("item")
                                .Callable("Member")
                                    .Arg(0, "item")
                                    .Atom(1, "_yql_sys_partition_id", TNodeFlags::Default)
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();
            }
        }

        TString offsetPredicateSerializedProto = SerializePredicate("_yql_sys_offset", maybeLambda.Cast(), ctx);
        TString writeTimePredicateSerializedProto = SerializePredicate("_yql_sys_write_time", maybeLambda.Cast(), ctx);

        YQL_CLOG(INFO, ProviderPq) << "Build new TCoFlatMap with predicate";
        

        if (sharedReadingPridicateSerializedProto.empty()
            && !isPartitionListUpdated
            && offsetPredicateSerializedProto.empty()
            && writeTimePredicateSerializedProto.empty()) {
            return node;
        }

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

    TExprNode::TPtr GetEvaluteListFromCompareNodes(const TCoLambda& lambda, TExprContext& ctx) const {
        auto body = lambda.Body().Ptr();
        TExprNode::TListType evaluatedExprs;

        ForEachNonMemberExpressions(lambda, [&](const TExprNode::TPtr& expr) {
            auto evaluated = ctx.Builder(expr->Pos())
                .Callable("EvaluateExpr")
                    .Add(0, expr)
                .Seal()
                .Build();
            evaluatedExprs.push_back(std::move(evaluated));
        });
        if (evaluatedExprs.empty()) {
            return {};
        }
        return ctx.NewList(lambda.Pos(), std::move(evaluatedExprs));
    }

    void ReplaceCompareNodes(const TCoLambda& lambda, TExprContext& ctx, const TExprNode::TPtr& list) const {
        Y_UNUSED(ctx);
        auto body = lambda.Body().Ptr();
        ui32 index = 0;

        auto containsMember = [](const TExprNode::TPtr& root) -> bool {
            return !!FindNode(root, [](const TExprNode::TPtr& n) {
                return TCoMember::Match(n.Get());
            });
        };

        VisitExpr(body, [&](const TExprNode::TPtr& exprNode) {
            if (TCoCompare::Match(exprNode.Get())) {
                auto right = exprNode->ChildPtr(TCoCompare::idx_Right);
                auto left = exprNode->ChildPtr(TCoCompare::idx_Left);

                if (!containsMember(right) && index < list->ChildrenSize()) {
                    exprNode->ChildRef(TCoCompare::idx_Right) = list->ChildPtr(index);
                    ++index;
                }
                if (!containsMember(left) && index < list->ChildrenSize()) {
                    exprNode->ChildRef(TCoCompare::idx_Left) = list->ChildPtr(index);
                    ++index;
                }
                return false;
            }
            return true;
        });
    }

    void ForEachNonMemberExpressions(const TCoLambda& lambda, std::function<void(const TExprNode::TPtr&)> callback) const {
        auto body = lambda.Body().Ptr();
        auto containsMember = [](const TExprNode::TPtr& root) -> bool {
            return !!FindNode(root, [](const TExprNode::TPtr& n) {
                return TCoMember::Match(n.Get());
            });
        };

        VisitExpr(body, [&](const TExprNode::TPtr& exprNode) {
            if (TCoCompare::Match(exprNode.Get())) {
                auto compare = TCoCompare(exprNode.Get());
                auto left = compare.Left().Ptr();
                auto right = compare.Right().Ptr();

                if (!containsMember(right)) {
                    callback(right);
                }
                if (!containsMember(left)) {
                    callback(left);
                }
                return false; // don't descend into compare children
            }
            return true; // continue traversal
        });
    }

    TString SerializePredicate(
        const TString& memberName,
        const NNodes::TCoLambda& lambda,
         TExprContext& ctx) const {
        auto settings = TPushdownSettings();
        settings.EnableMember(memberName);
        settings.Enable(NPushdown::TSettings::EFeatureFlag::TimestampCtor); // TODO
        NPushdown::TPredicateNode predicate = MakePushdownNode(lambda, ctx, lambda.Pos(), settings);
        if (predicate.IsEmpty()) {
            return {};
        }
        TStringBuilder err;
        TDisjointIntervalTree<ui64> tree;
        if (!NYql::CalculateFilterPredicate(ctx, predicate.ExprNode.Cast(), lambda.Args().Arg(0), tree, err)) {
            ctx.AddWarning(TIssue(ctx.GetPosition(lambda.Pos()), "Failed to calculate filter predicate for source: " + err));
            Cerr << "Failed to calculate filter predicate  " << err << Endl;
            return {};
        }
        if (tree.Empty()) {
            NPq::NProto::TOffsetPredicate proto;
            auto* item = proto.AddItem();
            item->SetBegin(0);
            item->SetEnd(0);
            TString result; 
            YQL_ENSURE(proto.SerializeToString(&result));
            return result;
        }
        NPq::NProto::TOffsetPredicate proto;
        auto* item = proto.AddItem();
        item->SetBegin(tree.Min()); // Copy only one interval.
        item->SetEnd(tree.Max());
        TString result; 
        YQL_ENSURE(proto.SerializeToString(&result));
        return result;
    }

    TPqState::TPtr State_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreatePqLogicalOptProposalTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqLogicalOptProposalTransformer>(state);
}

} // namespace NYql
