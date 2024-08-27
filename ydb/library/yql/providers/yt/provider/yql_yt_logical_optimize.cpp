#include "yql_yt_provider_impl.h"
#include "yql_yt_helpers.h"
#include "yql_yt_table.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_join.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/core/yql_aggregate_expander.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_window.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_opt_match_recognize.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/disjoint_sets/disjoint_sets.h>

#include <util/generic/set.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>

#include <utility>

namespace NYql {

using namespace NNodes;

class TYtLogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TYtLogicalOptProposalTransformer(TYtState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYt, state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()))
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TYtLogicalOptProposalTransformer::name)
        AddHandler(0, &TYtMap::Match, HNDL(DirectRow));
        AddHandler(0, Names({TYtReduce::CallableName(), TYtMapReduce::CallableName()}), HNDL(IsKeySwitch));
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
        AddHandler(0, &TCoCalcOverWindowBase::Match, HNDL(CalcOverWindow));
        AddHandler(0, &TCoCalcOverWindowGroup::Match, HNDL(CalcOverWindow));
        AddHandler(0, &TCoSort::Match, HNDL(SortOverAlreadySorted<false>));
        AddHandler(0, &TCoTopSort::Match, HNDL(SortOverAlreadySorted<true>));
        AddHandler(0, &TCoNth::Match, HNDL(Demux));
        AddHandler(0, &TYtMap::Match, HNDL(VarianItemOverInput));
        AddHandler(0, &TYtWithUserJobsOpBase::Match, HNDL(VisitOverInputWithEqualLambdas));
        AddHandler(0, &TYtWithUserJobsOpBase::Match, HNDL(UnorderedOverInput));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(DirectRowInFlatMap));
        AddHandler(0, &TCoUnorderedBase::Match, HNDL(Unordered));
        AddHandler(0, &TCoAggregate::Match, HNDL(CountAggregate));
        AddHandler(0, &TYtReadTable::Match, HNDL(ZeroSampleToZeroLimit));
        AddHandler(0, &TCoMatchRecognize::Match, HNDL(MatchRecognize));

        AddHandler(1, &TCoFilterNullMembers::Match, HNDL(FilterNullMemebers<TCoFilterNullMembers>));
        AddHandler(1, &TCoSkipNullMembers::Match, HNDL(FilterNullMemebers<TCoSkipNullMembers>));
        AddHandler(1, &TCoFlatMapBase::Match, HNDL(FuseFlatmaps));
        AddHandler(1, &TCoZip::Match, HNDL(Zip));
        AddHandler(1, &TCoZipAll::Match, HNDL(ZipAll));
        AddHandler(1, &TYtWithUserJobsOpBase::Match, HNDL(OutputInFlatMap));
        AddHandler(1, &TYtOutput::Match, HNDL(BypassCopy));
        AddHandler(1, &TCoAggregateBase::Match, HNDL(Aggregate));
        AddHandler(1, &TCoExtractMembers::Match, HNDL(ExtractMembers));
        AddHandler(1, &TCoExtractMembers::Match, HNDL(ExtractMembersOverContent));
        AddHandler(1, &TCoRight::Match, HNDL(PushdownReadColumns));
        AddHandler(1, &TYtTransientOpBase::Match, HNDL(PushdownOpColumns));
        AddHandler(1, &TCoCountBase::Match, HNDL(TakeOrSkip));
        AddHandler(1, &TCoEquiJoin::Match, HNDL(SelfInnerJoinWithSameKeys));
        AddHandler(1, &TCoExtendBase::Match, HNDL(ExtendOverSameMap));
        AddHandler(1, &TCoFlatMapBase::Match, HNDL(FlatMapOverExtend));
        AddHandler(1, &TCoTake::Match, HNDL(TakeOverExtend));

        AddHandler(2, &TCoEquiJoin::Match, HNDL(ConvertToCommonTypeForForcedMergeJoin));
        AddHandler(2, &TCoShuffleByKeys::Match, HNDL(ShuffleByKeys));
#undef HNDL
    }

protected:
    TYtSection PushdownSectionColumns(TYtSection section, TExprContext& ctx, const TGetParents& getParents) const {
        if (HasNonEmptyKeyFilter(section)) {
            // wait until key filter values are calculated and pushed to Path/Ranges
            return section;
        }
        bool hasNewPath = false;
        TVector<TExprBase> paths;
        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        for (auto path: section.Paths()) {
            paths.push_back(path);

            auto columns = TYtColumnsInfo(path.Columns());
            if (!columns.HasColumns()) {
                // No column filter
                continue;
            }

            auto type = path.Table().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            if (type->GetSize() <= columns.GetColumns()->size()) {
                // The same column set as original type
                continue;
            }

            // Use operation output only
            if (auto op = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtTransientOpBase>()) {
                if (op.Raw()->StartsExecution() || op.Raw()->HasResult()) {
                    // Operation is already executed
                    continue;
                }

                if (IsOutputUsedMultipleTimes(op.Cast().Ref(), *getParents())) {
                    // Operation output is used multiple times
                    continue;
                }

                if (op.Cast().Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]->Cast<TListExprType>()->GetItemType()->GetKind() == ETypeAnnotationKind::Variant) {
                    // Operation with multi-output
                    continue;
                }

                if (NYql::HasSetting(op.Settings().Ref(), EYtSettingType::SortLimitBy)) {
                    continue;
                }

                const TYqlRowSpecInfo::TPtr rowSpec = TYtTableBaseInfo::GetRowSpec(path.Table());
                YQL_ENSURE(rowSpec);

                if (rowSpec->HasAuxColumns()) {
                    continue;
                }

                TSet<TString> effectiveColumns;
                bool keepColumns = columns.GetRenames().Defined();
                for (auto& column : columns.GetColumns().GetRef()) {
                    keepColumns = keepColumns || !column.Type.empty();
                    effectiveColumns.insert(column.Name);
                }

                if (NYql::HasSetting(op.Settings().Ref(), EYtSettingType::KeepSorted)) {
                    for (size_t i = 0; i < rowSpec->SortedBy.size(); ++i) {
                        const bool inserted = effectiveColumns.insert(rowSpec->SortedBy[i]).second;
                        keepColumns = keepColumns || inserted;
                    }
                }

                if (!path.Ranges().Maybe<TCoVoid>()) {
                    // add columns which are implicitly used by path.Ranges(), but not included in path.Columns();
                    const auto ranges = TYtRangesInfo(path.Ranges());
                    const size_t usedKeyPrefix = ranges.GetUsedKeyPrefixLength();
                    YQL_ENSURE(usedKeyPrefix <= rowSpec->SortedBy.size());
                    for (size_t i = 0; i < usedKeyPrefix; ++i) {
                        const bool inserted = effectiveColumns.insert(rowSpec->SortedBy[i]).second;
                        keepColumns = keepColumns || inserted;
                    }
                }

                if (type->GetSize() <= effectiveColumns.size()) {
                    // The same column set as original type
                    continue;
                }

                if (auto maybeMap = op.Maybe<TYtMap>()) {
                    TYtMap map = maybeMap.Cast();
                    TVector<const TItemExprType*> structItems;

                    auto mapper = ctx.Builder(map.Mapper().Pos())
                        .Lambda()
                            .Param("stream")
                            .Callable(NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Ordered) ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                                .Apply(0, map.Mapper().Ref())
                                    .With(0, "stream")
                                .Seal()
                                .Lambda(1)
                                    .Param("item")
                                    .Callable(TCoJust::CallableName())
                                        .Callable(0, TCoAsStruct::CallableName())
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                size_t index = 0;
                                                for (const auto& column: effectiveColumns) {
                                                    auto pos = type->FindItem(column);
                                                    YQL_ENSURE(pos);
                                                    structItems.push_back(type->GetItems()[*pos]);
                                                    parent
                                                        .List(index++)
                                                            .Atom(0, column)
                                                            .Callable(1, TCoMember::CallableName())
                                                                .Arg(0, "item")
                                                                .Atom(1, column)
                                                            .Seal()
                                                        .Seal();
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();

                    auto outStructType = ctx.MakeType<TStructExprType>(structItems);
                    TYtOutTableInfo mapOut(outStructType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);

                    if (ctx.IsConstraintEnabled<TSortedConstraintNode>()) {
                        if (const auto s = path.Table().Ref().GetConstraint<TSortedConstraintNode>()) {
                            if (const auto sorted = s->FilterFields(ctx, [outStructType](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && outStructType->FindItem(path.front()); }) ) {
                                TKeySelectorBuilder builder(map.Mapper().Pos(), ctx, useNativeDescSort, outStructType);
                                builder.ProcessConstraint(*sorted);
                                builder.FillRowSpecSort(*mapOut.RowSpec);

                                if (builder.NeedMap()) {
                                    mapper = ctx.Builder(map.Mapper().Pos())
                                        .Lambda()
                                            .Param("stream")
                                            .Apply(builder.MakeRemapLambda(true))
                                                .With(0)
                                                    .Apply(*mapper)
                                                        .With(0, "stream")
                                                    .Seal()
                                                .Done()
                                            .Seal()
                                        .Seal()
                                        .Build();
                                }
                            }
                        }
                    } else {
                        mapOut.RowSpec->CopySortness(ctx, TYqlRowSpecInfo(map.Output().Item(0).RowSpec()));
                    }
                    mapOut.SetUnique(path.Ref().GetConstraint<TDistinctConstraintNode>(), map.Mapper().Pos(), ctx);

                    TExprBase newColumns = Build<TCoVoid>(ctx, path.Pos()).Done();
                    if (keepColumns) {
                        newColumns = path.Columns();
                    }
                    hasNewPath = true;
                    paths.back() = Build<TYtPath>(ctx, path.Pos())
                        .InitFrom(path)
                        .Table<TYtOutput>()
                            .Operation<TYtMap>()
                                .InitFrom(map)
                                .Output()
                                    .Add(mapOut.ToExprNode(ctx, map.Pos()).Cast<TYtOutTable>())
                                .Build()
                                .Mapper(mapper)
                            .Build()
                            .OutIndex().Value(TStringBuf("0")).Build()
                            .Mode(path.Table().Cast<TYtOutput>().Mode())
                        .Build()
                        .Columns(newColumns)
                        .Stat<TCoVoid>().Build()
                        .Done();
                }
                else if (auto maybeMerge = op.Maybe<TYtMerge>()) {
                    TYtMerge merge = maybeMerge.Cast();

                    auto prevRowSpec = TYqlRowSpecInfo(merge.Output().Item(0).RowSpec());
                    auto prevOutType = merge.Output().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                    TVector<const TItemExprType*> structItems;
                    for (const auto& column: effectiveColumns) {
                        auto pos = prevOutType->FindItem(column);
                        YQL_ENSURE(pos);
                        structItems.push_back(prevOutType->GetItems()[*pos]);
                    }

                    TYtOutTableInfo mergeOut(ctx.MakeType<TStructExprType>(structItems), prevRowSpec.GetNativeYtTypeFlags());
                    mergeOut.RowSpec->CopySortness(ctx, prevRowSpec, TYqlRowSpecInfo::ECopySort::WithDesc);
                    if (auto nativeType = prevRowSpec.GetNativeYtType()) {
                        mergeOut.RowSpec->CopyTypeOrders(*nativeType);
                    }
                    mergeOut.SetUnique(path.Ref().GetConstraint<TDistinctConstraintNode>(), merge.Pos(), ctx);

                    TSet<TStringBuf> columnSet(effectiveColumns.begin(), effectiveColumns.end());
                    if (mergeOut.RowSpec->HasAuxColumns()) {
                        for (auto item: mergeOut.RowSpec->GetAuxColumns()) {
                            columnSet.insert(item.first);
                        }
                    }
                    TExprBase newColumns = Build<TCoVoid>(ctx, path.Pos()).Done();
                    if (keepColumns) {
                        newColumns = path.Columns();
                    }

                    hasNewPath = true;
                    paths.back() = Build<TYtPath>(ctx, path.Pos())
                        .InitFrom(path)
                        .Table<TYtOutput>()
                            .Operation<TYtMerge>()
                                .InitFrom(merge)
                                .Input()
                                    .Add(UpdateInputFields(merge.Input().Item(0), std::move(columnSet), ctx, false))
                                .Build()
                                .Output()
                                    .Add(mergeOut.ToExprNode(ctx, merge.Pos()).Cast<TYtOutTable>())
                                .Build()
                            .Build()
                            .OutIndex().Value(TStringBuf("0")).Build()
                            .Mode(path.Table().Cast<TYtOutput>().Mode())
                        .Build()
                        .Columns(newColumns)
                        .Stat<TCoVoid>().Build()
                        .Done();
                }
            }
        }
        if (hasNewPath) {
            return Build<TYtSection>(ctx, section.Pos())
                .InitFrom(section)
                .Paths()
                    .Add(paths)
                .Build()
                .Done();
        }
        return section;
    }

protected:
    TMaybeNode<TExprBase> Aggregate(TExprBase node, TExprContext& ctx) const {
        auto aggregate = node.Cast<TCoAggregateBase>();

        auto input = aggregate.Input();
        if (!IsYtProviderInput(input)) {
            return node;
        }

        auto cluster = TString{GetClusterName(input)};
        TSyncMap syncList;

        for (auto handler: aggregate.Handlers()) {
            auto trait = handler.Trait();
            if (auto maybeAggTrait = trait.Maybe<TCoAggregationTraits>()) {
                const auto& t = maybeAggTrait.Cast();
                TVector<TExprBase> lambdas = {
                    t.InitHandler(),
                    t.UpdateHandler(),
                    t.SaveHandler(),
                    t.LoadHandler(),
                    t.MergeHandler(),
                    t.FinishHandler(),
                };
                for (auto lambda : lambdas) {
                    if (!IsYtCompleteIsolatedLambda(lambda.Ref(), syncList, cluster, false)) {
                        return node;
                    }
                }
            } else if (trait.Ref().IsCallable("AggApply")) {
                if (!IsYtCompleteIsolatedLambda(*trait.Ref().Child(2), syncList, cluster, false)) {
                    return node;
                }
            }
        }

        auto usePhases = State_->Configuration->UseAggPhases.Get().GetOrElse(false);
        auto usePartitionsByKeys = State_->Configuration->UsePartitionsByKeysForFinalAgg.Get().GetOrElse(true);
        TAggregateExpander aggExpander(usePartitionsByKeys, false, node.Ptr(), ctx, *State_->Types, false, false, usePhases);
        return aggExpander.ExpandAggregate();
    }

    TMaybeNode<TExprBase> DirectRow(TExprBase node, TExprContext& ctx) const {
        if (State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
            return node;
        }

        auto map = node.Cast<TYtMap>();

        auto mapper = map.Mapper();
        auto maybeLambda = GetFlatMapOverInputStream(mapper).Lambda();
        if (!maybeLambda) {
            return node;
        }

        TCoLambda lambda = maybeLambda.Cast();
        auto arg = lambda.Args().Arg(0);

        TNodeSet nodesToOptimize;
        TProcessedNodesSet processedNodes;
        processedNodes.insert(map.World().Ref().UniqueId());
        VisitExpr(lambda.Ptr(), [&nodesToOptimize, &processedNodes, arg](const TExprNode::TPtr& node) {
            if (TCoTablePath::Match(node.Get())) {
                if (node->ChildrenSize() == 0 || node->Child(0)->Child(0) == arg.Raw()) {
                    nodesToOptimize.insert(node.Get());
                }
            }
            else if (TCoTableRecord::Match(node.Get())) {
                if (node->ChildrenSize() == 0 || node->Child(0)->Child(0) == arg.Raw()) {
                    nodesToOptimize.insert(node.Get());
                }
            }
            else if (TYtOutput::Match(node.Get())) {
                // Stop traversing dependent operations
                processedNodes.insert(node->UniqueId());
                return false;
            }
            return true;
        });
        if (nodesToOptimize.empty()) {
            return node;
        }

        TExprNode::TPtr newBody = lambda.Body().Ptr();
        TPositionHandle tablePos;
        TOptimizeExprSettings settings(State_->Types);
        settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
        auto status = OptimizeExpr(newBody, newBody, [&nodesToOptimize, &tablePos, arg](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
            if (nodesToOptimize.find(input.Get()) != nodesToOptimize.end()) {
                if (TCoTablePath::Match(input.Get())) {
                    tablePos = input->Pos();
                    if (input->ChildrenSize() == 1) {
                        return ctx.RenameNode(*input, TYtTablePath::CallableName());
                    }
                    return Build<TYtTablePath>(ctx, input->Pos())
                        .DependsOn()
                            .Input(arg)
                        .Build()
                        .Done().Ptr();
                }
                else if (TCoTableRecord::Match(input.Get())) {
                    tablePos = input->Pos();
                    if (input->ChildrenSize() == 1) {
                        return ctx.RenameNode(*input, TYtTableRecord::CallableName());
                    }
                    return Build<TYtTableRecord>(ctx, input->Pos())
                        .DependsOn()
                            .Input(arg)
                        .Build()
                        .Done().Ptr();
                }
            }

            return input;
        }, ctx, settings);

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return {};
        }

        if (status.Level == IGraphTransformer::TStatus::Ok) {
            return node;
        }

        auto newLambda = Build<TCoLambda>(ctx, lambda.Pos())
            .Args({TStringBuf("row")})
            .Body<TExprApplier>()
                .Apply(TExprBase(newBody))
                .With(arg, TStringBuf("row"))
            .Build()
            .Done();

        auto newMapper = Build<TCoLambda>(ctx, mapper.Pos())
            .Args({TStringBuf("stream")})
            .Body<TExprApplier>()
                .Apply(mapper)
                .With(mapper.Args().Arg(0), TStringBuf("stream"))
                .With(lambda, newLambda)
            .Build()
            .Done();

        bool stop = false;
        for (auto section: map.Input()) {
            for (auto path: section.Paths()) {
                if (path.Table().Maybe<TYtOutput>()) {
                    auto issue = TIssue(ctx.GetPosition(tablePos), "TablePath(), TableName() and TableRecordIndex() will be empty for temporary tables.\n"
                        "Please consult documentation https://yql.yandex-team.ru/docs/yt/builtins/basic#tablepath for possible workaround");
                    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YT_TABLE_PATH_RECORD_FOR_TMP, issue);
                    if (!ctx.AddWarning(issue)) {
                        return nullptr;
                    }
                    stop = true;
                    break;
                }
            }
            if (stop) {
                break;
            }
        }

        return ctx.ChangeChild(node.Ref(), TYtMap::idx_Mapper, newMapper.Ptr());
    }

    TMaybeNode<TExprBase> IsKeySwitch(TExprBase node, TExprContext& ctx) const {
        if (State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
            return node;
        }

        auto lambda = node.Maybe<TYtMapReduce>() ? node.Cast<TYtMapReduce>().Reducer() : node.Cast<TYtReduce>().Reducer();
        TNodeSet nodesToOptimize;
        TProcessedNodesSet processedNodes;
        processedNodes.insert(node.Cast<TYtWithUserJobsOpBase>().World().Ref().UniqueId());
        VisitExpr(lambda.Ptr(), [&nodesToOptimize, &processedNodes](const TExprNode::TPtr& node) {
            if (TCoIsKeySwitch::Match(node.Get())) {
                nodesToOptimize.insert(node.Get());
            }
            else if (TYtOutput::Match(node.Get())) {
                // Stop traversing dependent operations
                processedNodes.insert(node->UniqueId());
                return false;
            }
            return true;
        });

        if (nodesToOptimize.empty()) {
            return node;
        }

        TExprNode::TPtr newBody = lambda.Body().Ptr();
        TPosition tablePos;
        TOptimizeExprSettings settings(State_->Types);
        settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
        TStatus status = OptimizeExpr(newBody, newBody, [&](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
            if (nodesToOptimize.find(input.Get()) != nodesToOptimize.end()) {
                if (TCoIsKeySwitch::Match(input.Get())) {
                    return
                        Build<TYtIsKeySwitch>(ctx, input->Pos())
                            .DependsOn()
                                .Input(input->HeadPtr())
                            .Build()
                        .Done().Ptr();
                }
            }
            return input;
        }, ctx, settings);

        if (status.Level == TStatus::Error) {
            return {};
        }

        if (status.Level == TStatus::Ok) {
            return node;
        }

        auto newLambda = Build<TCoLambda>(ctx, lambda.Pos())
            .Args({TStringBuf("stream")})
            .Body<TExprApplier>()
                .Apply(TExprBase(newBody))
                .With(lambda.Args().Arg(0), TStringBuf("stream"))
            .Build()
            .Done();

        return ctx.ChangeChild(node.Ref(), node.Maybe<TYtMapReduce>() ? TYtMapReduce::idx_Reducer : TYtReduce::idx_Reducer, newLambda.Ptr());
    }

    template <typename T>
    TMaybeNode<TExprBase> FilterNullMemebers(TExprBase node, TExprContext& ctx) const {
        auto filterNullMembers = node.Cast<T>();
        if (!IsYtProviderInput(filterNullMembers.Input())) {
            return node;
        }

        YQL_ENSURE(filterNullMembers.Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List);

        return Build<TCoOrderedFlatMap>(ctx, filterNullMembers.Pos())
            .Input(filterNullMembers.Input())
            .Lambda()
                .Args({"item"})
                .template Body<T>()
                    .template Input<TCoJust>()
                        .Input("item")
                        .Build()
                    .Members(filterNullMembers.Members())
                    .Build()
                .Build()
            .Done();
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        auto maybeRead = node.Cast<TCoLeft>().Input().Maybe<TYtReadTable>();
        if (!maybeRead) {
            return node;
        }

        auto read = maybeRead.Cast();
        TExprNode::TListType worlds(1, read.World().Ptr());
        for (auto section: read.Input()) {
            for (auto path: section.Paths()) {
                if (auto out = path.Table().Maybe<TYtOutput>()) {
                    worlds.push_back(
                        Build<TCoLeft>(ctx, node.Pos())
                            .Input(out.Cast().Operation())
                            .Done().Ptr()
                    );
                }
            }
        }

        return TExprBase(worlds.size() == 1 ? worlds.front() : ctx.NewCallable(node.Pos(), TCoSync::CallableName(), std::move(worlds)));
    }

    TMaybeNode<TExprBase> CalcOverWindow(TExprBase node, TExprContext& ctx) const {
        auto list = node.Cast<TCoInputBase>().Input();
        if (!IsYtProviderInput(list)) {
            return node;
        }

        TVector<TYtTableBaseInfo::TPtr> tableInfos = GetInputTableInfos(list);
        if (AllOf(tableInfos, [](const TYtTableBaseInfo::TPtr& info) { return !info->Meta->IsDynamic; })) {
            TExprNodeList calcs = ExtractCalcsOverWindow(node.Ptr(), ctx);
            TSet<TStringBuf> rowNumberCols;
            for (auto& calcNode : calcs) {
                TCoCalcOverWindowTuple calc(calcNode);
                if (calc.Keys().Size() != 0 || !calc.SessionSpec().Maybe<TCoVoid>() || !calc.SortSpec().Maybe<TCoVoid>()) {
                    continue;
                }
                bool needOptimize = false;
                auto frames = calc.Frames().Ref().ChildrenList();
                for (const auto& win : frames) {
                    YQL_ENSURE(TCoWinOnBase::Match(win.Get()));
                    auto args = win->ChildrenList();
                    needOptimize = args.size() > 1 &&
                        // We rewrite RowNumber() into YtMap if it is the only window function for some frame
                        // (hence AllOf is used below)
                        // If the frame consist of RowNumber() and other window functions, we just calculate RowNumber() along with them
                        AllOf(args.begin() + 1, args.end(),
                        [](const auto& arg) {
                            return arg->Child(1)->IsCallable("RowNumber");
                        });
                    if (needOptimize) {
                        break;
                    }
                }

                if (!needOptimize) {
                    continue;
                }

                for (auto& win : frames) {
                    YQL_ENSURE(TCoWinOnBase::Match(win.Get()));
                    auto winOnArgs = win->ChildrenList();

                    TExprNodeList newWinOnArgs;
                    newWinOnArgs.push_back(std::move(winOnArgs[0]));

                    for (size_t i = 1; i < winOnArgs.size(); ++i) {
                        auto labelAtom = winOnArgs[i]->Child(0);
                        YQL_ENSURE(labelAtom->IsAtom());
                        auto trait = winOnArgs[i]->Child(1);

                        if (trait->IsCallable("RowNumber")) {
                            rowNumberCols.insert(labelAtom->Content());
                        } else {
                            newWinOnArgs.push_back(std::move(winOnArgs[i]));
                        }
                    }

                    win = ctx.ChangeChildren(*win, std::move(newWinOnArgs));
                }

                calcNode = Build<TCoCalcOverWindowTuple>(ctx, calc.Pos())
                    .Keys(calc.Keys())
                    .SortSpec(calc.SortSpec())
                    .Frames(ctx.NewList(calc.Frames().Pos(), std::move(frames)))
                    .SessionSpec(calc.SessionSpec())
                    .SessionColumns(calc.SessionColumns())
                    .Done().Ptr();
            }

            if (rowNumberCols) {
                auto rowArg = ctx.NewArgument(node.Pos(), "row");
                auto body = rowArg;

                const bool useSysColumns = State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS);
                const auto sysColumnNum = TString(YqlSysColumnPrefix).append("num");
                if (useSysColumns) {
                    for (auto& col : rowNumberCols) {
                        body = ctx.Builder(node.Pos())
                            .Callable("AddMember")
                                .Add(0, body)
                                .Atom(1, col)
                                .Callable(2, "Member")
                                    .Add(0, rowArg)
                                    .Atom(1, sysColumnNum)
                                .Seal()
                            .Seal()
                            .Build();
                    }
                    body = ctx.Builder(node.Pos())
                        .Callable("ForceRemoveMember")
                            .Add(0, body)
                            .Atom(1, sysColumnNum)
                        .Seal()
                        .Build();

                    auto settings = Build<TCoNameValueTupleList>(ctx, list.Pos())
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::SysColumns))
                            .Build()
                            .Value(ToAtomList(TVector<TStringBuf>{"num"}, list.Pos(), ctx))
                        .Build()
                        .Done();

                    if (auto right = list.Maybe<TCoRight>()) {
                        list = right.Cast().Input();
                    }

                    list = Build<TCoRight>(ctx, list.Pos())
                        .Input(ConvertContentInputToRead(list, settings, ctx))
                        .Done();
                } else {
                    for (auto& col : rowNumberCols) {
                        body = ctx.Builder(node.Pos())
                            .Callable("AddMember")
                                .Add(0, body)
                                .Atom(1, col)
                                .Callable(2, "YtRowNumber")
                                    .Callable(0, "DependsOn")
                                        .Add(0, rowArg)
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Build();
                    }
                }

                auto input = ctx.Builder(node.Pos())
                    .Callable("OrderedMap")
                        .Add(0, list.Ptr())
                        .Add(1, ctx.NewLambda(node.Pos(), ctx.NewArguments(node.Pos(), {rowArg}), std::move(body)))
                    .Seal()
                    .Build();

                YQL_CLOG(INFO, ProviderYt) << "Replaced " << rowNumberCols.size() << " RowNumber()s with " << (useSysColumns ? sysColumnNum : TString("YtRowNumber()"));

                return RebuildCalcOverWindowGroup(node.Pos(), input, calcs, ctx);
            }
        }

        return ExpandCalcOverWindow(node.Ptr(), ctx, *State_->Types);
    }

    template<bool IsTop>
    TMaybeNode<TExprBase> SortOverAlreadySorted(TExprBase node, TExprContext& ctx) const {
        const auto sort = node.Cast<std::conditional_t<IsTop, TCoTopBase, TCoSort>>();

        if (!IsConstExpSortDirections(sort.SortDirections())) {
            return node;
        }

        auto list = sort.Input();
        if (!IsYtProviderInput(list)) {
            return node;
        }

        TVector<TYtPathInfo::TPtr> pathInfos = GetInputPaths(list);
        if (pathInfos.size() > 1) {
            return node;
        }
        TYtPathInfo::TPtr pathInfo = pathInfos.front();
        if (pathInfo->Columns || pathInfo->Ranges || !pathInfo->Table->RowSpec || !pathInfo->Table->RowSpec->IsSorted()) {
            return node;
        }
        const TStructExprType* itemType = nullptr;
        if (auto type = GetSequenceItemType(node, false, ctx)) {
            itemType = type->Cast<TStructExprType>();
        } else {
            return {};
        }

        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        TKeySelectorBuilder builder(node.Pos(), ctx, useNativeDescSort, itemType);
        builder.ProcessKeySelector(sort.KeySelectorLambda().Ptr(), sort.SortDirections().Ptr());

        if (builder.Members().size() < builder.Columns().size()) {
            return node;
        }

        auto rowSpec = pathInfo->Table->RowSpec;
        for (size_t i = 0; i < builder.Members().size(); ++i) {
            if (i >= rowSpec->SortMembers.size()) {
                return node;
            }

            if (rowSpec->SortMembers[i] != builder.Members()[i]) {
                return node;
            }

            if (!rowSpec->SortDirections[i] || !builder.SortDirections()[i]) {
                return node;
            }
        }

        if constexpr (IsTop)
            return Build<TCoTake>(ctx, sort.Pos())
                .Input(list)
                .Count(sort.Count())
                .Done();
        else
            return list;
    }

    TMaybeNode<TExprBase> Demux(TExprBase node, TExprContext& ctx) const {
        auto nth = node.Cast<TCoNth>();
        auto input = nth.Tuple().Maybe<TCoDemux>().Input().Maybe<TCoRight>().Input();
        if (!input) {
            return node;
        }

        if (auto op = input.Maybe<TYtOutputOpBase>()) {
            return Build<TYtOutput>(ctx, node.Pos())
                .Operation(op.Cast())
                .OutIndex(nth.Index())
                .Done();
        }
        if (auto maybeRead = input.Maybe<TYtReadTable>()) {
            auto read = maybeRead.Cast();
            auto ndx = FromString<size_t>(nth.Index().Value());
            YQL_ENSURE(ndx < read.Input().Size());
            return Build<TCoRight>(ctx, node.Pos())
                .Input<TYtReadTable>()
                    .InitFrom(read)
                    .Input()
                        .Add(read.Input().Item(ndx))
                    .Build()
                .Build()
                .Done();
        }
        return node;

    }

    TMaybeNode<TExprBase> VarianItemOverInput(TExprBase node, TExprContext& ctx) const {
        auto map = node.Cast<TYtMap>();
        if (map.Input().Size() == 1) {
            return node;
        }

        // All sections should have equal settings
        const TExprNode* sectionSettings = nullptr;
        for (auto section: map.Input()) {
            if (nullptr == sectionSettings) {
                sectionSettings = section.Settings().Raw();
            } else if (sectionSettings != section.Settings().Raw()) {
                return node;
            }
        }

        auto mapper = map.Mapper();

        TParentsMap parentsMap;
        GatherParents(mapper.Body().Ref(), parentsMap);

        auto maybeLambda = GetFlatMapOverInputStream(mapper, parentsMap).Lambda();
        if (!maybeLambda) {
            return node;
        }

        TCoLambda lambda = maybeLambda.Cast();
        auto arg = lambda.Args().Arg(0);

        // Check arg is used only in VariantItem
        auto it = parentsMap.find(arg.Raw());
        if (it == parentsMap.cend() || it->second.size() != 1 || !TCoVariantItem::Match(*it->second.begin())) {
            return node;
        }

        // Substitute VariantItem(arg) by arg
        TNodeOnNodeOwnedMap nodesToOptimize;
        nodesToOptimize.emplace(*it->second.begin(), arg.Ptr());
        TExprNode::TPtr newMapper;
        auto status = RemapExpr(mapper.Ptr(), newMapper, nodesToOptimize, ctx, TOptimizeExprSettings{nullptr});

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return {};
        }

        // Join all paths to single section
        auto newPaths = Build<TYtPathList>(ctx, map.Input().Pos());
        for (auto section: map.Input()) {
            newPaths.Add(section.Paths().Ref().ChildrenList());
        }

        return Build<TYtMap>(ctx, map.Pos())
            .InitFrom(map)
            .Input()
                .Add()
                    .Paths(newPaths.Done())
                    .Settings(map.Input().Item(0).Settings())
                .Build()
            .Build()
            .Mapper(TCoLambda(newMapper))
            .Done();
    }

    TMaybeNode<TExprBase> VisitOverInputWithEqualLambdas(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();
        if (op.Input().Size() == 1) {
            return node;
        }

        if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoVoid>()) {
            return node;
        }

        size_t lambdaIdx = op.Maybe<TYtMapReduce>()
            ? TYtMapReduce::idx_Mapper
            : op.Maybe<TYtReduce>()
                ? TYtReduce::idx_Reducer
                : TYtMap::idx_Mapper;

        auto opLambda = TCoLambda(op.Ref().ChildPtr(lambdaIdx));

        TParentsMap parentsMap;
        GatherParents(opLambda.Body().Ref(), parentsMap);

        auto maybeLambda = GetFlatMapOverInputStream(opLambda, parentsMap).Lambda();
        if (!maybeLambda) {
            return node;
        }

        TCoLambda lambda = maybeLambda.Cast();
        auto arg = lambda.Args().Arg(0);

        // Check arg is used only in Visit
        auto it = parentsMap.find(arg.Raw());
        if (it == parentsMap.cend() || it->second.size() != 1 || !TCoVisit::Match(*it->second.begin())) {
            return node;
        }

        using TGroupKey = std::pair<const TExprNode*, const TExprNode*>; // lambda, section settings
        TMap<TGroupKey, TVector<size_t>> groupedInputs; // key -> {section id}
        TVector<TExprNode::TPtr> visitLambdas;
        visitLambdas.resize(op.Input().Size());

        const TExprNode* visit = *it->second.begin();
        for (ui32 index = 1; index < visit->ChildrenSize(); ++index) {
            if (visit->Child(index)->IsAtom()) {
                size_t inputNum = FromString<size_t>(visit->Child(index)->Content());
                YQL_ENSURE(inputNum < op.Input().Size());

                ++index;

                groupedInputs[std::make_pair(visit->Child(index), op.Input().Item(inputNum).Settings().Raw())].push_back(inputNum);
                visitLambdas[inputNum] = visit->ChildPtr(index);
            }
        }

        if (groupedInputs.empty() || AllOf(groupedInputs, [](const decltype(groupedInputs)::value_type& grp) { return grp.second.size() <= 1; })) {
            return node;
        }

        TVector<TVector<size_t>> groups;
        groups.reserve(groupedInputs.size());
        for (auto& grp: groupedInputs) {
            groups.push_back(std::move(grp.second));
        }
        ::Sort(groups.begin(), groups.end(),
            [] (const decltype(groups)::value_type& v1, const decltype(groups)::value_type& v2) {
                return v1.front() < v2.front();
            }
        );

        // Rebuild input
        TVector<TYtSection> newSections;
        for (auto& grp: groups) {
            TVector<TYtPath> paths;
            for (size_t sectionNum: grp) {
                auto oldSection = op.Input().Item(sectionNum);
                paths.insert(paths.end(), oldSection.Paths().begin(), oldSection.Paths().end());
            }
            auto firstOldSection = op.Input().Item(grp.front());
            newSections.push_back(Build<TYtSection>(ctx, firstOldSection.Pos())
                .InitFrom(firstOldSection)
                .Paths()
                    .Add(paths)
                .Build()
                .Done());
        }

        // Rebuild lambda
        TExprNode::TPtr newVisit = ctx.Builder(visit->Pos())
            .Callable(TCoVisit::CallableName())
                .Add(0, visit->ChildPtr(0))
                .Do([&](TExprNodeBuilder& builder) -> TExprNodeBuilder& {
                    for (size_t i = 0; i < groups.size(); ++i) {
                        builder.Atom(i * 2 + 1, ToString(i));
                        builder.Add(i * 2 + 2, visitLambdas.at(groups[i].front()));
                    }
                    if (visit->ChildrenSize() % 2 == 0) { // has default value
                        builder.Add(groups.size() * 2 + 1, visit->TailPtr());
                    }
                    return builder;
                })
            .Seal()
            .Build();
        TNodeOnNodeOwnedMap nodesToOptimize;
        nodesToOptimize.emplace(visit, newVisit);
        TExprNode::TPtr newOpLambda;
        auto status = RemapExpr(opLambda.Ptr(), newOpLambda, nodesToOptimize, ctx, TOptimizeExprSettings{nullptr});
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return {};
        }

        auto res = ctx.ChangeChild(op.Ref(), TYtWithUserJobsOpBase::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(newSections)
            .Done().Ptr());

        res = ctx.ChangeChild(*res, lambdaIdx, std::move(newOpLambda));
        return TExprBase(res);
    }

    TMaybeNode<TExprBase> UnorderedOverInput(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();

        if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoVoid>()) {
            return node;
        }

        size_t lambdaIdx = op.Maybe<TYtMapReduce>()
            ? TYtMapReduce::idx_Mapper
            : op.Maybe<TYtReduce>()
                ? TYtReduce::idx_Reducer
                : TYtMap::idx_Mapper;

        auto opLambda = TCoLambda(op.Ref().ChildPtr(lambdaIdx));
        auto arg = opLambda.Args().Arg(0);

        TParentsMap parentsMap;
        GatherParents(opLambda.Body().Ref(), parentsMap);

        auto it = parentsMap.find(arg.Raw());
        if (it == parentsMap.cend()) {
            return node;
        }

        // Substitute Unordered(arg) by arg
        TNodeOnNodeOwnedMap nodesToOptimize;
        for (auto n: it->second) {
            if (TCoUnordered::Match(n)) {
                nodesToOptimize.emplace(n, arg.Ptr());
            } else if (!TCoDependsOn::Match(n)) {
                return node;
            }
        }
        if (nodesToOptimize.empty()) {
            return node;
        }

        TExprNode::TPtr newOpLambda;
        auto status = RemapExpr(opLambda.Ptr(), newOpLambda, nodesToOptimize, ctx, TOptimizeExprSettings{nullptr});
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return {};
        }

        auto res = ctx.ChangeChild(op.Ref(), lambdaIdx, std::move(newOpLambda));

        TVector<TYtSection> updatedSections;
        for (auto section: op.Input()) {
            updatedSections.push_back(MakeUnorderedSection(section, ctx));
        }
        res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(updatedSections)
            .Done().Ptr());

        if (NYql::HasSetting(op.Settings().Ref(), EYtSettingType::Ordered)) {
            res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Settings, NYql::RemoveSettings(op.Settings().Ref(), EYtSettingType::Ordered, ctx));
        }

        return TExprBase(res);
    }

    TMaybeNode<TExprBase> FuseFlatmaps(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto outerFlatmap = node.Cast<TCoFlatMapBase>();
        if (!outerFlatmap.Input().Maybe<TCoFlatMapBase>()) {
            return node;
        }

        auto innerFlatmap = outerFlatmap.Input().Cast<TCoFlatMapBase>();
        if (!IsYtProviderInput(innerFlatmap.Input())) {
            return node;
        }

        if (FindNode(innerFlatmap.Lambda().Body().Ptr(),
            [](const TExprNode::TPtr& node) { return !TYtOutput::Match(node.Get()); },
            [](const TExprNode::TPtr& node) { return TCoNonDeterministicBase::Match(node.Get()); })) {

            // If inner FlatMap uses non-deterministic functions then disallow to split it in case of multiusage
            const TParentsMap* parentsMap = getParents();
            auto parentsIt = parentsMap->find(innerFlatmap.Raw());
            YQL_ENSURE(parentsIt != parentsMap->cend());
            if (parentsIt->second.size() > 1) {
                return node;
            }
        }

        const auto flatMapName = outerFlatmap.Maybe<TCoOrderedFlatMap>() && innerFlatmap.Maybe<TCoOrderedFlatMap>()
            ? TCoOrderedFlatMap::CallableName()
            : TCoFlatMap::CallableName();

        auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerFlatmap.Lambda().Ptr(), ctx, State_->Types);
        if (!placeHolder) {
            return {};
        }

        return Build<TCoFlatMapBase>(ctx, outerFlatmap.Pos())
            .CallableName(flatMapName)
            .Input(innerFlatmap.Input())
            .Lambda()
                .Args({"item"})
                .Body<TCoFlatMapBase>()
                    .CallableName(flatMapName)
                    .Input<TExprApplier>()
                        .Apply(innerFlatmap.Lambda())
                        .With(0, "item")
                    .Build()
                    .Lambda()
                        .Args({"outerItem"})
                        .Body<TExprApplier>()
                            .Apply(TCoLambda(lambdaWithPlaceholder))
                            .With(0, "outerItem")
                            .With(TExprBase(placeHolder), "item")
                        .Build()
                    .Build()
                .Build()
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> Zip(TExprBase node, TExprContext& ctx) const {
        auto zip = node.Cast<TCoZip>();
        if (zip.ArgCount() != 2) {
            return node;
        }

        auto lhsList = zip.Arg(0);
        auto rhsList = zip.Arg(1);
        if (!IsYtProviderInput(lhsList) || !IsYtProviderInput(rhsList)) {
            return node;
        }

        return Build<TCoMap>(ctx, zip.Pos())
            .Input<TCoJoin>()
                .LeftInput<TCoEnumerate>()
                    .Input(lhsList)
                .Build()
                .RightInput<TCoEnumerate>()
                    .Input(rhsList)
                .Build()
                .LeftLambda()
                    .Args({"p"})
                    .Body<TCoNth>()
                        .Tuple("p")
                        .Index()
                            .Value("0")
                        .Build()
                    .Build()
                .Build()
                .RightLambda()
                    .Args({"p"})
                    .Body<TCoNth>()
                        .Tuple("p")
                        .Index()
                            .Value("0")
                        .Build()
                    .Build()
                .Build()
                .JoinKind()
                    .Value("Inner")
                .Build()
            .Build()
            .Lambda()
                .Args({"m"})
                .Body<TExprList>()
                    .Add<TCoNth>()
                        .Tuple<TCoNth>()
                            .Tuple("m")
                            .Index()
                                .Value("0")
                            .Build()
                        .Build()
                        .Index()
                            .Value("1")
                        .Build()
                    .Build()
                    .Add<TCoNth>()
                        .Tuple<TCoNth>()
                            .Tuple("m")
                            .Index()
                                .Value("1")
                            .Build()
                        .Build()
                        .Index()
                            .Value("1")
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
    }

    TMaybeNode<TExprBase> ZipAll(TExprBase node, TExprContext& ctx) const {
        auto zip = node.Cast<TCoZipAll>();
        if (zip.ArgCount() != 2) {
            return node;
        }

        auto lhsList = zip.Arg(0);
        auto rhsList = zip.Arg(1);
        if (!IsYtProviderInput(lhsList) || !IsYtProviderInput(rhsList)) {
            return node;
        }

        return Build<TCoMap>(ctx, zip.Pos())
            .Input<TCoJoin>()
                .LeftInput<TCoEnumerate>()
                    .Input(lhsList)
                .Build()
                .RightInput<TCoEnumerate>()
                    .Input(rhsList)
                .Build()
                .LeftLambda()
                    .Args({"p"})
                    .Body<TCoNth>()
                        .Tuple("p")
                        .Index()
                            .Value("0")
                        .Build()
                    .Build()
                .Build()
                .RightLambda()
                    .Args({"p"})
                    .Body<TCoNth>()
                        .Tuple("p")
                        .Index()
                            .Value("0")
                        .Build()
                    .Build()
                .Build()
                .JoinKind()
                    .Value("Full")
                .Build()
            .Build()
            .Lambda()
                .Args({"m"})
                .Body<TExprList>()
                    .Add<TCoMap>()
                        .Input<TCoNth>()
                            .Tuple("m")
                            .Index()
                                .Value("0")
                            .Build()
                        .Build()
                        .Lambda()
                            .Args({"p"})
                            .Body<TCoNth>()
                                .Tuple("p")
                                .Index()
                                    .Value("1")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Add<TCoMap>()
                        .Input<TCoNth>()
                            .Tuple("m")
                            .Index()
                                .Value("1")
                            .Build()
                        .Build()
                        .Lambda()
                            .Args({"p"})
                            .Body<TCoNth>()
                                .Tuple("p")
                                .Index()
                                    .Value("1")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
    }

    TMaybeNode<TExprBase> ExtractMembers(TExprBase node, TExprContext& ctx) const {
        auto extract = node.Cast<TCoExtractMembers>();
        auto input = extract.Input();
        if (!IsYtProviderInput(input)) {
            return node;
        }

        TExprNode::TPtr world;
        TVector<TYtSection> sections;
        if (auto out = input.Maybe<TYtOutput>()) {
            world = ctx.NewWorld(input.Pos());
            sections.push_back(Build<TYtSection>(ctx, input.Pos())
                .Paths()
                    .Add()
                        .Table(out.Cast())
                        .Columns(extract.Members())
                        .Ranges<TCoVoid>().Build()
                        .Stat<TCoVoid>().Build()
                    .Build()
                .Build()
                .Settings().Build()
                .Done());
        }
        else {
            auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
            YQL_ENSURE(read, "Unknown operation input");
            world = read.Cast().World().Ptr();

            for (auto section: read.Cast().Input()) {
                sections.push_back(UpdateInputFields(section, extract.Members(), ctx));
            }
        }

        return Build<TCoRight>(ctx, extract.Pos())
            .Input<TYtReadTable>()
                .World(world)
                .DataSource(GetDataSource(input, ctx))
                .Input()
                    .Add(sections)
                .Build()
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> ExtractMembersOverContent(TExprBase node, TExprContext& ctx) const {
        auto extractMembers = node.Cast<TCoExtractMembers>();

        TExprBase tableContent = extractMembers.Input();
        if (!tableContent.Maybe<TYtTableContent>()) {
            return node;
        }

        return Build<TYtTableContent>(ctx, tableContent.Pos())
            .InitFrom(tableContent.Cast<TYtTableContent>())
            .Input(ConvertContentInputToRead(tableContent.Cast<TYtTableContent>().Input(), {}, ctx, extractMembers.Members()))
            .Done();
    }

    TMaybeNode<TExprBase> TakeOrSkip(TExprBase node, TExprContext& ctx) const {
        auto countBase = node.Cast<TCoCountBase>();
        auto input = countBase.Input();
        if (!input.Maybe<TYtTableContent>()) {
            return node;
        }

        input = input.Cast<TYtTableContent>().Input();

        TYtDSource dataSource = GetDataSource(input, ctx);
        TString cluster = TString{dataSource.Cluster().Value()};
        TSyncMap syncList;
        if (!IsYtCompleteIsolatedLambda(countBase.Count().Ref(), syncList, cluster, false)) {
            return node;
        }

        EYtSettingType settingType = node.Maybe<TCoSkip>() ? EYtSettingType::Skip : EYtSettingType::Take;

        auto settings = Build<TCoNameValueTupleList>(ctx, countBase.Pos())
            .Add()
                .Name()
                    .Value(ToString(settingType))
                .Build()
                .Value(countBase.Count())
            .Build()
            .Done();

        return ctx.ChangeChild(countBase.Input().Ref(), TYtTableContent::idx_Input, ConvertContentInputToRead(input, settings, ctx).Ptr());
    }

    TMaybeNode<TExprBase> BypassCopy(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto srcOut = node.Cast<TYtOutput>();
        auto maybeCopy = srcOut.Operation().Maybe<TYtCopy>();
        if (!maybeCopy) {
            return node;
        }
        auto copy = maybeCopy.Cast();
        if (copy.World().Ref().Type() != TExprNode::World) {
            return node;
        }

        if (copy.Ref().HasResult()) {
            return node;
        }

        TYtPath path = copy.Input().Item(0).Paths().Item(0);
        if (!path.Table().Maybe<TYtOutput>()) {
            return node;
        }

        const auto parentsMap = getParents();
        auto parentsIt = parentsMap->find(copy.Raw());
        for (auto p: parentsIt->second) {
            if (TCoLeft::Match(p)) {
                return node;
            }
        }


        auto res = path.Table().Cast<TYtOutput>();
        if (IsUnorderedOutput(srcOut)) {
            res = Build<TYtOutput>(ctx, res.Pos())
                .InitFrom(res)
                .Mode(srcOut.Mode())
                .Done();
        }
        return res;
    }

    TMaybeNode<TExprBase> PushdownReadColumns(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto maybeRead = node.Cast<TCoRight>().Input().Maybe<TYtReadTable>();
        if (!maybeRead) {
            return node;
        }

        auto read = maybeRead.Cast();

        bool hasNewSection = false;
        TVector<TYtSection> sections;
        for (auto section: read.Input()) {
            sections.push_back(PushdownSectionColumns(section, ctx, getParents));
            if (section.Raw() != sections.back().Raw()) {
                hasNewSection = true;
            }
        }

        if (!hasNewSection) {
            return node;
        }

        return Build<TCoRight>(ctx, node.Pos())
            .Input<TYtReadTable>()
                .InitFrom(read)
                .Input()
                    .Add(sections)
                .Build()
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> PushdownOpColumns(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto op = node.Cast<TYtTransientOpBase>();

        bool hasNewSection = false;
        TVector<TYtSection> sections;
        for (auto section: op.Input()) {
            sections.push_back(PushdownSectionColumns(section, ctx, getParents));
            if (section.Raw() != sections.back().Raw()) {
                hasNewSection = true;
            }
        }

        if (!hasNewSection) {
            return node;
        }

        return TExprBase(ctx.ChangeChild(node.Ref(), TYtTransientOpBase::idx_Input, Build<TYtSectionList>(ctx, op.Input().Pos()).Add(sections).Done().Ptr()));
    }

    struct TExtendOverSameMapGroupLess {
        bool operator() (const std::pair<bool, TCoLambda>& left, const std::pair<bool, TCoLambda>& right) const {
            return std::make_pair(left.first, left.second.Ref().UniqueId()) < std::make_pair(right.first, right.second.Ref().UniqueId());
        }
    };

    TMaybeNode<TExprBase> ExtendOverSameMap(TExprBase node, TExprContext& ctx) const {
        auto extend = node.Cast<TCoExtendBase>();

        // Don't apply to OrderedExtend because input is reordered
        if (extend.Maybe<TCoOrderedExtend>()) {
            return node;
        }

        TVector<TExprBase> retChildren;
        TVector<TCoFlatMapBase> flatMaps;
        bool keepOrder = extend.Maybe<TCoMerge>().IsValid();
        for (auto child : extend) {
            if (auto maybeFlatMap = child.Maybe<TCoFlatMapBase>()) {
                auto flatMap = maybeFlatMap.Cast();
                keepOrder = keepOrder && flatMap.Maybe<TCoOrderedFlatMap>();
                auto input = flatMap.Input();
                if (!IsYtProviderInput(input)) {
                    return node;
                }
                if (input.Ref().UseCount() > 2) { // Additional reference is owned by 'input' local var
                    retChildren.push_back(child);
                } else {
                    flatMaps.push_back(flatMap);
                }
            }
            else {
                return node;
            }
        }

        // group by YAMR input and lambda nodes
        std::map<std::pair<bool, TCoLambda>, TVector<TCoFlatMapBase>, TExtendOverSameMapGroupLess> grouped;

        for (auto flatmap : flatMaps) {
            // All YtRead inputs either YAMR or not, so check only first one. YtOutput cannot be YAMR
            const bool yamr = flatmap.Input().Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input()
                .Item(0).Paths().Item(0).Table().Maybe<TYtTable>().RowSpec().Maybe<TCoVoid>().IsValid();

            grouped[std::make_pair(yamr, flatmap.Lambda())].emplace_back(flatmap);
        }

        size_t singleWithoutContextDependent = 0;
        TVector<std::pair<bool, TCoLambda>> contextDependentLambdas;
        for (auto& x : grouped) {
            if (IsTablePropsDependent(x.first.second.Ref())) {
                contextDependentLambdas.push_back(x.first);
            } else if (x.second.size() == 1) {
                ++singleWithoutContextDependent;
            }
        }

        if (grouped.size() == flatMaps.size() ||
            contextDependentLambdas.size() + singleWithoutContextDependent == grouped.size())
        {
            return node;
        }

        for (auto& x : contextDependentLambdas) {
            retChildren.insert(retChildren.end(), grouped[x].begin(), grouped[x].end());
            grouped.erase(x);
        }

        for (auto& x: grouped) {
            TExprNode::TListType subChildren;

            for (auto flatMap: x.second) {
                subChildren.push_back(flatMap.Input().Ptr());
            }

            auto flatMapInput = ctx.NewCallable(node.Pos(), extend.Ref().Content(), std::move(subChildren));

            retChildren.push_back(TExprBase(ctx.Builder(node.Pos())
                .Callable(keepOrder ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                    .Add(0, flatMapInput)
                    .Add(1, x.first.second.Ptr())
                .Seal()
                .Build()));
        }

        if (extend.Maybe<TCoMerge>()) {
            return Build<TCoMerge>(ctx, node.Pos()).Add(retChildren).Done();
        }

        return Build<TCoExtend>(ctx, node.Pos()).Add(retChildren).Done();
    }

    static bool IsExtendWithFlatMaps(TExprBase node, bool requireChildFlatMap) {
        if (!node.Maybe<TCoExtendBase>()) {
            return false;
        }

        auto extend = node.Cast<TCoExtendBase>();
        auto type = extend.Ref().GetTypeAnn();
        if (type->GetKind() != ETypeAnnotationKind::List ||
            type->Cast<TListExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Struct) {
            return false;
        }

        bool hasFlatMap = false;
        for (auto child : extend) {
            if (IsYtProviderInput(child)) {
                continue;
            }

            if (auto mayFlatMap = child.Maybe<TCoFlatMapBase>()) {
                if (IsYtProviderInput(mayFlatMap.Cast().Input())) {
                    hasFlatMap = true;
                    continue;
                }
            }
            return false;
        }

        return !requireChildFlatMap || hasFlatMap;
    }

    TMaybeNode<TExprBase> FlatMapOverExtend(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto flatMap = node.Cast<TCoFlatMapBase>();

        auto input = flatMap.Input();
        TVector<TCoInputBase> intermediates;
        while (input.Maybe<TCoCountBase>() || input.Maybe<TCoFilterNullMembersBase>()) {
            intermediates.push_back(input.Cast<TCoInputBase>());
            input = input.Cast<TCoInputBase>().Input();
        }
        if (!IsExtendWithFlatMaps(input, true)) {
            return node;
        }

        const TParentsMap* parentsMap = getParents();
        auto parentsIt = parentsMap->find(input.Raw());
        YQL_ENSURE(parentsIt != parentsMap->cend());
        if (parentsIt->second.size() > 1) {
            return node;
        }

        const bool ordered = flatMap.Maybe<TCoOrderedFlatMap>() && !input.Maybe<TCoExtend>();
        TExprNode::TListType extendChildren;
        for (auto child: input.Ref().Children()) {
            extendChildren.push_back(ctx.Builder(child->Pos())
                .Callable(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                    .Add(0, child)
                    .Add(1, flatMap.Lambda().Ptr())
                .Seal()
                .Build());
        }
        TStringBuf extendName = input.Maybe<TCoMerge>()
            ? TCoMerge::CallableName()
            : (ordered ? TCoOrderedExtend::CallableName() : TCoExtend::CallableName());

        auto res = ctx.NewCallable(node.Pos(), extendName, std::move(extendChildren));
        for (auto it = intermediates.rbegin(); it != intermediates.rend(); ++it) {
            res = ctx.ChangeChild(it->Ref(), TCoInputBase::idx_Input, std::move(res));
        }

        return TExprBase(res);
    }

    TMaybeNode<TExprBase> TakeOverExtend(TExprBase node, TExprContext& ctx) const {
        auto take = node.Cast<TCoTake>();

        if (!IsExtendWithFlatMaps(take.Input(), true)) {
            return node;
        }

        TExprNode::TListType extendChildren;
        for (auto child : take.Input().Ref().Children()) {
            extendChildren.push_back(ctx.Builder(child->Pos())
                .Callable(TCoTake::CallableName())
                    .Add(0, child)
                    .Add(1, take.Count().Ptr())
                .Seal()
                .Build());
        }

        return Build<TCoLimit>(ctx, node.Pos())
            .Input(ctx.NewCallable(node.Pos(), take.Input().Ref().Content(), std::move(extendChildren)))
            .Count(take.Count())
            .Done();
    }

    TMaybeNode<TExprBase> DirectRowInFlatMap(TExprBase node, TExprContext& ctx) const {
        if (State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
            return node;
        }

        auto flatMap = node.Cast<TCoFlatMapBase>();

        const auto& outItemType = GetSeqItemType(*flatMap.Ref().GetTypeAnn());
        if (outItemType.GetKind() != ETypeAnnotationKind::Struct) {
            return node;
        }

        auto input = flatMap.Input();
        TVector<TCoInputBase> intermediates;
        while (input.Maybe<TCoCountBase>() || input.Maybe<TCoFilterNullMembersBase>()) {
            intermediates.push_back(input.Cast<TCoInputBase>());
            input = input.Cast<TCoInputBase>().Input();
        }
        if (!IsExtendWithFlatMaps(input, false)) {
            return node;
        }

        TNodeSet nodesToOptimize;
        TProcessedNodesSet processedNodes;
        auto originalArg = flatMap.Lambda().Args().Arg(0).Raw();
        VisitExpr(flatMap.Lambda().Ptr(), [&nodesToOptimize, &processedNodes, originalArg](const TExprNode::TPtr& node) {
            if (TCoTablePath::Match(node.Get()) && (node->ChildrenSize() == 0 || node->Child(0)->Child(0) == originalArg)) {
                nodesToOptimize.insert(node.Get());
            }
            else if (TCoTableRecord::Match(node.Get()) && (node->ChildrenSize() == 0 || node->Child(0)->Child(0) == originalArg)) {
                nodesToOptimize.insert(node.Get());
            }
            else if (TYtOutput::Match(node.Get())) {
                // Stop traversing dependent operations
                processedNodes.insert(node->UniqueId());
                return false;
            }
            return true;
        });

        if (nodesToOptimize.empty()) {
            return node;
        }

        // move TablePath/TableRecord if any
        const auto& extend = input.Ref();
        const bool ordered = flatMap.Maybe<TCoOrderedFlatMap>() && !input.Maybe<TCoExtend>();
        TExprNode::TListType updatedExtendInputs;
        for (auto& x : extend.Children()) {
            auto updatedInput = ctx.Builder(flatMap.Pos())
                .Callable(ordered ? "OrderedMap" : "Map")
                    .Add(0, x)
                    .Lambda(1)
                        .Param("row")
                        .Callable("AddMember")
                            .Callable(0, "AddMember")
                                .Arg(0, "row")
                                .Atom(1, "_yql_table_path")
                                .Callable(2, "TablePath")
                                    .Callable(0, "DependsOn")
                                        .Arg(0, "row")
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Atom(1, "_yql_table_record")
                            .Callable(2, "TableRecord")
                                .Callable(0, "DependsOn")
                                    .Arg(0, "row")
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();

            updatedExtendInputs.push_back(updatedInput);
        }

        TStringBuf extendName = input.Maybe<TCoMerge>()
            ? TCoMerge::CallableName()
            : (ordered ? TCoOrderedExtend::CallableName() : TCoExtend::CallableName());

        auto newInput = ctx.NewCallable(extend.Pos(), extendName, std::move(updatedExtendInputs));
        for (auto it = intermediates.rbegin(); it != intermediates.rend(); ++it) {
            newInput = ctx.ChangeChild(it->Ref(), TCoInputBase::idx_Input, std::move(newInput));
        }
        auto arg = ctx.NewArgument(flatMap.Pos(), "row");
        TExprNode::TPtr newBody = flatMap.Lambda().Body().Ptr();

        TOptimizeExprSettings settings(State_->Types);
        settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
        auto status = OptimizeExpr(newBody, newBody, [&](const TExprNode::TPtr& input, TExprContext& ctx)->TExprNode::TPtr {
            if (nodesToOptimize.find(input.Get()) != nodesToOptimize.end()) {
                if (input->IsCallable("TablePath")) {
                    return ctx.NewCallable(input->Pos(), "Member", { arg, ctx.NewAtom(input->Pos(), "_yql_table_path") });
                }

                if (input->IsCallable("TableRecord")) {
                    return ctx.NewCallable(input->Pos(), "Member", { arg, ctx.NewAtom(input->Pos(), "_yql_table_record") });
                }
            }

            return input;
        }, ctx, settings);

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return {};
        }

        newBody = ctx.ReplaceNode(std::move(newBody), flatMap.Lambda().Args().Arg(0).Ref(), arg);
        auto newLambda = ctx.NewLambda(flatMap.Pos(), ctx.NewArguments(flatMap.Pos(), { arg }), std::move(newBody));
        auto res = ctx.NewCallable(flatMap.Pos(), ordered ? "OrderedFlatMap" : "FlatMap", { std::move(newInput), std::move(newLambda) });

        res = ctx.Builder(flatMap.Pos())
            .Callable(ordered ? "OrderedMap" : "Map")
                .Add(0, res)
                .Lambda(1)
                    .Param("row")
                    .Callable("ForceRemoveMember")
                        .Callable(0, "ForceRemoveMember")
                            .Arg(0, "row")
                            .Atom(1, "_yql_table_path")
                        .Seal()
                        .Atom(1, "_yql_table_record")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        return TExprBase(res);
    }

    TMaybeNode<TExprBase> OutputInFlatMap(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtWithUserJobsOpBase>();
        if (op.Input().Size() != 1) {
            return node;
        }

        TMaybeNode<TCoLambda> maybeOpLambda;
        size_t opLambdaIdx = 0;
        if (auto mapReduce = op.Maybe<TYtMapReduce>()) {
            maybeOpLambda = mapReduce.Mapper().Maybe<TCoLambda>();
            opLambdaIdx = TYtMapReduce::idx_Mapper;
        } else if (auto reduce = op.Maybe<TYtReduce>()) {
            maybeOpLambda = reduce.Reducer();
            opLambdaIdx = TYtReduce::idx_Reducer;
        } else {
            maybeOpLambda = op.Maybe<TYtMap>().Mapper();
            opLambdaIdx = TYtMap::idx_Mapper;
        }
        if (!maybeOpLambda) {
            return node;
        }
        auto opLambda = maybeOpLambda.Cast();

        auto maybeFlatMap = GetFlatMapOverInputStream(opLambda);
        if (!maybeFlatMap) {
            return node;
        }

        auto flatMap = maybeFlatMap.Cast();
        TCoLambda lambda = flatMap.Lambda();

        auto finalNode = lambda.Body();
        const bool isListIf = finalNode.Maybe<TCoListIf>().IsValid();
        const bool isAsList1 = finalNode.Maybe<TCoAsList>() && finalNode.Cast<TCoAsList>().ArgCount() == 1;
        const bool removeLastOp = finalNode.Maybe<TCoToList>() ||
            finalNode.Maybe<TCoForwardList>() || finalNode.Maybe<TCoIterator>();
        if (!isListIf && !isAsList1 && !removeLastOp) {
            return node;
        }

        TNodeOnNodeOwnedMap nodesToOptimize;
        if (isAsList1) {
            nodesToOptimize[flatMap.Raw()] = Build<TCoFlatMapBase>(ctx, flatMap.Pos())
                .CallableName(flatMap.CallableName())
                .Input(flatMap.Input())
                .Lambda()
                    .Args({"stream"})
                    .Body<TExprApplier>()
                        .Apply(TExprBase(ctx.NewCallable(lambda.Pos(), "Just", {finalNode.Ref().HeadPtr()})))
                        .With(lambda.Args().Arg(0), "stream")
                    .Build()
                .Build()
                .Done().Ptr();
        }

        if (isListIf) {
            nodesToOptimize[flatMap.Raw()] = Build<TCoFlatMapBase>(ctx, flatMap.Pos())
                .CallableName(flatMap.CallableName())
                .Input(flatMap.Input())
                .Lambda()
                    .Args({"stream"})
                    .Body<TExprApplier>()
                        .Apply(TExprBase(ctx.NewCallable(lambda.Pos(), "OptionalIf", {finalNode.Ref().HeadPtr(), finalNode.Ref().TailPtr()})))
                        .With(lambda.Args().Arg(0), "stream")
                    .Build()
                .Build()
                .Done().Ptr();
        }

        if (removeLastOp) {
            nodesToOptimize[flatMap.Raw()] = Build<TCoFlatMapBase>(ctx, flatMap.Pos())
                .CallableName(flatMap.CallableName())
                .Input(flatMap.Input())
                .Lambda()
                    .Args({"stream"})
                    .Body<TExprApplier>()
                        .Apply(TExprBase(finalNode.Ref().HeadPtr()))
                        .With(lambda.Args().Arg(0), "stream")
                    .Build()
                .Build()
                .Done().Ptr();
        }

        TProcessedNodesSet processedNodes;
        VisitExpr(opLambda.Ptr(), [&processedNodes](const TExprNode::TPtr& node) {
            if (TYtOutput::Match(node.Get())) {
                // Stop traversing dependent operations
                processedNodes.insert(node->UniqueId());
                return false;
            }
            return true;
        });

        TOptimizeExprSettings settings(State_->Types);
        settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
        TExprNode::TPtr newOpLambda;
        auto status = RemapExpr(opLambda.Ptr(), newOpLambda, nodesToOptimize, ctx, settings);
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return {};
        }

        return TExprBase(ctx.ChangeChild(op.Ref(), opLambdaIdx, ctx.DeepCopyLambda(*newOpLambda)));
    }

    static void CollectEquiJoinLinks(TCoEquiJoinTuple joinTree, TVector<std::pair<TString, TString>>& links,
        const std::function<bool(TExprBase linkSettings)>& collectPred)
    {
        if (!joinTree.LeftScope().Maybe<TCoAtom>()) {
            CollectEquiJoinLinks(joinTree.LeftScope().Cast<TCoEquiJoinTuple>(), links, collectPred);
        }

        if (!joinTree.RightScope().Maybe<TCoAtom>()) {
            CollectEquiJoinLinks(joinTree.RightScope().Cast<TCoEquiJoinTuple>(), links, collectPred);
        }

        if (collectPred(joinTree.Options())) {
            YQL_ENSURE(joinTree.LeftKeys().Size() == joinTree.RightKeys().Size());
            YQL_ENSURE(joinTree.LeftKeys().Size() % 2 == 0);
            for (ui32 i = 0; i < joinTree.LeftKeys().Size(); i += 2) {

                auto leftKey = FullColumnName(joinTree.LeftKeys().Item(i).Value(), joinTree.LeftKeys().Item(i + 1).Value());
                auto rightKey = FullColumnName(joinTree.RightKeys().Item(i).Value(), joinTree.RightKeys().Item(i + 1).Value());

                links.emplace_back(leftKey, rightKey);
            }
        }
    }

    struct TRemapTarget {
        TString Name;
        const TTypeAnnotationNode* Type;
    };

    static void ApplyJoinKeyRemapsLeaf(TExprNode::TPtr& keysNode, const THashMap<TStringBuf, THashMap<TStringBuf, TRemapTarget>>& memberRemapsByLabel,
        TExprContext& ctx)
    {
        YQL_ENSURE(keysNode->IsList());
        TExprNodeList keys = keysNode->ChildrenList();
        YQL_ENSURE(keys.size() % 2 == 0);
        for (size_t i = 0; i < keys.size(); i += 2) {
            TCoAtom label(keys[i]);
            if (auto it = memberRemapsByLabel.find(label.Value()); it != memberRemapsByLabel.end()) {
                TCoAtom column(keys[i + 1]);
                if (auto remapIt = it->second.find(column.Value()); remapIt != it->second.end()) {
                    keys[i + 1] = ctx.NewAtom(column.Pos(), remapIt->second.Name);
                }
            }
        }
        keysNode = ctx.NewList(keysNode->Pos(), std::move(keys));
    }

    static void ApplyJoinKeyRemaps(TExprNode::TPtr& joinTree, const THashMap<TStringBuf, THashMap<TStringBuf, TRemapTarget>>& memberRemapsByLabel,
        TExprContext& ctx)
    {
        YQL_ENSURE(joinTree->IsList());
        TExprNodeList children = joinTree->ChildrenList();

        auto& leftScope = children[TCoEquiJoinTuple::idx_LeftScope];
        if (!leftScope->IsAtom()) {
            ApplyJoinKeyRemaps(leftScope, memberRemapsByLabel, ctx);
        }

        auto& rightScope = children[TCoEquiJoinTuple::idx_RightScope];
        if (!rightScope->IsAtom()) {
            ApplyJoinKeyRemaps(rightScope, memberRemapsByLabel, ctx);
        }

        auto& leftKeys = children[TCoEquiJoinTuple::idx_LeftKeys];
        ApplyJoinKeyRemapsLeaf(leftKeys, memberRemapsByLabel, ctx);

        auto& rightKeys = children[TCoEquiJoinTuple::idx_RightKeys];
        ApplyJoinKeyRemapsLeaf(rightKeys, memberRemapsByLabel, ctx);

        joinTree = ctx.NewList(joinTree->Pos(), std::move(children));
    }

    TMaybeNode<TExprBase> ConvertToCommonTypeForForcedMergeJoin(TExprBase node, TExprContext& ctx) const {
        auto equiJoin = node.Cast<TCoEquiJoin>();
        YQL_ENSURE(equiJoin.ArgCount() >= 4);

        bool hasYtInputs = false;
        THashMap<TStringBuf, size_t> inputPosByLabel;
        TVector<const TStructExprType*> inputTypes;
        for (size_t i = 0; i < equiJoin.ArgCount() - 2; ++i) {
            auto input = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
            if (!input.Scope().Maybe<TCoAtom>()) {
                return node;
            }
            TStringBuf label = input.Scope().Cast<TCoAtom>().Value();
            bool inserted = inputPosByLabel.emplace(label, i).second;
            YQL_ENSURE(inserted);
            hasYtInputs = hasYtInputs || IsYtProviderInput(input.List());
            inputTypes.push_back(GetSeqItemType(*input.List().Ref().GetTypeAnn()).Cast<TStructExprType>());
        }

        if (!hasYtInputs) {
            return node;
        }

        bool forceSortedMerge = false;
        if (auto force = State_->Configuration->JoinMergeForce.Get()) {
            forceSortedMerge = *force;
        }

        auto collectPred = [forceSortedMerge](TExprBase linkSettingsNode) {
            return forceSortedMerge || GetEquiJoinLinkSettings(linkSettingsNode.Ref()).ForceSortedMerge;
        };

        TVector<std::pair<TString, TString>> links;
        CollectEquiJoinLinks(equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>(), links, collectPred);

        TVector<TStringBuf> joinKeys;
        for (const auto& [left, right] : links) {
            joinKeys.push_back(left);
            joinKeys.push_back(right);
        }
        SortUnique(joinKeys);

        TDisjointSets disjointSets(joinKeys.size());
        for (const auto& [left, right] : links) {
            size_t leftPos = LowerBound(joinKeys.begin(), joinKeys.end(), left) - joinKeys.begin();
            size_t rightPos = LowerBound(joinKeys.begin(), joinKeys.end(), right) - joinKeys.begin();

            YQL_ENSURE(leftPos < joinKeys.size());
            YQL_ENSURE(rightPos < joinKeys.size());

            disjointSets.UnionSets(leftPos, rightPos);
        }

        TMap<size_t, TSet<size_t>> keySetsByCanonicElement;
        for (size_t i = 0; i < joinKeys.size(); ++i) {
            size_t canonicElement = disjointSets.CanonicSetElement(i);
            keySetsByCanonicElement[canonicElement].insert(i);
        }

        THashMap<TStringBuf, THashMap<TStringBuf, TRemapTarget>> memberRemapsByLabel;
        size_t genIdx = 0;
        TVector<TString> toDrop;
        for (auto& [_, keySet] : keySetsByCanonicElement) {
            TVector<const TTypeAnnotationNode*> srcKeyTypes;
            TVector<TStringBuf> srcKeyNames;
            TVector<const TStructExprType*> srcInputTypes;
            for (auto& keyIdx : keySet) {
                const TStringBuf key = joinKeys[keyIdx];
                TStringBuf label;
                TStringBuf column;
                SplitTableName(key, label, column);

                auto it = inputPosByLabel.find(label);
                YQL_ENSURE(it != inputPosByLabel.end());

                const size_t inputIdx = it->second;
                YQL_ENSURE(inputIdx < inputTypes.size());

                auto columnType = inputTypes[inputIdx]->FindItemType(column);
                YQL_ENSURE(columnType);

                srcKeyNames.push_back(key);
                srcKeyTypes.push_back(columnType);
                srcInputTypes.push_back(inputTypes[inputIdx]);
            }

            // derive common type for all join keys in key set
            const TTypeAnnotationNode* commonType = UnifyJoinKeyType(equiJoin.Pos(), srcKeyTypes, ctx);
            YQL_ENSURE(commonType);

            const TTypeAnnotationNode* commonTypeNoOpt = RemoveOptionalType(commonType);

            bool needRemap = !IsDataOrOptionalOfData(commonType);
            for (size_t i = 0; i < srcKeyNames.size(); ++i) {
                TStringBuf srcKey = srcKeyNames[i];
                const TTypeAnnotationNode* srcKeyType = srcKeyTypes[i];
                const TStructExprType* inputType = srcInputTypes[i];

                if (needRemap || !IsSameAnnotation(*RemoveOptionalType(srcKeyType), *commonTypeNoOpt)) {
                    TStringBuf label;
                    TStringBuf column;
                    SplitTableName(srcKey, label, column);

                    TString targetName;
                    do {
                        targetName = TStringBuilder() << "_yql_normalized_join_key" << genIdx++;
                    } while (inputType->FindItem(targetName));

                    TRemapTarget target;
                    target.Type = commonType;
                    target.Name = targetName;
                    toDrop.push_back(FullColumnName(label, targetName));

                    memberRemapsByLabel[label][column] = target;
                }
            }
        }

        TExprNodeList equiJoinArgs = equiJoin.Ref().ChildrenList();
        for (auto& [label, remaps] : memberRemapsByLabel) {
            auto it = inputPosByLabel.find(label);
            YQL_ENSURE(it != inputPosByLabel.end());

            const size_t inputPos = it->second;

            TExprNode::TPtr& equiJoinInput = equiJoinArgs[inputPos];

            auto row = ctx.NewArgument(equiJoinInput->Pos(), "row");
            auto body = row;
            for (auto& [srcColumn, target] : remaps) {
                auto srcType = inputTypes[inputPos]->FindItemType(srcColumn);
                YQL_ENSURE(srcType);
                auto srcValue = ctx.Builder(body->Pos())
                    .Callable("Member")
                        .Add(0, row)
                        .Atom(1, srcColumn)
                    .Seal()
                    .Build();
                auto targetValue = RemapNonConvertibleMemberForJoin(srcValue->Pos(), srcValue, *srcType, *target.Type, ctx);
                body = ctx.Builder(body->Pos())
                    .Callable("AddMember")
                        .Add(0, body)
                        .Atom(1, target.Name)
                        .Add(2, targetValue)
                    .Seal()
                    .Build();
            }

            auto remapLambda = ctx.NewLambda(row->Pos(), ctx.NewArguments(row->Pos(), { row }), std::move(body));
            equiJoinInput = ctx.Builder(equiJoinInput->Pos())
                .List()
                    .Callable(0, "OrderedMap")
                        .Add(0, equiJoinInput->HeadPtr())
                        .Add(1, remapLambda)
                    .Seal()
                    .Add(1, equiJoinInput->TailPtr())
                .Seal()
                .Build();
        }

        if (!memberRemapsByLabel.empty()) {
            ApplyJoinKeyRemaps(equiJoinArgs[equiJoin.ArgCount() - 2], memberRemapsByLabel, ctx);
            TExprNode::TPtr result = ctx.ChangeChildren(equiJoin.Ref(), std::move(equiJoinArgs));

            auto row = ctx.NewArgument(equiJoin.Pos(), "row");
            auto removed = row;
            for (auto column : toDrop) {
                removed = ctx.Builder(equiJoin.Pos())
                    .Callable("ForceRemoveMember")
                        .Add(0, removed)
                        .Atom(1, column)
                    .Seal()
                    .Build();
            }

            auto removeLambda = ctx.NewLambda(equiJoin.Pos(), ctx.NewArguments(equiJoin.Pos(), { row }), std::move(removed));

            result = ctx.Builder(equiJoin.Pos())
                .Callable("OrderedMap")
                    .Add(0, result)
                    .Add(1, removeLambda)
                .Seal()
                .Build();

            return result;
        }

        return node;
    }

    TMaybeNode<TExprBase> SelfInnerJoinWithSameKeys(TExprBase node, TExprContext& ctx) const {
        auto equiJoin = node.Cast<TCoEquiJoin>();
        // optimize self intersect (inner join) over (filtered) tables with unique keys
        if (equiJoin.ArgCount() != 4) {
            return node;
        }

        auto tree = equiJoin.Arg(2).Cast<TCoEquiJoinTuple>();
        if (tree.Type().Value() != "Inner") {
            return node;
        }

        auto left = equiJoin.Arg(0).Cast<TCoEquiJoinInput>().List();
        auto right = equiJoin.Arg(1).Cast<TCoEquiJoinInput>().List();
        TMaybeNode<TExprBase> leftInput;
        TMaybeNode<TExprBase> rightInput;
        TMaybe<THashSet<TStringBuf>> leftPassthroughFields;
        TMaybe<THashSet<TStringBuf>> rightPassthroughFields;
        TMaybe<TSet<TString>> leftReadFields;
        TMaybe<TSet<TString>> rightReadFields;
        TExprNode::TPtr leftPredicate;
        TExprNode::TPtr leftArg;
        TExprNode::TPtr leftValue;
        TExprNode::TPtr rightPredicate;
        TExprNode::TPtr rightArg;
        TExprNode::TPtr rightValue;

        if (auto extract = left.Maybe<TCoExtractMembers>()) {
            leftReadFields.ConstructInPlace();
            for (auto f: extract.Cast().Members()) {
                leftReadFields->emplace(f.Value());
            }
            left = extract.Cast().Input();
        }

        if (auto extract = right.Maybe<TCoExtractMembers>()) {
            rightReadFields.ConstructInPlace();
            for (auto f: extract.Cast().Members()) {
                rightReadFields->emplace(f.Value());
            }
            right = extract.Cast().Input();
        }

        if (IsYtProviderInput(left)) {
            leftInput = left;
        } else if (auto maybeFlatMap = left.Maybe<TCoFlatMapBase>()) {
            auto flatMap = maybeFlatMap.Cast();
            const auto& input = flatMap.Input();
            if (IsYtProviderInput(input) && IsPassthroughFlatMap(flatMap, &leftPassthroughFields)) {
                const auto& lambda = flatMap.Lambda();
                const auto& body = lambda.Body();
                leftArg = lambda.Args().Arg(0).Ptr();
                if (body.Maybe<TCoOptionalIf>() || body.Maybe<TCoListIf>()) {
                    leftValue = body.Cast<TCoConditionalValueBase>().Value().Ptr();
                    leftPredicate = body.Cast<TCoConditionalValueBase>().Predicate().Ptr();
                } else {
                    leftValue = body.Ref().ChildPtr(0);
                }

                leftInput = input;
            }
        }

        if (IsYtProviderInput(right)) {
            rightInput = right;
        } else if (auto maybeFlatMap = right.Maybe<TCoFlatMapBase>()) {
            auto flatMap = maybeFlatMap.Cast();
            const auto& input = flatMap.Input();
            if (IsYtProviderInput(input) && IsPassthroughFlatMap(flatMap, &rightPassthroughFields)) {
                const auto& lambda = flatMap.Lambda();
                const auto& body = lambda.Body();
                rightArg = lambda.Args().Arg(0).Ptr();
                if (body.Maybe<TCoOptionalIf>() || body.Maybe<TCoListIf>()) {
                    rightValue = body.Cast<TCoConditionalValueBase>().Value().Ptr();
                    rightPredicate = body.Cast<TCoConditionalValueBase>().Predicate().Ptr();
                } else {
                    rightValue = body.Ref().ChildPtr(0);
                }

                rightInput = input;
            }
        }

        if (!leftInput || !rightInput) {
            return node;
        }

        auto leftCluster = GetClusterName(leftInput.Cast());
        auto rightCluster = GetClusterName(rightInput.Cast());
        if (leftCluster != rightCluster) {
            return node;
        }

        // check that key and settings are same
        if (leftInput.Maybe<TYtOutput>() && rightInput.Maybe<TYtOutput>()) {
            if (leftInput.Cast().Raw() != rightInput.Cast().Raw()) {
                return node;
            }
        }
        else {
            auto leftSection = leftInput.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0);
            auto rightSection = rightInput.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0);
            if (!leftSection || !rightSection) {
                return node;
            }

            if (!EqualSettingsExcept(leftSection.Settings().Cast().Ref(), rightSection.Settings().Cast().Ref(), EYtSettingType::Unordered)) {
                return node;
            }

            // only one table
            if (leftSection.Cast().Paths().Size() > 1 || rightSection.Cast().Paths().Size() > 1) {
                return node;
            }

            auto leftPath = leftSection.Cast().Paths().Item(0);
            auto rightPath = rightSection.Cast().Paths().Item(0);

            if (leftPath.Table().Raw() != rightPath.Table().Raw()) {
                return node;
            }

            if (leftPath.Ranges().Raw() != rightPath.Ranges().Raw()) {
                return node;
            }
        }

        TYtTableBaseInfo::TPtr tableInfo = GetInputTableInfos(leftInput.Cast()).front();
        if (tableInfo->IsUnordered || !tableInfo->RowSpec || !tableInfo->RowSpec->IsSorted() || !tableInfo->RowSpec->UniqueKeys) {
            return node;
        }

        // check keys
        THashSet<TStringBuf> joinColumns;
        for (ui32 i = 0; i < tree.LeftKeys().Size(); i += 2) {
            const auto& leftColumn = tree.LeftKeys().Item(i + 1).Value();
            const auto& rightColumn = tree.RightKeys().Item(i + 1).Value();
            if (leftColumn != rightColumn) {
                return node;
            }

            joinColumns.emplace(leftColumn);
        }

        if (tableInfo->RowSpec->SortedBy.size() != joinColumns.size()) {
            return node;
        }

        for (auto& field : tableInfo->RowSpec->SortedBy) {
            if (leftPassthroughFields && !leftPassthroughFields->contains(field)) {
                return node;
            }

            if (rightPassthroughFields && !rightPassthroughFields->contains(field)) {
                return node;
            }

            if (!joinColumns.contains(field)) {
                return node;
            }
        }

        // now we could rewrite join with single flatmap
        TExprBase commonInput = leftInput.Cast();
        if (leftInput.Maybe<TCoRight>()) {
            auto read = leftInput.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Cast();
            if (!leftReadFields) {
                if (auto columns = leftInput.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0).Paths().Item(0).Columns().Maybe<TCoAtomList>()) {
                    leftReadFields.ConstructInPlace();
                    for (auto f: columns.Cast()) {
                        leftReadFields->emplace(f.Value());
                    }
                }
            }
            if (!rightReadFields) {
                if (auto columns = rightInput.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0).Paths().Item(0).Columns().Maybe<TCoAtomList>()) {
                    rightReadFields.ConstructInPlace();
                    for (auto f: columns.Cast()) {
                        rightReadFields->emplace(f.Value());
                    }
                }
            }
            if (leftReadFields && rightReadFields) {
                TSet<TString> commonFields;
                commonFields.insert(leftReadFields->begin(), leftReadFields->end());
                commonFields.insert(rightReadFields->begin(), rightReadFields->end());
                auto columnsBuilder = Build<TCoAtomList>(ctx, node.Pos());
                for (auto f: commonFields) {
                    columnsBuilder.Add().Value(f).Build();
                }
                commonInput = Build<TCoRight>(ctx, leftInput.Cast().Pos())
                    .Input<TYtReadTable>()
                        .World(read.World())
                        .DataSource(read.DataSource())
                        .Input()
                            .Add()
                                .Paths()
                                    .Add()
                                        .InitFrom(read.Input().Item(0).Paths().Item(0))
                                        .Columns(columnsBuilder.Done())
                                    .Build()
                                .Build()
                                .Settings(read.Input().Item(0).Settings())
                            .Build()
                        .Build()
                    .Build()
                    .Done();
            }
        }

        auto joinLambdaArg = ctx.NewArgument(node.Pos(), "read");
        auto leftReadArg = joinLambdaArg;
        if (leftReadFields) {
            leftReadArg = FilterByFields(node.Pos(), leftReadArg, *leftReadFields, ctx, true);
        }

        auto rightReadArg = joinLambdaArg;
        if (rightReadFields) {
            rightReadArg = FilterByFields(node.Pos(), rightReadArg, *rightReadFields, ctx, true);
        }

        auto truth = ctx.NewCallable(node.Pos(), "Bool", { ctx.NewAtom(node.Pos(), "true") });
        auto leftPrefix = equiJoin.Arg(0).Cast<TCoEquiJoinInput>().Scope().Cast<TCoAtom>().Value();
        auto rightPrefix = equiJoin.Arg(1).Cast<TCoEquiJoinInput>().Scope().Cast<TCoAtom>().Value();
        auto joinLambdaBody = ctx.Builder(node.Pos())
            .Callable("OptionalIf")
                .Callable(0, "And")
                    .Add(0, leftPredicate ? ctx.ReplaceNode(std::move(leftPredicate), *leftArg, leftReadArg) : truth)
                    .Add(1, rightPredicate ? ctx.ReplaceNode(std::move(rightPredicate), *rightArg, rightReadArg) : truth)
                .Seal()
                .Callable(1, "FlattenMembers")
                    .List(0)
                        .Atom(0, TString(leftPrefix) + ".")
                        .Add(1, leftValue ? ctx.ReplaceNode(std::move(leftValue), *leftArg, leftReadArg) : leftReadArg)
                    .Seal()
                    .List(1)
                        .Atom(0, TString(rightPrefix) + ".")
                        .Add(1, rightValue ? ctx.ReplaceNode(std::move(rightValue), *rightArg, rightReadArg) : rightReadArg)
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        auto joinLambda = ctx.NewLambda(node.Pos(), ctx.NewArguments(node.Pos(), { joinLambdaArg }), std::move(joinLambdaBody));
        auto joinFlatMap = Build<TCoFlatMap>(ctx, node.Pos())
            .Input(commonInput)
            .Lambda(joinLambda)
            .Done();

        auto ret = joinFlatMap.Ptr();

        // have to apply renames
        auto settings = equiJoin.Arg(equiJoin.ArgCount() - 1);
        auto renameMap = LoadJoinRenameMap(settings.Ref());

        if (!renameMap.empty()) {
            const TStructExprType* itemType = nullptr;
            if (auto type = GetSequenceItemType(node, false, ctx)) {
                itemType = type->Cast<TStructExprType>();
            } else {
                return {};
            }

            ret = ctx.Builder(node.Pos())
                .Callable("Map")
                    .Add(0, ret)
                    .Add(1, BuildJoinRenameLambda(node.Pos(), renameMap, *itemType, ctx).Ptr())
                .Seal()
                .Build();
        }

        return TExprBase(ret);
    }

    TMaybeNode<TExprBase> Unordered(TExprBase node, TExprContext& ctx) const {
        const auto unordered = node.Cast<TCoUnorderedBase>();
        if (const auto tableContent = unordered.Input().Maybe<TYtTableContent>()) {
            if (const auto out = tableContent.Input().Maybe<TYtOutput>()) {
                if (IsUnorderedOutput(out.Cast()))
                    return tableContent;
                return Build<TYtTableContent>(ctx, node.Pos())
                    .InitFrom(tableContent.Cast())
                    .Input<TYtOutput>()
                        .InitFrom(out.Cast())
                        .Mode()
                            .Value(ToString(EYtSettingType::Unordered), TNodeFlags::Default)
                        .Build()
                    .Build()
                    .Done();
            } else if (const auto read = tableContent.Input().Maybe<TYtReadTable>()) {
                TVector<TYtSection> sections;
                for (auto section: read.Cast().Input()) {
                    sections.push_back(MakeUnorderedSection<true>(section, ctx));
                }
                return Build<TYtTableContent>(ctx, node.Pos())
                    .InitFrom(tableContent.Cast())
                    .Input<TYtReadTable>()
                        .InitFrom(read.Cast())
                        .Input()
                            .Add(sections)
                        .Build()
                    .Build()
                    .Done();
            }
        } else if (const auto input = unordered.Input(); IsYtProviderInput(input, true)) {
            if (const auto out = input.Maybe<TYtOutput>()) {
                if (unordered.Maybe<TCoUnorderedSubquery>() && out.Cast().Operation().Maybe<TYtSort>()) {
                    if (!WarnUnroderedSubquery(node.Ref(), ctx)) {
                        return {};
                    }
                }
                if (IsUnorderedOutput(out.Cast())) {
                    // Already unordered. Just drop
                    return input;
                }
                return Build<TYtOutput>(ctx, node.Pos())
                    .InitFrom(out.Cast())
                    .Mode()
                        .Value(ToString(EYtSettingType::Unordered), TNodeFlags::Default)
                    .Build()
                    .Done();
            } else if (const auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
                TExprNode::TListType sections(read.Cast().Input().Size());
                for (auto i = 0U; i < sections.size(); ++i) {
                    sections[i] = MakeUnorderedSection<true>(read.Cast().Input().Item(i), ctx).Ptr();
                }

                return Build<TCoRight>(ctx, node.Pos())
                    .Input<TYtReadTable>()
                        .InitFrom(read.Cast())
                        .Input()
                            .Add(std::move(sections))
                        .Build()
                    .Build()
                    .Done();
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> ShuffleByKeys(TExprBase node, TExprContext& ctx) const {
        auto list = node.Cast<TCoInputBase>().Input();
        if (!IsYtProviderInput(list)) {
            return node;
        }

        auto shuffle = node.Cast<TCoShuffleByKeys>();

        return Build<TCoPartitionsByKeys>(ctx, node.Pos())
            .Input(list)
            .SortDirections<TCoVoid>()
            .Build()
            .SortKeySelectorLambda<TCoVoid>()
            .Build()
            .KeySelectorLambda(shuffle.KeySelectorLambda())
            .ListHandlerLambda<TCoLambda>()
                .Args({ TStringBuf("stream") })
                .Body<TCoForwardList>()
                    .Stream<TCoToStream>()
                        .Input<TExprApplier>()
                            .Apply(shuffle.ListHandlerLambda().Body())
                            .With(shuffle.ListHandlerLambda().Args().Arg(0), TStringBuf("stream"))
                        .Build()
                    .Build()
                .Build()
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> MatchRecognize(TExprBase node, TExprContext& ctx) {
        return ExpandMatchRecognize(node.Ptr(), ctx, *Types);
    }

    TMaybeNode<TExprBase> CountAggregate(TExprBase node, TExprContext& ctx) const {
        auto aggregate = node.Cast<TCoAggregate>();

        auto input = aggregate.Input();
        if (!IsYtProviderInput(input)) {
            return node;
        }

        return TAggregateExpander::CountAggregateRewrite(aggregate, ctx, State_->Types->IsBlockEngineEnabled());
    }

    TMaybeNode<TExprBase> ZeroSampleToZeroLimit(TExprBase node, TExprContext& ctx) const {
        auto read = node.Cast<TYtReadTable>();

        bool hasUpdates = false;
        TVector<TExprBase> updatedSections;
        for (auto section: read.Input()) {
            TMaybe<TSampleParams> sampling = NYql::GetSampleParams(section.Settings().Ref());
            if (sampling && sampling->Percentage == 0.0) {
                hasUpdates = true;
                section = Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Settings(
                        NYql::AddSetting(
                            *NYql::RemoveSetting(section.Settings().Ref(), EYtSettingType::Sample, ctx),
                            EYtSettingType::Take,
                            Build<TCoUint64>(ctx, section.Pos()).Literal().Value(0).Build().Done().Ptr(),
                            ctx
                        )
                    )
                    .Done();
            }
            updatedSections.push_back(section);
        }

        if (!hasUpdates) {
            return node;
        }

        return ctx.ChangeChild(read.Ref(), TYtReadTable::idx_Input,
            Build<TYtSectionList>(ctx, read.Input().Pos())
                .Add(updatedSections)
                .Done().Ptr());
    }

private:
    TYtState::TPtr State_;
};


THolder<IGraphTransformer> CreateYtLogicalOptProposalTransformer(TYtState::TPtr state) {
    return THolder(new TYtLogicalOptProposalTransformer(state));
}

}
