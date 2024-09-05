#include "yql_yt_provider_impl.h"
#include "yql_yt_helpers.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_optimize.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <util/generic/size_literals.h>

#include <algorithm>
#include <type_traits>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;

class TYtDqHybridTransformer : public TOptimizeTransformerBase {
public:
    TYtDqHybridTransformer(TYtState::TPtr state, THolder<IGraphTransformer>&& finalizer)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYt, state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()))
        , State_(std::move(state)), Finalizer_(std::move(finalizer))
    {
#define HNDL(name) "YtDqHybrid-"#name, Hndl(&TYtDqHybridTransformer::name)
        AddHandler(0, &TYtFill::Match, HNDL(TryYtFillByDq));
        AddHandler(0, &TYtSort::Match, HNDL(TryYtSortByDq));
        AddHandler(0, &TYtMap::Match, HNDL(TryYtMapByDq));
        AddHandler(0, &TYtReduce::Match, HNDL(TryYtReduceByDq));
        AddHandler(0, &TYtMapReduce::Match, HNDL(TryYtMapReduceByDq));
        AddHandler(0, &TYtMerge::Match, HNDL(TryYtMergeByDq));
#undef HNDL
    }
private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        if (const auto status = Finalizer_->Transform(input, output, ctx); status.Level != TStatus::Ok)
            return status;

        return TOptimizeTransformerBase::DoTransform(input, output, ctx);
    }

    void Rewind() final {
        Finalizer_->Rewind();
        TOptimizeTransformerBase::Rewind();
    }

    bool CanReplaceOnHybrid(const TYtOutputOpBase& operation) const {
        const TStringBuf nodeName = operation.Raw()->Content();
        if (!State_->IsHybridEnabledForCluster(operation.DataSink().Cluster().Value())) {
            PushSkipStat("DisabledCluster", nodeName);
            return false;
        }

        if (State_->HybridTakesTooLong()) {
            PushSkipStat("TakesTooLong", nodeName);
            return false;
        }

        if (operation.Ref().StartsExecution() || operation.Ref().HasResult())
            return false;

        if (operation.Output().Size() != 1U) {
            PushSkipStat("MultipleOutputs", nodeName);
            return false;
        }

        if (const auto& trans = operation.Maybe<TYtTransientOpBase>(); trans && trans.Cast().Input().Size() != 1U) {
            PushSkipStat("MultipleInputs", nodeName);
            return false;
        }

        const auto& settings = *operation.Ref().Child(4U);
        if (HasSettingsExcept(settings, DqOpSupportedSettings)) {
            if (!NYql::HasSetting(settings, EYtSettingType::NoDq)) {
                PushSkipStat("UnsupportedDqOpSettings", nodeName);
                PushSettingsToStat(settings, nodeName, "SkipDqOpSettings", DqOpSupportedSettings);
            }
            return false;
        }

        return true;
    }

    bool HasDescOrderOutput(const TYtOutputOpBase& operation) const {
        TYqlRowSpecInfo outRowSpec(operation.Output().Item(0).RowSpec());
        if (outRowSpec.IsSorted() && outRowSpec.HasAuxColumns()) {
            PushSkipStat("DescSort", operation.Raw()->Content());
            return true;
        }
        return false;
    }

    bool CanReadHybrid(const TYtSection& section, const TStringBuf& nodeName, bool orderedInput) const {
        const auto& settings = section.Settings().Ref();
        if (HasSettingsExcept(settings, DqReadSupportedSettings)) {
            PushSkipStat("UnsupportedDqReadSettings", nodeName);
            PushSettingsToStat(settings, nodeName, "SkipDqReadSettings", DqReadSupportedSettings);
            return false;
        }
        if (HasNonEmptyKeyFilter(section)) {
            PushSkipStat("NonEmptyKeyFilter", nodeName);
            return false;
        }
        ui64 dataSize = 0ULL, dataChunks = 0ULL;
        for (const auto& path : section.Paths()) {
            const TYtPathInfo info(path);
            const auto& tableInfo = info.Table;
            if (!tableInfo || !tableInfo->Stat || !tableInfo->Meta || !tableInfo->RowSpec) {
                return false;
            }
            const auto canUseYtPartitioningApi = State_->Configuration->_EnableYtPartitioning.Get(tableInfo->Cluster).GetOrElse(false);
            if ((info.Ranges || tableInfo->Meta->IsDynamic) && !canUseYtPartitioningApi) {
                return false;
            }
            if (NYql::HasSetting(tableInfo->Settings.Ref(), EYtSettingType::WithQB)) {
                PushSkipStat("WithQB", nodeName);
                return false;
            }
            auto tableSize = tableInfo->Stat->DataSize;
            if (tableInfo->Meta->Attrs.Value("erasure_codec", "none") != "none") {
                if (const auto codecCpu = State_->Configuration->ErasureCodecCpuForDq.Get(tableInfo->Cluster)) {
                    tableSize *=* codecCpu;
                }
            }

            dataSize += tableSize;
            dataChunks += tableInfo->Stat->ChunkCount;
        }
        const auto chunksLimit = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ);

        const auto sizeLimit = orderedInput ?
            State_->Configuration->HybridDqDataSizeLimitForOrdered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForOrdered):
            State_->Configuration->HybridDqDataSizeLimitForUnordered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForUnordered);

        if (dataSize > sizeLimit || dataChunks > chunksLimit) {
            PushSkipStat("OverLimits", nodeName);
            return false;
        }
                
        return true;
    }

    bool CanExecuteInHybrid(const TExprNode::TPtr& handler, const TStringBuf& nodeName, bool orderedInput) const {
        bool flow = false;
        auto sources = FindNodes(handler,
                                        [](const TExprNode::TPtr& node) {
                                            return !TYtOutput::Match(node.Get());
                                        },
                                        [](const TExprNode::TPtr& node) {
                                            return TYtTableContent::Match(node.Get());
                                        });
        TNodeSet flowSources;
        std::for_each(sources.cbegin(), sources.cend(), [&flowSources](const TExprNode::TPtr& node) { flowSources.insert(node.Get()); });
        bool noNonTransparentNode = !FindNonYieldTransparentNode(handler, *State_->Types, flowSources);
        if (!noNonTransparentNode) {
            PushSkipStat("NonTransparentNode", nodeName);
        }
        return noNonTransparentNode &&
            !FindNode(handler, [&flow, this, &nodeName, orderedInput] (const TExprNode::TPtr& node) {
                if (TCoScriptUdf::Match(node.Get()) && NKikimr::NMiniKQL::IsSystemPython(NKikimr::NMiniKQL::ScriptTypeFromStr(node->Head().Content()))) {
                    return true;
                }

                if (const auto& tableContent = TMaybeNode<TYtTableContent>(node)) {
                    if (!flow)
                        return true;
                    if (const auto& maybeRead = tableContent.Cast().Input().Maybe<TYtReadTable>()) {
                        const auto& read = maybeRead.Cast();
                        if (1U != read.Input().Size()) {
                            PushSkipStat("MultipleInputs", nodeName);
                            return true;
                        }
                        if(!CanReadHybrid(read.Input().Item(0), nodeName, orderedInput)) {
                            return true;
                        }
                        return false;
                    }
                }
                flow = node->IsCallable(TCoToFlow::CallableName()) && node->Head().IsCallable(TYtTableContent::CallableName());
                return false;
            });
    }

    TMaybeNode<TExprBase> TryYtFillByDq(TExprBase node, TExprContext& ctx) const {
        const TStringBuf nodeName = node.Raw()->Content();
        const auto fill = node.Cast<TYtFill>();
        if (CanReplaceOnHybrid(fill) && CanExecuteInHybrid(fill.Content().Ptr(), nodeName, true)) {
            YQL_CLOG(INFO, ProviderYt) << "Rewrite " << nodeName << " node by hybrid";
            PushHybridStat("Try", nodeName);
            return Build<TYtTryFirst>(ctx, fill.Pos())
                .First<TYtDqProcessWrite>()
                    .World(fill.World())
                    .DataSink(fill.DataSink())
                    .Output(fill.Output())
                    .Input<TDqCnResult>()
                        .Output()
                            .Stage<TDqStage>()
                                .Inputs().Build()
                                .Program<TCoLambda>()
                                    .Args({})
                                    .Body<TDqWrite>()
                                        .Input(CloneCompleteFlow(fill.Content().Body().Ptr(), ctx))
                                        .Provider().Value(YtProviderName).Build()
                                        .Settings<TCoNameValueTupleList>().Build()
                                    .Build()
                                .Build()
                                .Settings(TDqStageSettings{.PartitionMode = TDqStageSettings::EPartitionMode::Single}.BuildNode(ctx, fill.Pos()))
                            .Build()
                            .Index().Build(ctx.GetIndexAsString(0), TNodeFlags::Default)
                        .Build()
                        .ColumnHints().Build()
                    .Build()
                    .Flags().Add(GetHybridFlags(fill, ctx)).Build()
                .Build()
                .Second<TYtFill>()
                    .InitFrom(fill)
                    .Settings(NYql::AddSetting(fill.Settings().Ref(), EYtSettingType::NoDq, {}, ctx))
                .Build()
                .Done();
        }
        return node;
    }

    TExprBase MakeYtSortByDq(const TYtTransientOpBase& sort, TExprContext& ctx) const {
        TSyncMap syncList;
        const auto& paths = sort.Input().Item(0).Paths();
        for (auto i = 0U; i < paths.Size(); ++i) {
            if (const auto mayOut = paths.Item(i).Table().Maybe<TYtOutput>()) {
                syncList.emplace(GetOutputOp(mayOut.Cast()).Ptr(), syncList.size());
            }
        }
        auto newWorld = ApplySyncListToWorld(sort.World().Ptr(), syncList, ctx);

        const auto input = Build<TCoToFlow>(ctx, sort.Pos())
            .Input<TYtTableContent>()
                .Input<TYtReadTable>()
                    .World<TCoWorld>().Build()
                    .DataSource<TYtDSource>()
                        .Category(sort.DataSink().Category())
                        .Cluster(sort.DataSink().Cluster())
                        .Build()
                    .Input(sort.Input())
                    .Build()
                .Settings().Build()
                .Build()
            .Done();

        TExprNode::TPtr limit;
        if (const auto& limitNode = NYql::GetSetting(sort.Settings().Ref(), EYtSettingType::Limit)) {
            limit = GetLimitExpr(limitNode, ctx);
        }

        auto [direct, selector] = GetOutputSortSettings(sort, ctx);
        auto work = direct && selector ?
            limit ?
                Build<TCoTopSort>(ctx, sort.Pos())
                    .Input(input)
                    .Count(std::move(limit))
                    .SortDirections(std::move(direct))
                    .KeySelectorLambda(std::move(selector))
                    .Done().Ptr():
                Build<TCoSort>(ctx, sort.Pos())
                    .Input(input)
                    .SortDirections(std::move(direct))
                    .KeySelectorLambda(std::move(selector))
                    .Done().Ptr():
            limit ?
                Build<TCoTake>(ctx, sort.Pos())
                    .Input(input)
                    .Count(std::move(limit))
                    .Done().Ptr():
                input.Ptr();

        auto settings = NYql::AddSetting(sort.Settings().Ref(), EYtSettingType::NoDq, {}, ctx);
        auto operation = ctx.ChangeChild(sort.Ref(), TYtTransientOpBase::idx_Settings, std::move(settings));

        return Build<TYtTryFirst>(ctx, sort.Pos())
            .First<TYtDqProcessWrite>()
                .World(std::move(newWorld))
                .DataSink(sort.DataSink())
                .Output(sort.Output())
                .Input<TDqCnResult>()
                    .Output()
                        .Stage<TDqStage>()
                            .Inputs().Build()
                            .Program<TCoLambda>()
                                .Args({})
                                .Body<TDqWrite>()
                                    .Input(std::move(work))
                                    .Provider().Value(YtProviderName).Build()
                                    .Settings<TCoNameValueTupleList>().Build()
                                .Build()
                            .Build()
                            .Settings(TDqStageSettings{.PartitionMode = TDqStageSettings::EPartitionMode::Single}.BuildNode(ctx, sort.Pos()))
                        .Build()
                        .Index().Build(ctx.GetIndexAsString(0), TNodeFlags::Default)
                    .Build()
                    .ColumnHints().Build()
                .Build()
                .Flags().Add(GetHybridFlags(sort, ctx)).Build()
            .Build()
            .Second(std::move(operation))
            .Done();
    }

    TMaybeNode<TExprBase> TryYtSortByDq(TExprBase node, TExprContext& ctx) const {
        const auto sort = node.Cast<TYtSort>();
        const TStringBuf nodeName = node.Raw()->Content();
        if (CanReplaceOnHybrid(sort) && CanReadHybrid(sort.Input().Item(0), node.Raw()->Content(), true) && !HasDescOrderOutput(sort)) {
            PushHybridStat("Try", nodeName);
            YQL_CLOG(INFO, ProviderYt) << "Rewrite " << nodeName << " node by hybrid";
            return MakeYtSortByDq(sort, ctx);
        }
        return node;
    }

    TMaybeNode<TExprBase> TryYtMergeByDq(TExprBase node, TExprContext& ctx) const {
        const auto merge = node.Cast<TYtMerge>();
        const TStringBuf nodeName = node.Raw()->Content();
        if (CanReplaceOnHybrid(merge) && CanReadHybrid(merge.Input().Item(0), node.Raw()->Content(), true) && !HasDescOrderOutput(merge)) {
            PushHybridStat("Try", nodeName);
            YQL_CLOG(INFO, ProviderYt) << "Rewrite " << nodeName << " node by hybrid";
            return MakeYtSortByDq(merge, ctx);
        }
        return node;
    }

    TMaybeNode<TExprBase> TryYtMapByDq(TExprBase node, TExprContext& ctx) const {
        const TStringBuf& nodeName = node.Raw()->Content();
        const auto map = node.Cast<TYtMap>();
        bool ordered = NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Ordered);
        if (!ordered) {
            auto setting = NYql::GetSetting(map.Settings().Ref(), EYtSettingType::JobCount);
            if (setting && FromString<ui64>(setting->Child(1)->Content()) == 1) {
                ordered = true;
            }
        }
        if (CanReplaceOnHybrid(map) && CanReadHybrid(map.Input().Item(0), nodeName, ordered) && CanExecuteInHybrid(map.Mapper().Ptr(), nodeName, ordered)) {
            YQL_CLOG(INFO, ProviderYt) << "Rewrite " << nodeName << " node by hybrid";
            PushHybridStat("Try", nodeName);
            TSyncMap syncList;
            const auto& paths = map.Input().Item(0).Paths();
            for (auto i = 0U; i < paths.Size(); ++i) {
                if (const auto mayOut = paths.Item(i).Table().Maybe<TYtOutput>()) {
                    syncList.emplace(GetOutputOp(mayOut.Cast()).Ptr(), syncList.size());
                }
            }
            auto newWorld = ApplySyncListToWorld(map.World().Ptr(), syncList, ctx);

            auto settings = ctx.NewList(map.Input().Pos(), {});
            if (!ordered) {
                settings = NYql::AddSetting(*settings, EYtSettingType::Split, nullptr, ctx);
            }

            auto stage = Build<TDqStage>(ctx, map.Pos())
                .Inputs().Build()
                .Program<TCoLambda>()
                    .Args({})
                    .Body<TDqWrite>()
                        .Input<TExprApplier>()
                            .Apply(map.Mapper())
                            .With<TCoToFlow>(0)
                                .Input<TYtTableContent>()
                                    .Input<TYtReadTable>()
                                        .World<TCoWorld>().Build()
                                        .DataSource<TYtDSource>()
                                            .Category(map.DataSink().Category())
                                            .Cluster(map.DataSink().Cluster())
                                        .Build()
                                        .Input(map.Input())
                                    .Build()
                                    .Settings(std::move(settings))
                                .Build()
                            .Build()
                        .Build()
                        .Provider().Value(YtProviderName).Build()
                        .Settings<TCoNameValueTupleList>().Build()
                    .Build()
                .Build()
                .Settings(TDqStageSettings{.PartitionMode = ordered ? TDqStageSettings::EPartitionMode::Single : TDqStageSettings::EPartitionMode::Default}.BuildNode(ctx, map.Pos()))
                .Done();

            if (!ordered) {
                stage = Build<TDqStage>(ctx, map.Pos())
                    .Inputs()
                        .Add<TDqCnUnionAll>()
                            .Output()
                                .Stage(std::move(stage))
                                .Index().Build(ctx.GetIndexAsString(0), TNodeFlags::Default)
                                .Build()
                            .Build()
                        .Build()
                    .Program().Args({"pass"}).Body("pass").Build()
                    .Settings(TDqStageSettings().BuildNode(ctx, map.Pos()))
                    .Done();
            }

            return Build<TYtTryFirst>(ctx, map.Pos())
                .First<TYtDqProcessWrite>()
                    .World(std::move(newWorld))
                    .DataSink(map.DataSink())
                    .Output(map.Output())
                    .Input<TDqCnResult>()
                        .Output()
                            .Stage(std::move(stage))
                            .Index().Build(ctx.GetIndexAsString(0), TNodeFlags::Default)
                            .Build()
                        .ColumnHints().Build()
                        .Build()
                    .Flags().Add(GetHybridFlags(map, ctx)).Build()
                    .Build()
                .Second<TYtMap>()
                    .InitFrom(map)
                    .Settings(NYql::AddSetting(map.Settings().Ref(), EYtSettingType::NoDq, {}, ctx))
                    .Build()
                .Done();
        }
        return node;
    }

    template<class TYtOperation>
    TExprBase MakeYtReduceByDq(const TYtOperation& reduce, TExprContext& ctx) const {
        TSyncMap syncList;
        const auto& paths = reduce.Input().Item(0).Paths();
        for (auto i = 0U; i < paths.Size(); ++i) {
            if (const auto mayOut = paths.Item(i).Table().template Maybe<TYtOutput>()) {
                syncList.emplace(GetOutputOp(mayOut.Cast()).Ptr(), syncList.size());
            }
        }
        auto newWorld = ApplySyncListToWorld(reduce.World().Ptr(), syncList, ctx);

        auto keys = NYql::GetSettingAsColumnAtomList(reduce.Settings().Ref(), EYtSettingType::ReduceBy);

        auto sortKeys = ctx.NewCallable(reduce.Pos(), "Void", {});
        auto sortDirs = sortKeys;

        const auto keysBuilder = [](TExprNode::TListType& keys, TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            if (1U == keys.size()) {
                parent.Callable("Member")
                    .Arg(0, "row")
                    .Add(1, std::move(keys.front()))
                .Seal();
            } else {
                parent.List()
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (auto i = 0U; i < keys.size(); ++i) {
                            parent.Callable(i, "Member")
                                .Arg(0, "row")
                                .Add(1, std::move(keys[i]))
                            .Seal();
                        }
                        return parent;
                    })
                .Seal();
            }
            return parent;
        };

        if (auto sortBy =  NYql::GetSettingAsColumnAtomPairList(reduce.Settings().Ref(), EYtSettingType::SortBy); !sortBy.empty()) {
            for (auto it = keys.cbegin(); !sortBy.empty() && keys.cend() != it && *it == sortBy.front().first; ++it)
                sortBy.erase(sortBy.cbegin());

            if (!sortBy.empty()) {
                TExprNode::TListType sort, dirs;
                sort.reserve(sortBy.size());
                dirs.reserve(sortBy.size());

                for (auto& item : sortBy) {
                    sort.emplace_back(std::move(item.first));
                    dirs.emplace_back(MakeBool(reduce.Pos(), item.second, ctx));
                }

                sortDirs = 1U == dirs.size() ? std::move(dirs.front()) : ctx.NewList(reduce.Pos(), std::move(dirs));
                sortKeys = ctx.Builder(reduce.Pos())
                    .Lambda()
                        .Param("row")
                        .Do(std::bind(keysBuilder, std::ref(sort), std::placeholders::_1))
                    .Seal().Build();
            }
        }

        const auto extract = TCoLambda(ctx.Builder(reduce.Pos())
            .Lambda()
                .Param("row")
                .Do(std::bind(keysBuilder, std::ref(keys), std::placeholders::_1))
            .Seal().Build());

        const bool hasGetSysKeySwitch = bool(FindNode(reduce.Reducer().Body().Ptr(),
            [](const TExprNode::TPtr& node) {
                return !TYtOutput::Match(node.Get());
            },
            [](const TExprNode::TPtr& node) {
                return TCoMember::Match(node.Get()) && node->Head().IsArgument() && node->Tail().IsAtom(YqlSysColumnKeySwitch);
            }
        ));

        auto input = Build<TCoToFlow>(ctx, reduce.Pos())
            .template Input<TYtTableContent>()
                .template Input<TYtReadTable>()
                    .template World<TCoWorld>().Build()
                    .template DataSource<TYtDSource>()
                        .Category(reduce.DataSink().Category())
                        .Cluster(reduce.DataSink().Cluster())
                        .Build()
                    .Input(reduce.Input())
                    .Build()
                .Settings().Build()
                .Build()
            .Done().Ptr();

        if constexpr (std::is_same<TYtOperation, TYtMapReduce>::value) {
            if (const auto maybeMapper = reduce.Mapper().template Maybe<TCoLambda>()) {
                input = Build<TExprApplier>(ctx, reduce.Pos()).Apply(maybeMapper.Cast()).With(0, TExprBase(std::move(input))).Done().Ptr();
            }
        }

        auto reducer = Build<TCoLambda>(ctx, reduce.Pos())
                            .Args({"list"})
                            .Body("list")
                            .Done();

        if (hasGetSysKeySwitch) {
            reducer = Build<TCoLambda>(ctx, reducer.Pos())
                            .Args({"list"})
                            .template Body<TCoChain1Map>()
                                .Input("list")
                                .InitHandler()
                                    .Args({"first"})
                                    .template Body<TCoAddMember>()
                                        .Struct("first")
                                        .Name().Value(YqlSysColumnKeySwitch).Build()
                                        .Item(MakeBool<false>(reduce.Pos(), ctx))
                                        .Build()
                                    .Build()
                                .UpdateHandler()
                                    .Args({"next", "prev"})
                                    .template Body<TCoAddMember>()
                                        .Struct("next")
                                        .Name().Value(YqlSysColumnKeySwitch).Build()
                                        .template Item<TCoAggrNotEqual>()
                                            .template Left<TExprApplier>()
                                                .Apply(extract).With(0, "next")
                                                .Build()
                                            .template Right<TExprApplier>()
                                                .Apply(extract).With(0, "prev")
                                                .Build()
                                            .Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Done();
        }

        const auto& items = GetSeqItemType(*reduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()).template Cast<TStructExprType>()->GetItems();
        TExprNode::TListType fields(items.size());
        std::transform(items.cbegin(), items.cend(), fields.begin(), [&](const TItemExprType* item) { return ctx.NewAtom(reduce.Pos(), item->GetName()); });
        auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(reduce.Reducer().Ptr(), ctx, State_->Types);
        reducer = Build<TCoLambda>(ctx, reduce.Pos())
                    .Args({"list"})
                    .template Body<TExprApplier>()
                        .Apply(TCoLambda(lambdaWithPlaceholder))
                        .template With<TCoExtractMembers>(0)
                            .template Input<TExprApplier>()
                                .Apply(reducer)
                                .With(0, "list")
                                .Build()
                            .Members().Add(std::move(fields)).Build()
                            .Build()
                        .With(TExprBase(placeHolder), "list")
                        .Build()
                    .Done();

        auto partitionsByKeys = Build<TCoPartitionsByKeys>(ctx, reduce.Pos())
                                                .Input(std::move(input))
                                                .KeySelectorLambda(extract)
                                                .SortDirections(std::move(sortDirs))
                                                .SortKeySelectorLambda(std::move(sortKeys))
                                                .ListHandlerLambda(std::move(reducer))
                                            .Done().Ptr();

        auto [direct, selector] = GetOutputSortSettings(reduce, ctx);
        if (direct && selector) {
            partitionsByKeys = Build<TCoSort>(ctx, reduce.Pos())
                    .Input(std::move(partitionsByKeys))
                    .SortDirections(std::move(direct))
                    .KeySelectorLambda(std::move(selector))
                    .Done().Ptr();
        }

        return Build<TYtTryFirst>(ctx, reduce.Pos())
            .template First<TYtDqProcessWrite>()
                .World(std::move(newWorld))
                .DataSink(reduce.DataSink())
                .Output(reduce.Output())
                .template Input<TDqCnResult>()
                    .Output()
                        .template Stage<TDqStage>()
                            .Inputs().Build()
                            .template Program<TCoLambda>()
                                .Args({})
                                .template Body<TDqWrite>()
                                    .Input(std::move(partitionsByKeys))
                                    .Provider().Value(YtProviderName).Build()
                                    .template Settings<TCoNameValueTupleList>().Build()
                                .Build()
                            .Build()
                            .Settings(TDqStageSettings{.PartitionMode = TDqStageSettings::EPartitionMode::Single}.BuildNode(ctx, reduce.Pos()))
                        .Build()
                        .Index().Build(ctx.GetIndexAsString(0), TNodeFlags::Default)
                    .Build()
                    .ColumnHints().Build()
                .Build()
                .Flags().Add(GetHybridFlags(reduce, ctx)).Build()
            .Build()
            .template Second<TYtOperation>()
                .InitFrom(reduce)
                .Settings(NYql::AddSetting(reduce.Settings().Ref(), EYtSettingType::NoDq, {}, ctx))
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> TryYtReduceByDq(TExprBase node, TExprContext& ctx) const {
        const TStringBuf& nodeName = node.Raw()->Content();
        const auto reduce = node.Cast<TYtReduce>();
        if (CanReplaceOnHybrid(reduce) && CanReadHybrid(reduce.Input().Item(0), nodeName, true) && CanExecuteInHybrid(reduce.Reducer().Ptr(), nodeName, true)) {
            if (ETypeAnnotationKind::Struct != GetSeqItemType(*reduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()).GetKind()) {
                PushSkipStat("NotStructReducerType", nodeName);
                return node;
            }
            YQL_CLOG(INFO, ProviderYt) << "Rewrite " << nodeName << " node by hybrid";
            PushHybridStat("Try", nodeName);
            return MakeYtReduceByDq(reduce, ctx);
        }
        return node;
    }

    TMaybeNode<TExprBase> TryYtMapReduceByDq(TExprBase node, TExprContext& ctx) const {
        const TStringBuf& nodeName = node.Raw()->Content();
        const auto mapReduce = node.Cast<TYtMapReduce>();
        if (CanReplaceOnHybrid(mapReduce) && CanReadHybrid(mapReduce.Input().Item(0), nodeName, true) &&
            CanExecuteInHybrid(mapReduce.Reducer().Ptr(), nodeName, true) && CanExecuteInHybrid(mapReduce.Mapper().Ptr(), nodeName, true)) {
            if (ETypeAnnotationKind::Struct != GetSeqItemType(*mapReduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()).GetKind()) {
                PushSkipStat("NotStructReducerType", nodeName);
                return node;
            }
            YQL_CLOG(INFO, ProviderYt) << "Rewrite " << nodeName << " node by hybrid";
            PushHybridStat("Try", nodeName);
            return MakeYtReduceByDq(mapReduce, ctx);
        }
        return node;
    }

    std::pair<TExprNode::TPtr, TExprNode::TPtr> GetOutputSortSettings(const TYtOutputOpBase& op, TExprContext& ctx) const {
        TExprNode::TPtr direct, selector;
        if (const auto& sorted = TYtOutTableInfo(op.Output().Item(0)).RowSpec->GetForeignSort(); !sorted.empty()) {
            TExprNode::TListType nodes(sorted.size());
            std::transform(sorted.cbegin(), sorted.cend(), nodes.begin(),
                                [&](const std::pair<TString, bool>& item) { return MakeBool(op.Pos(), item.second, ctx); });
            direct = nodes.size() > 1U ? ctx.NewList(op.Pos(), std::move(nodes)) : std::move(nodes.front());
            nodes.resize(sorted.size());
            auto arg = ctx.NewArgument(op.Pos(), "row");
            std::transform(sorted.cbegin(), sorted.cend(), nodes.begin(), [&](const std::pair<TString, bool>& item) {
                return ctx.NewCallable(op.Pos(), "Member", {arg, ctx.NewAtom(op.Pos(), item.first)});
            });
            selector = ctx.NewLambda(op.Pos(), ctx.NewArguments(op.Pos(), {std::move(arg)}),
                                nodes.size() > 1U ? ctx.NewList(op.Pos(), std::move(nodes)) : std::move(nodes.front()));
        }
        return std::make_pair(direct, selector);
    }

    void PushSkipStat(const TStringBuf& statName, const TStringBuf& nodeName) const {
        PushHybridStat(statName, nodeName, "SkipReasons");
        PushHybridStat("Skip", nodeName);
    }

    void PushHybridStat(const TStringBuf& statName, const TStringBuf& nodeName, const TStringBuf& folderName = "") const {
        with_lock(State_->StatisticsMutex) {
            State_->HybridStatistics[folderName].Entries.emplace_back(TString{statName}, 0, 0, 0, 0, 1);
            State_->HybridOpStatistics[nodeName][folderName].Entries.emplace_back(TString{statName}, 0, 0, 0, 0, 1);
        }
    };

    void PushSettingsToStat(const TExprNode & settings, const TStringBuf& nodeName, const TStringBuf& folderName, EYtSettingTypes types) const {
        for (auto& setting: settings.Children()) {
            if (setting->ChildrenSize() != 0 && !types.HasFlags(FromString<EYtSettingType>(setting->Child(0)->Content()))) {
                PushHybridStat(setting->Child(0)->Content(), nodeName, folderName);
            }
        }
    }

    TExprNode::TListType GetHybridFlags(TExprBase node, TExprContext& ctx) const {
        TExprNode::TListType flags;
        flags.emplace_back(ctx.NewAtom(node.Pos(), "FallbackOnError", TNodeFlags::Default));
        flags.emplace_back(ctx.NewAtom(node.Pos(), "FallbackOp" + TString(node.Raw()->Content()), TNodeFlags::Default));
        return flags;
    }

    const TYtState::TPtr State_;
    const THolder<IGraphTransformer> Finalizer_;
};

} // namespce

THolder<IGraphTransformer> CreateYtDqHybridTransformer(TYtState::TPtr state, THolder<IGraphTransformer>&& finalizer) {
    return THolder<IGraphTransformer>(new TYtDqHybridTransformer(std::move(state), std::move(finalizer)));
}

} // namespace NYql
