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
        if (!State_->IsHybridEnabledForCluster(operation.DataSink().Cluster().Value()))
            return false;

        if (operation.Ref().StartsExecution() || operation.Ref().HasResult())
            return false;

        if (operation.Output().Size() != 1U)
            return false;

        if (const auto& trans = operation.Maybe<TYtTransientOpBase>(); trans && trans.Cast().Input().Size() != 1U)
            return false;

        return !HasSetting(*operation.Ref().Child(4U), EYtSettingType::NoDq);
    }

    std::optional<std::array<ui64, 2U>> CanReadHybrid(const TYtSection& section) const {
        if (HasSettingsExcept(section.Settings().Ref(), DqReadSupportedSettings))
            return std::nullopt;

        std::array<ui64, 2U> stat = {{0ULL, 0ULL}};
        for (const auto& path : section.Paths()) {
            if (const TYtPathInfo info(path); info.Ranges)
                return std::nullopt;
            else if (const auto& tableInfo = info.Table;
                !tableInfo || !tableInfo->Stat || !tableInfo->Meta || !tableInfo->RowSpec || tableInfo->Meta->IsDynamic || NYql::HasSetting(tableInfo->Settings.Ref(), EYtSettingType::WithQB))
                return std::nullopt;
            else {
                auto tableSize = tableInfo->Stat->DataSize;
                if (tableInfo->Meta->Attrs.Value("erasure_codec", "none") != "none") {
                    if (const auto codecCpu = State_->Configuration->ErasureCodecCpuForDq.Get(tableInfo->Cluster)) {
                        tableSize *=* codecCpu;
                    }
                }

                stat.front() += tableSize;
                stat.back() += tableInfo->Stat->ChunkCount;
            }
        }
        return stat;
    }

    TMaybeNode<TExprBase> TryYtFillByDq(TExprBase node, TExprContext& ctx) const {
        if (const auto fill = node.Cast<TYtFill>(); CanReplaceOnHybrid(fill)) {
            const auto sizeLimit = State_->Configuration->HybridDqDataSizeLimitForOrdered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForOrdered);
            const auto chunksLimit = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ);
            if (bool flow = false; !FindNode(fill.Content().Ptr(), [&flow, sizeLimit, chunksLimit, this] (const TExprNode::TPtr& node) {
                if (node->IsCallable(TCoForwardList::CallableName()))
                    return true;
                if (node->IsCallable(TCoCollect::CallableName()) && ETypeAnnotationKind::List != node->Head().GetTypeAnn()->GetKind())
                    return true;
                if (const auto tableContent = TMaybeNode<TYtTableContent>(node)) {
                    if (!flow)
                        return true;
                    if (const auto& maybeRead = tableContent.Cast().Input().Maybe<TYtReadTable>()) {
                        if (const auto& read = maybeRead.Cast(); 1U != read.Input().Size())
                            return true;
                        else {
                            const auto stat = CanReadHybrid(read.Input().Item(0));
                            return !stat || stat->front() > sizeLimit || stat->back() > chunksLimit;
                        }
                    }
                }
                flow = node->IsCallable(TCoToFlow::CallableName()) && node->Head().IsCallable(TYtTableContent::CallableName());
                return false;
            })) {
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
                                            .Settings<TCoNameValueTupleList>()
                                                .Add()
                                                    .Name().Value("table").Build()
                                                    .Value(fill.Output().Item(0)).Build()
                                                .Build()
                                            .Build()
                                        .Build()
                                    .Settings(TDqStageSettings{.SinglePartition = true}.BuildNode(ctx, fill.Pos()))
                                    .Build()
                                .Index().Build(ctx.GetIndexAsString(0), TNodeFlags::Default)
                                .Build()
                            .ColumnHints().Build()
                            .Build()
                        .Flags().Add().Build("FallbackOnError", TNodeFlags::Default).Build()
                        .Build()
                    .Second<TYtFill>()
                        .InitFrom(fill)
                        .Settings(NYql::AddSetting(fill.Settings().Ref(), EYtSettingType::NoDq, {}, ctx))
                        .Build()
                    .Done();
            }
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

        TExprNode::TPtr direct, selector;
        if (const auto& sorted = TYtOutTableInfo(sort.Output().Item(0)).RowSpec->GetForeignSort(); !sorted.empty()) {
            TExprNode::TListType nodes(sorted.size());
            std::transform(sorted.cbegin(), sorted.cend(), nodes.begin(), [&](const std::pair<TString, bool>& item) { return MakeBool(sort.Pos(), item.second, ctx); });
            direct = nodes.size() > 1U ? ctx.NewList(sort.Pos(), std::move(nodes)) : std::move(nodes.front());
            nodes.resize(sorted.size());
            auto arg = ctx.NewArgument(sort.Pos(), "row");
            std::transform(sorted.cbegin(), sorted.cend(), nodes.begin(), [&](const std::pair<TString, bool>& item) {
                return ctx.NewCallable(sort.Pos(), "Member", {arg, ctx.NewAtom(sort.Pos(), item.first)});
            });
            selector = ctx.NewLambda(sort.Pos(), ctx.NewArguments(sort.Pos(), {std::move(arg)}), nodes.size() > 1U ? ctx.NewList(sort.Pos(), std::move(nodes)) : std::move(nodes.front()));
        }

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
                                    .Settings<TCoNameValueTupleList>()
                                        .Add()
                                            .Name().Value("table", TNodeFlags::Default).Build()
                                            .Value(sort.Output().Item(0)).Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Settings(TDqStageSettings{.SinglePartition = true}.BuildNode(ctx, sort.Pos()))
                            .Build()
                        .Index().Build(ctx.GetIndexAsString(0), TNodeFlags::Default)
                        .Build()
                    .ColumnHints().Build()
                    .Build()
                .Flags().Add().Build("FallbackOnError", TNodeFlags::Default).Build()
                .Build()
            .Second(std::move(operation))
            .Done();
    }

    TMaybeNode<TExprBase> TryYtSortByDq(TExprBase node, TExprContext& ctx) const {
        if (const auto sort = node.Cast<TYtSort>(); CanReplaceOnHybrid(sort)) {
            if (const auto sizeLimit = State_->Configuration->HybridDqDataSizeLimitForOrdered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForOrdered)) {
                const auto info = TYtTableBaseInfo::Parse(sort.Input().Item(0).Paths().Item(0).Table());
                const auto chunksLimit = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ);
                if (const auto stat = CanReadHybrid(sort.Input().Item(0))) {
                    if (stat->front() <= sizeLimit && stat->back() <= chunksLimit) {
                        YQL_CLOG(INFO, ProviderYt) << "Sort on DQ with equivalent input size " << stat->front() << " and " << stat->back() << " chunks.";
                        PushStat("Hybrid_Sort_try");
                        return MakeYtSortByDq(sort, ctx);
                    }
                    PushStat("Hybrid_Sort_over_limits");
                }
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> TryYtMergeByDq(TExprBase node, TExprContext& ctx) const {
        if (const auto merge = node.Cast<TYtMerge>(); CanReplaceOnHybrid(merge) && !NYql::HasSetting(merge.Settings().Ref(), EYtSettingType::CombineChunks)) {
            if (const auto sizeLimit = State_->Configuration->HybridDqDataSizeLimitForOrdered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForOrdered)) {
                const auto info = TYtTableBaseInfo::Parse(merge.Input().Item(0).Paths().Item(0).Table());
                const auto chunksLimit = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ);
                if (const auto stat = CanReadHybrid(merge.Input().Item(0))) {
                    if (stat->front() <= sizeLimit && stat->back() <= chunksLimit) {
                        YQL_CLOG(INFO, ProviderYt) << "Merge on DQ with equivalent input size " << stat->front() << " and " << stat->back() << " chunks.";
                        PushStat("Hybrid_Merge_try");
                        return MakeYtSortByDq(merge, ctx);
                    }
                    PushStat("Hybrid_Merge_over_limits");
                }
            }
        }

        return node;
    }

    bool CanExecuteInHybrid(const TExprNode::TPtr& handler, ui64 chunksLimit, ui64 sizeLimit) const {
        bool flow = false;
        return IsYieldTransparent(handler, *State_->Types) &&
            !FindNode(handler, [&flow, sizeLimit, chunksLimit, this] (const TExprNode::TPtr& node) {
                if (TCoScriptUdf::Match(node.Get()) && NKikimr::NMiniKQL::IsSystemPython(NKikimr::NMiniKQL::ScriptTypeFromStr(node->Head().Content()))) {
                    return true;
                }

                if (const auto& tableContent = TMaybeNode<TYtTableContent>(node)) {
                    if (!flow)
                        return true;
                    if (const auto& maybeRead = tableContent.Cast().Input().Maybe<TYtReadTable>()) {
                        if (const auto& read = maybeRead.Cast(); 1U != read.Input().Size())
                            return true;
                        else {
                            const auto stat = CanReadHybrid(read.Input().Item(0));
                            return !stat || stat->front() > sizeLimit || stat->back() > chunksLimit;
                        }
                    }
                }
                flow = node->IsCallable(TCoToFlow::CallableName()) && node->Head().IsCallable(TYtTableContent::CallableName());
                return false;
            });
    }

    TMaybeNode<TExprBase> TryYtMapByDq(TExprBase node, TExprContext& ctx) const {
        if (const auto map = node.Cast<TYtMap>(); CanReplaceOnHybrid(map)) {
            const auto chunksLimit = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ);
            const bool ordered = NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Ordered);
            const auto sizeLimit = ordered ?
                State_->Configuration->HybridDqDataSizeLimitForOrdered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForOrdered):
                State_->Configuration->HybridDqDataSizeLimitForUnordered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForUnordered);
            if (const auto stat = CanReadHybrid(map.Input().Item(0))) {
                if (stat->front() <= sizeLimit && stat->back() <= chunksLimit) {
                    if (CanExecuteInHybrid(map.Mapper().Ptr(), chunksLimit, sizeLimit)) {
                        YQL_CLOG(INFO, ProviderYt) << "Map on DQ with equivalent input size " << stat->front() << " and " << stat->back() << " chunks.";
                        PushStat("Hybrid_Map_try");
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
                                    .Settings<TCoNameValueTupleList>()
                                        .Add()
                                            .Name().Value("table", TNodeFlags::Default).Build()
                                            .Value(map.Output().Item(0)).Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Settings(TDqStageSettings{.SinglePartition = ordered}.BuildNode(ctx, map.Pos()))
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
                                .Flags().Add().Build("FallbackOnError", TNodeFlags::Default).Build()
                                .Build()
                            .Second<TYtMap>()
                                .InitFrom(map)
                                .Settings(NYql::AddSetting(map.Settings().Ref(), EYtSettingType::NoDq, {}, ctx))
                                .Build()
                            .Done();
                    }
                }
                PushStat("Hybrid_Map_over_limits");
            }
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

        auto arg = ctx.NewArgument(reduce.Pos(), "list");

        auto body = hasGetSysKeySwitch ? Build<TCoChain1Map>(ctx, reduce.Pos())
            .Input(arg)
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
            .Done().Ptr() : arg;

        const auto& items = GetSeqItemType(*reduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()).template Cast<TStructExprType>()->GetItems();
        TExprNode::TListType fields(items.size());
        std::transform(items.cbegin(), items.cend(), fields.begin(), [&](const TItemExprType* item) { return ctx.NewAtom(reduce.Pos(), item->GetName()); });
        body = Build<TExprApplier>(ctx, reduce.Pos())
            .Apply(reduce.Reducer())
                .template With<TCoExtractMembers>(0)
                    .Input(std::move(body))
                    .Members().Add(std::move(fields)).Build()
                    .Build()
            .Done().Ptr();

        auto reducer = ctx.NewLambda(reduce.Pos(), ctx.NewArguments(reduce.Pos(), {std::move(arg)}), std::move(body));

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
                                    .template Input<TCoPartitionsByKeys>()
                                        .Input(std::move(input))
                                        .KeySelectorLambda(extract)
                                        .SortDirections(std::move(sortDirs))
                                        .SortKeySelectorLambda(std::move(sortKeys))
                                        .ListHandlerLambda(std::move(reducer))
                                        .Build()
                                    .Provider().Value(YtProviderName).Build()
                                    .template Settings<TCoNameValueTupleList>()
                                        .Add()
                                            .Name().Value("table", TNodeFlags::Default).Build()
                                            .Value(reduce.Output().Item(0)).Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Settings(TDqStageSettings{.SinglePartition = true}.BuildNode(ctx, reduce.Pos()))
                            .Build()
                        .Index().Build(ctx.GetIndexAsString(0), TNodeFlags::Default)
                        .Build()
                    .ColumnHints().Build()
                    .Build()
                .Flags().Add().Build("FallbackOnError", TNodeFlags::Default).Build()
                .Build()
            .template Second<TYtOperation>()
                .InitFrom(reduce)
                .Settings(NYql::AddSetting(reduce.Settings().Ref(), EYtSettingType::NoDq, {}, ctx))
                .Build()
            .Done();
    }

    TMaybeNode<TExprBase> TryYtReduceByDq(TExprBase node, TExprContext& ctx) const {
        if (const auto reduce = node.Cast<TYtReduce>(); CanReplaceOnHybrid(reduce)) {
            const auto chunksLimit = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ);
            const auto sizeLimit = State_->Configuration->HybridDqDataSizeLimitForOrdered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForOrdered);
            if (const auto stat = CanReadHybrid(reduce.Input().Item(0))) {
                if (stat->front() <= sizeLimit && stat->back() <= chunksLimit) {
                    if (CanExecuteInHybrid(reduce.Reducer().Ptr(), chunksLimit, sizeLimit)) {
                        if (ETypeAnnotationKind::Struct == GetSeqItemType(*reduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()).GetKind()) {
                            YQL_CLOG(INFO, ProviderYt) << "Reduce on DQ with equivalent input size " << stat->front() << " and " << stat->back() << " chunks.";
                            PushStat("Hybrid_Reduce_try");
                            return MakeYtReduceByDq(reduce, ctx);
                        }
                    }
                }
                PushStat("Hybrid_Reduce_over_limits");
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> TryYtMapReduceByDq(TExprBase node, TExprContext& ctx) const {
        if (const auto mapReduce = node.Cast<TYtMapReduce>(); CanReplaceOnHybrid(mapReduce)) {
            const auto chunksLimit = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ);
            const auto sizeLimit = State_->Configuration->HybridDqDataSizeLimitForOrdered.Get().GetOrElse(DefaultHybridDqDataSizeLimitForOrdered);
            if (const auto stat = CanReadHybrid(mapReduce.Input().Item(0))) {
                if (stat->front() <= sizeLimit && stat->back() <= chunksLimit) {
                    if (CanExecuteInHybrid(mapReduce.Reducer().Ptr(), chunksLimit, sizeLimit) && CanExecuteInHybrid(mapReduce.Mapper().Ptr(), chunksLimit, sizeLimit)) {
                        if (ETypeAnnotationKind::Struct == GetSeqItemType(*mapReduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()).GetKind()) {
                            YQL_CLOG(INFO, ProviderYt) << "MapReduce on DQ with equivalent input size " << stat->front() << " and " << stat->back() << " chunks.";
                            PushStat("Hybrid_MapReduce_try");
                            return MakeYtReduceByDq(mapReduce, ctx);
                        }
                    }
                }
                PushStat("Hybrid_MapReduce_over_limits");
            }
        }

        return node;
    }

    void PushStat(const std::string_view& name) const {
        with_lock(State_->StatisticsMutex) {
            State_->Statistics[Max<ui32>()].Entries.emplace_back(TString{name}, 0, 0, 0, 0, 1);
        }
    };

    const TYtState::TPtr State_;
    const THolder<IGraphTransformer> Finalizer_;
};

} // namespce

THolder<IGraphTransformer> CreateYtDqHybridTransformer(TYtState::TPtr state, THolder<IGraphTransformer>&& finalizer) {
    return THolder<IGraphTransformer>(new TYtDqHybridTransformer(std::move(state), std::move(finalizer)));
}

} // namespace NYql
