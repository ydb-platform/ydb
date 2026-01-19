#include "kqp_rbo.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include "kqp_rbo_physical_sort_builder.h"
#include "kqp_rbo_physical_aggregation_builder.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/yql_expr_optimize.h>

using namespace NYql::NNodes;

namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp;

TString GetValidJoinKind(const TString& joinKind) {
    const auto joinKindLowered = to_lower(joinKind);
    if (joinKindLowered == "left") {
        return "Left";
    } else if (joinKindLowered == "inner") {
        return "Inner";
    } else if (joinKindLowered == "cross") {
        return "Cross";
    }
    return joinKind;
}

TExprNode::TPtr BuildMultiConsumerHandler(TExprNode::TPtr input, const ui32 numConsumers, TExprContext& ctx, TPositionHandle pos) {
    TVector<TExprBase> branches;
    auto inputIndex = NDq::BuildAtomList("0", pos, ctx);
    for (ui32 i = 0; i < numConsumers; ++i) {
        branches.emplace_back(inputIndex);
        // Just an empty lambda.
        // clang-format off
        auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({"arg"})
            .Body("arg")
        .Done();
        // clang-format on
        branches.push_back(lambda);
    }

    // clang-format off
    return Build<TCoSwitch>(ctx, pos)
        .Input(input)
        .BufferBytes()
            .Value(ToString(128_MB))
        .Build()
        .FreeArgs()
            .Add(branches)
        .Build()
     .Done().Ptr();
     // clang-format on
}

TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext &ctx, bool removeAliases = false) {
    // FIXME: This is not always correct, for example:
    // lambda($arg) { $val = expr($arg); return member($val `name)}
    // will replace only member arg but leave expr with free arg.
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        auto memberName = member.Name();
        if (removeAliases) {
            auto strippedName = memberName.StringValue();
            if (auto idx = strippedName.find_last_of('.'); idx != TString::npos) {
                strippedName = strippedName.substr(idx + 1);
            }
            // clang-format off
            memberName = Build<TCoAtom>(ctx, input->Pos()).Value(strippedName).Done();
            // clang-format on
        }
        // clang-format off
            return Build<TCoMember>(ctx, input->Pos())
                .Struct(arg)
                .Name(memberName)
            .Done().Ptr();
        // clang-format on
    } else if (input->IsCallable()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx, removeAliases));
        }
        // clang-format off
            return ctx.Builder(input->Pos())
                .Callable(input->Content())
                    .Add(std::move(newChildren))
                    .Seal()
                .Build();
        // clang-format on
    } else if (input->IsList()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx, removeAliases));
        }
        // clang-format off
            return ctx.Builder(input->Pos())
                .List()
                    .Add(std::move(newChildren))
                    .Seal()
                .Build();
        // clang-format on
    } else {
        return input;
    }
}

[[maybe_unused]]
TExprNode::TPtr ExtractMembers(TExprNode::TPtr input, TExprContext &ctx, TVector<TInfoUnit> members) {
    TVector<TExprBase> items;
    // clang-format off
    auto arg = Build<TCoArgument>(ctx, input->Pos())
        .Name("arg")
    .Done().Ptr();
    // clang-format on

    for (const auto& iu : members) {
        auto name = iu.GetFullName();
        // clang-format off
            auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
                .Name().Build(name)
                .Value<TCoMember>()
                    .Struct(arg)
                    .Name().Build(name)
                .Build()
            .Done();
        // clang-format on
        items.push_back(tuple);
    }

    // clang-format off
    return Build<TCoFlatMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add(items)
                .Build()
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
}

bool CanApplyPeepHole(TExprNode::TPtr input, const std::initializer_list<std::string_view>& callableNames) {
    auto blackList = [&](const TExprNode::TPtr& node) -> bool {
        if (node->IsCallable(callableNames)) {
            return true;
        }
        return false;
    };
    return !FindNode(input, blackList);
}

TExprNode::TPtr KqpPeepholeStageLambda(TExprNode::TPtr stageLambda, TRBOContext& rboCtx) {
    auto lambda = TCoLambda(stageLambda);
    // Compute types of inputs to stage lambda
    TVector<const TTypeAnnotationNode *> argTypes;
    for (const auto& arg : lambda.Args()) {
        const auto* argTypeAnn = arg.Ptr()->GetTypeAnn();
        Y_ENSURE(argTypeAnn);
        argTypes.push_back(argTypeAnn);
    }

    // Yql has a strange bug in final stage peephole for `WideCombiner` with empty keys.
    const bool withFinalStageRules = CanApplyPeepHole(lambda.Body().Ptr(), {"WideCombiner"});
    // clang-format off
    auto program = Build<TKqpProgram>(rboCtx.ExprCtx, stageLambda->Pos())
        .Lambda(rboCtx.ExprCtx.DeepCopyLambda(*stageLambda.Get()))
        .ArgsType(ExpandType(stageLambda->Pos(), *rboCtx.ExprCtx.MakeType<TTupleExprType>(argTypes), rboCtx.ExprCtx))
    .Done();
    // clang-format on

    TExprNode::TPtr newProgram;
    auto status = PeepHoleOptimize(program, newProgram, rboCtx.ExprCtx, rboCtx.PeepholeTypeAnnTransformer.GetRef(), rboCtx.TypeCtx, rboCtx.KqpCtx.Config, false,
                                   withFinalStageRules, {});
    if (status != IGraphTransformer::TStatus::Ok) {
        rboCtx.ExprCtx.AddError(TIssue(rboCtx.ExprCtx.GetPosition(program.Pos()), "Peephole optimization failed for stage in NEW RBO"));
        return nullptr;
    }

    return TKqpProgram(newProgram).Lambda().Ptr();
}

void ApplyTypeAnnotation(TExprNode::TPtr input, TRBOContext& rboCtx) {
    rboCtx.TypeAnnTransformer->Rewind();
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = rboCtx.TypeAnnTransformer->Transform(input, input, rboCtx.ExprCtx);
    } while (status == IGraphTransformer::TStatus::Repeat);
    Y_ENSURE(status == IGraphTransformer::TStatus::Ok);
}

// This function assumes that stages already sorted in topological orders.
TVector<TExprNode::TPtr> ApplyKqpPeepholeForPhysicalStages(TVector<std::pair<TExprNode::TPtr, TPositionHandle>>&& physicalStages, TRBOContext& rboCtx) {
    Y_ENSURE(physicalStages.size());
    auto root = physicalStages.back().first;
    // Type is required to wrap stage lambda to `KqpProgram`
    if (!root->GetTypeAnn()) {
        ApplyTypeAnnotation(root, rboCtx);
    }
    auto& ctx = rboCtx.ExprCtx;

    TNodeOnNodeOwnedMap replaces;
    TVector<TExprNode::TPtr> newStages;
    for (auto& [stage, pos]: physicalStages) {
        auto dqPhyStage = TDqPhyStage(stage);
        auto stageLambdaAfterPeephole = KqpPeepholeStageLambda(dqPhyStage.Program().Ptr(), rboCtx);
        Y_ENSURE(stageLambdaAfterPeephole);
        // clang-format off
        auto newStage = Build<TDqPhyStage>(ctx, pos)
            .Inputs(ctx.ReplaceNodes(dqPhyStage.Inputs().Ptr(), replaces))
            .Program(stageLambdaAfterPeephole)
            .Settings(dqPhyStage.Settings())
            .Outputs(dqPhyStage.Outputs())
        .Done().Ptr();
        // clang-format on
        newStages.push_back(newStage);
        replaces[dqPhyStage.Raw()] = newStage;
    }
    return newStages;
}

TExprNode::TPtr BuildDqPhyStage(const TVector<TExprNode::TPtr>& inputs, const TVector<TExprNode::TPtr>& args, TExprNode::TPtr physicalStageBody,
                                TExprContext& ctx, TPositionHandle pos) {
    // clang-format off
    return Build<TDqPhyStage>(ctx, pos)
        .Inputs()
            .Add(inputs)
        .Build()
        .Program()
            .Args(args)
            .Body(physicalStageBody)
        .Build()
        .Settings().Build()
    .Done().Ptr();
    // clang-format on
}

bool IsMultiConsumerHandlerNeeded(const std::shared_ptr<IOperator>& op) {
    return op->Props.NumOfConsumers.has_value() && op->Props.NumOfConsumers.value() > 1;
}

TExprNode::TPtr BuildCrossJoin(TOpJoin &join, TExprNode::TPtr leftInput, TExprNode::TPtr rightInput, TExprContext &ctx,
                               TPositionHandle pos) {

    TCoArgument leftArg{ctx.NewArgument(pos, "_kqp_left")};
    TCoArgument rightArg{ctx.NewArgument(pos, "_kqp_right")};

    TVector<TExprNode::TPtr> keys;
    for (const auto& iu : join.GetLeftInput()->GetOutputIUs()) {
        YQL_CLOG(TRACE, CoreDq) << "Converting Cross Join, left key: " << iu.GetFullName();

        // clang-format off
        auto keyPtr = Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(iu.GetFullName())
            .Value<TCoMember>()
                .Struct(leftArg)
                .Name().Build(iu.GetFullName())
            .Build()
            .Done().Ptr();
        // clang-format on
        keys.push_back(keyPtr);
    }

    for (const auto& iu : join.GetRightInput()->GetOutputIUs()) {
        YQL_CLOG(TRACE, CoreDq) << "Converting Cross Join, right key: " << iu.GetFullName();

        // clang-format off
        auto keyPtr = Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(iu.GetFullName())
            .Value<TCoMember>()
                .Struct(rightArg)
                .Name().Build(iu.GetFullName())
            .Build()
            .Done().Ptr();
        // clang-format on
        keys.push_back(keyPtr);
    }

    // clang-format off
    // We have to `Condense` right input as single-element stream of lists (single list of all elements from the right),
    // because stream supports single iteration only
    auto itemArg = Build<TCoArgument>(ctx, pos).Name("item").Done();
    auto rightAsStreamOfLists = Build<TCoCondense1>(ctx, pos)
        .Input<TCoToFlow>()
            .Input(rightInput)
            .Build()
        .InitHandler()
            .Args({itemArg})
            .Body<TCoAsList>()
                .Add(itemArg)
                .Build()
            .Build()
        .SwitchHandler()
            .Args({"item", "state"})
            .Body<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Build()
        .UpdateHandler()
            .Args({"item", "state"})
            .Body<TCoAppend>()
                .List("state")
                .Item("item")
            .Build()
        .Build()
    .Done();

    auto flatMap = Build<TCoFlatMap>(ctx, pos)
        .Input(rightAsStreamOfLists)
        .Lambda()
            .Args({"rightAsList"})
            .Body<TCoFlatMap>()
                .Input(leftInput)
                .Lambda()
                    .Args({leftArg})
                    .Body<TCoMap>()
                        // here we have `List`, so we can iterate over it many times (for every `leftArg`)
                        .Input("rightAsList")
                        .Lambda()
                            .Args({rightArg})
                            .Body<TCoAsStruct>()
                                .Add(keys)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Build()
    .Done().Ptr();

    return Build<TCoFromFlow>(ctx, pos)
        .Input(flatMap)
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr BuildGraceJoinCore(TOpJoin &join, TExprNode::TPtr leftInput, TExprNode::TPtr rightInput, TExprContext &ctx,
                                   TPositionHandle pos) {
    TVector<TCoAtom> leftColumnIdxs;
    TVector<TCoAtom> rightColumnIdxs;
    TVector<TCoAtom> leftRenames, rightRenames;
    TVector<TCoAtom> leftKeyColumnNames;
    TVector<TCoAtom> rightKeyColumnNames;

    auto leftIUs = join.GetLeftInput()->GetOutputIUs();
    auto rightIUs = join.GetRightInput()->GetOutputIUs();
    const bool leftSideOnly = (join.JoinKind == "LeftSemi" || join.JoinKind == "LeftOnly");

    if (leftSideOnly) {
        rightIUs.clear();
        for (const auto &[left, right] : join.JoinKeys) {
            rightIUs.push_back(right);
        }
    }

    const auto leftTupleSize = leftIUs.size();
    for (size_t i = 0; i < leftIUs.size(); i++) {
        leftRenames.push_back(Build<TCoAtom>(ctx, pos).Value(i).Done());
        leftRenames.push_back(Build<TCoAtom>(ctx, pos).Value(i).Done());
    }

    for (size_t i = 0; i < rightIUs.size(); i++) {
        rightRenames.push_back(Build<TCoAtom>(ctx, pos).Value(i).Done());
        rightRenames.push_back(Build<TCoAtom>(ctx, pos).Value(i + leftTupleSize).Done());
    }

    for (const auto& k : join.JoinKeys) {
        auto leftIdx = std::distance(leftIUs.begin(), std::find(leftIUs.begin(), leftIUs.end(), k.first));
        auto rightIdx = std::distance(rightIUs.begin(), std::find(rightIUs.begin(), rightIUs.end(), k.second));

        leftColumnIdxs.push_back(Build<TCoAtom>(ctx, pos).Value(leftIdx).Done());
        rightColumnIdxs.push_back(Build<TCoAtom>(ctx, pos).Value(rightIdx).Done());

        leftKeyColumnNames.push_back(Build<TCoAtom>(ctx, pos).Value(k.first.GetFullName()).Done());
        rightKeyColumnNames.push_back(Build<TCoAtom>(ctx, pos).Value(k.second.GetFullName()).Done());
    }

    // Convert to wide flow
    // clang-format off
    leftInput = Build<TCoToFlow>(ctx, pos)
        .Input(leftInput)
    .Done().Ptr();
    // clang-forat on

    leftInput = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(leftInput, join.GetLeftInput()->GetOutputIUs(), ctx);

    // clang-format off
    rightInput = Build<TCoToFlow>(ctx, pos)
        .Input(rightInput)
    .Done().Ptr();
    // clang-format on

    rightInput = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(rightInput, rightIUs, ctx);

    // clang-format off
    auto graceJoin = Build<TCoGraceJoinCore>(ctx, pos)
        .LeftInput(leftInput)
        .RightInput(rightInput)
        .JoinKind<TCoAtom>()
            .Value(GetValidJoinKind(join.JoinKind))
        .Build()
        .LeftKeysColumns<TCoAtomList>()
            .Add(leftColumnIdxs)
        .Build()
        .RightKeysColumns<TCoAtomList>()
            .Add(rightColumnIdxs)
        .Build()
        .LeftRenames()
            .Add(leftRenames)
        .Build()
        .RightRenames()
            .Add(rightRenames)
        .Build()
        .LeftKeysColumnNames<TCoAtomList>()
            .Add(leftKeyColumnNames)
        .Build()
        .RightKeysColumnNames<TCoAtomList>()
            .Add(rightKeyColumnNames)
        .Build()
        .Flags().Build()
    .Done().Ptr();

    // Convert back to narrow stream
    return Build<TCoFromFlow>(ctx, pos)
        .Input(NPhysicalConvertionUtils::BuildNarrowMapForWideInput(graceJoin, join.GetOutputIUs(), ctx))
    .Done().Ptr();
    // clang-format on
}

[[maybe_unused]]
TExprNode::TPtr BuildDqGraceJoin(TOpJoin &join, TExprNode::TPtr leftInput, TExprNode::TPtr rightInput, TExprContext &ctx,
                                 TPositionHandle pos) {
    TVector<TDqJoinKeyTuple> joinKeys;
    TVector<TCoAtom> leftKeyColumnNames;
    TVector<TCoAtom> rightKeyColumnNames;

    for (const auto &p : join.JoinKeys) {
        TString leftFullName = p.first.GetFullName();
        TString rightFullName = p.second.GetFullName();

        // clang-format off
        joinKeys.push_back(Build<TDqJoinKeyTuple>(ctx, pos)
            .LeftLabel().Value(p.first.GetAlias()).Build()
            .LeftColumn().Value(p.first.GetColumnName()).Build()
            .RightLabel().Value(p.second.GetAlias()).Build()
            .RightColumn().Value(p.second.GetColumnName()).Build()
        .Done());

        leftKeyColumnNames.push_back(Build<TCoAtom>(ctx, pos).Value(leftFullName).Done());
        rightKeyColumnNames.push_back(Build<TCoAtom>(ctx, pos).Value(rightFullName).Done());
        // clang-format on
    }

    // clang-format off
    return Build<TDqPhyGraceJoin>(ctx, pos)
        .LeftInput(leftInput)
        .LeftLabel<TCoVoid>().Build()
        .RightInput(rightInput)
        .RightLabel<TCoVoid>().Build()
        .JoinType<TCoAtom>()
            .Value("Inner")
        .Build()
        .JoinKeys<TDqJoinKeyTupleList>()
            .Add(joinKeys)
        .Build()
        .LeftJoinKeyNames<TCoAtomList>()
            .Add(leftKeyColumnNames)
        .Build()
        .RightJoinKeyNames<TCoAtomList>()
            .Add(rightKeyColumnNames)
        .Build()
    .Done().Ptr();
     // clang-format on
}

[[maybe_unused]]
TExprNode::TPtr BuildSort(TExprNode::TPtr input, TOrderEnforcer& enforcer, TExprContext &ctx, TPositionHandle pos) {
    if (enforcer.Action != EOrderEnforcerAction::REQUIRE) {
        return input;
    }

    auto [selector, dirs] = BuildSortKeySelector(enforcer.SortElements, ctx, pos);

    TExprNode::TPtr dirList;
    if (dirs.size() == 1) {
        dirList = dirs[0];
    } else {
        dirList = Build<TExprList>(ctx, pos).Add(dirs).Done().Ptr();
    }

    // clang-format off
    return Build<TCoSort>(ctx, pos)
        .Input(input)
        .SortDirections(dirList)
        .KeySelectorLambda(selector)
        .Done().Ptr();
    // clang-format on
}

void BuildPhysicalStageGraph(TStageGraph& graph, const THashMap<int, TVector<TExprNode::TPtr>>& stageArgs, const THashMap<int, TExprNode::TPtr>& stages,
                             const THashMap<int, TPositionHandle>& stagePos, TVector<std::pair<TExprNode::TPtr, TPositionHandle>>& physicalStages,
                             TRBOContext& rboCtx) {
    graph.TopologicalSort();
    const auto& stageIds = graph.StageIds;
    const auto& stageInputIds = graph.StageInputs;
    auto& ctx = rboCtx.ExprCtx;

    THashMap<int, TExprNode::TPtr> finalizedStages;
    for (const auto id : stageIds) {
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputConnections;
        THashSet<int> processedInputsIds;
        for (const auto inputStageId : stageInputIds.at(id)) {
            if (processedInputsIds.contains(inputStageId)) {
                continue;
            }
            processedInputsIds.insert(inputStageId);

            auto inputStage = finalizedStages.at(inputStageId);
            const auto connections = graph.GetConnections(inputStageId, id);
            for (const auto& connection : connections) {
                YQL_CLOG(TRACE, CoreDq) << "Building connection: " << inputStageId << "->" << id << ", " << connection->Type;
                TExprNode::TPtr newStage;
                auto dqConnection = connection->BuildConnection(inputStage, stagePos.at(inputStageId), newStage, ctx);
                if (newStage) {
                    physicalStages.emplace_back(newStage, stagePos.at(inputStageId));
                }
                YQL_CLOG(TRACE, CoreDq) << "Built connection: " << inputStageId << "->" << id << ", " << connection->Type;
                inputConnections.push_back(dqConnection);
            }
        }

        TExprNode::TPtr stage;
        if (graph.IsSourceStageRowType(id)) {
            stage = stages.at(id);
        } else {
            TVector<TExprNode::TPtr> stageInputConnections;
            TVector<TExprNode::TPtr> stageInputArgs;
            if (!graph.IsSourceStageColumnType(id)) {
                stageInputConnections = inputConnections;
                stageInputArgs = stageArgs.at(id);
            }

            stage = BuildDqPhyStage(stageInputConnections, stageInputArgs, stages.at(id), ctx, stagePos.at(id));
            physicalStages.emplace_back(stage, stagePos.at(id));
            YQL_CLOG(TRACE, CoreDq) << "Added stage " << stage->UniqueId();
        }

        finalizedStages[id] = stage;
        YQL_CLOG(TRACE, CoreDq) << "Finalized stage " << id;
    }
}

TExprNode::TPtr BuildPhysicalQuery(TOpRoot& root, TVector<TExprNode::TPtr>&& physicalStages, TRBOContext& rboCtx) {
    Y_ENSURE(physicalStages.size());

    auto& ctx = rboCtx.ExprCtx;
    TVector<TCoAtom> columnAtomList;
    for (const auto& column : root.ColumnOrder) {
        columnAtomList.push_back(Build<TCoAtom>(ctx, root.Pos).Value(column).Done());
    }
    auto columnOrder = Build<TCoAtomList>(ctx, root.Pos).Add(columnAtomList).Done().Ptr();

    // clang-format off
    // wrap in DqResult
    auto dqResult = Build<TDqCnResult>(ctx, root.Pos)
        .Output()
            .Stage(physicalStages.back())
            .Index().Build("0")
        .Build()
        .ColumnHints(columnOrder)
    .Done().Ptr();
    // clang-format on

    TVector<TExprNode::TPtr> txSettings;
    // clang-format off
    txSettings.push_back(Build<TCoNameValueTuple>(ctx, root.Pos)
                            .Name().Build("type")
                            .Value<TCoAtom>().Build("compute")
                        .Done().Ptr());
    // Build PhysicalTx
    auto physTx = Build<TKqpPhysicalTx>(ctx, root.Pos)
        .Stages()
            .Add(physicalStages)
        .Build()
        .Results()
            .Add({dqResult})
        .Build()
        .ParamBindings().Build()
        .Settings()
            .Add(txSettings)
        .Build()
    .Done().Ptr();
    // clang-format on

    ApplyTypeAnnotation(dqResult, rboCtx);

    YQL_CLOG(TRACE, CoreDq) << "Inferred final type: " << *dqResult->GetTypeAnn();
    // clang-format off
    TVector<TExprNode::TPtr> querySettings;
    querySettings.push_back(Build<TCoNameValueTuple>(ctx, root.Pos)
                                .Name().Build("type")
                                .Value<TCoAtom>().Build("data_query")
                            .Done().Ptr());

    auto binding = Build<TKqpTxResultBinding>(ctx, root.Pos)
        .Type(ExpandType(root.Pos, *dqResult->GetTypeAnn(), ctx))
        .TxIndex().Build("0")
        .ResultIndex().Build("0")
    .Done();

    // Build Physical query
    return Build<TKqpPhysicalQuery>(ctx, root.Pos)
        .Transactions()
            .Add({physTx})
        .Build()
        .Results()
            .Add({binding})
        .Build()
        .Settings()
            .Add(querySettings)
        .Build()
    .Done().Ptr();
    // clang-format on
}

template <typename T>
TExprNode::TPtr BuildMap(TExprNode::TPtr input, TExprNode::TPtr arg, TVector<TExprBase>&& items, TExprContext &ctx, TPositionHandle pos) {
    // clang-format off
    return Build<T>(ctx, pos)
        .Input(input)
        .template Lambda<TCoLambda>()
            .Args({arg})
            .template Body<TCoAsStruct>()
                .Add(items)
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
}

} // namespace

namespace NKikimr {
namespace NKqp {

TExprNode::TPtr ConvertToPhysical(TOpRoot &root, TRBOContext& rboCtx) {
    TExprContext& ctx = rboCtx.ExprCtx;

    THashMap<int, TExprNode::TPtr> stages;
    THashMap<int, TVector<TExprNode::TPtr>> stageArgs;
    THashMap<int, TPositionHandle> stagePos;
    auto &graph = root.PlanProps.StageGraph;
    for (auto id : graph.StageIds) {
        stageArgs[id] = TVector<TExprNode::TPtr>();
    }

    int stageInputCounter = 0;
    for (const auto& iter : root) {
        auto op = iter.Current;
        auto opStageId = *(op->Props.StageId);

        TExprNode::TPtr currentStageBody;
        if (stages.contains(opStageId)) {
            currentStageBody = stages.at(opStageId);
        }

        if (op->Kind == EOperator::EmptySource) {
            TVector<TExprBase> listElements;
            listElements.push_back(Build<TCoAsStruct>(ctx, op->Pos).Done());

            // clang-format off
            currentStageBody = Build<TCoIterator>(ctx, op->Pos)
                .List<TCoAsList>()
                    .Add(listElements)
                .Build()
            .Done().Ptr();
            // clang-format on
            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Empty Source " << opStageId;
        } else if (op->Kind == EOperator::Source) {
            auto opSource = CastOperator<TOpRead>(op);

            auto source = ctx.NewCallable(op->Pos, "DataSource", {ctx.NewAtom(op->Pos, "KqpReadRangesSource")});
            TVector<TExprNode::TPtr> columns;
            for (const auto& c : opSource->Columns) {
                columns.push_back(ctx.NewAtom(op->Pos, c));
            }

            switch (opSource->GetTableStorageType()) {
                case NYql::EStorageType::RowStorage: {
                    // clang-format off
                    currentStageBody = Build<TDqSource>(ctx, op->Pos)
                        .DataSource(source)
                        .Settings<TKqpReadRangesSourceSettings>()
                            .Table(opSource->TableCallable)
                            .Columns().Add(columns).Build()
                            .Settings<TCoNameValueTupleList>().Build()
                            .RangesExpr<TCoVoid>().Build()
                            .ExplainPrompt<TCoNameValueTupleList>().Build()
                        .Build()
                    .Done().Ptr();
                    // clang-format on
                    break;
                }
                case NYql::EStorageType::ColumnStorage: {
                    // clang-format off
                    auto processLambda = Build<TCoLambda>(ctx, op->Pos)
                        .Args({"arg"})
                        .Body("arg")
                    .Done().Ptr();
                    // clang-format on

                    if (opSource->OlapFilterLambda) {
                        processLambda = opSource->OlapFilterLambda;
                    }

                    // clang-format off
                    auto olapRead = Build<TKqpBlockReadOlapTableRanges>(ctx, op->Pos)
                        .Table(opSource->TableCallable)
                        .Ranges<TCoVoid>().Build()
                        .Columns().Add(columns).Build()
                        .Settings<TCoNameValueTupleList>().Build()
                        .ExplainPrompt<TCoNameValueTupleList>().Build()
                        .Process(processLambda)
                    .Done().Ptr();

                    auto flowNonBlockRead = Build<TCoToFlow>(ctx, op->Pos)
                        .Input<TCoWideFromBlocks>()
                            .Input<TCoFromFlow>()
                                .Input(olapRead)
                            .Build()
                        .Build()
                    .Done().Ptr();
                    // clang-format on

                    auto narrowMap = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(flowNonBlockRead, opSource->Columns, ctx);

                    // clang-format off
                    currentStageBody = Build<TCoFromFlow>(ctx, op->Pos)
                        .Input(narrowMap)
                    .Done().Ptr();
                    // clang-format on

                    if (IsMultiConsumerHandlerNeeded(op)) {
                        currentStageBody = BuildMultiConsumerHandler(currentStageBody, op->Props.NumOfConsumers.value(), ctx, op->Pos);
                    }
                    break;
                }
                default:
                    Y_ENSURE(false, "Unsupported table source type");
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Read " << opStageId;
        } else if (op->Kind == EOperator::Filter) {
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            auto filter = CastOperator<TOpFilter>(op);

            auto filterLambda = TCoLambda(filter->FilterLambda);
            auto filterBody = filterLambda.Body();
            auto filterArg = filterLambda.Args().Arg(0);
            auto mapArg = Build<TCoArgument>(ctx, op->Pos).Name("arg").Done().Ptr();

            // FIXME: Eliminate this for YQL pipeline.
            auto newFilterBody = ctx.Builder(op->Pos).Callable("FromPg").Add(0, filterBody.Ptr()).Seal().Build();

            TVector<TExprBase> items;
            for (const auto& iu : op->GetOutputIUs()) {
                auto memberName = iu.GetFullName();
                // clang-format off
                auto tuple = Build<TCoNameValueTuple>(ctx, op->Pos)
                    .Name().Build(memberName)
                    .Value<TCoMember>()
                        .Struct(mapArg)
                        .Name().Build(memberName)
                    .Build()
                .Done();
                // clang-format on
                items.push_back(tuple);
            }

            // clang-format off
            currentStageBody = Build<TCoMap>(ctx, op->Pos)
                .Input<TCoFilter>()
                    .Input(TExprBase(currentStageBody))
                    .Lambda<TCoLambda>()
                        .Args({"filter_arg"})
                        .Body<TCoCoalesce>()
                            .Predicate<TExprApplier>()
                                .Apply(TExprBase(newFilterBody))
                                .With(filterArg, "filter_arg")
                            .Build()
                            .Value<TCoBool>()
                                .Literal().Build("false")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Lambda<TCoLambda>()
                    .Args({mapArg})
                    .Body<TCoAsStruct>()
                        .Add(items)
                    .Build()
                .Build()
            .Done().Ptr();
            // clang-format off

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Filter " << opStageId;
        } else if (op->Kind == EOperator::Map) {
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            auto map = CastOperator<TOpMap>(op);
            const bool isOrdered = map->IsOrdered();

            auto arg = Build<TCoArgument>(ctx, op->Pos).Name("arg").Done().Ptr();

            TVector<TExprBase> items;
            if (!map->Project) {
                for (const auto& iu : map->GetInput()->GetOutputIUs()) {
                    // clang-format off
                    auto tuple = Build<TCoNameValueTuple>(ctx, op->Pos)
                        .Name().Value(iu.GetFullName()).Build()
                        .Value<TCoMember>()
                            .Struct(arg)
                            .Name().Value(iu.GetFullName()).Build()
                        .Build()
                    .Done();
                    // clang-format on

                    tuple = TCoNameValueTuple(ReplaceArg(tuple.Ptr(), arg, ctx));
                    items.push_back(tuple);
                }
            }

            for (const auto& mapElement : map->MapElements) {
                TMaybeNode<TCoLambda> mapLambda;
                if (mapElement.IsExpression()) {
                    auto lambda = TCoLambda(mapElement.GetExpression());
                    // clang-format off
                    mapLambda = Build<TCoLambda>(ctx, op->Pos)
                        .Args({arg})
                        .Body<TExprApplier>()
                            .Apply(lambda.Body())
                            .With(TExprBase(lambda.Args().Arg(0)), TExprBase(arg))
                        .Build()
                    .Done();
                    // clang-format on
                } else {
                    const auto var = mapElement.GetRename();
                    // clang-format off
                    mapLambda = Build<TCoLambda>(ctx, op->Pos)
                        .Args({arg})
                        .Body<TCoMember>()
                            .Struct(arg)
                            .Name().Value(var.GetFullName()).Build()
                        .Build()
                    .Done();
                    // clang-format on
                }

                // clang-format off
                auto tuple = Build<TCoNameValueTuple>(ctx, op->Pos)
                    .Name().Value(mapElement.GetElementName().GetFullName()).Build()
                    .Value(mapLambda.Body())
                .Done();
                // clang-format on

                items.push_back(tuple);
            }

            currentStageBody = isOrdered ? BuildMap<TCoOrderedMap>(currentStageBody, arg, std::move(items), ctx, op->Pos)
                                         : BuildMap<TCoMap>(currentStageBody, arg, std::move(items), ctx, op->Pos);

            if (IsMultiConsumerHandlerNeeded(op)) {
                currentStageBody = BuildMultiConsumerHandler(currentStageBody, op->Props.NumOfConsumers.value(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Map " << opStageId;
        } else if (op->Kind == EOperator::Limit) {
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            auto limit = CastOperator<TOpLimit>(op);

            // clang-format off
            currentStageBody = Build<TCoTake>(ctx, op->Pos)
                .Input(TExprBase(currentStageBody))
                .Count(limit->LimitCond)
            .Done().Ptr();
            // clang-format on

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Limit " << opStageId;
        } else if (op->Kind == EOperator::Sort) {
            auto sort = CastOperator<TOpSort>(op);
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            TPhysicalSortBuilder physicalSortBuilder(sort, ctx, op->Pos);
            currentStageBody = physicalSortBuilder.BuildPhysicalSort(currentStageBody);
            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Sort " << opStageId;
        } else if (op->Kind == EOperator::Join) {
            auto join = CastOperator<TOpJoin>(op);

            auto [leftArg, leftInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *join->GetLeftInput()->Props.StageId);
            stageArgs[opStageId].push_back(leftArg);
            auto [rightArg, rightInput] =
                graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *join->GetRightInput()->Props.StageId);
            stageArgs[opStageId].push_back(rightArg);
            if (join->JoinKind == "Cross") {
                currentStageBody = BuildCrossJoin(*join, leftInput, rightInput, ctx, op->Pos);
            } else if (const auto joinKind = to_lower(join->JoinKind); (joinKind == "inner" || joinKind == "left" || joinKind == "leftonly" || joinKind == "leftsemi")) {
                currentStageBody = BuildGraceJoinCore(*join, leftInput, rightInput, ctx, op->Pos);
            } else {
                TStringBuilder builder;
                builder << "Unsupported join kind " << join->JoinKind;
                Y_ENSURE(false, builder.c_str());
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Join " << opStageId;
        } else if (op->Kind == EOperator::UnionAll) {
            auto unionAll = CastOperator<TOpUnionAll>(op);

            auto [leftArg, leftInput] =
                graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *unionAll->GetLeftInput()->Props.StageId);
            stageArgs[opStageId].push_back(leftArg);

            auto [rightArg, rightInput] =
                graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *unionAll->GetRightInput()->Props.StageId);
            stageArgs[opStageId].push_back(rightArg);
            TVector<TExprNode::TPtr> extendArgs{leftArg, rightArg};

            if (unionAll->Ordered) {
                // clang-format off
                currentStageBody = Build<TCoOrderedExtend>(ctx, op->Pos)
                    .Add(extendArgs)
                .Done().Ptr();
                // clang-format on
            }
            else {
                // clang-format off
                currentStageBody = Build<TCoExtend>(ctx, op->Pos)
                    .Add(extendArgs)
                .Done().Ptr();
                // clang-format on
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted UnionAll " << opStageId;
        } else if (op->Kind == EOperator::Aggregate) {
            auto aggregate = CastOperator<TOpAggregate>(op);

            auto [stageArg, stageInput] =
                graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *aggregate->GetInput()->Props.StageId);
            stageArgs[opStageId].push_back(stageArg);

            TPhysicalAggregationBuilder aggregationBuilder(aggregate, ctx, op->Pos);
            stages[opStageId] = aggregationBuilder.BuildPhysicalAggregation(stageInput);
            stagePos[opStageId] = op->Pos;
        } else {
            Y_ENSURE(false, "Could not generate physical plan");
        }
    }

    TVector<std::pair<TExprNode::TPtr, TPositionHandle>> physicalStages;
    BuildPhysicalStageGraph(graph, stageArgs, stages, stagePos, physicalStages, rboCtx);
    auto phyStagesAfterPeephole = ApplyKqpPeepholeForPhysicalStages(std::move(physicalStages), rboCtx);
    return BuildPhysicalQuery(root, std::move(phyStagesAfterPeephole), rboCtx);
}
} // namespace NKqp
} // namespace NKikimr