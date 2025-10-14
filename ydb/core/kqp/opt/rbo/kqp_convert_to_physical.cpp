#include "kqp_rbo.h"

#include <yql/essentials/core/yql_graph_transformer.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>

#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;

namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp;

TString GetValidJoinKind(const TString &joinKind) {
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

TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext &ctx, bool removeAliases = false) {
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

    for (auto iu : members) {
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

[[maybe_unused]]
TExprNode::TPtr PeepholeStageLambda(TExprNode::TPtr stageLambda, TVector<TExprNode::TPtr> inputs, TExprContext &ctx,
                                    TTypeAnnotationContext &types, TAutoPtr<IGraphTransformer> typeAnnTransformer,
                                    TAutoPtr<IGraphTransformer> peepholeTransformer, TKikimrConfiguration::TPtr config) {

    Y_UNUSED(peepholeTransformer);
    // Compute types of inputs to stage lambda
    TVector<const TTypeAnnotationNode *> argTypes;

    for (auto input : inputs) {
        typeAnnTransformer->Rewind();

        IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
        do {
            status = typeAnnTransformer->Transform(input, input, ctx);
        } while (status == IGraphTransformer::TStatus::Repeat);

        if (status != IGraphTransformer::TStatus::Ok) {
            YQL_CLOG(ERROR, ProviderKqp) << "RBO type annotation failed." << Endl << ctx.IssueManager.GetIssues().ToString();
            return nullptr;
        }

        argTypes.push_back(input->GetTypeAnn());
    }

    // clang-format off
    // Build a temporary KqpProgram to run peephole on
    auto program = Build<TKqpProgram>(ctx, stageLambda->Pos())
        .Lambda(stageLambda)
        .ArgsType(ExpandType(stageLambda->Pos(), *ctx.MakeType<TTupleExprType>(argTypes), ctx))
    .Done();
    // clang-format on

    auto programPtr = program.Ptr();

    const bool allowNonDeterministicFunctions = false;
    TExprNode::TPtr newProgram;
    auto status =
        PeepHoleOptimize(program, newProgram, ctx, typeAnnTransformer.GetRef(), types, config, allowNonDeterministicFunctions, true, {});
    if (status != IGraphTransformer::TStatus::Ok) {
        ctx.AddError(TIssue(ctx.GetPosition(program.Pos()), "Peephole optimization failed for KQP transaction"));
        return {};
    }

    return TKqpProgram(newProgram).Lambda().Ptr();
}

TExprNode::TPtr BuildCrossJoin(TOpJoin &join, TExprNode::TPtr leftInput, TExprNode::TPtr rightInput, TExprContext &ctx,
                               TPositionHandle pos) {

    TCoArgument leftArg{ctx.NewArgument(pos, "_kqp_left")};
    TCoArgument rightArg{ctx.NewArgument(pos, "_kqp_right")};

    TVector<TExprNode::TPtr> keys;
    for (auto iu : join.GetLeftInput()->GetOutputIUs()) {
        // clang-format off
                auto keyPtr = Build<TCoNameValueTuple>(ctx, pos)
                    .Name().Build(iu.GetFullName())
                    .Value<TCoMember>()
                        .Struct(leftArg)
                        .Name().Build(iu.GetFullName())
                    .Build()
                .Done().Ptr();
                // clang-forat on
                keys.push_back(keyPtr);
            }

            for (auto iu : join.GetRightInput()->GetOutputIUs()) {
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

TExprNode::TPtr ExpandJoinInput(TExprNode::TPtr input, TVector<TInfoUnit> ius, TExprContext &ctx) {
    // clang-format off
    return ctx.Builder(input->Pos())
        .Callable("ExpandMap")
            .Add(0, input)
            .Lambda(1)
                .Param("item")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    auto i = 0U;
                    for (const auto& iu: ius) {
                        parent.Callable(i, "Member")
                        .Arg(0, "item")
                        .Atom(1, iu.GetFullName())
                        .Seal();
                        i++;
                    }
                    return parent;
                })
            .Seal()
        .Seal()
    .Build();
    // clang-format on
}

TExprNode::TPtr CollapseJoinOutput(TExprNode::TPtr graceJoin, TVector<TInfoUnit> ius, TExprContext &ctx) {
    // clang-format off
    return ctx.Builder(graceJoin->Pos())
        .Callable("NarrowMap")
            .Add(0, graceJoin)
            .Lambda(1)
                .Params("output", ius.size())
                .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 i = 0U;
                    for (const auto& iu : ius) {
                        parent.List(i)
                        .Atom(0, iu.GetFullName())
                        .Arg(1, "output", i)
                        .Seal();
                        i++;
                    }
                    return parent;
                })
                .Seal()
            .Seal()
        .Seal()
    .Build();
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

    auto leftTupleSize = leftIUs.size();
    for (size_t i = 0; i < leftIUs.size(); i++) {
        leftRenames.push_back(Build<TCoAtom>(ctx, pos).Value(i).Done());
        leftRenames.push_back(Build<TCoAtom>(ctx, pos).Value(i).Done());
    }

    for (size_t i = 0; i < rightIUs.size(); i++) {
        rightRenames.push_back(Build<TCoAtom>(ctx, pos).Value(i).Done());
        rightRenames.push_back(Build<TCoAtom>(ctx, pos).Value(i + leftTupleSize).Done());
    }

    for (auto k : join.JoinKeys) {
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

    leftInput = ExpandJoinInput(leftInput, join.GetLeftInput()->GetOutputIUs(), ctx);

    // clang-format off
    rightInput = Build<TCoToFlow>(ctx, pos)
        .Input(rightInput)
    .Done().Ptr();
    // clang-format on

    rightInput = ExpandJoinInput(rightInput, join.GetRightInput()->GetOutputIUs(), ctx);

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
        .Input(CollapseJoinOutput(graceJoin, join.GetOutputIUs(), ctx))
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
        TString leftFullName = "_alias_" + p.first.Alias + "." + p.first.ColumnName;
        TString rightFullName = "_alias_" + p.second.Alias + "." + p.second.ColumnName;

        // clang-format off
        joinKeys.push_back(Build<TDqJoinKeyTuple>(ctx, pos)
            .LeftLabel().Value("_alias_" + p.first.Alias).Build()
            .LeftColumn().Value(p.first.ColumnName).Build()
            .RightLabel().Value("_alias_" + p.second.Alias).Build()
            .RightColumn().Value(p.second.ColumnName).Build()
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

TExprNode::TPtr BuildSort(TExprNode::TPtr input, TOrderEnforcer & enforcer, TExprContext &ctx, TPositionHandle pos) {
    if (enforcer.Action != EOrderEnforcerAction::REQUIRE) {
        return input;
    }

    auto [selector, dirs] = BuildSortKeySelector(enforcer.SortElements, ctx, pos);

    TExprNode::TPtr dirList;
    if (dirs.size()==1){
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

} // namespace

namespace NKikimr {
namespace NKqp {

TExprNode::TPtr ConvertToPhysical(TOpRoot &root, TExprContext &ctx, TTypeAnnotationContext &types,
                                  TAutoPtr<IGraphTransformer> typeAnnTransformer, TAutoPtr<IGraphTransformer> peepholeTransformer,
                                  TKikimrConfiguration::TPtr config) {
    Y_UNUSED(types);
    Y_UNUSED(peepholeTransformer);
    Y_UNUSED(config);

    THashMap<int, TExprNode::TPtr> stages;
    THashMap<int, TVector<TExprNode::TPtr>> stageArgs;
    THashMap<int, TPositionHandle> stagePos;

    auto &graph = root.PlanProps.StageGraph;
    for (auto id : graph.StageIds) {
        stageArgs[id] = TVector<TExprNode::TPtr>();
    }

    int stageInputCounter = 0;
    for (auto iter : root) {
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
            for (auto c : opSource->Columns) {
                columns.push_back(ctx.NewAtom(op->Pos, c));
            }

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

            if (opSource->Props.OrderEnforcer.has_value()) {
                currentStageBody = BuildSort(currentStageBody, *op->Props.OrderEnforcer, ctx, op->Pos);
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

            if (filter->GetInput()->Props.OrderEnforcer.has_value()) {
                currentStageBody = BuildSort(currentStageBody, *filter->GetInput()->Props.OrderEnforcer, ctx, filter->GetInput()->Pos);
            }

            auto filterBody = TCoLambda(filter->FilterLambda).Body();
            auto filter_arg = Build<TCoArgument>(ctx, op->Pos).Name("arg").Done().Ptr();
            auto map_arg = Build<TCoArgument>(ctx, op->Pos).Name("arg").Done().Ptr();

            auto newFilterBody = ReplaceArg(filterBody.Ptr(), filter_arg, ctx);
            newFilterBody = ctx.Builder(op->Pos).Callable("FromPg").Add(0, newFilterBody).Seal().Build();

            TVector<TExprBase> items;
            for (auto iu : op->GetOutputIUs()) {
                auto memberName = iu.GetFullName();
                // clang-format off
                auto tuple = Build<TCoNameValueTuple>(ctx, op->Pos)
                    .Name().Build(memberName)
                    .Value<TCoMember>()
                        .Struct(map_arg)
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
                        .Args({filter_arg})
                        .Body<TCoCoalesce>()
                            .Predicate(newFilterBody)
                            .Value<TCoBool>()
                                .Literal().Build("false")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Lambda<TCoLambda>()
                    .Args({map_arg})
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

            if (map->GetInput()->Props.OrderEnforcer.has_value()) {
                currentStageBody = BuildSort(currentStageBody, *map->GetInput()->Props.OrderEnforcer, ctx, map->GetInput()->Pos);
            }

            auto arg = Build<TCoArgument>(ctx, op->Pos).Name("arg").Done().Ptr();

            TVector<TExprBase> items;
            if (!map->Project) {
                for (auto iu : map->GetInput()->GetOutputIUs()) {
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

            for (auto mapElement : map->MapElements) {
                TMaybeNode<TCoLambda> mapLambda;

                if (std::holds_alternative<TExprNode::TPtr>(mapElement.second)) {
                    mapLambda = TCoLambda(std::get<TExprNode::TPtr>(mapElement.second));
                } else {
                    auto var = std::get<TInfoUnit>(mapElement.second);

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
                    .Name().Value(mapElement.first.GetFullName()).Build()
                    .Value(mapLambda.Body())
                .Done();
                // clang-format on

                tuple = TCoNameValueTuple(ReplaceArg(tuple.Ptr(), arg, ctx));
                items.push_back(tuple);
            }

            // clang-format off
            currentStageBody = Build<TCoMap>(ctx, op->Pos)
                .Input(TExprBase(currentStageBody))
                .Lambda<TCoLambda>()
                    .Args({arg})
                    .Body<TCoAsStruct>()
                        .Add(items)
                    .Build()
                .Build()
            .Done().Ptr();
            // clang-fort on

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

            if (limit->GetInput()->Props.OrderEnforcer.has_value()) {
                currentStageBody = BuildSort(currentStageBody, *limit->GetInput()->Props.OrderEnforcer, ctx, limit->GetInput()->Pos);
            }

            // clang-format off
            currentStageBody = Build<TCoTake>(ctx, op->Pos)
                .Input(TExprBase(currentStageBody))
                .Count(limit->LimitCond)
            .Done().Ptr();
            // clang-format on

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Limit " << opStageId;
        } else if (op->Kind == EOperator::Join) {
            auto join = CastOperator<TOpJoin>(op);

            auto [leftArg, leftInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *join->GetLeftInput()->Props.StageId);
            stageArgs[opStageId].push_back(leftArg);
            auto [rightArg, rightInput] =
                graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *join->GetRightInput()->Props.StageId);
            stageArgs[opStageId].push_back(rightArg);
            if (join->JoinKind == "Cross") {
                currentStageBody = BuildCrossJoin(*join, leftInput, rightInput, ctx, op->Pos);
            } else if (const auto joinKind = to_lower(join->JoinKind); (joinKind == "inner" || joinKind == "left")) {
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

            // clang-format off
            currentStageBody = Build<TCoExtend>(ctx, op->Pos)
                .Add(extendArgs)
            .Done().Ptr();
            // clang-format on

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted UnionAll " << opStageId;
        } else {
            Y_ENSURE(false, "Could not generate physical plan");
        }
    }

    // We need to build up stages in a topological sort order
    graph.TopologicalSort();

    THashMap<int, TExprNode::TPtr> finalizedStages;
    TVector<TExprNode::TPtr> txStages;

    auto stageIds = graph.StageIds;
    auto stageInputIds = graph.StageInputs;

    for (auto id : stageIds) {
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputs;
        for (auto inputStageId : stageInputIds.at(id)) {
            auto inputStage = finalizedStages.at(inputStageId);
            auto connection = graph.GetConnection(inputStageId, id);
            YQL_CLOG(TRACE, CoreDq) << "Building connection: " << inputStageId << "->" << id << ", " << connection->Type;
            TExprNode::TPtr newStage;
            auto dqConnection = connection->BuildConnection(inputStage, stagePos.at(inputStageId), newStage, ctx);
            if (newStage) {
                txStages.push_back(newStage);
            }
            YQL_CLOG(TRACE, CoreDq) << "Built connection: " << inputStageId << "->" << id << ", " << connection->Type;
            inputs.push_back(dqConnection);
        }

        TExprNode::TPtr stage;
        if (graph.IsSourceStage(id)) {
            stage = stages.at(id);
        } else {
            // clang-format off
            stage = Build<TDqPhyStage>(ctx, stagePos.at(id))
                .Inputs()
                    .Add(inputs)
                    .Build()
                .Program()
                    .Args(stageArgs.at(id))
                    .Body(stages.at(id))
                    .Build()
                .Settings().Build()
            .Done().Ptr();
            // clang-format on

            txStages.push_back(stage);
            YQL_CLOG(TRACE, CoreDq) << "Added stage " << stage->UniqueId();
        }

        finalizedStages[id] = stage;
        YQL_CLOG(TRACE, CoreDq) << "Finalized stage " << id;
    }

    // Build a union all for the last stage
    int lastStageIdx = stageIds[stageIds.size() - 1];
    auto lastStage = finalizedStages.at(lastStageIdx);

    // clang-format off
    // wrap in DqResult
    auto dqResult = Build<TDqCnResult>(ctx, root.Pos)
        .Output()
            .Stage(lastStage)
            .Index().Build("0")
        .Build()
        .ColumnHints().Build()
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
            .Add(txStages)
        .Build()
        .Results()
            .Add({dqResult})
        .Build()
        .ParamBindings().Build()
        .Settings()
            .Add(txSettings)
        .Build()
    .Done().Ptr();

    TVector<TExprNode::TPtr> querySettings;
    querySettings.push_back(Build<TCoNameValueTuple>(ctx, root.Pos)
                                .Name().Build("type")
                                .Value<TCoAtom>().Build("data_query")
                            .Done().Ptr());
    // clang-format off

    // Build result type
    typeAnnTransformer->Rewind();
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = typeAnnTransformer->Transform(dqResult, dqResult, ctx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    if (status != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(ERROR, ProviderKqp) << "RBO type annotation failed." << Endl << ctx.IssueManager.GetIssues().ToString();
        return nullptr;
    }

    YQL_CLOG(TRACE, CoreDq) << "Inferred final type: " << *dqResult->GetTypeAnn();

    // clang-format off
    auto binding = Build<TKqpTxResultBinding>(ctx, root.Pos)
        .Type(ExpandType(root.Pos, *dqResult->GetTypeAnn(), ctx))
        .TxIndex().Build("0")
        .ResultIndex().Build("0")
    .Done();

    // Build Physical query
    auto physQuery = Build<TKqpPhysicalQuery>(ctx, root.Pos)
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

    // typeAnnTransformer->Rewind();
    // do {
    //    status = typeAnnTransformer->Transform(physQuery, physQuery, ctx);
    // } while (status == IGraphTransformer::TStatus::Repeat);

    YQL_CLOG(TRACE, CoreDq) << "Final plan built";

    return physQuery;
}
} // namespace NKqp
} // namespace NKikimr