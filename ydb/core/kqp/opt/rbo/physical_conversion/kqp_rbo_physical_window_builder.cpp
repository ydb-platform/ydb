#include "kqp_rbo_physical_window_builder.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace {

bool IsSupportedAggregationFunction(const TString& function) {
    return function == "sum" || function == "min" || function == "max" || function == "count" || function == "avg";
}

bool IsSupportedNativeFunction(const TString& function) {
    return function == "rank" || function == "denserank" || function == "rownumber";
}

bool IsDecimal(const TTypeAnnotationNode* type) {
    if (type->IsOptionalOrNull()) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    return type->GetKind() == ETypeAnnotationKind::Data && type->Cast<TDataExprType>()->GetName().starts_with("Decimal");
}

std::pair<TString, TString> DecimalParams(const TTypeAnnotationNode* type) {
    if (type->IsOptionalOrNull()) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    const auto* params = dynamic_cast<const TDataExprParamsType*>(type);
    Y_ENSURE(params, "Expected a Decimal type");
    return {TString(params->GetParamOne()), TString(params->GetParamTwo())};
}

} // anonymous namespace

bool TPhysicalWindowBuilder::CanBuildWindow(const TOpWindow& window) {
    const auto& frame = window.GetFrame();
    // TODO: Add support for other frame types.
    const bool runningFrame = frame.Type == EWindowFrameType::Rows && frame.BeginKind == EWindowFrameBound::UnboundedPreceding &&
                              (frame.EndKind == EWindowFrameBound::CurrentRow ||
                               (frame.EndKind == EWindowFrameBound::Following && frame.EndValue == 0));

    for (const auto& func : window.GetWindowFuncs()) {
        if (func.Kind == EWindowFuncKind::Native) {
            if (!IsSupportedNativeFunction(func.Function) || !func.Arguments.empty()) {
                return false;
            }
            continue;
        }
        if (!IsSupportedAggregationFunction(func.Function) || !runningFrame) {
            return false;
        }
    }
    return true;
}

void TPhysicalWindowBuilder::Prepare(const TVector<TInfoUnit>& inputs) {
    Inputs = inputs;
    for (ui32 i = 0; i < inputs.size(); ++i) {
        Indexes.emplace(inputs[i].GetFullName(), i);
    }

    const auto* inputType = Window->GetInput()->Type;
    Y_ENSURE(inputType, "Window input has no type annotation");
    InputStruct = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    OutputLayout = inputs;
    for (const auto& func : Window->GetWindowFuncs()) {
        OutputLayout.push_back(func.ResultColName);
        NeedsPeerKey = NeedsPeerKey || func.Function == "rank" || func.Function == "denserank";
    }
    NeedsPeerKey = NeedsPeerKey && !Window->GetSortElements().empty();
}

ui32 TPhysicalWindowBuilder::IndexOf(const TInfoUnit& column) const {
    auto it = Indexes.find(column.GetFullName());
    Y_ENSURE(it != Indexes.end(), "Cannot find window column " << column.GetFullName() << " in the wide input");
    return it->second;
}

const TTypeAnnotationNode* TPhysicalWindowBuilder::InputItemType(const TInfoUnit& column) const {
    const auto* type = InputStruct->FindItemType(column.GetFullName());
    Y_ENSURE(type, "Cannot find a type for window column " << column.GetFullName());
    return type;
}

TString TPhysicalWindowBuilder::AccumulatorName(ui32 funcIndex) const {
    return "__kqp_win_acc_" + ToString(funcIndex);
}

TString TPhysicalWindowBuilder::PositionName(ui32 funcIndex) const {
    return "__kqp_win_pos_" + ToString(funcIndex);
}

TString TPhysicalWindowBuilder::PeerName(ui32 sortIndex) const {
    return "__kqp_win_peer_" + ToString(sortIndex);
}

TExprNode::TPtr TPhysicalWindowBuilder::Member(TExprNode::TPtr from, const TString& name) const {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Member")
            .Add(0, from)
            .Atom(1, name)
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildStruct(const TVector<std::pair<TString, TExprNode::TPtr>>& members) const {
    TExprNode::TListType items;
    for (const auto& [name, value] : members) {
        // clang-format off
        items.push_back(Ctx.Builder(Pos)
            .List()
                .Atom(0, name)
                .Add(1, value)
            .Seal().Build());
        // clang-format on
    }
    return Ctx.NewCallable(Pos, "AsStruct", std::move(items));
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildUint64(ui64 value) const {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Uint64")
            .Atom(0, ToString(value))
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::MakeOptional(TExprNode::TPtr value, bool alreadyOptional) const {
    if (alreadyOptional) {
        return value;
    }
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Just")
            .Add(0, value)
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildSumCastTarget(const TInfoUnit& column) const {
    const auto* itemType = InputItemType(column);
    const TTypeAnnotationNode* sumType = nullptr;
    Y_ENSURE(GetSumResultType(Pos, *itemType, sumType, Ctx), "Unsupported type for sum over a window");
    if (sumType->IsOptionalOrNull()) {
        sumType = sumType->Cast<TOptionalExprType>()->GetItemType();
    }
    return ExpandType(Pos, *sumType, Ctx);
}

TVector<TExprNode::TPtr> TPhysicalWindowBuilder::BuildSortKeys() const {
    TVector<TExprNode::TPtr> keys;

    auto add = [&](ui32 index, bool ascending) {
        // clang-format off
        keys.push_back(Ctx.Builder(Pos)
            .List()
                .Atom(0, ToString(index))
                .Callable(1, "Bool")
                    .Atom(0, ascending ? "true" : "false")
                .Seal()
            .Seal().Build());
        // clang-format on
    };

    for (const auto& key : Window->GetPartitionKeys()) {
        add(IndexOf(key), true);
    }
    for (const auto& element : Window->GetSortElements()) {
        add(IndexOf(element.SortColumn), element.Ascending);
    }
    return keys;
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildAvgAccumulatorDataType(const TInfoUnit& column) const {
    const auto* itemType = InputItemType(column);
    if (!IsDecimal(itemType)) {
        // clang-format off
        return Ctx.Builder(Pos)
            .Callable("DataType")
            .Atom(0, "Double")
        .Seal().Build();
        // clang-format on
    }

    const auto [precision, scale] = DecimalParams(itemType);
    Y_UNUSED(precision);

    // For decimal we use 35 precision for accumulator.
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("DataType")
            .Atom(0, "Decimal")
            .Atom(1, "35")
            .Atom(2, scale)
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildAvgAccumulatorType(const TInfoUnit& column) const {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("TupleType")
            .Add(0, BuildAvgAccumulatorDataType(column))
            .Callable(1, "DataType")
                .Atom(0, "Uint64")
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildKeyExtractorLambda() const {
    TExprNode::TListType args;
    for (ui32 i = 0; i < Inputs.size(); ++i) {
        args.push_back(Ctx.NewArgument(Pos, "key_row_" + ToString(i)));
    }

    TExprNode::TListType results;
    for (const auto& key : Window->GetPartitionKeys()) {
        results.push_back(args[IndexOf(key)]);
    }
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(args)), std::move(results));
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildGroupSwitchLambda() const {
    const auto& partitionKeys = Window->GetPartitionKeys();

    TExprNode::TListType args;
    TExprNode::TListType keyArgs;
    for (ui32 i = 0; i < partitionKeys.size(); ++i) {
        keyArgs.push_back(Ctx.NewArgument(Pos, "switch_key_" + ToString(i)));
        args.push_back(keyArgs.back());
    }
    TExprNode::TListType rowArgs;
    for (ui32 i = 0; i < Inputs.size(); ++i) {
        rowArgs.push_back(Ctx.NewArgument(Pos, "switch_row_" + ToString(i)));
        args.push_back(rowArgs.back());
    }

    // Checking for a new group.
    TExprNode::TListType comparisons;
    for (ui32 i = 0; i < partitionKeys.size(); ++i) {
        // clang-format off
        comparisons.push_back(Ctx.Builder(Pos)
            .Callable("AggrNotEquals")
                .Add(0, keyArgs[i])
                .Add(1, rowArgs[IndexOf(partitionKeys[i])])
            .Seal().Build());
        // clang-format on
    }

    TExprNode::TPtr body = comparisons.size() == 1 ? comparisons.front() : Ctx.NewCallable(Pos, "Or", std::move(comparisons));
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(args)), std::move(body));
}

// ChainMap has 2 lambdas:
// 1) init row has an access to the first row.
// 2) update row has an access to state and a current row.
TExprNode::TPtr TPhysicalWindowBuilder::BuildChainLambda(bool update) const {
    auto itemArg = Ctx.NewArgument(Pos, "win_item");
    TExprNode::TListType args{itemArg};

    TExprNode::TPtr previousState;
    if (update) {
        // Get the state/prev row.
        auto previousArg = Ctx.NewArgument(Pos, "win_prev");
        args.push_back(previousArg);
        // clang-format off
        previousState = Ctx.Builder(Pos)
            .Callable("Nth")
                .Add(0, previousArg)
                .Atom(1, "1")
            .Seal().Build();
        // clang-format on
    }

    const auto& sortElements = Window->GetSortElements();
    TExprNode::TPtr sortKeyChanged;
    if (update && NeedsPeerKey) {
        TExprNode::TListType comparisons;
        for (ui32 k = 0; k < sortElements.size(); ++k) {
            // clang-format off
            comparisons.push_back(Ctx.Builder(Pos)
                .Callable("AggrNotEquals")
                    .Add(0, Member(itemArg, sortElements[k].SortColumn.GetFullName()))
                    .Add(1, Member(previousState, PeerName(k)))
                .Seal().Build());
            // clang-format on
        }
        sortKeyChanged = comparisons.size() == 1 ? comparisons.front() : Ctx.NewCallable(Pos, "Or", std::move(comparisons));
    }

    TVector<std::pair<TString, TExprNode::TPtr>> stateMembers;
    TVector<std::pair<TString, TExprNode::TPtr>> outputMembers;
    for (const auto& column : Inputs) {
        outputMembers.emplace_back(column.GetFullName(), Member(itemArg, column.GetFullName()));
    }

    const auto& funcs = Window->GetWindowFuncs();
    for (ui32 f = 0; f < funcs.size(); ++f) {
        const auto& func = funcs[f];
        const auto accName = AccumulatorName(f);
        TExprNode::TPtr accumulator;

        if (func.Kind == EWindowFuncKind::Native) {
            if (!update) {
                accumulator = BuildUint64(1);
                if (func.Function == "rank") {
                    stateMembers.emplace_back(PositionName(f), BuildUint64(1));
                }
            } else if (func.Function == "rownumber") {
                // clang-format off
                accumulator = Ctx.Builder(Pos)
                    .Callable("Inc")
                        .Add(0, Member(previousState, accName))
                    .Seal().Build();
                // clang-format on
            } else if (func.Function == "denserank") {
                // clang-format off
                accumulator = Ctx.Builder(Pos)
                    .Callable("If")
                        .Add(0, sortKeyChanged)
                        .Callable(1, "Inc")
                            .Add(0, Member(previousState, accName))
                        .Seal()
                        .Add(2, Member(previousState, accName))
                    .Seal().Build();
                // clang-format on
            } else {
                // clang-format off
                auto position = Ctx.Builder(Pos)
                    .Callable("Inc")
                        .Add(0, Member(previousState, PositionName(f)))
                    .Seal().Build();

                accumulator = Ctx.Builder(Pos)
                    .Callable("If")
                        .Add(0, sortKeyChanged)
                        .Add(1, position)
                        .Add(2, Member(previousState, accName))
                    .Seal().Build();
                // clang-format on
                stateMembers.emplace_back(PositionName(f), position);
            }
        } else {
            const auto& argument = func.Arguments.front();
            auto value = Member(itemArg, argument.GetFullName());
            const bool isOptional = InputItemType(argument)->IsOptionalOrNull();

            if (func.Function == "count") {
                if (!update) {
                    // clang-format off
                    accumulator = isOptional
                        ? Ctx.Builder(Pos)
                            .Callable("AggrCountInit")
                                .Add(0, value)
                        .Seal().Build()
                        : BuildUint64(1);
                    // clang-format off
                } else {
                    // clang-format off
                    accumulator = isOptional
                        ? Ctx.Builder(Pos)
                            .Callable("AggrCountUpdate")
                                .Add(0, value)
                                .Add(1, Member(previousState, accName))
                            .Seal()
                        .Build()

                        : Ctx.Builder(Pos)
                            .Callable("Inc")
                            .Add(0, Member(previousState, accName))
                        .Seal().Build();
                    // clang-format on
                }
            } else if (func.Function == "sum") {
                // clang-format off
                auto casted = MakeOptional(Ctx.Builder(Pos)
                    .Callable("SafeCast")
                        .Add(0, value)
                        .Add(1, BuildSumCastTarget(argument))
                    .Seal().Build(), isOptional);

                accumulator = update
                    ? Ctx.Builder(Pos)
                        .Callable("AggrAdd")
                            .Add(0, Member(previousState, accName))
                            .Add(1, casted)
                        .Seal().Build()
                    : casted;
                // clang-format on
            } else if (func.Function == "avg") {
                auto accType = BuildAvgAccumulatorType(argument);
                auto accData = BuildAvgAccumulatorDataType(argument);

                // clang-format off
                auto nothing = Ctx.Builder(Pos)
                    .Callable("Nothing")
                    .Callable(0, "OptionalType")
                .Add(0, accType).Seal().Seal().Build();
                // clang-format on

                // clang-format off
                auto firstPair = [&](TExprNode::TPtr present) {
                    return Ctx.Builder(Pos)
                        .Callable("Just")
                            .List(0)
                                .Callable(0, "SafeCast")
                                    .Add(0, present)
                                    .Add(1, accData)
                                .Seal()
                                .Callable(1, "Uint64").Atom(0, "1").Seal()
                            .Seal()
                        .Seal().Build();
                };
                // clang-format on

                if (!update) {
                    if (isOptional) {
                        auto initArg = Ctx.NewArgument(Pos, "avg_init");
                        auto initLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {initArg}), firstPair(initArg));
                        // clang-format off
                        accumulator = Ctx.Builder(Pos)
                            .Callable("IfPresent")
                                .Add(0, value)
                                .Add(1, initLambda)
                                .Add(2, nothing)
                            .Seal().Build();
                        // clang-format on
                    } else {
                        accumulator = firstPair(value);
                    }
                } else {
                    auto previous = Member(previousState, accName);
                    // clang-format off
                    auto addAndInc = [&](TExprNode::TPtr state, TExprNode::TPtr present) {
                        return Ctx.Builder(Pos)
                            .Callable("Just")
                                .List(0)
                                    .Callable(0, "AggrAdd")
                                        .Callable(0, "Nth")
                                            .Add(0, state)
                                            .Atom(1, "0")
                                        .Seal()
                                        .Callable(1, "SafeCast")
                                            .Add(0, present)
                                            .Add(1, accData)
                                        .Seal()
                                    .Seal()
                                    .Callable(1, "Inc")
                                        .Callable(0, "Nth")
                                            .Add(0, state)
                                            .Atom(1, "1")
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal().Build();
                    };
                    // clang-format on

                    auto valueArg = Ctx.NewArgument(Pos, "avg_value");
                    auto stateArg = Ctx.NewArgument(Pos, "avg_state");
                    auto withValue = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {stateArg}), addAndInc(stateArg, valueArg));
                    // clang-format off
                    auto merged = Ctx.Builder(Pos)
                        .Callable("IfPresent")
                            .Add(0, previous)
                            .Add(1, withValue)
                            .Add(2, firstPair(valueArg))
                        .Seal().Build();
                    // clang-format on

                    if (isOptional) {
                        auto outer = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {valueArg}), std::move(merged));
                        // clang-format off
                        accumulator = Ctx.Builder(Pos)
                            .Callable("IfPresent")
                                .Add(0, value)
                                .Add(1, outer)
                                .Add(2, previous)
                            .Seal().Build();
                        // clang-format on
                    } else {
                        accumulator = Ctx.ReplaceNode(std::move(merged), *valueArg, value);
                    }
                }
            } else {
                auto current = MakeOptional(value, isOptional);
                // clang-format off
                accumulator = update
                    ? Ctx.Builder(Pos)
                        .Callable(func.Function == "min" ? "AggrMin" : "AggrMax")
                            .Add(0, Member(previousState, accName))
                            .Add(1, current)
                        .Seal().Build()
                    : current;
                // clang-format on
            }
        }

        stateMembers.emplace_back(accName, accumulator);
        outputMembers.emplace_back(func.ResultColName.GetFullName(), BuildResultFromAccumulator(func, accumulator));
    }

    if (NeedsPeerKey) {
        for (ui32 k = 0; k < sortElements.size(); ++k) {
            stateMembers.emplace_back(PeerName(k), Member(itemArg, sortElements[k].SortColumn.GetFullName()));
        }
    }

    // clang-format off
    auto body = Ctx.Builder(Pos)
        .List()
            .Add(0, BuildStruct(outputMembers))
            .Add(1, BuildStruct(stateMembers))
        .Seal().Build();
    // clang-format on

    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(args)), std::move(body));
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildResultFromAccumulator(const TOpWindowFunc& func, TExprNode::TPtr accumulator) const {
    if (func.Kind == EWindowFuncKind::Native || func.Function != "avg") {
        return accumulator;
    }

    const auto& argument = func.Arguments.front();
    const auto* itemType = InputItemType(argument);

    auto pairArg = Ctx.NewArgument(Pos, "avg_result");
    // clang-format off
    auto sum = Ctx.Builder(Pos)
        .Callable("Nth")
            .Add(0, pairArg)
            .Atom(1, "0")
        .Seal().Build();
    // clang-format on

    // clang-format off
    auto count = Ctx.Builder(Pos)
        .Callable("Nth")
            .Add(0, pairArg)
            .Atom(1, "1")
        .Seal().Build();
    // clang-format on

    TExprNode::TPtr resultType;
    TExprNode::TPtr value;
    if (IsDecimal(itemType)) {
        const auto [precision, scale] = DecimalParams(itemType);
        // clang-format off
        resultType = Ctx.Builder(Pos)
            .Callable("DataType")
                .Atom(0, "Decimal")
                .Atom(1, precision)
                .Atom(2, scale)
            .Seal().Build();

        value = Ctx.Builder(Pos)
            .Callable("SafeCast")
                .Callable(0, "DecimalDiv")
                    .Add(0, sum)
                    .Add(1, count)
                .Seal()
                .Add(1, resultType)
            .Seal().Build();
        // clang-format on
    } else {
        // clang-format off
        resultType = Ctx.Builder(Pos)
            .Callable("DataType")
                .Atom(0, "Double")
            .Seal().Build();

        value = Ctx.Builder(Pos)
            .Callable("Div")
                .Add(0, sum)
                .Add(1, count)
            .Seal().Build();
        // clang-format on
    }

    // clang-format off
    auto divide = Ctx.Builder(Pos)
        .Callable("Just")
            .Add(0, value)
        .Seal().Build();

    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, accumulator)
            .Add(1, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {pairArg}), std::move(divide)))
            .Callable(2, "Nothing")
                .Callable(0, "OptionalType")
                    .Add(0, resultType)
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildExpandFromChain(TExprNode::TPtr chained) const {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("ExpandMap")
            .Add(0, chained)
            .Lambda(1)
                .Param("win_chained")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < OutputLayout.size(); ++i) {
                        parent
                            .Callable(i, "Member")
                                .Callable(0, "Nth")
                                    .Arg(0, "win_chained")
                                    .Atom(1, "0")
                                .Seal()
                                .Atom(1, OutputLayout[i].GetFullName())
                            .Seal();
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildChain(TExprNode::TPtr wideFlow) const {
    auto narrow = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(wideFlow, Inputs, Ctx);

    // clang-format off
    auto chained = Ctx.Builder(Pos)
        .Callable("Chain1Map")
            .Add(0, narrow)
            // Only for the first row.
            .Add(1, BuildChainLambda(/*update=*/false))
            // Computes a state it has access to the prev row.
            .Add(2, BuildChainLambda(/*update=*/true))
        .Seal().Build();
    // clang-format on

    return BuildExpandFromChain(chained);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildPhysicalOp(TExprNode::TPtr input) {
    Y_ENSURE(CanBuildWindow(*Window), "This window cannot be evaluated by a single forward pass");

    Prepare(NPhysicalConvertionUtils::GetLiveInputIUs(*Window, 0));

    // clang-format off
    input = Build<TCoToFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    input = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(input, Inputs, Ctx);

    // Sort for the partitioning keys + order by keys.
    if (const auto sortKeys = BuildSortKeys(); !sortKeys.empty()) {
        // clang-format off
        input = Build<TCoWideSort>(Ctx, Pos)
            .Input(input)
            .Keys<TCoSortKeys>()
                .Add(sortKeys)
            .Build()
        .Done().Ptr();
        // clang-format on
    }

    if (Window->GetPartitionKeys().empty()) {
        // If no partitions we use whole stage as a partition.
        input = BuildChain(input);
    } else {
        TExprNode::TListType handlerArgs;
        for (ui32 i = 0; i < Window->GetPartitionKeys().size(); ++i) {
            handlerArgs.push_back(Ctx.NewArgument(Pos, "chop_key_" + ToString(i)));
        }
        auto flowArg = Ctx.NewArgument(Pos, "chop_flow");
        handlerArgs.push_back(flowArg);
        auto handler = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(handlerArgs)), BuildChain(flowArg));

        // clang-format off
        input = Ctx.Builder(Pos)
            .Callable("WideChopper")
                .Add(0, input)
                .Add(1, BuildKeyExtractorLambda())
                .Add(2, BuildGroupSwitchLambda())
                .Add(3, handler)
            .Seal().Build();
        // clang-format on
    }

    input = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(
        input,
        OutputLayout,
        NPhysicalConvertionUtils::BuildNameSet(NPhysicalConvertionUtils::GetLiveOutputIUs(*Window)),
        Ctx);

    // clang-format off
    input = Build<TCoFromFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical window] " << KqpExprToPrettyString(TExprBase(input), Ctx);
    return input;
}
