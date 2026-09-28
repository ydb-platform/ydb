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

bool IsRunningFrame(const TOpWindowFrame& frame) {
    return frame.Type == EWindowFrameType::Rows && frame.BeginKind == EWindowFrameBound::UnboundedPreceding &&
           (frame.EndKind == EWindowFrameBound::CurrentRow ||
            (frame.EndKind == EWindowFrameBound::Following && frame.EndValue == 0));
}

bool IsWholePartitionFrame(const TOpWindowFrame& frame) {
    return (frame.Type == EWindowFrameType::Rows || frame.Type == EWindowFrameType::Range) &&
           frame.BeginKind == EWindowFrameBound::UnboundedPreceding && frame.EndKind == EWindowFrameBound::UnboundedFollowing;
}

bool IsRangeRunningFrame(const TOpWindowFrame& frame) {
    return frame.Type == EWindowFrameType::Range && frame.BeginKind == EWindowFrameBound::UnboundedPreceding &&
           frame.EndKind == EWindowFrameBound::CurrentRow;
}

bool IsRangeComparableType(const TTypeAnnotationNode* type) {
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    if (type->GetKind() != ETypeAnnotationKind::Data) {
        return false;
    }

    switch (type->Cast<TDataExprType>()->GetSlot()) {
        case NUdf::EDataSlot::Int8:
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Int16:
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Float:
        case NUdf::EDataSlot::Double:
        case NUdf::EDataSlot::Decimal:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Interval64:
        case NUdf::EDataSlot::Date:
        case NUdf::EDataSlot::Datetime:
        case NUdf::EDataSlot::Timestamp:
        case NUdf::EDataSlot::TzDate:
        case NUdf::EDataSlot::TzDatetime:
        case NUdf::EDataSlot::TzTimestamp:
        case NUdf::EDataSlot::Date32:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
        case NUdf::EDataSlot::TzDate32:
        case NUdf::EDataSlot::TzDatetime64:
        case NUdf::EDataSlot::TzTimestamp64:
            return true;
        default:
            return false;
    }
}

const TTypeAnnotationNode* SortColumnType(const TOpWindow& window, const TSortElement& sortElement) {
    const auto* inputType = window.GetInput()->Type;
    Y_ENSURE(inputType, "Window input has no type annotation");
    const auto* type = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->FindItemType(
        sortElement.SortColumn.GetFullName());
    Y_ENSURE(type, "Cannot find a type for the window sort column");
    return type;
}

bool HasAggregate(const TOpWindow& window) {
    for (const auto& func : window.GetWindowFuncs()) {
        if (func.Kind == EWindowFuncKind::Aggregate) {
            return true;
        }
    }
    return false;
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

bool TPhysicalWindowBuilder::UsesWholePartition(const TOpWindow& window) {
    if (!IsWholePartitionFrame(window.GetFrame())) {
        return false;
    }

    // For native function the frame means nothing.
    for (const auto& func : window.GetWindowFuncs()) {
        if (func.Kind == EWindowFuncKind::Native) {
            return false;
        }
    }
    return true;
}

bool TPhysicalWindowBuilder::UsesRangeCarry(const TOpWindow& window) {
    if (!IsRangeRunningFrame(window.GetFrame()) || window.GetSortElements().size() != 1) {
        return false;
    }

    if (!IsRangeComparableType(SortColumnType(window, window.GetSortElements().front()))) {
        return false;
    }
    return HasAggregate(window);
}

bool TPhysicalWindowBuilder::UsesRangePeerGroups(const TOpWindow& window) {
    if (!IsRangeRunningFrame(window.GetFrame()) || window.GetSortElements().empty() || UsesRangeCarry(window)) {
        return false;
    }

    for (const auto& sortElement : window.GetSortElements()) {
        const auto* type = SortColumnType(window, sortElement);
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }
        if (type->GetKind() != ETypeAnnotationKind::Data) {
            return false;
        }
    }
    return HasAggregate(window);
}

bool TPhysicalWindowBuilder::CanBuildWindow(const TOpWindow& window) {
    const bool running = IsRunningFrame(window.GetFrame());
    const bool wholePartition = UsesWholePartition(window);
    const bool rangeRunning = UsesRangeCarry(window) || UsesRangePeerGroups(window);

    for (const auto& func : window.GetWindowFuncs()) {
        if (func.Kind == EWindowFuncKind::Native) {
            if (!IsSupportedNativeFunction(func.Function) || !func.Arguments.empty()) {
                return false;
            }
            continue;
        }
        if (!IsSupportedAggregationFunction(func.Function) || !(running || wholePartition || rangeRunning)) {
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
    WholePartition = UsesWholePartition(*Window);
    RangeCarry = UsesRangeCarry(*Window);
    RangePeerGroups = UsesRangePeerGroups(*Window);
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
TExprNode::TPtr TPhysicalWindowBuilder::BuildAccumulator(const TOpWindowFunc& func, ui32 funcIndex, TExprNode::TPtr itemArg,
                                                         TExprNode::TPtr previousState, TExprNode::TPtr sortKeyChanged,
                                                         TVector<std::pair<TString, TExprNode::TPtr>>& stateMembers) const {
    const bool update = static_cast<bool>(previousState);
    const auto accName = AccumulatorName(funcIndex);

    if (func.Kind == EWindowFuncKind::Native) {
        if (!update) {
            if (func.Function == "rank") {
                stateMembers.emplace_back(PositionName(funcIndex), BuildUint64(1));
            }
            return BuildUint64(1);
        }
        if (func.Function == "rownumber") {
            return Ctx.Builder(Pos).Callable("Inc").Add(0, Member(previousState, accName)).Seal().Build();
        }
        if (func.Function == "denserank") {
            // clang-format off
            return Ctx.Builder(Pos)
                .Callable("If")
                    .Add(0, sortKeyChanged)
                    .Callable(1, "Inc")
                        .Add(0, Member(previousState, accName))
                    .Seal()
                    .Add(2, Member(previousState, accName))
                .Seal().Build();
            // clang-format on
        }

        // clang-format off
        auto position = Ctx.Builder(Pos)
            .Callable("Inc")
                .Add(0, Member(previousState, PositionName(funcIndex)))
            .Seal().Build();
        // clang-format on

        stateMembers.emplace_back(PositionName(funcIndex), position);
        // clang-format off
        return Ctx.Builder(Pos)
            .Callable("If")
                .Add(0, sortKeyChanged)
                .Add(1, position)
                .Add(2, Member(previousState, accName))
            .Seal().Build();
        // clang-format on
    }

    const auto& argument = func.Arguments.front();
    auto value = Member(itemArg, argument.GetFullName());
    const bool isOptional = InputItemType(argument)->IsOptionalOrNull();

    if (func.Function == "count") {
        if (!update) {
            // clang-format off
            return isOptional
                ? Ctx.Builder(Pos).Callable("AggrCountInit").Add(0, value).Seal().Build()
                : BuildUint64(1);
            // clang-format on
        }
        // clang-format off
        return isOptional
            ? Ctx.Builder(Pos).Callable("AggrCountUpdate").Add(0, value).Add(1, Member(previousState, accName)).Seal().Build()
            : Ctx.Builder(Pos).Callable("Inc").Add(0, Member(previousState, accName)).Seal().Build();
        // clang-format on
    }

    if (func.Function == "sum") {
        // clang-format off
        auto casted = MakeOptional(Ctx.Builder(Pos)
            .Callable("SafeCast")
                .Add(0, value)
                .Add(1, BuildSumCastTarget(argument))
            .Seal().Build(), isOptional);
        return update
            ? Ctx.Builder(Pos)
                .Callable("AggrAdd")
                    .Add(0, Member(previousState, accName))
                    .Add(1, casted)
                .Seal().Build()
            : casted;
        // clang-format on
    }

    if (func.Function == "avg") {
        // The accumulator is optional (sum, count).
        auto accType = BuildAvgAccumulatorType(argument);
        auto accData = BuildAvgAccumulatorDataType(argument);

        // clang-format off
        auto nothing = Ctx.Builder(Pos)
            .Callable("Nothing")
                .Callable(0, "OptionalType")
                    .Add(0, accType)
                .Seal()
            .Seal().Build();
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
            if (!isOptional) {
                return firstPair(value);
            }
            auto initArg = Ctx.NewArgument(Pos, "avg_init");
            auto initLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {initArg}), firstPair(initArg));
            // clang-format off
            return Ctx.Builder(Pos)
                .Callable("IfPresent")
                    .Add(0, value)
                    .Add(1, initLambda)
                    .Add(2, nothing)
                .Seal().Build();
            // clang-format on
        }

        auto previous = Member(previousState, accName);
        // clang-format off
        auto addTo = [&](TExprNode::TPtr state, TExprNode::TPtr present) {
            return Ctx.Builder(Pos)
                .Callable("Just")
                    .List(0)
                        .Callable(0, "AggrAdd")
                            .Callable(0, "Nth").Add(0, state).Atom(1, "0").Seal()
                            .Callable(1, "SafeCast")
                                .Add(0, present)
                                .Add(1, accData)
                            .Seal()
                        .Seal()
                        .Callable(1, "Inc")
                            .Callable(0, "Nth").Add(0, state).Atom(1, "1").Seal()
                        .Seal()
                    .Seal()
                .Seal().Build();
        };
        // clang-format on

        auto valueArg = Ctx.NewArgument(Pos, "avg_value");
        auto stateArg = Ctx.NewArgument(Pos, "avg_state");
        auto withValue = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {stateArg}), addTo(stateArg, valueArg));
        // clang-format off
        auto merged = Ctx.Builder(Pos)
            .Callable("IfPresent")
                .Add(0, previous)
                .Add(1, withValue)
                .Add(2, firstPair(valueArg))
            .Seal().Build();
        // clang-format on

        if (!isOptional) {
            return Ctx.ReplaceNode(std::move(merged), *valueArg, value);
        }
        auto outer = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {valueArg}), std::move(merged));
        // clang-format off
        return Ctx.Builder(Pos)
            .Callable("IfPresent")
                .Add(0, value)
                .Add(1, outer)
                .Add(2, previous)
            .Seal().Build();
        // clang-format on
    }

    auto current = MakeOptional(value, isOptional);
    // clang-format off
    return update
        ? Ctx.Builder(Pos)
            .Callable(func.Function == "min" ? "AggrMin" : "AggrMax")
                .Add(0, Member(previousState, accName))
                .Add(1, current)
            .Seal().Build()
        : current;
    // clang-format on
}

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
        auto accumulator = BuildAccumulator(func, f, itemArg, previousState, sortKeyChanged, stateMembers);
        stateMembers.emplace_back(AccumulatorName(f), accumulator);
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

// Processes whole partition and accumulates it into one value.
TExprNode::TPtr TPhysicalWindowBuilder::BuildFoldLambda(bool update) const {
    auto itemArg = Ctx.NewArgument(Pos, "fold_item");
    TExprNode::TListType args{itemArg};

    TExprNode::TPtr previousState;
    if (update) {
        previousState = Ctx.NewArgument(Pos, "fold_state");
        args.push_back(previousState);
    }

    TVector<std::pair<TString, TExprNode::TPtr>> stateMembers;
    const auto& funcs = Window->GetWindowFuncs();
    for (ui32 f = 0; f < funcs.size(); ++f) {
        stateMembers.emplace_back(AccumulatorName(f), BuildAccumulator(funcs[f], f, itemArg, previousState, nullptr, stateMembers));
    }

    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(args)), BuildStruct(stateMembers));
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildExpandFromStructs(TExprNode::TPtr list) const {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("ExpandMap")
            .Callable(0, "ToFlow")
                .Add(0, list)
            .Seal()
            .Lambda(1)
                .Param("win_row")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < OutputLayout.size(); ++i) {
                        parent
                            .Callable(i, "Member")
                                .Arg(0, "win_row")
                                .Atom(1, OutputLayout[i].GetFullName())
                            .Seal();
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildWholePartition(TExprNode::TPtr wideFlow) const {
    auto narrow = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(wideFlow, Inputs, Ctx);

    // clang-format off
    auto rows = Ctx.Builder(Pos)
        .Callable("Collect")
            .Add(0, narrow)
        .Seal().Build();
    // clang-format on

    // clang-format off
    auto folded = Ctx.Builder(Pos)
        .Callable("Fold1")
            .Add(0, rows)
            .Add(1, BuildFoldLambda(/*update=*/false))
            .Add(2, BuildFoldLambda(/*update=*/true))
        .Seal().Build();
    // clang-format on

    auto stateArg = Ctx.NewArgument(Pos, "win_partition_state");
    auto rowArg = Ctx.NewArgument(Pos, "win_partition_row");

    TVector<std::pair<TString, TExprNode::TPtr>> outputMembers;
    for (const auto& column : Inputs) {
        outputMembers.emplace_back(column.GetFullName(), Member(rowArg, column.GetFullName()));
    }
    const auto& funcs = Window->GetWindowFuncs();
    for (ui32 f = 0; f < funcs.size(); ++f) {
        outputMembers.emplace_back(funcs[f].ResultColName.GetFullName(),
                                   BuildResultFromAccumulator(funcs[f], Member(stateArg, AccumulatorName(f))));
    }

    auto rowLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {rowArg}), BuildStruct(outputMembers));
    // clang-format off
    auto attach = Ctx.Builder(Pos)
        .Callable("OrderedMap")
            .Add(0, rows)
            .Add(1, rowLambda)
        .Seal().Build();

    auto perState = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {stateArg}), std::move(attach));
    auto result = Ctx.Builder(Pos)
        .Callable("OrderedFlatMap")
            .Callable(0, "ToList")
                .Add(0, folded)
            .Seal()
            .Add(1, perState)
        .Seal().Build();
    // clang-format on

    return BuildExpandFromStructs(result);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRangeCarry(TExprNode::TPtr wideFlow) const {
    Y_ENSURE(Window->GetSortElements().size() == 1, "A RANGE frame needs exactly one sort column here");
    const auto& sortElement = Window->GetSortElements().front();
    const auto sortedColumn = sortElement.SortColumn.GetFullName();

    auto narrow = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(wideFlow, Inputs, Ctx);

    // clang-format off
    auto chained = Ctx.Builder(Pos)
        .Callable("Chain1Map")
            .Add(0, narrow)
            .Add(1, BuildChainLambda(/*update=*/false))
            .Add(2, BuildChainLambda(/*update=*/true))
        .Seal().Build();

    auto outputs = Ctx.Builder(Pos)
        .Callable("OrderedMap")
            .Add(0, chained)
            .Lambda(1)
                .Param("chained_row")
                .Callable("Nth")
                    .Arg(0, "chained_row")
                    .Atom(1, "0")
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on

    Y_ENSURE(Window->Type, "Window has no type annotation");
    auto queueRowType = ExpandType(Pos, *Window->Type->Cast<TListExprType>()->GetItemType(), Ctx);
    auto zero = Ctx.Builder(Pos).Callable("Uint64").Atom(0, "0").Seal().Build();

    // clang-format off
    auto queue = Ctx.Builder(Pos)
        .Callable("QueueCreate")
            .Add(0, queueRowType)
            .Callable(1, "Void").Seal()
            .Add(2, zero)
            .Callable(3, "DependsOn")
                .Callable(0, "FromFlow")
                    .Add(0, wideFlow)
                .Seal()
            .Seal()
        .Seal().Build();

    auto bounds = Ctx.Builder(Pos)
        .Callable("AsStruct")
            .List(0)
                .Atom(0, "RangeIncrementals")
                .List(1)
                    .Callable(0, "AsStruct")
                        .List(0).Atom(0, "Direction").Callable(1, "String").Atom(0, "Following").Seal().Seal()
                        .List(1)
                            .Atom(0, "Number")
                            .Callable(1, "AsTagged")
                                .Callable(0, "Void").Seal()
                                .Atom(1, "zero")
                            .Seal()
                        .Seal()
                        .List(2).Atom(0, "SortedColumn").Callable(1, "String").Atom(0, sortedColumn).Seal().Seal()
                    .Seal()
                .Seal()
            .Seal()
            .List(1).Atom(0, "RangeIntervals").List(1).Seal().Seal()
            .List(2).Atom(0, "RowIncrementals").List(1).Seal().Seal()
            .List(3).Atom(0, "RowIntervals").List(1).Seal().Seal()
        .Seal().Build();

    auto collected = Ctx.Builder(Pos)
        .Callable("WinFramesCollector")
            .Callable(0, "FromFlow")
                .Add(0, outputs)
            .Seal()
            .Add(1, queue)
            .Callable(2, "AsStruct")
                .List(0).Atom(0, "Bounds").Add(1, bounds).Seal()
                .List(1)
                    .Atom(0, "SortOrder")
                    .Callable(1, "String").Atom(0, sortElement.Ascending ? "Asc" : "Desc").Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on

    auto rowArg = Ctx.NewArgument(Pos, "range_row");
    // clang-format off
    auto groupLastRow = Ctx.Builder(Pos)
        .Callable("Unwrap")
            .Callable(0, "WinFrame")
                .Add(0, queue)
                .Add(1, zero)
                .Callable(2, "Bool").Atom(0, "true").Seal()
                .Callable(3, "Bool").Atom(0, "true").Seal()
                .Callable(4, "Bool").Atom(0, "true").Seal()
                .Callable(5, "DependsOn").Add(0, rowArg).Seal()
            .Seal()
        .Seal().Build();
    // clang-format on

    TVector<std::pair<TString, TExprNode::TPtr>> members;
    for (const auto& column : Inputs) {
        members.emplace_back(column.GetFullName(), Member(rowArg, column.GetFullName()));
    }
    for (const auto& func : Window->GetWindowFuncs()) {
        const auto name = func.ResultColName.GetFullName();
        members.emplace_back(name, func.Kind == EWindowFuncKind::Aggregate ? Member(groupLastRow, name) : Member(rowArg, name));
    }

    auto rowLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {rowArg}), BuildStruct(members));
    // clang-format off
    auto result = Ctx.Builder(Pos)
        .Callable("OrderedMap")
            .Callable(0, "ToFlow")
                .Add(0, collected)
            .Seal()
            .Add(1, rowLambda)
        .Seal().Build();
    // clang-format on

    return BuildExpandFromStructs(result);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRangePeerGroups(TExprNode::TPtr wideFlow) const {
    const auto& sortElements = Window->GetSortElements();
    auto narrow = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(wideFlow, Inputs, Ctx);

    // clang-format off
    auto chained = Ctx.Builder(Pos)
        .Callable("Chain1Map")
            .Add(0, narrow)
            .Add(1, BuildChainLambda(/*update=*/false))
            .Add(2, BuildChainLambda(/*update=*/true))
        .Seal().Build();

    auto outputs = Ctx.Builder(Pos)
        .Callable("OrderedMap")
            .Add(0, chained)
            .Lambda(1)
                .Param("chained_row")
                .Callable("Nth")
                    .Arg(0, "chained_row")
                    .Atom(1, "0")
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on

    Y_ENSURE(Window->Type, "Window has no type annotation");
    auto rowType = ExpandType(Pos, *Window->Type->Cast<TListExprType>()->GetItemType(), Ctx);
    // clang-format off
    auto variantType = Ctx.Builder(Pos)
        .Callable("VariantType")
            .Callable(0, "StructType")
                .List(0)
                    .Atom(0, "singleRow")
                    .Add(1, rowType)
                .Seal()
                .List(1)
                    .Atom(0, "group")
                    .Callable(1, "ListType")
                        .Add(0, rowType)
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on

    auto makeVariant = [&](TExprNode::TPtr value, const TString& name) {
        return Ctx.NewCallable(Pos, "Variant", {std::move(value), Ctx.NewAtom(Pos, name), variantType});
    };
    auto sortKey = [&](TExprNode::TPtr row) {
        TExprNode::TListType items;
        for (const auto& sortElement : sortElements) {
            items.push_back(Member(row, sortElement.SortColumn.GetFullName()));
        }
        return Ctx.NewList(Pos, std::move(items));
    };
    auto nth = [&](TExprNode::TPtr tuple, ui32 index) {
        return Ctx.NewCallable(Pos, "Nth", {std::move(tuple), Ctx.NewAtom(Pos, ToString(index))});
    };

    auto initRow = Ctx.NewArgument(Pos, "peer_row");
    auto initLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {initRow}),
                                    Ctx.NewList(Pos, {sortKey(initRow), makeVariant(initRow, "singleRow")}));

    auto switchRow = Ctx.NewArgument(Pos, "peer_row");
    auto switchState = Ctx.NewArgument(Pos, "peer_state");
    TExprNode::TListType comparisons;
    for (ui32 k = 0; k < sortElements.size(); ++k) {
        comparisons.push_back(Ctx.NewCallable(Pos, "AggrNotEquals",
                                              {Member(switchRow, sortElements[k].SortColumn.GetFullName()), nth(nth(switchState, 0), k)}));
    }
    auto switchBody = comparisons.size() == 1 ? comparisons.front() : Ctx.NewCallable(Pos, "Or", std::move(comparisons));
    auto switchLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {switchRow, switchState}), std::move(switchBody));

    auto updateRow = Ctx.NewArgument(Pos, "peer_row");
    auto updateState = Ctx.NewArgument(Pos, "peer_state");
    auto singleArg = Ctx.NewArgument(Pos, "single_row");
    auto groupArg = Ctx.NewArgument(Pos, "group");
    // clang-format off
    auto grown = Ctx.Builder(Pos)
        .Callable("Visit")
            .Add(0, nth(updateState, 1))
            .Atom(1, "singleRow")
            .Add(2, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {singleArg}),
                                  makeVariant(Ctx.NewCallable(Pos, "AsList", {singleArg, updateRow}), "group")))
            .Atom(3, "group")
            .Add(4, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {groupArg}),
                                  makeVariant(Ctx.NewCallable(Pos, "Append", {groupArg, updateRow}), "group")))
        .Seal().Build();
    // clang-format on
    auto updateLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {updateRow, updateState}),
                                      Ctx.NewList(Pos, {nth(updateState, 0), std::move(grown)}));

    // clang-format off
    auto groups = Ctx.Builder(Pos)
        .Callable("OrderedMap")
            .Callable(0, "Condense1")
                .Add(0, outputs)
                .Add(1, initLambda)
                .Add(2, switchLambda)
                .Add(3, updateLambda)
            .Seal()
            .Lambda(1)
                .Param("peer_state")
                .Callable("Nth")
                    .Arg(0, "peer_state")
                    .Atom(1, "1")
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on

    auto rowArg = Ctx.NewArgument(Pos, "group_row");
    auto lastRowArg = Ctx.NewArgument(Pos, "last_row");
    TVector<std::pair<TString, TExprNode::TPtr>> members;
    for (const auto& column : Inputs) {
        members.emplace_back(column.GetFullName(), Member(rowArg, column.GetFullName()));
    }
    for (const auto& func : Window->GetWindowFuncs()) {
        const auto name = func.ResultColName.GetFullName();
        members.emplace_back(name, func.Kind == EWindowFuncKind::Aggregate ? Member(lastRowArg, name) : Member(rowArg, name));
    }
    auto rowLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {rowArg}), BuildStruct(members));

    auto itemArg = Ctx.NewArgument(Pos, "peer_item");
    auto singleItem = Ctx.NewArgument(Pos, "single_row");
    auto groupItem = Ctx.NewArgument(Pos, "group");
    // clang-format off
    auto overwritten = Ctx.Builder(Pos)
        .Callable("Coalesce")
            .Callable(0, "Map")
                .Callable(0, "Last")
                    .Add(0, groupItem)
                .Seal()
                .Add(1, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {lastRowArg}),
                                      Ctx.NewCallable(Pos, "OrderedMap", {groupItem, rowLambda})))
            .Seal()
            .Callable(1, "EmptyList").Seal()
        .Seal().Build();

    auto expanded = Ctx.Builder(Pos)
        .Callable("Visit")
            .Add(0, itemArg)
            .Atom(1, "singleRow")
            .Add(2, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {singleItem}), Ctx.NewCallable(Pos, "AsList", {singleItem})))
            .Atom(3, "group")
            .Add(4, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {groupItem}), std::move(overwritten)))
        .Seal().Build();

    auto result = Ctx.Builder(Pos)
        .Callable("OrderedFlatMap")
            .Add(0, groups)
            .Add(1, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {itemArg}), std::move(expanded)))
        .Seal().Build();
    // clang-format on

    return BuildExpandFromStructs(result);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildPartitionHandler(TExprNode::TPtr wideFlow) const {
    if (WholePartition) {
        return BuildWholePartition(wideFlow);
    }
    if (RangeCarry) {
        return BuildRangeCarry(wideFlow);
    }
    if (RangePeerGroups) {
        return BuildRangePeerGroups(wideFlow);
    }
    return BuildChain(wideFlow);
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
        input = BuildPartitionHandler(input);
    } else {
        TExprNode::TListType handlerArgs;
        for (ui32 i = 0; i < Window->GetPartitionKeys().size(); ++i) {
            handlerArgs.push_back(Ctx.NewArgument(Pos, "chop_key_" + ToString(i)));
        }
        auto flowArg = Ctx.NewArgument(Pos, "chop_flow");
        handlerArgs.push_back(flowArg);

        auto handlerBody = BuildPartitionHandler(flowArg);
        auto handlerLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(handlerArgs)), std::move(handlerBody));

        // clang-format off
        input = Ctx.Builder(Pos)
            .Callable("WideChopper")
                .Add(0, input)
                .Add(1, BuildKeyExtractorLambda())
                .Add(2, BuildGroupSwitchLambda())
                .Add(3, handlerLambda)
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
