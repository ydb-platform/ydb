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

bool IsRangeRunningFrame(const TOpWindowFrame& frame) {
    return frame.Type == EWindowFrameType::Range && frame.BeginKind == EWindowFrameBound::UnboundedPreceding &&
           frame.EndKind == EWindowFrameBound::CurrentRow;
}

bool IsRowIncrementalFrame(const TOpWindowFrame& frame) {
    return frame.Type == EWindowFrameType::Rows && frame.BeginKind == EWindowFrameBound::UnboundedPreceding &&
           (frame.EndKind == EWindowFrameBound::Preceding || frame.EndKind == EWindowFrameBound::Following) && !IsRunningFrame(frame);
}

bool IsRowSuffixFrame(const TOpWindowFrame& frame) {
    const bool startsAtCurrentRow = frame.BeginKind == EWindowFrameBound::CurrentRow ||
                                    ((frame.BeginKind == EWindowFrameBound::Preceding || frame.BeginKind == EWindowFrameBound::Following) &&
                                     frame.BeginValue == 0);
    return frame.Type == EWindowFrameType::Rows && startsAtCurrentRow && frame.EndKind == EWindowFrameBound::UnboundedFollowing;
}

bool IsRangeOffsetFrame(const TOpWindowFrame& frame) {
    return frame.Type == EWindowFrameType::Range && !IsRangeRunningFrame(frame) && !frame.IsWholePartition();
}

bool IsRangeIncrementalFrame(const TOpWindowFrame& frame) {
    return frame.Type == EWindowFrameType::Range && frame.BeginKind == EWindowFrameBound::UnboundedPreceding &&
           (frame.EndKind == EWindowFrameBound::Preceding || frame.EndKind == EWindowFrameBound::Following);
}

bool HasFrameOffset(const TOpWindowFrame& frame) {
    auto isOffset = [](EWindowFrameBound kind) {
        return kind == EWindowFrameBound::Preceding || kind == EWindowFrameBound::Following;
    };
    return isOffset(frame.BeginKind) || isOffset(frame.EndKind);
}

bool IsRangeOffsetType(const TTypeAnnotationNode* type) {
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    if (type->GetKind() != ETypeAnnotationKind::Data) {
        return false;
    }
    const auto slot = type->Cast<TDataExprType>()->GetSlot();
    return IsDataTypeIntegral(slot) || slot == EDataSlot::Float || slot == EDataSlot::Double;
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

const TTypeAnnotationNode* SortColumnType(const TOpWindow& window, TInfoUnitId column) {
    const auto* inputType = window.GetInput()->Type;
    Y_ENSURE(inputType, "Window input has no type annotation");
    const auto* type = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->FindItemType(ToString(column));
    Y_ENSURE(type, "Cannot find a type for the window sort column");
    return type;
}

bool HasAggregate(const TOpWindow& window) {
    for (const auto& [output, func] : window.GetWindowFuncs().Items()) {
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
    if (!window.GetFrame().IsWholePartition()) {
        return false;
    }

    // For native function the frame means nothing.
    for (const auto& [output, func] : window.GetWindowFuncs().Items()) {
        if (func.Kind == EWindowFuncKind::Native) {
            return false;
        }
    }
    return true;
}

bool TPhysicalWindowBuilder::UsesRangeCarry(const TOpWindow& window) {
    if (!IsRangeRunningFrame(window.GetFrame()) || window.GetSortElements().Items().size() != 1) {
        return false;
    }

    if (!IsRangeComparableType(SortColumnType(window, window.GetSortElements().Items().front().first))) {
        return false;
    }
    return HasAggregate(window);
}

bool TPhysicalWindowBuilder::UsesRangePeerGroups(const TOpWindow& window) {
    if (!IsRangeRunningFrame(window.GetFrame()) || window.GetSortElements().Items().empty() || UsesRangeCarry(window)) {
        return false;
    }

    for (const auto& sortElement : window.GetSortElements().Items()) {
        const auto* type = SortColumnType(window, sortElement.first);
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }
        if (type->GetKind() != ETypeAnnotationKind::Data) {
            return false;
        }
    }
    return HasAggregate(window);
}

bool TPhysicalWindowBuilder::UsesRowFrames(const TOpWindow& window) {
    const auto& frame = window.GetFrame();
    if (frame.Type != EWindowFrameType::Rows || IsRunningFrame(frame) || frame.IsWholePartition()) {
        return false;
    }
    return HasAggregate(window);
}

bool TPhysicalWindowBuilder::UsesRangeFrames(const TOpWindow& window) {
    const auto& frame = window.GetFrame();
    if (!IsRangeOffsetFrame(frame) || window.GetSortElements().Items().size() != 1) {
        return false;
    }

    const auto* sortColumnType = SortColumnType(window, window.GetSortElements().Items().front().first);
    if (!(HasFrameOffset(frame) ? IsRangeOffsetType(sortColumnType) : IsRangeComparableType(sortColumnType))) {
        return false;
    }
    return HasAggregate(window);
}

bool TPhysicalWindowBuilder::CanBuildWindow(const TOpWindow& window) {
    const bool running = IsRunningFrame(window.GetFrame());
    const bool wholePartition = UsesWholePartition(window);
    const bool rangeRunning = UsesRangeCarry(window) || UsesRangePeerGroups(window);
    const bool rowFrames = UsesRowFrames(window) || UsesRangeFrames(window);

    for (const auto& [output, func] : window.GetWindowFuncs().Items()) {
        if (func.Kind == EWindowFuncKind::Native) {
            if (!IsSupportedNativeFunction(func.Function) || !func.Arguments.Items().empty()) {
                return false;
            }
            continue;
        }
        if (!IsSupportedAggregationFunction(func.Function) || !(running || wholePartition || rangeRunning || rowFrames)) {
            return false;
        }
    }

    return true;
}

void TPhysicalWindowBuilder::Prepare(const TVector<TInfoUnitId>& inputs) {
    Inputs = inputs;
    for (ui32 i = 0; i < inputs.size(); ++i) {
        Indexes.Add(inputs[i], i);
    }

    OutputLayout = inputs;
    for (const auto output : Window.GetWindowFuncs().Keys()) {
        const auto& func = *Window.GetWindowFuncs().Find(output);
        Functions.push_back(output);
        OutputLayout.push_back(output);
        NeedsPeerKey = NeedsPeerKey || func.Function == "rank" || func.Function == "denserank";
    }
    NeedsPeerKey = NeedsPeerKey && !Window.GetSortElements().Items().empty();
    WholePartition = UsesWholePartition(Window);
    RangeCarry = UsesRangeCarry(Window);
    RangePeerGroups = UsesRangePeerGroups(Window);
    RowFrames = UsesRowFrames(Window);
    RowIncremental = RowFrames && IsRowIncrementalFrame(Window.GetFrame());
    RowSuffix = RowFrames && IsRowSuffixFrame(Window.GetFrame());
    RangeFrames = UsesRangeFrames(Window);
    RangeIncremental = RangeFrames && IsRangeIncrementalFrame(Window.GetFrame());
}

ui32 TPhysicalWindowBuilder::IndexOf(TInfoUnitId column) const {
    const auto* index = Indexes.Find(column);
    Y_ENSURE(index, "Cannot find window column " << Names.Get(column) << " in the wide input");
    return *index;
}

const TTypeAnnotationNode* TPhysicalWindowBuilder::InputItemType(TInfoUnitId column) const {
    const auto* type = Window.GetInput()->GetIUType(column, Ctx);
    Y_ENSURE(type, "Cannot find a type for window column " << Names.Get(column));
    return type;
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildOutputRowType() const {
    TVector<const TItemExprType*> fields;
    fields.reserve(OutputLayout.size());
    for (const auto id : OutputLayout) {
        const auto* type = Window.GetIUType(id, Ctx);
        Y_ENSURE(type, "Cannot find the window result type for " << Names.Get(id));
        fields.push_back(Ctx.MakeType<TItemExprType>(Names.Get(id), type));
    }
    return ExpandType(Pos, *Ctx.MakeType<TStructExprType>(fields), Ctx);
}

TString TPhysicalWindowBuilder::AccumulatorName(ui32 funcIndex) const {
    return Names.GetTemporaryName("__kqp_win_acc_" + ToString(funcIndex) + "_");
}

TString TPhysicalWindowBuilder::PositionName(ui32 funcIndex) const {
    return Names.GetTemporaryName("__kqp_win_pos_" + ToString(funcIndex) + "_");
}

TString TPhysicalWindowBuilder::PeerName(ui32 sortIndex) const {
    return Names.GetTemporaryName("__kqp_win_peer_" + ToString(sortIndex) + "_");
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

TExprNode::TPtr TPhysicalWindowBuilder::BuildSumCastTarget(TInfoUnitId column) const {
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

    for (const auto& key : Window.GetPartitionKeys().Items()) {
        add(IndexOf(key), true);
    }
    for (const auto& element : Window.GetSortElements().Items()) {
        add(IndexOf(element.first), element.second.Ascending);
    }
    return keys;
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildAvgAccumulatorDataType(TInfoUnitId column) const {
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

TExprNode::TPtr TPhysicalWindowBuilder::BuildAvgAccumulatorType(TInfoUnitId column) const {
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
    for (const auto& key : Window.GetPartitionKeys().Items()) {
        results.push_back(args[IndexOf(key)]);
    }
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(args)), std::move(results));
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildGroupSwitchLambda() const {
    const auto& partitionKeys = Window.GetPartitionKeys().Items();

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

    const auto& argument = func.Arguments.Items().front();
    auto value = Member(itemArg, Names.Get(argument));
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

    const auto& sortElements = Window.GetSortElements().Items();
    TExprNode::TPtr sortKeyChanged;
    if (update && NeedsPeerKey) {
        TExprNode::TListType comparisons;
        for (ui32 k = 0; k < sortElements.size(); ++k) {
            // clang-format off
            comparisons.push_back(Ctx.Builder(Pos)
                .Callable("AggrNotEquals")
                    .Add(0, Member(itemArg, Names.Get(sortElements[k].first)))
                    .Add(1, Member(previousState, PeerName(k)))
                .Seal().Build());
            // clang-format on
        }
        sortKeyChanged = comparisons.size() == 1 ? comparisons.front() : Ctx.NewCallable(Pos, "Or", std::move(comparisons));
    }

    TVector<std::pair<TString, TExprNode::TPtr>> stateMembers;
    TVector<std::pair<TString, TExprNode::TPtr>> outputMembers;
    for (const auto& column : Inputs) {
        outputMembers.emplace_back(Names.Get(column), Member(itemArg, Names.Get(column)));
    }

    const auto& funcs = Functions;
    for (ui32 f = 0; f < funcs.size(); ++f) {
        const auto& func = *Window.GetWindowFuncs().Find(funcs[f]);
        auto accumulator = BuildAccumulator(func, f, itemArg, previousState, sortKeyChanged, stateMembers);
        stateMembers.emplace_back(AccumulatorName(f), accumulator);
        outputMembers.emplace_back(Names.Get(funcs[f]), BuildResultFromAccumulator(funcs[f], accumulator));
    }

    if (NeedsPeerKey) {
        for (ui32 k = 0; k < sortElements.size(); ++k) {
            stateMembers.emplace_back(PeerName(k), Member(itemArg, Names.Get(sortElements[k].first)));
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

// Accumulators of sum, min, max and avg are optional, while a frame that is never empty declares
// a non-optional result for a non-optional input.
bool TPhysicalWindowBuilder::IsNonOptionalAggregate(TInfoUnitId column) const {
    const auto& func = *Window.GetWindowFuncs().Find(column);
    if (func.Kind != EWindowFuncKind::Aggregate || func.Function == "count") {
        return false;
    }
    const auto* type = Window.GetIUType(column, Ctx);
    Y_ENSURE(type, "Cannot find the window result type for " << Names.Get(column));
    return !type->IsOptionalOrNull();
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildResultFromAccumulator(TInfoUnitId column, TExprNode::TPtr accumulator) const {
    const auto& func = *Window.GetWindowFuncs().Find(column);
    auto result = func.Kind == EWindowFuncKind::Aggregate && func.Function == "avg" ? BuildAverage(func, std::move(accumulator))
                                                                                   : std::move(accumulator);
    return IsNonOptionalAggregate(column) ? Ctx.NewCallable(Pos, "Unwrap", {std::move(result)}) : result;
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildAverage(const TOpWindowFunc& func, TExprNode::TPtr accumulator) const {
    const auto& argument = func.Arguments.Items().front();
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
                                .Atom(1, Names.Get(OutputLayout[i]))
                            .Seal();
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildChain(TExprNode::TPtr wideFlow) const {
    auto narrow = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(wideFlow, Inputs, Ctx, Names);

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
    const auto& funcs = Functions;
    for (ui32 f = 0; f < funcs.size(); ++f) {
        const auto& func = *Window.GetWindowFuncs().Find(funcs[f]);
        if (func.Kind == EWindowFuncKind::Native) {
            continue;
        }
        stateMembers.emplace_back(AccumulatorName(f), BuildAccumulator(func, f, itemArg, previousState, nullptr, stateMembers));
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
                                .Atom(1, Names.Get(OutputLayout[i]))
                            .Seal();
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildWholePartition(TExprNode::TPtr wideFlow) const {
    auto narrow = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(wideFlow, Inputs, Ctx, Names);
    auto rows = Ctx.NewArgument(Pos, "win_partition_rows");

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
        outputMembers.emplace_back(Names.Get(column), Member(rowArg, Names.Get(column)));
    }
    const auto& funcs = Functions;
    for (ui32 f = 0; f < funcs.size(); ++f) {
        outputMembers.emplace_back(Names.Get(funcs[f]), BuildResultFromAccumulator(funcs[f], Member(stateArg, AccumulatorName(f))));
    }

    auto rowLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {rowArg}), BuildStruct(outputMembers));
    // clang-format off
    auto attach = Ctx.Builder(Pos)
        .Callable("OrderedMap")
            .Add(0, rows)
            .Add(1, rowLambda)
        .Seal().Build();

    auto perState = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {stateArg}), std::move(attach));
    auto perPartition = Ctx.Builder(Pos)
        .Callable("OrderedFlatMap")
            .Callable(0, "ToList")
                .Add(0, folded)
            .Seal()
            .Add(1, perState)
        .Seal().Build();

    auto result = Ctx.Builder(Pos)
        .Callable("OrderedFlatMap")
            .Add(0, BuildPartitionList(narrow))
            .Add(1, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {rows}), std::move(perPartition)))
        .Seal().Build();
    // clang-format on

    return BuildExpandFromStructs(result);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildChainOutputs(TExprNode::TPtr wideFlow) const {
    auto narrow = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(wideFlow, Inputs, Ctx, Names);

    // clang-format off
    auto chained = Ctx.Builder(Pos)
        .Callable("Chain1Map")
            .Add(0, narrow)
            .Add(1, BuildChainLambda(/*update=*/false))
            .Add(2, BuildChainLambda(/*update=*/true))
        .Seal().Build();

    return Ctx.Builder(Pos)
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
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildPartitionList(TExprNode::TPtr flow) const {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Condense1")
            .Add(0, flow)
            .Lambda(1)
                .Param("partition_row")
                .Callable("AsList")
                    .Arg(0, "partition_row")
                .Seal()
            .Seal()
            .Lambda(2)
                .Param("partition_row")
                .Param("partition_rows")
                .Callable("Bool")
                    .Atom(0, "false")
                .Seal()
            .Seal()
            .Lambda(3)
                .Param("partition_row")
                .Param("partition_rows")
                .Callable("Append")
                    .Arg(0, "partition_rows")
                    .Arg(1, "partition_row")
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildQueue(TExprNode::TPtr wideFlow) const {
    Y_ENSURE(Window.Type, "Window has no type annotation");

    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("QueueCreate")
            .Add(0, BuildOutputRowType())
            .Callable(1, "Void").Seal()
            .Add(2, BuildUint64(0))
            .Callable(3, "DependsOn")
                .Callable(0, "FromFlow")
                    .Add(0, wideFlow)
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildFrameBounds(TExprNode::TListType rangeIncrementals, TExprNode::TListType rowIntervals,
                                                         TExprNode::TListType rowIncrementals, TExprNode::TListType rangeIntervals) const {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AsStruct")
            .List(0).Atom(0, "RangeIncrementals").Add(1, Ctx.NewList(Pos, std::move(rangeIncrementals))).Seal()
            .List(1).Atom(0, "RangeIntervals").Add(1, Ctx.NewList(Pos, std::move(rangeIntervals))).Seal()
            .List(2).Atom(0, "RowIncrementals").Add(1, Ctx.NewList(Pos, std::move(rowIncrementals))).Seal()
            .List(3).Atom(0, "RowIntervals").Add(1, Ctx.NewList(Pos, std::move(rowIntervals))).Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildCollector(TExprNode::TPtr outputs, TExprNode::TPtr queue, TExprNode::TPtr bounds, bool ascending) const {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("WinFramesCollector")
            .Callable(0, "FromFlow")
                .Add(0, outputs)
            .Seal()
            .Add(1, queue)
            .Callable(2, "AsStruct")
                .List(0).Atom(0, "Bounds").Add(1, bounds).Seal()
                .List(1)
                    .Atom(0, "SortOrder")
                    .Callable(1, "String").Atom(0, ascending ? "Asc" : "Desc").Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildIncrementalCarry(TExprNode::TPtr wideFlow, TExprNode::TPtr bounds, bool isRange, bool ascending,
                                                              bool mayBeEmpty) const {
    auto queue = BuildQueue(wideFlow);
    auto collected = BuildCollector(BuildChainOutputs(wideFlow), queue, bounds, ascending);

    auto rowArg = Ctx.NewArgument(Pos, "carry_row");
    // clang-format off
    auto carriedRow = Ctx.Builder(Pos)
        .Callable("WinFrame")
            .Add(0, queue)
            .Add(1, BuildUint64(0))
            .Callable(2, "Bool").Atom(0, "true").Seal()
            .Callable(3, "Bool").Atom(0, isRange ? "true" : "false").Seal()
            .Callable(4, "Bool").Atom(0, "true").Seal()
            .Callable(5, "DependsOn").Add(0, rowArg).Seal()
        .Seal().Build();
    // clang-format on

    TVector<std::pair<TString, TExprNode::TPtr>> members;
    for (const auto& column : Inputs) {
        members.emplace_back(Names.Get(column), Member(rowArg, Names.Get(column)));
    }
    for (const auto& [output, func] : Window.GetWindowFuncs().Items()) {
        const auto name = Names.Get(output);
        if (func.Kind == EWindowFuncKind::Native) {
            members.emplace_back(name, Member(rowArg, name));
        } else if (!mayBeEmpty) {
            members.emplace_back(name, Member(Ctx.NewCallable(Pos, "Unwrap", {carriedRow}), name));
        } else {
            auto carriedArg = Ctx.NewArgument(Pos, "carried");
            auto value = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {carriedArg}), Member(carriedArg, name));
            if (func.Function == "count") {
                members.emplace_back(name, Ctx.NewCallable(Pos, "Coalesce", {Ctx.NewCallable(Pos, "Map", {carriedRow, value}), BuildUint64(0)}));
            } else {
                members.emplace_back(name, Ctx.NewCallable(Pos, "FlatMap", {carriedRow, value}));
            }
        }
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

TExprNode::TPtr TPhysicalWindowBuilder::BuildRangeCarry(TExprNode::TPtr wideFlow) const {
    Y_ENSURE(Window.GetSortElements().Items().size() == 1, "A RANGE frame needs exactly one sort column here");
    const auto& sortElement = Window.GetSortElements().Items().front();

    auto bound = BuildRangeBound(EWindowFrameBound::CurrentRow, 0, Names.Get(sortElement.first));
    return BuildIncrementalCarry(wideFlow, BuildFrameBounds({bound}, {}, {}), /*isRange=*/true, sortElement.second.Ascending,
                                 /*mayBeEmpty=*/false);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRangeIncremental(TExprNode::TPtr wideFlow) const {
    const auto& frame = Window.GetFrame();
    const auto& sortElement = Window.GetSortElements().Items().front();
    const bool mayBeEmpty = frame.EndKind == EWindowFrameBound::Preceding && frame.EndValue > 0;
    auto bound = BuildRangeBound(frame.EndKind, frame.EndValue, Names.Get(sortElement.first));
    return BuildIncrementalCarry(wideFlow, BuildFrameBounds({bound}, {}, {}), /*isRange=*/true, sortElement.second.Ascending, mayBeEmpty);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRowIncremental(TExprNode::TPtr wideFlow) const {
    const auto& frame = Window.GetFrame();
    const bool mayBeEmpty = frame.EndKind == EWindowFrameBound::Preceding && frame.EndValue > 0;
    return BuildIncrementalCarry(wideFlow, BuildFrameBounds({}, {}, {BuildRowBound(frame.EndKind, frame.EndValue)}),
                                 /*isRange=*/false, /*ascending=*/true, mayBeEmpty);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRowSuffix(TExprNode::TPtr wideFlow) const {
    auto rows = Ctx.NewArgument(Pos, "suffix_rows");

    // clang-format off
    auto suffixStates = Ctx.Builder(Pos)
        .Callable("Reverse")
            .Callable(0, "Chain1Map")
                .Callable(0, "Reverse")
                    .Add(0, rows)
                .Seal()
                .Add(1, BuildFoldLambda(/*update=*/false))
                .Add(2, BuildFoldLambda(/*update=*/true))
            .Seal()
        .Seal().Build();
    // clang-format on

    auto pairArg = Ctx.NewArgument(Pos, "suffix_pair");
    auto rowOf = Ctx.NewCallable(Pos, "Nth", {pairArg, Ctx.NewAtom(Pos, "0")});
    auto stateOf = Ctx.NewCallable(Pos, "Nth", {pairArg, Ctx.NewAtom(Pos, "1")});

    TVector<std::pair<TString, TExprNode::TPtr>> members;
    for (const auto& column : Inputs) {
        members.emplace_back(Names.Get(column), Member(rowOf, Names.Get(column)));
    }
    const auto& funcs = Functions;
    for (ui32 f = 0; f < funcs.size(); ++f) {
        const auto& func = *Window.GetWindowFuncs().Find(funcs[f]);
        const auto name = Names.Get(funcs[f]);
        members.emplace_back(name, func.Kind == EWindowFuncKind::Native
                                       ? Member(rowOf, name)
                                       : BuildResultFromAccumulator(funcs[f], Member(stateOf, AccumulatorName(f))));
    }

    // clang-format off
    auto perPartition = Ctx.Builder(Pos)
        .Callable("OrderedMap")
            .Callable(0, "Zip")
                .Add(0, rows)
                .Add(1, suffixStates)
            .Seal()
            .Add(1, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {pairArg}), BuildStruct(members)))
        .Seal().Build();

    auto result = Ctx.Builder(Pos)
        .Callable("OrderedFlatMap")
            .Add(0, BuildPartitionList(BuildChainOutputs(wideFlow)))
            .Add(1, Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {rows}), std::move(perPartition)))
        .Seal().Build();
    // clang-format on

    return BuildExpandFromStructs(result);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRangePeerGroups(TExprNode::TPtr wideFlow) const {
    const auto& sortElements = Window.GetSortElements().Items();
    auto outputs = BuildChainOutputs(wideFlow);

    Y_ENSURE(Window.Type, "Window has no type annotation");
    auto rowType = BuildOutputRowType();
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
            items.push_back(Member(row, Names.Get(sortElement.first)));
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
                                              {Member(switchRow, Names.Get(sortElements[k].first)), nth(nth(switchState, 0), k)}));
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
        members.emplace_back(Names.Get(column), Member(rowArg, Names.Get(column)));
    }
    for (const auto& [output, func] : Window.GetWindowFuncs().Items()) {
        const auto name = Names.Get(output);
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

TExprNode::TPtr TPhysicalWindowBuilder::BuildRowBound(EWindowFrameBound kind, ui64 value) const {
    TString direction = "Following";
    TExprNode::TPtr number;
    switch (kind) {
        case EWindowFrameBound::UnboundedPreceding:
        case EWindowFrameBound::UnboundedFollowing:
            direction = kind == EWindowFrameBound::UnboundedPreceding ? "Preceding" : "Following";
            number = Ctx.NewCallable(Pos, "AsTagged", {Ctx.NewCallable(Pos, "Void", {}), Ctx.NewAtom(Pos, "inf")});
            break;
        case EWindowFrameBound::Preceding:
        case EWindowFrameBound::Following:
        case EWindowFrameBound::CurrentRow:
            direction = kind == EWindowFrameBound::Preceding ? "Preceding" : "Following";
            if (kind == EWindowFrameBound::CurrentRow) {
                value = 0;
            }
            // clang-format off
            number = Ctx.Builder(Pos)
                .Callable("AsStruct")
                    .List(0)
                        .Atom(0, "FiniteValue")
                        .Add(1, BuildUint64(value))
                    .Seal()
                .Seal().Build();
            // clang-format on
            break;
    }

    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AsStruct")
            .List(0).Atom(0, "Direction").Callable(1, "String").Atom(0, direction).Seal().Seal()
            .List(1).Atom(0, "Number").Add(1, number).Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRangeBound(EWindowFrameBound kind, ui64 value, const TString& sortedColumn) const {
    TString direction = "Following";
    TExprNode::TPtr number;
    switch (kind) {
        case EWindowFrameBound::UnboundedPreceding:
        case EWindowFrameBound::UnboundedFollowing:
            direction = kind == EWindowFrameBound::UnboundedPreceding ? "Preceding" : "Following";
            number = Ctx.NewCallable(Pos, "AsTagged", {Ctx.NewCallable(Pos, "Void", {}), Ctx.NewAtom(Pos, "inf")});
            break;
        case EWindowFrameBound::CurrentRow:
            number = Ctx.NewCallable(Pos, "AsTagged", {Ctx.NewCallable(Pos, "Void", {}), Ctx.NewAtom(Pos, "zero")});
            break;
        case EWindowFrameBound::Preceding:
        case EWindowFrameBound::Following:
            direction = kind == EWindowFrameBound::Preceding ? "Preceding" : "Following";
            // clang-format off
            number = Ctx.Builder(Pos)
                .Callable("AsStruct")
                    .List(0)
                        .Atom(0, "FiniteValue")
                        .Callable(1, "Int32").Atom(0, ToString(value)).Seal()
                    .Seal()
                .Seal().Build();
            // clang-format on
            break;
    }

    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AsStruct")
            .List(0).Atom(0, "Direction").Callable(1, "String").Atom(0, direction).Seal().Seal()
            .List(1).Atom(0, "Number").Add(1, number).Seal()
            .List(2).Atom(0, "SortedColumn").Callable(1, "String").Atom(0, sortedColumn).Seal().Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRowFrames(TExprNode::TPtr wideFlow) const {
    const auto& frame = Window.GetFrame();

    // clang-format off
    auto interval = Ctx.Builder(Pos)
        .Callable("AsStruct")
            .List(0).Atom(0, "Min").Add(1, BuildRowBound(frame.BeginKind, frame.BeginValue)).Seal()
            .List(1).Atom(0, "Max").Add(1, BuildRowBound(frame.EndKind, frame.EndValue)).Seal()
        .Seal().Build();
    // clang-format on

    return BuildFrameFold(wideFlow, BuildFrameBounds({}, {interval}, {}), /*isRange=*/false, /*ascending=*/true);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildRangeFrames(TExprNode::TPtr wideFlow) const {
    const auto& frame = Window.GetFrame();
    const auto& sortElement = Window.GetSortElements().Items().front();
    const auto sortedColumn = Names.Get(sortElement.first);

    // clang-format off
    auto interval = Ctx.Builder(Pos)
        .Callable("AsStruct")
            .List(0).Atom(0, "Min").Add(1, BuildRangeBound(frame.BeginKind, frame.BeginValue, sortedColumn)).Seal()
            .List(1).Atom(0, "Max").Add(1, BuildRangeBound(frame.EndKind, frame.EndValue, sortedColumn)).Seal()
        .Seal().Build();
    // clang-format on

    return BuildFrameFold(wideFlow, BuildFrameBounds({}, {}, {}, {interval}), /*isRange=*/true, sortElement.second.Ascending);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildFrameFold(TExprNode::TPtr wideFlow, TExprNode::TPtr bounds, bool isRange, bool ascending) const {
    auto queue = BuildQueue(wideFlow);
    auto zero = BuildUint64(0);
    auto collected = BuildCollector(BuildChainOutputs(wideFlow), queue, bounds, ascending);

    auto rowArg = Ctx.NewArgument(Pos, "frame_row");
    // clang-format off
    auto frameRows = Ctx.Builder(Pos)
        .Callable("OrderedMap")
            .Callable(0, "WinFrame")
                .Add(0, queue)
                .Add(1, zero)
                .Callable(2, "Bool").Atom(0, "false").Seal()
                .Callable(3, "Bool").Atom(0, isRange ? "true" : "false").Seal()
                .Callable(4, "Bool").Atom(0, "false").Seal()
                .Callable(5, "DependsOn").Add(0, rowArg).Seal()
            .Seal()
            .Lambda(1)
                .Param("frame_item")
                .Arg("frame_item")
            .Seal()
        .Seal().Build();

    auto folded = Ctx.Builder(Pos)
        .Callable("Fold1")
            .Add(0, frameRows)
            .Add(1, BuildFoldLambda(/*update=*/false))
            .Add(2, BuildFoldLambda(/*update=*/true))
        .Seal().Build();
    // clang-format on

    TVector<std::pair<TString, TExprNode::TPtr>> members;
    for (const auto& column : Inputs) {
        members.emplace_back(Names.Get(column), Member(rowArg, Names.Get(column)));
    }
    const auto& funcs = Functions;
    for (ui32 f = 0; f < funcs.size(); ++f) {
        const auto& func = *Window.GetWindowFuncs().Find(funcs[f]);
        const auto name = Names.Get(funcs[f]);
        if (func.Kind == EWindowFuncKind::Native) {
            members.emplace_back(name, Member(rowArg, name));
            continue;
        }

        auto stateArg = Ctx.NewArgument(Pos, "frame_state");
        auto result = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {stateArg}),
                                    BuildResultFromAccumulator(funcs[f], Member(stateArg, AccumulatorName(f))));
        if (func.Function == "count") {
            members.emplace_back(name, Ctx.NewCallable(Pos, "Coalesce", {Ctx.NewCallable(Pos, "Map", {folded, result}), BuildUint64(0)}));
        } else if (IsNonOptionalAggregate(funcs[f])) {
            members.emplace_back(name, Ctx.NewCallable(Pos, "Unwrap", {Ctx.NewCallable(Pos, "Map", {folded, result})}));
        } else {
            members.emplace_back(name, Ctx.NewCallable(Pos, "FlatMap", {folded, result}));
        }
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

TExprNode::TPtr TPhysicalWindowBuilder::BuildPartitionHandler(TExprNode::TPtr wideFlow) const {
    // f(x) OVER (PARTITION BY p), i.e. ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
    if (WholePartition) {
        return BuildWholePartition(wideFlow);
    }
    // f(x) OVER (ORDER BY num), i.e. RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW over one numeric key.
    if (RangeCarry) {
        return BuildRangeCarry(wideFlow);
    }
    // f(x) OVER (ORDER BY name, id), the same RANGE frame over any other keys.
    if (RangePeerGroups) {
        return BuildRangePeerGroups(wideFlow);
    }
    // RANGE BETWEEN UNBOUNDED PRECEDING AND k PRECEDING (or k FOLLOWING).
    if (RangeIncremental) {
        return BuildRangeIncremental(wideFlow);
    }
    // RANGE BETWEEN n PRECEDING AND k FOLLOWING, or RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING.
    if (RangeFrames) {
        return BuildRangeFrames(wideFlow);
    }
    // ROWS BETWEEN UNBOUNDED PRECEDING AND k PRECEDING (or k FOLLOWING).
    if (RowIncremental) {
        return BuildRowIncremental(wideFlow);
    }
    // ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING.
    if (RowSuffix) {
        return BuildRowSuffix(wideFlow);
    }
    // ROWS BETWEEN n PRECEDING AND k FOLLOWING.
    if (RowFrames) {
        return BuildRowFrames(wideFlow);
    }
    // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, or ranking functions only.
    return BuildChain(wideFlow);
}

TExprNode::TPtr TPhysicalWindowBuilder::BuildPhysicalOp(TExprNode::TPtr input) {
    Y_ENSURE(CanBuildWindow(Window), "This window cannot be evaluated by a single forward pass");

    Prepare(NPhysicalConvertionUtils::GetLiveInputIUs(Window, 0));

    // clang-format off
    input = Build<TCoToFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    input = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(input, Inputs, Ctx, Names);

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

    if (Window.GetPartitionKeys().Items().empty()) {
        // If no partitions we use whole stage as a partition.
        input = BuildPartitionHandler(input);
    } else {
        TExprNode::TListType handlerArgs;
        for (ui32 i = 0; i < Window.GetPartitionKeys().Items().size(); ++i) {
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
        NPhysicalConvertionUtils::BuildNameSet(NPhysicalConvertionUtils::GetLiveOutputIUs(Window), Names),
        Ctx, Names);

    // clang-format off
    input = Build<TCoFromFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical window] " << KqpExprToPrettyString(TExprBase(input), Ctx);
    return input;
}
