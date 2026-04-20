#include "mkql_window_range_pg_caller.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_saturated_math.h>
#include <yql/essentials/parser/pg_wrapper/interface/compare.h>

namespace NKikimr::NMiniKQL {

namespace {

class TInRangeComputationValue: public TComputationValue<TInRangeComputationValue> {
public:
    TInRangeComputationValue(TMemoryUsageInfo* memInfo, const NYql::TPgInRange& inRange)
        : TComputationValue(memInfo)
        , State_(inRange.MakeCallState())
    {
    }

    NUdf::TUnboxedValue Call(NUdf::TUnboxedValue from, NUdf::TUnboxedValue to, NUdf::TUnboxedValue columnToTest, bool sub, bool less) const {
        return State_->Call(to, from, columnToTest, sub, less);
    }

private:
    THolder<NYql::TPgInRange::TCallState> State_;
};

TInRangeComputationValue& GetInRangeState(const NYql::TPgInRange& inRange, TComputationContext& ctx, ui32 ctxIndex) {
    auto& result = ctx.MutableValues[ctxIndex];
    if (!result.HasValue()) {
        result = ctx.HolderFactory.Create<TInRangeComputationValue>(inRange);
    }
    return *static_cast<TInRangeComputationValue*>(result.AsBoxed().Get());
}

} // namespace
TPgWindowRangeCaller::TPgWindowRangeCaller(NKikimr::NMiniKQL::TPgType* columnType, std::variant<TCurrentRowTag, std::tuple<ui32, IComputationNode*, NKikimr::NMiniKQL::TPgType*>> delta, ui32 ctxIndex)
    : Delta_(std::visit(TOverloaded{
                            [&](TCurrentRowTag) -> std::variant<NYql::NUdf::IEquate::TPtr, std::pair<IComputationNode*, NYql::TPgInRange>> {
                                return MakePgEquate(columnType);
                            },
                            [&](std::tuple<ui32, IComputationNode*, NKikimr::NMiniKQL::TPgType*> tuple) -> std::variant<NYql::NUdf::IEquate::TPtr, std::pair<IComputationNode*, NYql::TPgInRange>> {
                                auto [procId, deltaNode, offsetType] = tuple;
                                MKQL_ENSURE(offsetType, "Offset type is not specified.");
                                MKQL_ENSURE(deltaNode, "Delta node is not specified.");
                                return std::pair<IComputationNode*, NYql::TPgInRange>(deltaNode, NYql::TPgInRange(procId, columnType, offsetType));
                            }}, delta))
    , CtxIndex_(ctxIndex)
{
}

bool TPgWindowRangeCaller::IsBelongToInterval(EInfBoundary infBoundary, EDirection direction, NUdf::TUnboxedValue from, NUdf::TUnboxedValue to, TComputationContext& ctx) const {
    return std::visit(TOverloaded{
                          [&](const NYql::NUdf::IEquate::TPtr& equate) -> bool {
                              return equate->Equals(from, to);
                          },
                          [&](const std::pair<IComputationNode*, NYql::TPgInRange>& pair) -> bool {
                              bool less = EInfBoundary::Left == infBoundary;
                              bool sub = direction == EDirection::Preceding ? true : false;
                              auto& state = GetInRangeState(pair.second, ctx, CtxIndex_);
                              return state.Call(from, to, pair.first->GetValue(ctx), sub, less).Get<bool>();
                          }}, Delta_);
}

TPgWindowRangeCaller::~TPgWindowRangeCaller() = default;

} // namespace NKikimr::NMiniKQL
