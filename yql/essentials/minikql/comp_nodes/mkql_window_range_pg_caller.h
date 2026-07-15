#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <yql/essentials/minikql/mkql_saturated_math.h>
#include <yql/essentials/parser/pg_wrapper/interface/in_range.h>
#include <yql/essentials/minikql/mkql_window_comparator_bounds.h>

namespace NKikimr::NMiniKQL {

class TPgWindowRangeCaller: public TBlackboxTypeData<TComputationContext, NUdf::TUnboxedValue> {
public:
    struct TCurrentRowTag {};

    TPgWindowRangeCaller(NKikimr::NMiniKQL::TPgType* columnType, std::variant<TCurrentRowTag, std::tuple<ui32, IComputationNode*, NKikimr::NMiniKQL::TPgType*>> delta, ui32 ctxIndex);

    ~TPgWindowRangeCaller() override;

    bool IsBelongToInterval(EInfBoundary infBoundary, EDirection direction, NUdf::TUnboxedValue from, NUdf::TUnboxedValue to, TComputationContext& ctx) const override;

private:
    const std::variant<NYql::NUdf::IEquate::TPtr, std::pair<IComputationNode*, NYql::TPgInRange>> Delta_;
    const ui32 CtxIndex_;
};

} // namespace NKikimr::NMiniKQL
