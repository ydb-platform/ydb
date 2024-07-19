#include "mkql_builtins_compare.h"
#include "mkql_builtins_datetime.h"
#include "mkql_builtins_decimal.h" // Y_IGNORE
#include "mkql_builtins_string_kernels.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_signed<T1>::value == std::is_signed<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Greater(T1 x, T2 y) {
    return x > y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Greater(T1 x, T2 y) {
    return x > T1(0) && static_cast<std::make_unsigned_t<T1>>(x) > y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Greater(T1 x, T2 y) {
    return T2(0) > y || x > static_cast<std::make_unsigned_t<T2>>(y);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Greater(T1 x, T2 y) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    const auto l = static_cast<FT>(x);
    const auto r = static_cast<FT>(y);
    if constexpr (Aggr) {
        if (std::isunordered(l, r))
            return !std::isnan(r);
    }
    return l > r;
}

#ifndef MKQL_DISABLE_CODEGEN
Value* GenGreaterUnsigned(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, lhs, rhs, "greater", block);
}

Value* GenGreaterSigned(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, lhs, rhs, "greater", block);
}

template <bool Aggr>
Value* GenGreaterFloats(Value* lhs, Value* rhs, BasicBlock* block);

template <>
Value* GenGreaterFloats<false>(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OGT, lhs, rhs, "greater", block);
}

template <>
Value* GenGreaterFloats<true>(Value* lhs, Value* rhs, BasicBlock* block) {
    const auto ugt = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UGT, lhs, rhs, "greater", block);
    const auto ord = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ORD, ConstantFP::get(rhs->getType(), 0.0), rhs, "ordered", block);
    return BinaryOperator::CreateAnd(ugt, ord, "and", block);
}

template <typename T1, typename T2>
Value* GenGreaterIntegralLeftSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(x->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, x, zero, "negative", block);
    using T = std::conditional_t<(sizeof(std::make_unsigned_t<T1>) > sizeof(T2)), std::make_unsigned_t<T1>, T2>;
    const auto comp = GenGreaterUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getFalse(context), comp, "result", block);
 }

template <typename T1, typename T2>
Value* GenGreaterIntegralRightSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(y->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, y, zero, "negative", block);
    using T = std::conditional_t<(sizeof(T1) > sizeof(std::make_unsigned_t<T2>)), T1, std::make_unsigned_t<T2>>;
    const auto comp = GenGreaterUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getTrue(context), comp, "result", block);
}


template <typename T1, typename T2,
    std::enable_if_t<std::is_unsigned<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenGreater(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenGreaterUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_signed<T1>::value && std::is_signed<T2>::value &&
    std::is_integral<T1>::value && std::is_integral<T2>::value, bool> Aggr>
inline Value* GenGreater(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenGreaterSigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenGreater(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenGreaterIntegralLeftSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
inline Value* GenGreater(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenGreaterIntegralRightSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
inline Value* GenGreater(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    return GenGreaterFloats<Aggr>(StaticCast<T1, FT>(x, context, block), StaticCast<T2, FT>(y, context, block), block);
}
#endif

struct TAggrGreater {
    static bool Simple(bool left, bool right)
    {
        return left && !right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static constexpr CmpInst::Predicate SimplePredicate = ICmpInst::ICMP_UGT;
#endif
};

template<typename TLeft, typename TRight, bool Aggr>
struct TGreater : public TCompareArithmeticBinary<TLeft, TRight, TGreater<TLeft, TRight, Aggr>>, public TAggrGreater {
    static bool Do(TLeft left, TRight right)
    {
        return Greater<TLeft, TRight, Aggr>(left, right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenGreater<TLeft, TRight, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TGreaterOp;

template<typename TLeft, typename TRight>
struct TGreaterOp<TLeft, TRight, bool> : public TGreater<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template<typename TLeft, typename TRight, bool Aggr>
struct TDiffDateGreater : public TCompareArithmeticBinary<typename TLeft::TLayout, typename TRight::TLayout, TDiffDateGreater<TLeft, TRight, Aggr>>, public TAggrGreater {
    static bool Do(typename TLeft::TLayout left, typename TRight::TLayout right)
    {
        return std::is_same<TLeft, TRight>::value ?
            Greater<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right):
            Greater<TScaledDate, TScaledDate, Aggr>(ToScaledDate<TLeft>(left), ToScaledDate<TRight>(right));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        return std::is_same<TLeft, TRight>::value ?
            GenGreater<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right, context, block):
            GenGreater<TScaledDate, TScaledDate, Aggr>(GenToScaledDate<TLeft>(left, context, block), GenToScaledDate<TRight>(right, context, block), context, block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TDiffDateGreaterOp;

template<typename TLeft, typename TRight>
struct TDiffDateGreaterOp<TLeft, TRight, NUdf::TDataType<bool>> : public TDiffDateGreater<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template<typename TLeft, typename TRight, bool Aggr>
struct TAggrTzDateGreater : public TCompareArithmeticBinaryWithTimezone<TLeft, TRight, TAggrTzDateGreater<TLeft, TRight, Aggr>>, public TAggrGreater {
    static bool Do(TLeft left, TRight right)
    {
        return Greater<TLeft, TRight, Aggr>(left, right);
    }

    static bool DoTz(ui16 left, ui16 right)
    {
        return Greater<ui16, ui16, Aggr>(left, right);
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenGreater<TLeft, TRight, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }

    static Value* GenTz(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenGreater<ui16, ui16, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TCustomGreater : public TAggrGreater {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod left, NUdf::TUnboxedValuePod right) {
        return NUdf::TUnboxedValuePod(CompareCustomsWithCleanup<Slot>(left, right) > 0);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto res = CallBinaryUnboxedValueFunction(&CompareCustoms<Slot>, Type::getInt32Ty(context), left, right, ctx.Codegen, block);
        const auto comp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, res, ConstantInt::get(res->getType(), 0), "greater", block);
        ValueCleanup(EValueRepresentation::String, left, ctx, block);
        ValueCleanup(EValueRepresentation::String, right, ctx, block);
        return MakeBoolean(comp, context, block);
    }
#endif
};

struct TDecimalGreater {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(NYql::NDecimal::IsComparable(l) && NYql::NDecimal::IsComparable(r) && l > r);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto lok = NDecimal::GenIsComparable(l, context, block);
        const auto rok = NDecimal::GenIsComparable(r, context, block);
        const auto both = BinaryOperator::CreateAnd(lok, rok, "both", block);
        const auto gt = GenGreaterSigned(l, r, block);
        const auto res = BinaryOperator::CreateAnd(both, gt, "res", block);
        return MakeBoolean(res, context, block);
    }
#endif
};

struct TDecimalAggrGreater : public TAggrGreater {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(l > r);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto gt = GenGreaterSigned(l, r, block);
        return MakeBoolean(gt, context, block);
    }
#endif
};

}

void RegisterGreater(IBuiltinFunctionRegistry& registry) {
    const auto name = "Greater";

    RegisterComparePrimitive<TGreater, TCompareArgsOpt>(registry, name);
    RegisterCompareDatetime<TDiffDateGreater, TCompareArgsOpt>(registry, name);
    RegisterCompareBigDatetime<TDiffDateGreater, TCompareArgsOpt>(registry, name);

    RegisterCompareStrings<TCustomGreater, TCompareArgsOpt>(registry, name);
    RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, TDecimalGreater, TCompareArgsOpt>(registry, name);

    const auto aggrName = "AggrGreater";
    RegisterAggrComparePrimitive<TGreater, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareDatetime<TDiffDateGreater, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareTzDatetime<TAggrTzDateGreater, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigDatetime<TDiffDateGreater, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigTzDatetime<TAggrTzDateGreater, TCompareArgsOpt>(registry, aggrName);

    RegisterAggrCompareStrings<TCustomGreater, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, TDecimalAggrGreater, TCompareArgsOpt>(registry, aggrName);
}

void RegisterGreater(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();

    AddNumericComparisonKernels<TGreaterOp>(*family);
    AddDateComparisonKernels<TDiffDateGreaterOp>(*family);
    RegisterStringKernelGreater(*family);

    kernelFamilyMap["Greater"] = std::move(family);
}

} // namespace NMiniKQL
} // namespace NKikimr
