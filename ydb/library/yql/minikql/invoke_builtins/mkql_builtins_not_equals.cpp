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
Y_FORCE_INLINE bool NotEquals(T1 x, T2 y) {
    return x != y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool NotEquals(T1 x, T2 y) {
    return x < T1(0) || static_cast<std::make_unsigned_t<T1>>(x) != y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool NotEquals(T1 x, T2 y) {
    return y < T2(0) || x != static_cast<std::make_unsigned_t<T2>>(y);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool NotEquals(T1 x, T2 y) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    const auto l = static_cast<FT>(x);
    const auto r = static_cast<FT>(y);
    if constexpr (Aggr) {
        if (std::isunordered(l, r))
            return std::isnan(l) != std::isnan(r);
    }
    return l != r;
}

#ifndef MKQL_DISABLE_CODEGEN
Value* GenNotEqualsIntegral(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, lhs, rhs, "not_equals", block);
}

template <bool Aggr>
Value* GenNotEqualsFloats(Value* lhs, Value* rhs, BasicBlock* block);

template <>
Value* GenNotEqualsFloats<false>(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNE, lhs, rhs, "not_equals", block);
}

template <>
Value* GenNotEqualsFloats<true>(Value* lhs, Value* rhs, BasicBlock* block) {
    const auto one = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ONE, lhs, rhs, "not_equals", block);
    const auto lnan = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, ConstantFP::get(lhs->getType(), 0.0), lhs, "lnan", block);
    const auto rnan = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, ConstantFP::get(rhs->getType(), 0.0), rhs, "rnan", block);
    const auto once = BinaryOperator::CreateXor(lnan, rnan, "xor", block);
    return BinaryOperator::CreateOr(one, once, "or", block);
}

template <typename T1, typename T2>
Value* GenNotEqualsIntegralLeftSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(x->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, x, zero, "negative", block);
    using T = std::conditional_t<(sizeof(std::make_unsigned_t<T1>) > sizeof(T2)), std::make_unsigned_t<T1>, T2>;
    const auto comp = GenNotEqualsIntegral(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getTrue(context), comp, "result", block);
 }

template <typename T1, typename T2>
Value* GenNotEqualsIntegralRightSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(y->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, y, zero, "negative", block);
    using T = std::conditional_t<(sizeof(T1) > sizeof(std::make_unsigned_t<T2>)), T1, std::make_unsigned_t<T2>>;
    const auto comp = GenNotEqualsIntegral(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getTrue(context), comp, "result", block);
}


template <typename T1, typename T2,
    std::enable_if_t<std::is_unsigned<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenNotEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenNotEqualsIntegral(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_signed<T1>::value && std::is_signed<T2>::value &&
    std::is_integral<T1>::value && std::is_integral<T2>::value, bool> Aggr>
inline Value* GenNotEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenNotEqualsIntegral(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenNotEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenNotEqualsIntegralLeftSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
inline Value* GenNotEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenNotEqualsIntegralRightSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
inline Value* GenNotEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    return GenNotEqualsFloats<Aggr>(StaticCast<T1, FT>(x, context, block), StaticCast<T2, FT>(y, context, block), block);
}
#endif

struct TAggrNotEquals {
    static bool Simple(bool left, bool right)
    {
        return left != right;
    }
    static bool Join(bool one, bool two)
    {
        return one || two;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static constexpr CmpInst::Predicate SimplePredicate = ICmpInst::ICMP_NE;

    static Value* GenJoin(Value* one, Value* two, BasicBlock* block)
    {
        return BinaryOperator::CreateOr(one, two, "or", block);
    }
#endif
};

template<typename TLeft, typename TRight, bool Aggr>
struct TNotEquals : public TCompareArithmeticBinary<TLeft, TRight, TNotEquals<TLeft, TRight, Aggr>>, public TAggrNotEquals {
    static bool Do(TLeft left, TRight right)
    {
        return NotEquals<TLeft, TRight, Aggr>(left, right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenNotEquals<TLeft, TRight, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TNotEqualsOp;

template<typename TLeft, typename TRight>
struct TNotEqualsOp<TLeft, TRight, bool> : public TNotEquals<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template<typename TLeft, typename TRight, bool Aggr>
struct TDiffDateNotEquals : public TCompareArithmeticBinary<typename TLeft::TLayout, typename TRight::TLayout, TDiffDateNotEquals<TLeft, TRight, Aggr>>, public TAggrNotEquals {
    static bool Do(typename TLeft::TLayout left, typename TRight::TLayout right)
    {
        return std::is_same<TLeft, TRight>::value ?
            NotEquals<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right):
            NotEquals<TScaledDate, TScaledDate, Aggr>(ToScaledDate<TLeft>(left), ToScaledDate<TRight>(right));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        return std::is_same<TLeft, TRight>::value ?
            GenNotEquals<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right, context, block):
            GenNotEquals<TScaledDate, TScaledDate, Aggr>(GenToScaledDate<TLeft>(left, context, block), GenToScaledDate<TRight>(right, context, block), context, block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TDiffDateNotEqualsOp;

template<typename TLeft, typename TRight>
struct TDiffDateNotEqualsOp<TLeft, TRight, NUdf::TDataType<bool>> : public TDiffDateNotEquals<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template <typename TLeft, typename TRight, bool Aggr>
struct TAggrTzDateNotEquals : public TArithmeticConstraintsBinary<TLeft, TRight, bool>, public TAggrNotEquals {
    static_assert(std::is_same<TLeft, TRight>::value, "Must be same type.");
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        return NUdf::TUnboxedValuePod(Join(NotEquals<TLeft, TRight, Aggr>(left.template Get<TLeft>(), right.template Get<TRight>()), NotEquals<ui16, ui16, Aggr>(left.GetTimezoneId(), right.GetTimezoneId())));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<TLeft>(left, context, block);
        const auto rhs = GetterFor<TRight>(right, context, block);
        const auto ltz = GetterForTimezone(context, left, block);
        const auto rtz = GetterForTimezone(context, right, block);
        const auto result = GenJoin(GenNotEquals<TLeft, TRight, Aggr>(lhs, rhs, context, block), GenNotEquals<ui16, ui16, Aggr>(ltz, rtz, context, block), block);
        const auto wide = MakeBoolean(result, context, block);
        return wide;
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TCustomNotEquals : public TAggrNotEquals {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod left, NUdf::TUnboxedValuePod right) {
        return NUdf::TUnboxedValuePod(CompareCustomsWithCleanup<Slot>(left, right) != 0);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto res = CallBinaryUnboxedValueFunction(&CompareCustoms<Slot>, Type::getInt32Ty(context), left, right, ctx.Codegen, block);
        const auto comp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, res, ConstantInt::get(res->getType(), 0), "not_equals", block);
        ValueCleanup(EValueRepresentation::String, left, ctx, block);
        ValueCleanup(EValueRepresentation::String, right, ctx, block);
        return MakeBoolean(comp, context, block);
    }
#endif
};

struct TDecimalNotEquals {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(!NYql::NDecimal::IsComparable(r) || l != r);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto bad = NDecimal::GenIsNonComparable(r, context, block);
        const auto neq = GenNotEqualsIntegral(l, r, block);
        const auto res = BinaryOperator::CreateOr(bad, neq, "res", block);
        return MakeBoolean(res, context, block);
    }
#endif
};

struct TDecimalAggrNotEquals : public TAggrNotEquals {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(l != r);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto neq = GenNotEqualsIntegral(l, r, block);
        return MakeBoolean(neq, context, block);
    }
#endif
};

}

void RegisterNotEquals(IBuiltinFunctionRegistry& registry) {
    const auto name = "NotEquals";

    RegisterComparePrimitive<TNotEquals, TCompareArgsOpt>(registry, name);
    RegisterCompareDatetime<TDiffDateNotEquals, TCompareArgsOpt>(registry, name);
    RegisterCompareBigDatetime<TDiffDateNotEquals, TCompareArgsOpt>(registry, name);

    RegisterCompareStrings<TCustomNotEquals, TCompareArgsOpt>(registry, name);
    RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, TDecimalNotEquals, TCompareArgsOpt>(registry, name);

    const auto aggrName = "AggrNotEquals";
    RegisterAggrComparePrimitive<TNotEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareDatetime<TDiffDateNotEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareTzDatetime<TAggrTzDateNotEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigDatetime<TDiffDateNotEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigTzDatetime<TAggrTzDateNotEquals, TCompareArgsOpt>(registry, aggrName);

    RegisterAggrCompareStrings<TCustomNotEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, TDecimalAggrNotEquals, TCompareArgsOpt>(registry, aggrName);
}

void RegisterNotEquals(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();

    AddNumericComparisonKernels<TNotEqualsOp>(*family);
    AddDateComparisonKernels<TDiffDateNotEqualsOp>(*family);
    RegisterStringKernelNotEquals(*family);

    kernelFamilyMap["NotEquals"] = std::move(family);
}


} // namespace NMiniKQL
} // namespace NKikimr
