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
Y_FORCE_INLINE bool Equals(T1 x, T2 y) {
    return x == y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Equals(T1 x, T2 y) {
    return x >= T1(0) && static_cast<std::make_unsigned_t<T1>>(x) == y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Equals(T1 x, T2 y) {
    return y >= T2(0) && x == static_cast<std::make_unsigned_t<T2>>(y);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Equals(T1 x, T2 y) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    const auto l = static_cast<FT>(x);
    const auto r = static_cast<FT>(y);
    if constexpr (Aggr) {
        if (std::isunordered(l, r))
            return std::isnan(l) == std::isnan(r);
    }
    return l == r;
}

#ifndef MKQL_DISABLE_CODEGEN
Value* GenEqualsIntegral(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, lhs, rhs, "equals", block);
}

template <bool Aggr>
Value* GenEqualsFloats(Value* lhs, Value* rhs, BasicBlock* block);

template <>
Value* GenEqualsFloats<false>(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OEQ, lhs, rhs, "equals", block);
}

template <>
Value* GenEqualsFloats<true>(Value* lhs, Value* rhs, BasicBlock* block) {
    const auto ueq = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UEQ, lhs, rhs, "equals", block);
    const auto lord = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ORD, ConstantFP::get(lhs->getType(), 0.0), lhs, "lord", block);
    const auto runo = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, ConstantFP::get(rhs->getType(), 0.0), rhs, "runo", block);
    const auto once = BinaryOperator::CreateXor(lord, runo, "xor", block);
    return BinaryOperator::CreateAnd(ueq, once, "and", block);
}

template <typename T1, typename T2>
Value* GenEqualsIntegralLeftSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(x->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, x, zero, "negative", block);
    using T = std::conditional_t<(sizeof(std::make_unsigned_t<T1>) > sizeof(T2)), std::make_unsigned_t<T1>, T2>;
    const auto comp = GenEqualsIntegral(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getFalse(context), comp, "result", block);
 }

template <typename T1, typename T2>
Value* GenEqualsIntegralRightSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(y->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, y, zero, "negative", block);
    using T = std::conditional_t<(sizeof(T1) > sizeof(std::make_unsigned_t<T2>)), T1, std::make_unsigned_t<T2>>;
    const auto comp = GenEqualsIntegral(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getFalse(context), comp, "result", block);
}


template <typename T1, typename T2,
    std::enable_if_t<std::is_unsigned<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenEqualsIntegral(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_signed<T1>::value && std::is_signed<T2>::value &&
    std::is_integral<T1>::value && std::is_integral<T2>::value, bool> Aggr>
inline Value* GenEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenEqualsIntegral(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenEqualsIntegralLeftSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
inline Value* GenEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenEqualsIntegralRightSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
inline Value* GenEquals(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    return GenEqualsFloats<Aggr>(StaticCast<T1, FT>(x, context, block), StaticCast<T2, FT>(y, context, block), block);
}
#endif

struct TAggrEquals {
    static bool Simple(bool left, bool right)
    {
        return left == right;
    }
    static bool Join(bool one, bool two)
    {
        return one && two;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static constexpr CmpInst::Predicate SimplePredicate = ICmpInst::ICMP_EQ;

    static Value* GenJoin(Value* one, Value* two, BasicBlock* block)
    {
        return BinaryOperator::CreateAnd(one, two, "and", block);
    }
#endif
};

template<typename TLeft, typename TRight, bool Aggr>
struct TEquals : public TCompareArithmeticBinary<TLeft, TRight, TEquals<TLeft, TRight, Aggr>>, public TAggrEquals {
    static bool Do(TLeft left, TRight right)
    {
        return Equals<TLeft, TRight, Aggr>(left, right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenEquals<TLeft, TRight, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TEqualsOp;

template<typename TLeft, typename TRight>
struct TEqualsOp<TLeft, TRight, bool> : public TEquals<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template<typename TLeft, typename TRight, bool Aggr>
struct TDiffDateEquals : public TCompareArithmeticBinary<typename TLeft::TLayout, typename TRight::TLayout, TDiffDateEquals<TLeft, TRight, Aggr>>, public TAggrEquals {
    static bool Do(typename TLeft::TLayout left, typename TRight::TLayout right)
    {
        return std::is_same<TLeft, TRight>::value ?
            Equals<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right):
            Equals<TScaledDate, TScaledDate, Aggr>(ToScaledDate<TLeft>(left), ToScaledDate<TRight>(right));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        return std::is_same<TLeft, TRight>::value ?
            GenEquals<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right, context, block):
            GenEquals<TScaledDate, TScaledDate, Aggr>(GenToScaledDate<TLeft>(left, context, block), GenToScaledDate<TRight>(right, context, block), context, block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TDiffDateEqualsOp;

template<typename TLeft, typename TRight>
struct TDiffDateEqualsOp<TLeft, TRight, NUdf::TDataType<bool>> : public TDiffDateEquals<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template <typename TLeft, typename TRight, bool Aggr>
struct TAggrTzDateEquals : public TArithmeticConstraintsBinary<TLeft, TRight, bool>, public TAggrEquals {
    static_assert(std::is_same<TLeft, TRight>::value, "Must be same type.");
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        return NUdf::TUnboxedValuePod(Join(Equals<TLeft, TRight, Aggr>(left.template Get<TLeft>(), right.template Get<TRight>()), Equals<ui16, ui16, Aggr>(left.GetTimezoneId(), right.GetTimezoneId())));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<TLeft>(left, context, block);
        const auto rhs = GetterFor<TRight>(right, context, block);
        const auto ltz = GetterForTimezone(context, left, block);
        const auto rtz = GetterForTimezone(context, right, block);
        const auto result = GenJoin(GenEquals<TLeft, TRight, Aggr>(lhs, rhs, context, block), GenEquals<ui16, ui16, Aggr>(ltz, rtz, context, block), block);
        const auto wide = MakeBoolean(result, context, block);
        return wide;
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TCustomEquals : public TAggrEquals {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod left, NUdf::TUnboxedValuePod right) {
        return NUdf::TUnboxedValuePod(CompareCustomsWithCleanup<Slot>(left, right) == 0);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto res = CallBinaryUnboxedValueFunction(&CompareCustoms<Slot>, Type::getInt32Ty(context), left, right, ctx.Codegen, block);
        const auto comp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, res, ConstantInt::get(res->getType(), 0), "equals", block);
        ValueCleanup(EValueRepresentation::String, left, ctx, block);
        ValueCleanup(EValueRepresentation::String, right, ctx, block);
        return MakeBoolean(comp, context, block);
    }
#endif
};

struct TDecimalEquals {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(NYql::NDecimal::IsComparable(l) && l == r);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto good = NDecimal::GenIsComparable(l, context, block);
        const auto eq = GenEqualsIntegral(l, r, block);
        const auto res = BinaryOperator::CreateAnd(good, eq, "res", block);
        return MakeBoolean(res, context, block);
    }
#endif
};

struct TDecimalAggrEquals : public TAggrEquals {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(l == r);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto eq = GenEqualsIntegral(l, r, block);
        return MakeBoolean(eq, context, block);
    }
#endif
};

}

void RegisterEquals(IBuiltinFunctionRegistry& registry) {
    const auto name = "Equals";

    RegisterComparePrimitive<TEquals, TCompareArgsOpt>(registry, name);
    RegisterCompareDatetime<TDiffDateEquals, TCompareArgsOpt>(registry, name);
    RegisterCompareBigDatetime<TDiffDateEquals, TCompareArgsOpt>(registry, name);

    RegisterCompareStrings<TCustomEquals, TCompareArgsOpt>(registry, name);
    RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, TDecimalEquals, TCompareArgsOpt>(registry, name);

    const auto aggrName = "AggrEquals";
    RegisterAggrComparePrimitive<TEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareDatetime<TDiffDateEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareTzDatetime<TAggrTzDateEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigDatetime<TDiffDateEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigTzDatetime<TAggrTzDateEquals, TCompareArgsOpt>(registry, aggrName);

    RegisterAggrCompareStrings<TCustomEquals, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, TDecimalAggrEquals, TCompareArgsOpt>(registry, aggrName);
}

void RegisterEquals(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();

    AddNumericComparisonKernels<TEqualsOp>(*family);
    AddDateComparisonKernels<TDiffDateEqualsOp>(*family);
    RegisterStringKernelEquals(*family);

    kernelFamilyMap["Equals"] = std::move(family);
}

} // namespace NMiniKQL
} // namespace NKikimr
