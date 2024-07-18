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
Y_FORCE_INLINE bool Less(T1 x, T2 y) {
    return x < y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Less(T1 x, T2 y) {
    return x < T1(0) || static_cast<std::make_unsigned_t<T1>>(x) < y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Less(T1 x, T2 y) {
    return T2(0) < y && x < static_cast<std::make_unsigned_t<T2>>(y);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool Less(T1 x, T2 y) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    const auto l = static_cast<FT>(x);
    const auto r = static_cast<FT>(y);
    if constexpr (Aggr) {
        if (std::isunordered(l, r))
            return !std::isnan(l);
    }
    return l < r;
}

#ifndef MKQL_DISABLE_CODEGEN
Value* GenLessUnsigned(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, lhs, rhs, "less", block);
}

Value* GenLessSigned(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, lhs, rhs, "less", block);
}

template <bool Aggr>
Value* GenLessFloats(Value* lhs, Value* rhs, BasicBlock* block);

template <>
Value* GenLessFloats<false>(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OLT, lhs, rhs, "less", block);
}

template <>
Value* GenLessFloats<true>(Value* lhs, Value* rhs, BasicBlock* block) {
    const auto ult = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ULT, lhs, rhs, "less", block);
    const auto ord = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ORD, ConstantFP::get(lhs->getType(), 0.0), lhs, "ordered", block);
    return BinaryOperator::CreateAnd(ult, ord, "and", block);
}

template <typename T1, typename T2>
Value* GenLessIntegralLeftSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(x->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, x, zero, "negative", block);
    using T = std::conditional_t<(sizeof(std::make_unsigned_t<T1>) > sizeof(T2)), std::make_unsigned_t<T1>, T2>;
    const auto comp = GenLessUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getTrue(context), comp, "result", block);
 }

template <typename T1, typename T2>
Value* GenLessIntegralRightSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(y->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, y, zero, "negative", block);
    using T = std::conditional_t<(sizeof(T1) > sizeof(std::make_unsigned_t<T2>)), T1, std::make_unsigned_t<T2>>;
    const auto comp = GenLessUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getFalse(context), comp, "result", block);
}


template <typename T1, typename T2,
    std::enable_if_t<std::is_unsigned<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenLess(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenLessUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_signed<T1>::value && std::is_signed<T2>::value &&
    std::is_integral<T1>::value && std::is_integral<T2>::value, bool> Aggr>
inline Value* GenLess(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenLessSigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenLess(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenLessIntegralLeftSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
inline Value* GenLess(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenLessIntegralRightSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
inline Value* GenLess(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    return GenLessFloats<Aggr>(StaticCast<T1, FT>(x, context, block), StaticCast<T2, FT>(y, context, block), block);
}
#endif

struct TAggrLess {
    static bool Simple(bool left, bool right)
    {
        return !left && right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static constexpr CmpInst::Predicate SimplePredicate = ICmpInst::ICMP_ULT;
#endif
};

template<typename TLeft, typename TRight, bool Aggr>
struct TLess : public TCompareArithmeticBinary<TLeft, TRight, TLess<TLeft, TRight, Aggr>>, public TAggrLess {
    static bool Do(TLeft left, TRight right)
    {
        return Less<TLeft, TRight, Aggr>(left, right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenLess<TLeft, TRight, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TLessOp;

template<typename TLeft, typename TRight>
struct TLessOp<TLeft, TRight, bool> : public TLess<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template<typename TLeft, typename TRight, bool Aggr>
struct TDiffDateLess : public TCompareArithmeticBinary<typename TLeft::TLayout, typename TRight::TLayout, TDiffDateLess<TLeft, TRight, Aggr>>, public TAggrLess {
    static bool Do(typename TLeft::TLayout left, typename TRight::TLayout right)
    {
        return std::is_same<TLeft, TRight>::value ?
            Less<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right):
            Less<TScaledDate, TScaledDate, Aggr>(ToScaledDate<TLeft>(left), ToScaledDate<TRight>(right));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        return std::is_same<TLeft, TRight>::value ?
            GenLess<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right, context, block):
            GenLess<TScaledDate, TScaledDate, Aggr>(GenToScaledDate<TLeft>(left, context, block), GenToScaledDate<TRight>(right, context, block), context, block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TDiffDateLessOp;

template<typename TLeft, typename TRight>
struct TDiffDateLessOp<TLeft, TRight, NUdf::TDataType<bool>> : public TDiffDateLess<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template<typename TLeft, typename TRight, bool Aggr>
struct TAggrTzDateLess : public TCompareArithmeticBinaryWithTimezone<TLeft, TRight, TAggrTzDateLess<TLeft, TRight, Aggr>>, public TAggrLess {
    static bool Do(TLeft left, TRight right)
    {
        return Less<TLeft, TRight, Aggr>(left, right);
    }

    static bool DoTz(ui16 left, ui16 right)
    {
        return Less<ui16, ui16, Aggr>(left, right);
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenLess<TLeft, TRight, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }

    static Value* GenTz(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenLess<ui16, ui16, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TCustomLess : public TAggrLess {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod left, NUdf::TUnboxedValuePod right) {
        return NUdf::TUnboxedValuePod(CompareCustomsWithCleanup<Slot>(left, right) < 0);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto res = CallBinaryUnboxedValueFunction(&CompareCustoms<Slot>, Type::getInt32Ty(context), left, right, ctx.Codegen, block);
        const auto comp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, res, ConstantInt::get(res->getType(), 0), "less", block);
        ValueCleanup(EValueRepresentation::String, left, ctx, block);
        ValueCleanup(EValueRepresentation::String, right, ctx, block);
        return MakeBoolean(comp, context, block);
    }
#endif
};

struct TDecimalLess {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(NYql::NDecimal::IsComparable(l) && NYql::NDecimal::IsComparable(r) && l < r);
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
        const auto ls = GenLessSigned(l, r, block);
        const auto res = BinaryOperator::CreateAnd(both, ls, "res", block);
        return MakeBoolean(res, context, block);
    }
#endif
};

struct TDecimalAggrLess : public TAggrLess {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(l < r);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto ls = GenLessSigned(l, r, block);
        return MakeBoolean(ls, context, block);
    }
#endif
};

}

void RegisterLess(IBuiltinFunctionRegistry& registry) {
    const auto name = "Less";

    RegisterComparePrimitive<TLess, TCompareArgsOpt>(registry, name);
    RegisterCompareDatetime<TDiffDateLess, TCompareArgsOpt>(registry, name);
    RegisterCompareBigDatetime<TDiffDateLess, TCompareArgsOpt>(registry, name);

    RegisterCompareStrings<TCustomLess, TCompareArgsOpt>(registry, name);
    RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, TDecimalLess, TCompareArgsOpt>(registry, name);

    const auto aggrName = "AggrLess";
    RegisterAggrComparePrimitive<TLess, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareDatetime<TDiffDateLess, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareTzDatetime<TAggrTzDateLess, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigDatetime<TDiffDateLess, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigTzDatetime<TAggrTzDateLess, TCompareArgsOpt>(registry, aggrName);

    RegisterAggrCompareStrings<TCustomLess, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, TDecimalAggrLess, TCompareArgsOpt>(registry, aggrName);
}

void RegisterLess(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();

    AddNumericComparisonKernels<TLessOp>(*family);
    AddDateComparisonKernels<TDiffDateLessOp>(*family);
    RegisterStringKernelLess(*family);

    kernelFamilyMap["Less"] = std::move(family);
}

} // namespace NMiniKQL
} // namespace NKikimr
