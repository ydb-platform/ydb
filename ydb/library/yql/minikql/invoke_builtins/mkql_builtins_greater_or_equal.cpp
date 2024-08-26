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
Y_FORCE_INLINE bool GreaterOrEqual(T1 x, T2 y) {
    return x >= y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool GreaterOrEqual(T1 x, T2 y) {
    return x >= T1(0) && static_cast<std::make_unsigned_t<T1>>(x) >= y;
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool GreaterOrEqual(T1 x, T2 y) {
    return T2(0) >= y || x >= static_cast<std::make_unsigned_t<T2>>(y);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
Y_FORCE_INLINE bool GreaterOrEqual(T1 x, T2 y) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    const auto l = static_cast<FT>(x);
    const auto r = static_cast<FT>(y);
    if constexpr (Aggr) {
        if (std::isunordered(l, r))
            return std::isnan(l);
    }
    return l >= r;
}

#ifndef MKQL_DISABLE_CODEGEN
Value* GenGreaterOrEqualUnsigned(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, lhs, rhs, "greater_or_equal", block);
}

Value* GenGreaterOrEqualSigned(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, lhs, rhs, "greater_or_equal", block);
}

template <bool Aggr>
Value* GenGreaterOrEqualFloats(Value* lhs, Value* rhs, BasicBlock* block);

template <>
Value* GenGreaterOrEqualFloats<false>(Value* lhs, Value* rhs, BasicBlock* block) {
    return CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OGE, lhs, rhs, "greater_or_equal", block);
}

template <>
Value* GenGreaterOrEqualFloats<true>(Value* lhs, Value* rhs, BasicBlock* block) {
    const auto oge = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OGE, lhs, rhs, "greater_or_equal", block);
    const auto uno = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UNO, ConstantFP::get(lhs->getType(), 0.0), lhs, "unordered", block);
    return BinaryOperator::CreateOr(oge, uno, "or", block);
}

template <typename T1, typename T2>
Value* GenGreaterOrEqualIntegralLeftSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(x->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, x, zero, "negative", block);
    using T = std::conditional_t<(sizeof(std::make_unsigned_t<T1>) > sizeof(T2)), std::make_unsigned_t<T1>, T2>;
    const auto comp = GenGreaterOrEqualUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getFalse(context), comp, "result", block);
 }

template <typename T1, typename T2>
Value* GenGreaterOrEqualIntegralRightSigned(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    const auto zero = ConstantInt::get(y->getType(), 0);
    const auto neg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, y, zero, "negative", block);
    using T = std::conditional_t<(sizeof(T1) > sizeof(std::make_unsigned_t<T2>)), T1, std::make_unsigned_t<T2>>;
    const auto comp = GenGreaterOrEqualUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
    return SelectInst::Create(neg, ConstantInt::getTrue(context), comp, "result", block);
}


template <typename T1, typename T2,
    std::enable_if_t<std::is_unsigned<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenGreaterOrEqual(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenGreaterOrEqualUnsigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_signed<T1>::value && std::is_signed<T2>::value &&
    std::is_integral<T1>::value && std::is_integral<T2>::value, bool> Aggr>
inline Value* GenGreaterOrEqual(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using T = std::conditional_t<(sizeof(T1) > sizeof(T2)), T1, T2>;
    return GenGreaterOrEqualSigned(StaticCast<T1, T>(x, context, block), StaticCast<T2, T>(y, context, block), block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_signed<T1>::value && std::is_unsigned<T2>::value, bool> Aggr>
inline Value* GenGreaterOrEqual(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenGreaterOrEqualIntegralLeftSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_integral<T1>::value && std::is_integral<T2>::value
    && std::is_unsigned<T1>::value && std::is_signed<T2>::value, bool> Aggr>
inline Value* GenGreaterOrEqual(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    return GenGreaterOrEqualIntegralRightSigned<T1, T2>(x, y, context, block);
}

template <typename T1, typename T2,
    std::enable_if_t<std::is_floating_point<T1>::value || std::is_floating_point<T2>::value, bool> Aggr>
inline Value* GenGreaterOrEqual(Value* x, Value* y, LLVMContext &context, BasicBlock* block) {
    using F1 = std::conditional_t<std::is_floating_point<T1>::value, T1, T2>;
    using F2 = std::conditional_t<std::is_floating_point<T2>::value, T2, T1>;
    using FT = std::conditional_t<(sizeof(F1) > sizeof(F2)), F1, F2>;
    return GenGreaterOrEqualFloats<Aggr>(StaticCast<T1, FT>(x, context, block), StaticCast<T2, FT>(y, context, block), block);
}
#endif

struct TAggrGreaterOrEqual {
    static bool Simple(bool left, bool right)
    {
        return left || !right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static constexpr CmpInst::Predicate SimplePredicate = ICmpInst::ICMP_UGE;
#endif
};

template<typename TLeft, typename TRight, bool Aggr>
struct TGreaterOrEqual : public TCompareArithmeticBinary<TLeft, TRight, TGreaterOrEqual<TLeft, TRight, Aggr>>, public TAggrGreaterOrEqual {
    static bool Do(TLeft left, TRight right)
    {
        return GreaterOrEqual<TLeft, TRight, Aggr>(left, right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenGreaterOrEqual<TLeft, TRight, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TGreaterOrEqualOp;

template<typename TLeft, typename TRight>
struct TGreaterOrEqualOp<TLeft, TRight, bool> : public TGreaterOrEqual<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template<typename TLeft, typename TRight, bool Aggr>
struct TDiffDateGreaterOrEqual : public TCompareArithmeticBinary<typename TLeft::TLayout, typename TRight::TLayout, TDiffDateGreaterOrEqual<TLeft, TRight, Aggr>>, public TAggrGreaterOrEqual {
    static bool Do(typename TLeft::TLayout left, typename TRight::TLayout right)
    {
        return std::is_same<TLeft, TRight>::value ?
            GreaterOrEqual<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right):
            GreaterOrEqual<TScaledDate, TScaledDate, Aggr>(ToScaledDate<TLeft>(left), ToScaledDate<TRight>(right));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        return std::is_same<TLeft, TRight>::value ?
            GenGreaterOrEqual<typename TLeft::TLayout, typename TRight::TLayout, Aggr>(left, right, context, block):
            GenGreaterOrEqual<TScaledDate, TScaledDate, Aggr>(GenToScaledDate<TLeft>(left, context, block), GenToScaledDate<TRight>(right, context, block), context, block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TDiffDateGreaterOrEqualOp;

template<typename TLeft, typename TRight>
struct TDiffDateGreaterOrEqualOp<TLeft, TRight, NUdf::TDataType<bool>> : public TDiffDateGreaterOrEqual<TLeft, TRight, false> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;
};

template<typename TLeft, typename TRight, bool Aggr>
struct TAggrTzDateGreaterOrEqual : public TCompareArithmeticBinaryWithTimezone<TLeft, TRight, TAggrTzDateGreaterOrEqual<TLeft, TRight, Aggr>>, public TAggrGreaterOrEqual {
    static bool Do(TLeft left, TRight right)
    {
        return GreaterOrEqual<TLeft, TRight, Aggr>(left, right);
    }

    static bool DoTz(ui16 left, ui16 right)
    {
        return GreaterOrEqual<ui16, ui16, Aggr>(left, right);
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenGreaterOrEqual<TLeft, TRight, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }

    static Value* GenTz(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return GenGreaterOrEqual<ui16, ui16, Aggr>(left, right, ctx.Codegen.GetContext(), block);
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TCustomGreaterOrEqual : public TAggrGreaterOrEqual {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod left, NUdf::TUnboxedValuePod right) {
        return NUdf::TUnboxedValuePod(CompareCustomsWithCleanup<Slot>(left, right) >= 0);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto res = CallBinaryUnboxedValueFunction(&CompareCustoms<Slot>, Type::getInt32Ty(context), left, right, ctx.Codegen, block);
        const auto comp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, res, ConstantInt::get(res->getType(), 0), "greater_or_equal", block);
        ValueCleanup(EValueRepresentation::String, left, ctx, block);
        ValueCleanup(EValueRepresentation::String, right, ctx, block);
        return MakeBoolean(comp, context, block);
    }
#endif
};

struct TDecimalGreaterOrEqual {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(NYql::NDecimal::IsComparable(l) && NYql::NDecimal::IsComparable(r) && l >= r);
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
        const auto ge = GenGreaterOrEqualSigned(l, r, block);
        const auto res = BinaryOperator::CreateAnd(both, ge, "res", block);
        return MakeBoolean(res, context, block);
    }
#endif
};

struct TDecimalAggrGreaterOrEqual : public TAggrGreaterOrEqual {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        return NUdf::TUnboxedValuePod(l >= r);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto ge = GenGreaterOrEqualSigned(l, r, block);
        return MakeBoolean(ge, context, block);
    }
#endif
};

}

void RegisterGreaterOrEqual(IBuiltinFunctionRegistry& registry) {
    const auto name = "GreaterOrEqual";

    RegisterComparePrimitive<TGreaterOrEqual, TCompareArgsOpt>(registry, name);
    RegisterCompareDatetime<TDiffDateGreaterOrEqual, TCompareArgsOpt>(registry, name);
    RegisterCompareBigDatetime<TDiffDateGreaterOrEqual, TCompareArgsOpt>(registry, name);

    RegisterCompareStrings<TCustomGreaterOrEqual, TCompareArgsOpt>(registry, name);
    RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, TDecimalGreaterOrEqual, TCompareArgsOpt>(registry, name);

    const auto aggrName = "AggrGreaterOrEqual";
    RegisterAggrComparePrimitive<TGreaterOrEqual, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareDatetime<TDiffDateGreaterOrEqual, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareTzDatetime<TAggrTzDateGreaterOrEqual, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigDatetime<TDiffDateGreaterOrEqual, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareBigTzDatetime<TAggrTzDateGreaterOrEqual, TCompareArgsOpt>(registry, aggrName);

    RegisterAggrCompareStrings<TCustomGreaterOrEqual, TCompareArgsOpt>(registry, aggrName);
    RegisterAggrCompareCustomOpt<NUdf::TDataType<NUdf::TDecimal>, TDecimalAggrGreaterOrEqual, TCompareArgsOpt>(registry, aggrName);
}

void RegisterGreaterOrEqual(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();

    AddNumericComparisonKernels<TGreaterOrEqualOp>(*family);
    AddDateComparisonKernels<TDiffDateGreaterOrEqualOp>(*family);
    AddDecimalComparisonKernels<TDecimalGreaterOrEqual>(*family);
    RegisterStringKernelGreaterOrEqual(*family);

    kernelFamilyMap["GreaterOrEqual"] = std::move(family);
}

} // namespace NMiniKQL
} // namespace NKikimr
