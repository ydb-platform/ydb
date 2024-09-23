#include "mkql_builtins_impl.h"  // Y_IGNORE
#include "mkql_builtins_datetime.h"
#include "mkql_builtins_decimal.h" // Y_IGNORE

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TAdd : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TAdd<TLeft, TRight, TOutput>> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;

    static TOutput Do(TOutput left, TOutput right)
    {
        return left + right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return std::is_integral<TOutput>() ? BinaryOperator::CreateAdd(left, right, "add", block) : BinaryOperator::CreateFAdd(left, right, "add", block);
    }
#endif
};

template<typename TType>
using TAggrAdd = TAdd<TType, TType, TType>;

template<ui8 Precision>
struct TDecimalAdd {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        const auto a = l + r;

        using namespace NYql::NDecimal;

        if (IsNormal<Precision>(l) && IsNormal<Precision>(r) && IsNormal<Precision>(a))
            return NUdf::TUnboxedValuePod(a);
        if (IsNan(l) || IsNan(r) || !a)
            return NUdf::TUnboxedValuePod(Nan());
        else
            return NUdf::TUnboxedValuePod(a > 0 ? +Inf() : -Inf());
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto& bounds = NDecimal::GenBounds<Precision>(context);

        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto add = BinaryOperator::CreateAdd(l, r, "add", block);

        const auto lok = NDecimal::GenInBounds(l, bounds.first, bounds.second, block);
        const auto rok = NDecimal::GenInBounds(r, bounds.first, bounds.second, block);
        const auto aok = NDecimal::GenInBounds(add, bounds.first, bounds.second, block);

        const auto bok = BinaryOperator::CreateAnd(lok, rok, "bok", block);
        const auto ok = BinaryOperator::CreateAnd(aok, bok, "ok", block);

        const auto bads = BasicBlock::Create(context, "bads", ctx.Func);
        const auto infs = BasicBlock::Create(context, "infs", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto result = PHINode::Create(add->getType(), 3, "result", done);
        result->addIncoming(add, block);
        BranchInst::Create(done, bads, ok, block);

        block = bads;

        const auto lnan = NDecimal::GenIsNonComparable(l, context, block);
        const auto rnan = NDecimal::GenIsNonComparable(r, context, block);

        const auto anan = BinaryOperator::CreateOr(lnan, rnan, "anan", block);
        const auto null = ConstantInt::get(add->getType(), 0);
        const auto zero = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, add, null, "zero", block);
        const auto nan = BinaryOperator::CreateOr(anan, zero, "nan", block);
        result->addIncoming(GetDecimalNan(context), block);
        BranchInst::Create(done, infs, nan, block);

        block = infs;

        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, add, null, "plus", block);
        const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);
        result->addIncoming(inf, block);
        BranchInst::Create(done, block);

        block = done;
        return SetterForInt128(result, block);
    }
#endif
    static_assert(Precision <= NYql::NDecimal::MaxPrecision, "Too large precision!");
};

template<typename TLeft, typename TRight, typename TOutput, bool Tz = false>
struct TDateTimeAddT {
    static_assert(std::is_integral<typename TLeft::TLayout>::value, "left must be integral");
    static_assert(std::is_integral<typename TRight::TLayout>::value, "right must be integral");
    static_assert(std::is_integral<typename TOutput::TLayout>::value, "output must be integral");

    static constexpr auto NullMode = TKernel::ENullMode::AlwaysNull;

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = ToScaledDate<TLeft>(left.template Get<typename TLeft::TLayout>());
        const auto rv = ToScaledDate<TRight>(right.template Get<typename TRight::TLayout>());
        const auto ret = lv + rv;
        if (IsBadScaledDate<TOutput>(ret)) {
            return NUdf::TUnboxedValuePod();
        }

        auto data = NUdf::TUnboxedValuePod(FromScaledDate<TOutput>(ret));
        if constexpr (Tz) {
            data.SetTimezoneId(((std::is_same<TLeft, NUdf::TDataType<NUdf::TInterval>>() || std::is_same<TLeft, NUdf::TDataType<NUdf::TInterval64>>()) ? right : left).GetTimezoneId());
        }
        return data;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GenToScaledDate<TLeft>(GetterFor<typename TLeft::TLayout>(left, context, block), context, block);
        const auto rhs = GenToScaledDate<TRight>(GetterFor<typename TRight::TLayout>(right, context, block), context, block);
        const auto add = BinaryOperator::CreateAdd(lhs, rhs, "add", block);
        const auto wide = SetterFor<typename TOutput::TLayout>(GenFromScaledDate<TOutput>(add, context, block), context, block);
        const auto bad = GenIsBadScaledDate<TOutput>(add, context, block);
        const auto type = Type::getInt128Ty(context);
        const auto zero = ConstantInt::get(type, 0);

        if constexpr (Tz) {
            const uint64_t init[] = {0ULL, 0xFFFFULL};
            const auto mask = ConstantInt::get(type, APInt(128, 2, init));
            const auto tzid = BinaryOperator::CreateAnd((std::is_same<TLeft, NUdf::TDataType<NUdf::TInterval>>() || std::is_same<TLeft, NUdf::TDataType<NUdf::TInterval64>>()) ? right : left, mask, "tzid",  block);
            const auto full = BinaryOperator::CreateOr(wide, tzid, "full",  block);
            const auto sel = SelectInst::Create(bad, zero, full, "sel", block);
            return sel;
        } else {
            const auto sel = SelectInst::Create(bad, zero, wide, "sel", block);
            return sel;
        }

    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TBigIntervalAdd {
    static_assert(std::is_same_v<typename TLeft::TLayout, i64>, "Left must be i64");
    static_assert(std::is_same_v<typename TRight::TLayout, i64>, "Right must be i64");
    static_assert(std::is_same_v<typename TOutput::TLayout, i64>, "Output must be i64");

    static constexpr auto NullMode = TKernel::ENullMode::AlwaysNull;

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        i64 lv = left.Get<i64>();
        i64 rv = right.Get<i64>();
        i64 ret = lv + rv;
        // detect overflow
        if (lv > 0 && rv > 0 && ret < 0) {
            return NUdf::TUnboxedValuePod();
        } else if (lv < 0 && rv < 0 && ret > 0) {
            return NUdf::TUnboxedValuePod();
        } else if (IsBadInterval<NUdf::TDataType<NUdf::TInterval64>>(ret)) {
            return NUdf::TUnboxedValuePod();
        }
        return NUdf::TUnboxedValuePod(ret);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<i64>(left, context, block);
        const auto rhs = GetterFor<i64>(right, context, block);
        const auto add = BinaryOperator::CreateAdd(lhs, rhs, "add", block);
        const auto wide = SetterFor<i64>(add, context, block);

        const auto lneg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, lhs, ConstantInt::get(lhs->getType(), 0), "lneg", block);
        const auto rneg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, rhs, ConstantInt::get(rhs->getType(), 0), "rneg", block);
        const auto apos = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, add, ConstantInt::get(add->getType(), 0), "apos", block);
        const auto posAddNegArg = BinaryOperator::CreateAnd(apos, BinaryOperator::CreateAnd(lneg, rneg, "negArg", block), "posAddNegArg", block);

        const auto lpos = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, lhs, ConstantInt::get(lhs->getType(), 0), "lpos", block);
        const auto rpos = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, rhs, ConstantInt::get(rhs->getType(), 0), "rpos", block);
        const auto aneg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, add, ConstantInt::get(add->getType(), 0), "aneg", block);
        const auto negAddPosArg = BinaryOperator::CreateAnd(aneg, BinaryOperator::CreateAnd(lpos, rpos, "posArg", block), "negAddPosArg", block);

        const auto bad = BinaryOperator::CreateOr(
                BinaryOperator::CreateOr(posAddNegArg, negAddPosArg, "overflow", block),
                GenIsBadInterval<NUdf::TDataType<NUdf::TInterval64>>(add, context, block),
                "bad", block);
        const auto zero = ConstantInt::get(Type::getInt128Ty(context), 0);
        const auto sel = SelectInst::Create(bad, zero, wide, "sel", block);
        return sel;
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
using TDateTimeAdd = TDateTimeAddT<TLeft, TRight, TOutput, false>;

template<typename TLeft, typename TRight, typename TOutput>
using TDateTimeAddTz = TDateTimeAddT<TLeft, TRight, TOutput, true>;

template <bool Tz, typename TIntervalType, template<typename, typename, typename> class TAdder>
void RegisterAddDateAndInterval(IBuiltinFunctionRegistry& registry) {
    using TDate1 = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>>;
    using TDate2 = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>>;
    using TDate3 = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>>;

    using TDate1Big = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TDate32>>;
    using TDate2Big = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TDatetime64>>;
    using TDate3Big = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTimestamp64>>;

    RegisterFunctionBinPolyOpt<TDate1, TIntervalType,
        TDate1, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinPolyOpt<TDate2, TIntervalType,
        TDate2, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinPolyOpt<TDate3, TIntervalType,
        TDate3, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");

    RegisterFunctionBinPolyOpt<TIntervalType, TDate1,
        TDate1, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinPolyOpt<TIntervalType, TDate2,
        TDate2, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinPolyOpt<TIntervalType, TDate3,
        TDate3, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");

    RegisterFunctionBinPolyOpt<TDate1Big, TIntervalType,
        TDate1Big, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinPolyOpt<TDate2Big, TIntervalType,
        TDate2Big, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinPolyOpt<TDate3Big, TIntervalType,
        TDate3Big, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");

    RegisterFunctionBinPolyOpt<TIntervalType, TDate1Big,
        TDate1Big, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinPolyOpt<TIntervalType, TDate2Big,
        TDate2Big, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinPolyOpt<TIntervalType, TDate3Big,
        TDate3Big, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
}

template<typename TType>
using TIntervalAggrAdd = TDateTimeAdd<TType, TType, TType>;

}

void RegisterAdd(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNumericFunctionOpt<TAdd, TBinaryArgsOpt>(registry, "Add");
    NDecimal::RegisterBinaryFunctionForAllPrecisions<TDecimalAdd, TBinaryArgsOpt>(registry, "Add_");

    RegisterAddDateAndInterval<false, NUdf::TDataType<NUdf::TInterval>, TDateTimeAdd>(registry);
    RegisterAddDateAndInterval<true, NUdf::TDataType<NUdf::TInterval>, TDateTimeAddTz>(registry);
    RegisterAddDateAndInterval<false, NUdf::TDataType<NUdf::TInterval64>, TDateTimeAdd>(registry);
    RegisterAddDateAndInterval<true, NUdf::TDataType<NUdf::TInterval64>, TDateTimeAddTz>(registry);

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TDateTimeAdd, TBinaryArgsOptWithNullableResult>(registry, "Add");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TInterval64>, TBigIntervalAdd, TBinaryArgsOptWithNullableResult>(registry, "Add");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval64>, TBigIntervalAdd, TBinaryArgsOptWithNullableResult>(registry, "Add");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TInterval64>, TBigIntervalAdd, TBinaryArgsOptWithNullableResult>(registry, "Add");
}

template <bool Tz, bool BigDate, bool BigInterval>
void RegisterDateAddInterval(TKernelFamilyBase& owner) {
    using TDateLeft1 = std::conditional_t<BigDate,
        std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TDate32>>,
        std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>>>;
    using TDateLeft2 = std::conditional_t<BigDate,
          std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TDatetime64>>,
          std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>>>;
    using TDateLeft3 = std::conditional_t<BigDate,
          std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTimestamp64>>,
          std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>>>;

    using TIntervalRight = std::conditional_t<BigInterval,
          NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>>;

    if constexpr (Tz) {
        AddBinaryKernelPoly<TDateLeft1, TIntervalRight, TDateLeft1, TDateTimeAddTz>(owner);
        AddBinaryKernelPoly<TDateLeft2, TIntervalRight, TDateLeft2, TDateTimeAddTz>(owner);
        AddBinaryKernelPoly<TDateLeft3, TIntervalRight, TDateLeft3, TDateTimeAddTz>(owner);
    } else {
        AddBinaryKernelPoly<TDateLeft1, TIntervalRight, TDateLeft1, TDateTimeAdd>(owner);
        AddBinaryKernelPoly<TDateLeft2, TIntervalRight, TDateLeft2, TDateTimeAdd>(owner);
        AddBinaryKernelPoly<TDateLeft3, TIntervalRight, TDateLeft3, TDateTimeAdd>(owner);
    }
}

template <bool Tz, bool BigDate, bool BigInterval>
void RegisterIntervalAddDate(TKernelFamilyBase& owner) {
    using TIntervalLeft = std::conditional_t<BigInterval,
          NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>>;

    using TDateRight1 = std::conditional_t<BigDate,
        std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TDate32>>,
        std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>>>;
    using TDateRight2 = std::conditional_t<BigDate,
          std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TDatetime64>>,
          std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>>>;
    using TDateRight3 = std::conditional_t<BigDate,
          std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTimestamp64>>,
          std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>>>;

    if constexpr (Tz) {
        AddBinaryKernelPoly<TIntervalLeft, TDateRight1, TDateRight1, TDateTimeAddTz>(owner);
        AddBinaryKernelPoly<TIntervalLeft, TDateRight2, TDateRight2, TDateTimeAddTz>(owner);
        AddBinaryKernelPoly<TIntervalLeft, TDateRight3, TDateRight3, TDateTimeAddTz>(owner);
    } else {
        AddBinaryKernelPoly<TIntervalLeft, TDateRight1, TDateRight1, TDateTimeAdd>(owner);
        AddBinaryKernelPoly<TIntervalLeft, TDateRight2, TDateRight2, TDateTimeAdd>(owner);
        AddBinaryKernelPoly<TIntervalLeft, TDateRight3, TDateRight3, TDateTimeAdd>(owner);
    }
}

template <bool BigInterval1, bool BigInterval2>
void RegisterIntervalAddInterval(TKernelFamilyBase& owner) {
    using TLeft = std::conditional_t<BigInterval1,
        NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>>;
    using TRight = std::conditional_t<BigInterval2,
        NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>>;

    using TOutput = std::conditional_t<BigInterval1 || BigInterval2,
          NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>>;

    if constexpr (BigInterval1 || BigInterval2) {
        AddBinaryKernelPoly<TLeft, TRight, TOutput, TBigIntervalAdd>(owner);
    } else {
        AddBinaryKernelPoly<TLeft, TRight, TOutput, TDateTimeAdd>(owner);
    }
}

void RegisterAdd(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();

    AddBinaryIntegralKernels<TAdd>(*family);
    AddBinaryRealKernels<TAdd>(*family);

    RegisterDateAddInterval<false, false, false>(*family);
    RegisterDateAddInterval<true, false, false>(*family);
    RegisterDateAddInterval<false, true, false>(*family);
    RegisterDateAddInterval<true, true, false>(*family);

    RegisterDateAddInterval<false, false, true>(*family);
    RegisterDateAddInterval<true, false, true>(*family);
    RegisterDateAddInterval<false, true, true>(*family);
    RegisterDateAddInterval<true, true, true>(*family);

    RegisterIntervalAddDate<false, false, false>(*family);
    RegisterIntervalAddDate<true, false, false>(*family);
    RegisterIntervalAddDate<false, true, false>(*family);
    RegisterIntervalAddDate<true, true, false>(*family);

    RegisterIntervalAddDate<false, false, true>(*family);
    RegisterIntervalAddDate<true, false, true>(*family);
    RegisterIntervalAddDate<false, true, true>(*family);
    RegisterIntervalAddDate<true, true, true>(*family);

    RegisterIntervalAddInterval<false, false>(*family);
    RegisterIntervalAddInterval<false, true>(*family);
    RegisterIntervalAddInterval<true, false>(*family);
    RegisterIntervalAddInterval<true, true>(*family);

    kernelFamilyMap["Add"] = std::move(family);
}

void RegisterAggrAdd(IBuiltinFunctionRegistry& registry) {
    RegisterNumericAggregateFunction<TAggrAdd, TBinaryArgsSameOpt>(registry, "AggrAdd");
    RegisterAggregateFunctionPoly<NUdf::TDataType<NUdf::TInterval>, TIntervalAggrAdd, TBinaryArgsSameOptArgsWithNullableResult>(registry, "AggrAdd");
    NDecimal::RegisterAggregateFunctionForAllPrecisions<TDecimalAdd, TBinaryArgsSameOpt>(registry, "AggrAdd_");
}

} // namespace NMiniKQL
} // namespace NKikimr
