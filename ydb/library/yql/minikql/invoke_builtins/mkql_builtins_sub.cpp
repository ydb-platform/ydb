#include "mkql_builtins_impl.h"  // Y_IGNORE
#include "mkql_builtins_datetime.h"
#include "mkql_builtins_decimal.h" // Y_IGNORE

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TSub : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TSub<TLeft, TRight, TOutput>> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;

    static TOutput Do(TOutput left, TOutput right)
    {
        return left - right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return std::is_integral<TOutput>() ? BinaryOperator::CreateSub(left, right, "sub", block) :  BinaryOperator::CreateFSub(left, right, "sub", block);
    }
#endif
};

template<ui8 Precision>
struct TDecimalSub {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.GetInt128();
        const auto r = right.GetInt128();
        const auto s = l - r;

        using namespace NYql::NDecimal;

        if (IsNormal<Precision>(l) && IsNormal<Precision>(r) && IsNormal<Precision>(s))
            return NUdf::TUnboxedValuePod(s);

        if (IsNan(l) || IsNan(r) || !s)
            return NUdf::TUnboxedValuePod(Nan());
        else
            return NUdf::TUnboxedValuePod(s > 0 ? +Inf() : -Inf());
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto& bounds = NDecimal::GenBounds<Precision>(context);

        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto sub = BinaryOperator::CreateSub(l, r, "sub", block);

        const auto lok = NDecimal::GenInBounds(l, bounds.first, bounds.second, block);
        const auto rok = NDecimal::GenInBounds(r, bounds.first, bounds.second, block);
        const auto sok = NDecimal::GenInBounds(sub, bounds.first, bounds.second, block);

        const auto bok = BinaryOperator::CreateAnd(lok, rok, "bok", block);
        const auto ok = BinaryOperator::CreateAnd(sok, bok, "ok", block);

        const auto bads = BasicBlock::Create(context, "bads", ctx.Func);
        const auto infs = BasicBlock::Create(context, "infs", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto result = PHINode::Create(sub->getType(), 3, "result", done);
        result->addIncoming(sub, block);
        BranchInst::Create(done, bads, ok, block);

        block = bads;

        const auto lnan = NDecimal::GenIsNonComparable(l, context, block);
        const auto rnan = NDecimal::GenIsNonComparable(r, context, block);

        const auto anan = BinaryOperator::CreateOr(lnan, rnan, "anan", block);
        const auto null = ConstantInt::get(sub->getType(), 0);
        const auto zero = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, sub, null, "zero", block);
        const auto nan = BinaryOperator::CreateOr(anan, zero, "nan", block);
        result->addIncoming(GetDecimalNan(context), block);
        BranchInst::Create(done, infs, nan, block);

        block = infs;

        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, sub, null, "plus", block);
        const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);
        result->addIncoming(inf, block);
        BranchInst::Create(done, block);

        block = done;
        return SetterForInt128(result, block);
    }
#endif
    static_assert(Precision <= NYql::NDecimal::MaxPrecision, "Too large precision!");
};

template<typename TLeft, typename TRight, typename TOutput>
struct TDateTimeSub {
    static_assert(TOutput::Features & NYql::NUdf::TimeIntervalType, "Interval type expected");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = left.template Get<typename TLeft::TLayout>();
        const auto rv = right.template Get<typename TRight::TLayout>();
        return NUdf::TUnboxedValuePod(ToScaledDate<TLeft>(lv) - ToScaledDate<TRight>(rv));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GenToScaledDate<TLeft>(GetterFor<typename TLeft::TLayout>(left, context, block), context, block);
        const auto rhs = GenToScaledDate<TRight>(GetterFor<typename TRight::TLayout>(right, context, block), context, block);
        const auto sub = BinaryOperator::CreateSub(lhs, rhs, "sub", block);
        const auto full = SetterFor<typename TOutput::TLayout>(sub, context, block);
        return full;
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TIntervalSubInterval {
    static_assert(TLeft::Features & NYql::NUdf::TimeIntervalType, "Left must be interval type");
    static_assert(TRight::Features & NYql::NUdf::TimeIntervalType, "Right must be interval type");
    static_assert(TOutput::Features & NYql::NUdf::TimeIntervalType, "Output must be interval type");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = left.template Get<typename TLeft::TLayout>();
        const auto rv = right.template Get<typename TRight::TLayout>();
        const auto ret = lv - rv;
        return IsBadInterval<TOutput>(ret) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(ret);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<typename TLeft::TLayout>(left, context, block);
        const auto rhs = GetterFor<typename TRight::TLayout>(right, context, block);
        const auto sub = BinaryOperator::CreateSub(lhs, rhs, "sub", block);
        const auto full = SetterFor<typename TOutput::TLayout>(sub, context, block);
        const auto bad = GenIsBadInterval<TOutput>(sub, context, block);
        const auto zero = ConstantInt::get(Type::getInt128Ty(context), 0);
        const auto sel = SelectInst::Create(bad, zero, full, "sel", block);
        return sel;
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TBigIntervalSub {
    static_assert(std::is_same_v<TLeft, i64>, "Left must be i64");
    static_assert(std::is_same_v<TRight, i64>, "Right must be i64");
    static_assert(std::is_same_v<TOutput, i64>, "Output must be i64");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        i64 lv = left.Get<i64>();
        i64 rv = right.Get<i64>();
        i64 ret = lv - rv;
        // detect overflow
        if (lv > 0 && rv < 0 && ret < 0) {
            return NUdf::TUnboxedValuePod();
        } else if (lv < 0 && rv > 0 && ret > 0) {
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
        const auto sub = BinaryOperator::CreateSub(lhs, rhs, "sub", block);
        const auto wide = SetterFor<i64>(sub, context, block);

        const auto lneg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, lhs, ConstantInt::get(lhs->getType(), 0), "lneg", block);
        const auto rpos = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, rhs, ConstantInt::get(rhs->getType(), 0), "rpos", block);
        const auto apos = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, sub, ConstantInt::get(sub->getType(), 0), "apos", block);
        const auto npp = BinaryOperator::CreateAnd(apos, BinaryOperator::CreateAnd(lneg, rpos, "np", block), "npp", block);

        const auto lpos = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, lhs, ConstantInt::get(lhs->getType(), 0), "lpos", block);
        const auto rneg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, rhs, ConstantInt::get(rhs->getType(), 0), "rneg", block);
        const auto aneg = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, sub, ConstantInt::get(sub->getType(), 0), "aneg", block);
        const auto pnn = BinaryOperator::CreateAnd(aneg, BinaryOperator::CreateAnd(lpos, rneg, "pn", block), "pnn", block);

        const auto bad = BinaryOperator::CreateOr(
                BinaryOperator::CreateOr(npp, pnn, "overflow", block),
                GenIsBadInterval<NUdf::TDataType<NUdf::TInterval64>>(sub, context, block),
                "bad", block);
        const auto zero = ConstantInt::get(Type::getInt128Ty(context), 0);
        const auto sel = SelectInst::Create(bad, zero, wide, "sel", block);
        return sel;
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput, bool Tz>
struct TAnyDateTimeSubIntervalT {
    static_assert(TRight::Features & NYql::NUdf::TimeIntervalType, "right must be interval type");
    static_assert(std::is_same<TLeft, TOutput>::value, "left and output must be same");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = ToScaledDate<TLeft>(left.template Get<typename TLeft::TLayout>());
        const auto rv = ToScaledDate<TRight>(right.template Get<typename TRight::TLayout>());
        const auto ret = lv - rv;
        if (IsBadDateTime<TOutput>(ret)) {
            return NUdf::TUnboxedValuePod();
        }

        auto data = NUdf::TUnboxedValuePod(FromScaledDate<TOutput>(ret));
        if (Tz) {
            data.SetTimezoneId(left.GetTimezoneId());
        }
        return data;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GenToScaledDate<TLeft>(GetterFor<typename TLeft::TLayout>(left, context, block), context, block);
        const auto rhs = GenToScaledDate<TRight>(GetterFor<typename TRight::TLayout>(right, context, block), context, block);
        const auto sub = BinaryOperator::CreateSub(lhs, rhs, "sub", block);
        const auto wide = SetterFor<typename TOutput::TLayout>(GenFromScaledDate<TOutput>(sub, context, block), context, block);
        const auto bad = GenIsBadDateTime<TOutput>(sub, context, block);
        const auto type = Type::getInt128Ty(context);
        const auto zero = ConstantInt::get(type, 0);

        if (Tz) {
            const uint64_t init[] = {0ULL, 0xFFFFULL};
            const auto mask = ConstantInt::get(type, APInt(128, 2, init));
            const auto tzid = BinaryOperator::CreateAnd(left, mask, "tzid",  block);
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
using TAnyDateTimeSubInterval = TAnyDateTimeSubIntervalT<TLeft, TRight, TOutput, false>;

template<typename TLeft, typename TRight, typename TOutput>
using TAnyDateTimeSubIntervalTz = TAnyDateTimeSubIntervalT<TLeft, TRight, TOutput, true>;

}

template <bool LeftTz, bool RightTz, bool LeftBig, bool RightBig>
void RegisterDateSub(IBuiltinFunctionRegistry& registry) {
    static_assert(!(LeftTz && LeftBig), "Expect either Tz or Big date type");
    static_assert(!(RightTz && RightBig), "Expect either Tz or Big date type");

    using TDateLeft1 = std::conditional_t<LeftBig,
        NUdf::TDataType<NUdf::TDate32>,
        std::conditional_t<LeftTz, NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>>>;
    using TDateLeft2 = std::conditional_t<LeftBig,
          NUdf::TDataType<NUdf::TDatetime64>,
          std::conditional_t<LeftTz, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>>>;
    using TDateLeft3 = std::conditional_t<LeftBig,
          NUdf::TDataType<NUdf::TTimestamp64>,
          std::conditional_t<LeftTz, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>>>;

    using TDateRight1 = std::conditional_t<RightBig,
          NUdf::TDataType<NUdf::TDate32>,
          std::conditional_t<RightTz, NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>>>;
    using TDateRight2 = std::conditional_t<RightBig,
          NUdf::TDataType<NUdf::TDatetime64>,
          std::conditional_t<RightTz, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>>>;
    using TDateRight3 = std::conditional_t<RightBig,
          NUdf::TDataType<NUdf::TTimestamp64>,
          std::conditional_t<RightTz, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>>>;

    using TOutput = std::conditional_t<LeftBig || RightBig, 
          NUdf::TDataType<NUdf::TInterval64>,
          NUdf::TDataType<NUdf::TInterval>>;

    RegisterFunctionBinPolyOpt<TDateLeft1, TDateRight1,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinPolyOpt<TDateLeft1, TDateRight2,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinPolyOpt<TDateLeft1, TDateRight3,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");

    RegisterFunctionBinPolyOpt<TDateLeft2, TDateRight1,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinPolyOpt<TDateLeft2, TDateRight2,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinPolyOpt<TDateLeft2, TDateRight3,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");

    RegisterFunctionBinPolyOpt<TDateLeft3, TDateRight1,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinPolyOpt<TDateLeft3, TDateRight2,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinPolyOpt<TDateLeft3, TDateRight3,
        TOutput, TDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
}

void RegisterSub(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNumericFunctionOpt<TSub, TBinaryArgsOpt>(registry, "Sub");
    NDecimal::RegisterBinaryFunctionForAllPrecisions<TDecimalSub, TBinaryArgsOpt>(registry, "Sub_");

    RegisterDateSub<false, false, false, false>(registry);
    RegisterDateSub<false, true,  false, false>(registry);
    RegisterDateSub<true,  false, false, false>(registry);
    RegisterDateSub<true,  true,  false, false>(registry);

    // NarrowDate minus BigDate
    RegisterDateSub<false, false, false, true>(registry);
    RegisterDateSub<true,  false, false, true>(registry);
    // BigDate minus NarrowDate
    RegisterDateSub<false, false, true, false>(registry);
    RegisterDateSub<false, true,  true, false>(registry);
    // BigDate minus BigDate
    RegisterDateSub<false, false, true, true>(registry);

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TIntervalSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TInterval64>, TBigIntervalSub, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval64>, TBigIntervalSub, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TInterval64>, TBigIntervalSub, TBinaryArgsOptWithNullableResult>(registry, "Sub");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TDate>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TDatetime>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTimestamp>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTzDate>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTzDatetime>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTzTimestamp>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TDate>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TDatetime>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TTimestamp>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TTzDate>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TTzDatetime>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TTzTimestamp>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TDate32>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TDatetime64>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TInterval64>,
        NUdf::TDataType<NUdf::TTimestamp64>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");

    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TDate32>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TDatetime64>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTimestamp64>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
}

void RegisterSub(TKernelFamilyMap& kernelFamilyMap) {
    kernelFamilyMap["Sub"] = std::make_unique<TBinaryNumericKernelFamily<TSub, TSub>>();
}

} // namespace NMiniKQL
} // namespace NKikimr
