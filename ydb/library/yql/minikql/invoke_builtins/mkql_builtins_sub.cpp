#include "mkql_builtins_impl.h"
#include "mkql_builtins_datetime.h"
#include "mkql_builtins_decimal.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TSub : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TSub<TLeft, TRight, TOutput>> {
    static constexpr bool DefaultNulls = true;

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
struct TDiffDateTimeSub {
    static_assert(!std::is_same<TLeft, TRight>::value, "left and right must not be same");
    static_assert(std::is_same<TOutput, NUdf::TDataType<NUdf::TInterval>::TLayout>::value, "expected output interval");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = static_cast<std::make_signed_t<TLeft>>(left.template Get<TLeft>());
        const auto rv = static_cast<std::make_signed_t<TRight>>(right.template Get<TRight>());
        return NUdf::TUnboxedValuePod(ToScaledDate(lv) - ToScaledDate(rv));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GenToScaledDate<std::make_signed_t<TLeft>>(GetterFor<TLeft>(left, context, block), context, block);
        const auto rhs = GenToScaledDate<std::make_signed_t<TRight>>(GetterFor<TRight>(right, context, block), context, block);
        const auto sub = BinaryOperator::CreateSub(lhs, rhs, "sub", block);
        const auto full = SetterFor<TOutput>(sub, context, block);
        return full;
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TSameDateTimeSub {
    static_assert(std::is_same<TOutput, NUdf::TDataType<NUdf::TInterval>::TLayout>::value, "expected output interval");
    static_assert(std::is_same<TLeft, TRight>::value, "left and right must be same");
    using TSignedArg = std::make_signed_t<TLeft>;

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = static_cast<TSignedArg>(left.template Get<TLeft>());
        const auto rv = static_cast<TSignedArg>(right.template Get<TRight>());
        return NUdf::TUnboxedValuePod(ToScaledDate<TSignedArg>(lv - rv));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<TLeft>(left, context, block);
        const auto rhs = GetterFor<TRight>(right, context, block);
        const auto sub = BinaryOperator::CreateSub(lhs, rhs, "sub", block);
        const auto full = SetterFor<TOutput>(GenToScaledDate<TSignedArg>(sub, context, block), context, block);
        return full;
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TIntervalSubInterval {
    static_assert(std::is_same<TOutput, NUdf::TDataType<NUdf::TInterval>::TLayout>::value, "expected output interval");
    static_assert(std::is_same<TLeft, TRight>::value, "left and right must be same");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = left.template Get<TLeft>();
        const auto rv = right.template Get<TRight>();
        const auto ret = lv - rv;
        return IsBadInterval(ret) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(FromScaledDate<TOutput>(ret));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<TLeft>(left, context, block);
        const auto rhs = GetterFor<TRight>(right, context, block);
        const auto sub = BinaryOperator::CreateSub(lhs, rhs, "sub", block);
        const auto full = SetterFor<TOutput>(sub, context, block);
        const auto bad = GenIsBadInterval(sub, context, block);
        const auto zero = ConstantInt::get(Type::getInt128Ty(context), 0);
        const auto sel = SelectInst::Create(bad, zero, full, "sel", block);
        return sel;
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput, bool Tz>
struct TAnyDateTimeSubIntervalT {
    static_assert(std::is_same<TRight, NUdf::TDataType<NUdf::TInterval>::TLayout>::value, "expected right interval");
    static_assert(std::is_same<TLeft, TOutput>::value, "left and output must be same");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = ToScaledDate<TLeft>(left.template Get<TLeft>());
        const auto rv = ToScaledDate<TRight>(right.template Get<TRight>());
        const auto ret = lv - rv;
        if (IsBadDateTime(ret)) {
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
        const auto lhs = GenToScaledDate<TLeft>(GetterFor<TLeft>(left, context, block), context, block);
        const auto rhs = GenToScaledDate<TRight>(GetterFor<TRight>(right, context, block), context, block);
        const auto sub = BinaryOperator::CreateSub(lhs, rhs, "sub", block);
        const auto wide = SetterFor<TOutput>(GenFromScaledDate<TOutput>(sub, context, block), context, block);
        const auto bad = GenIsBadDateTime(sub, context, block);
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

template <bool LeftTz, bool RightTz>
void RegisterDateSub(IBuiltinFunctionRegistry& registry) {
    using TDateLeft1 = std::conditional_t<LeftTz, NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>>;
    using TDateLeft2 = std::conditional_t<LeftTz, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>>;
    using TDateLeft3 = std::conditional_t<LeftTz, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>>;

    using TDateRight1 = std::conditional_t<RightTz, NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>>;
    using TDateRight2 = std::conditional_t<RightTz, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>>;
    using TDateRight3 = std::conditional_t<RightTz, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>>;

    RegisterFunctionBinOpt<TDateLeft1, TDateRight1,
        NUdf::TDataType<NUdf::TInterval>, TSameDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinOpt<TDateLeft1, TDateRight2,
        NUdf::TDataType<NUdf::TInterval>, TDiffDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinOpt<TDateLeft1, TDateRight3,
        NUdf::TDataType<NUdf::TInterval>, TDiffDateTimeSub, TBinaryArgsOpt>(registry, "Sub");

    RegisterFunctionBinOpt<TDateLeft2, TDateRight1,
        NUdf::TDataType<NUdf::TInterval>, TDiffDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinOpt<TDateLeft2, TDateRight2,
        NUdf::TDataType<NUdf::TInterval>, TSameDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinOpt<TDateLeft2, TDateRight3,
        NUdf::TDataType<NUdf::TInterval>, TDiffDateTimeSub, TBinaryArgsOpt>(registry, "Sub");

    RegisterFunctionBinOpt<TDateLeft3, TDateRight1,
        NUdf::TDataType<NUdf::TInterval>, TDiffDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinOpt<TDateLeft3, TDateRight2,
        NUdf::TDataType<NUdf::TInterval>, TDiffDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
    RegisterFunctionBinOpt<TDateLeft3, TDateRight3,
        NUdf::TDataType<NUdf::TInterval>, TSameDateTimeSub, TBinaryArgsOpt>(registry, "Sub");
}

void RegisterSub(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNumericFunctionOpt<TSub, TBinaryArgsOpt>(registry, "Sub");
    NDecimal::RegisterBinaryFunctionForAllPrecisions<TDecimalSub, TBinaryArgsOpt>(registry, "Sub_");

    RegisterDateSub<false, false>(registry);
    RegisterDateSub<false, true>(registry);
    RegisterDateSub<true, false>(registry);
    RegisterDateSub<true, true>(registry);

    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TIntervalSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");

    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TDate>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TDatetime>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTimestamp>, TAnyDateTimeSubInterval, TBinaryArgsOptWithNullableResult>(registry, "Sub");

    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTzDate>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTzDatetime>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TTzTimestamp>, TAnyDateTimeSubIntervalTz, TBinaryArgsOptWithNullableResult>(registry, "Sub");
}

void RegisterSub(TKernelFamilyMap& kernelFamilyMap) {
    kernelFamilyMap["Sub"] = std::make_unique<TBinaryNumericKernelFamily<TSub>>();
}

} // namespace NMiniKQL
} // namespace NKikimr
