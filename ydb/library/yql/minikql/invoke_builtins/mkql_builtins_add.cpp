#include "mkql_builtins_impl.h"
#include "mkql_builtins_datetime.h"
#include "mkql_builtins_decimal.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TAdd : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TAdd<TLeft, TRight, TOutput>> {
    static constexpr bool DefaultNulls = true;

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
    static_assert(std::is_integral<TLeft>::value, "left must be integral");
    static_assert(std::is_integral<TRight>::value, "right must be integral");
    static_assert(std::is_integral<TOutput>::value, "output must be integral");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = ToScaledDate<TLeft>(left.template Get<TLeft>());
        const auto rv = ToScaledDate<TRight>(right.template Get<TRight>());
        const auto ret = lv + rv;
        if (std::is_same<TOutput, NUdf::TDataType<NUdf::TInterval>::TLayout>() ? IsBadInterval(ret) : IsBadDateTime(ret)) {
            return NUdf::TUnboxedValuePod();
        }

        auto data = NUdf::TUnboxedValuePod(FromScaledDate<TOutput>(ret));
        if constexpr (Tz) {
            data.SetTimezoneId((std::is_same<TLeft, NUdf::TDataType<NUdf::TInterval>::TLayout>() ? right : left).GetTimezoneId());
        }
        return data;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GenToScaledDate<TLeft>(GetterFor<TLeft>(left, context, block), context, block);
        const auto rhs = GenToScaledDate<TRight>(GetterFor<TRight>(right, context, block), context, block);
        const auto add = BinaryOperator::CreateAdd(lhs, rhs, "add", block);
        const auto wide = SetterFor<TOutput>(GenFromScaledDate<TOutput>(add, context, block), context, block);
        const auto bad = std::is_same<TOutput, NUdf::TDataType<NUdf::TInterval>::TLayout>() ?
            GenIsBadInterval(add, context, block):
            GenIsBadDateTime(add, context, block);
        const auto type = Type::getInt128Ty(context);
        const auto zero = ConstantInt::get(type, 0);

        if constexpr (Tz) {
            const uint64_t init[] = {0ULL, 0xFFFFULL};
            const auto mask = ConstantInt::get(type, APInt(128, 2, init));
            const auto tzid = BinaryOperator::CreateAnd(std::is_same<TLeft, NUdf::TDataType<NUdf::TInterval>::TLayout>() ? right : left, mask, "tzid",  block);
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
using TDateTimeAdd = TDateTimeAddT<TLeft, TRight, TOutput, false>;

template<typename TLeft, typename TRight, typename TOutput>
using TDateTimeAddTz = TDateTimeAddT<TLeft, TRight, TOutput, true>;

template <bool Tz, template<typename, typename, typename> class TAdder>
void RegisterAddDateAndInterval(IBuiltinFunctionRegistry& registry) {
    using TDate1 = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>>;
    using TDate2 = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>>;
    using TDate3 = std::conditional_t<Tz, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>>;

    RegisterFunctionBinOpt<TDate1, NUdf::TDataType<NUdf::TInterval>,
        TDate1, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinOpt<TDate2, NUdf::TDataType<NUdf::TInterval>,
        TDate2, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinOpt<TDate3, NUdf::TDataType<NUdf::TInterval>,
        TDate3, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");

    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, TDate1,
        TDate1, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, TDate2,
        TDate2, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, TDate3,
        TDate3, TAdder, TBinaryArgsOptWithNullableResult>(registry, "Add");
}

template<typename TType>
using TIntervalAggrAdd = TDateTimeAdd<TType, TType, TType>;

}

void RegisterAdd(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNumericFunctionOpt<TAdd, TBinaryArgsOpt>(registry, "Add");
    NDecimal::RegisterBinaryFunctionForAllPrecisions<TDecimalAdd, TBinaryArgsOpt>(registry, "Add_");

    RegisterAddDateAndInterval<false, TDateTimeAdd>(registry);
    RegisterAddDateAndInterval<true, TDateTimeAddTz>(registry);

    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TDateTimeAdd, TBinaryArgsOptWithNullableResult>(registry, "Add");
}

void RegisterAdd(TKernelFamilyMap& kernelFamilyMap) {
    kernelFamilyMap["Add"] = std::make_unique<TBinaryNumericKernelFamily<TAdd>>();
}

void RegisterAggrAdd(IBuiltinFunctionRegistry& registry) {
    RegisterNumericAggregateFunction<TAggrAdd, TBinaryArgsSameOpt>(registry, "AggrAdd");
    RegisterAggregateFunction<NUdf::TDataType<NUdf::TInterval>, TIntervalAggrAdd, TBinaryArgsSameOptArgsWithNullableResult>(registry, "AggrAdd");
    NDecimal::RegisterAggregateFunctionForAllPrecisions<TDecimalAdd, TBinaryArgsSameOpt>(registry, "AggrAdd_");
}

} // namespace NMiniKQL
} // namespace NKikimr
