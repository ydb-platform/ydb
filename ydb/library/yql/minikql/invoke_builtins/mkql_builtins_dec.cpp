#include "mkql_builtins_decimal.h" // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TDecrement : public TSimpleArithmeticUnary<TInput, TOutput, TDecrement<TInput, TOutput>> {
    static TOutput Do(TInput val)
    {
        return --val;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext&, BasicBlock*& block)
    {
        return std::is_integral<TOutput>() ?
            BinaryOperator::CreateSub(arg, ConstantInt::get(arg->getType(), 1), "dec", block):
            BinaryOperator::CreateFSub(arg, ConstantFP::get(arg->getType(), 1.0), "dec", block);
    }
#endif
};

template <ui8 Precision>
struct TDecimalDec {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        auto v = arg.GetInt128();

        using namespace NYql::NDecimal;

        const auto& bounds = GetBounds<Precision, true, false>();

        if (v > bounds.first && v < bounds.second)
            return NUdf::TUnboxedValuePod(--v);

        return NUdf::TUnboxedValuePod(IsNan(v) ? Nan() : (v > 0 ? +Inf() : -Inf()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto& bounds = NDecimal::GenBounds<Precision, true, false>(context);

        const auto val = GetterForInt128(arg, block);
        const auto sub = BinaryOperator::CreateSub(val, ConstantInt::get(val->getType(), 1), "sub", block);

        const auto gt = CmpInst::Create(Instruction::ICmp, FCmpInst::ICMP_SGT, sub, bounds.first, "gt", block);
        const auto lt = CmpInst::Create(Instruction::ICmp, FCmpInst::ICMP_SLT, val, bounds.second, "lt", block);

        const auto good = BinaryOperator::CreateAnd(lt, gt, "and", block);
        const auto nan = NDecimal::GenIsNonComparable(val, context, block);
        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, ConstantInt::get(val->getType(), 0), "plus", block);

        const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);
        const auto bad = SelectInst::Create(nan, GetDecimalNan(context), inf, "bad", block);
        const auto dec = SelectInst::Create(good, sub, bad, "dec", block);
        return SetterForInt128(dec, block);
    }
#endif
    static_assert(Precision <= NYql::NDecimal::MaxPrecision, "Too large precision!");
};

}

void RegisterDecrement(IBuiltinFunctionRegistry& registry) {
    RegisterUnaryNumericFunctionOpt<TDecrement, TUnaryArgsOpt>(registry, "Decrement");
    NDecimal::RegisterUnaryFunctionForAllPrecisions<TDecimalDec, TUnaryArgsOpt>(registry, "Dec_");
}

} // namespace NMiniKQL
} // namespace NKikimr
