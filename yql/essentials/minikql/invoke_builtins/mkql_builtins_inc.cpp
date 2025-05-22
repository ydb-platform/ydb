#include "mkql_builtins_decimal.h" // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TIncrement : public TSimpleArithmeticUnary<TInput, TOutput, TIncrement<TInput, TOutput>> {
    static TOutput Do(TInput val)
    {
        return ++val;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext&, BasicBlock*& block)
    {
        return std::is_integral<TOutput>() ?
            BinaryOperator::CreateAdd(arg, ConstantInt::get(arg->getType(), 1), "inc", block):
            BinaryOperator::CreateFAdd(arg, ConstantFP::get(arg->getType(), 1.0), "inc", block);
    }
#endif
};

template <ui8 Precision>
struct TDecimalInc {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        auto v = arg.GetInt128();

        using namespace NYql::NDecimal;

        const auto& bounds = GetBounds<Precision, false, true>();

        if (v > bounds.first && v < bounds.second)
            return NUdf::TUnboxedValuePod(++v);

        return NUdf::TUnboxedValuePod(IsNan(v) ? Nan() : (v > 0 ? +Inf() : -Inf()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto& bounds = NDecimal::GenBounds<Precision, false, true>(context);

        const auto val = GetterForInt128(arg, block);
        const auto add = BinaryOperator::CreateAdd(val, ConstantInt::get(val->getType(), 1), "add", block);

        const auto gt = CmpInst::Create(Instruction::ICmp, FCmpInst::ICMP_SGT, val, bounds.first, "gt", block);
        const auto lt = CmpInst::Create(Instruction::ICmp, FCmpInst::ICMP_SLT, add, bounds.second, "lt", block);

        const auto good = BinaryOperator::CreateAnd(lt, gt, "and", block);
        const auto nan = NDecimal::GenIsNonComparable(val, context, block);
        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, ConstantInt::get(val->getType(), 0), "plus", block);

        const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);
        const auto bad = SelectInst::Create(nan, GetDecimalNan(context), inf, "bad", block);
        const auto inc = SelectInst::Create(good, add, bad, "inc", block);
        return SetterForInt128(inc, block);
    }
#endif
    static_assert(Precision <= NYql::NDecimal::MaxPrecision, "Too large precision!");
};

}

void RegisterIncrement(IBuiltinFunctionRegistry& registry) {
    RegisterUnaryNumericFunctionOpt<TIncrement, TUnaryArgsOpt>(registry, "Increment");
    NDecimal::RegisterUnaryFunctionForAllPrecisions<TDecimalInc, TUnaryArgsOpt>(registry, "Inc_");
}

} // namespace NMiniKQL
} // namespace NKikimr
