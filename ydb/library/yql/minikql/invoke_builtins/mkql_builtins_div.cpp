#include "mkql_builtins_impl.h"  // Y_IGNORE
#include "mkql_builtins_datetime.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TDiv : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TDiv<TLeft, TRight, TOutput>> {
    static_assert(std::is_floating_point<TOutput>::value, "expected floating point");

    static constexpr auto NullMode = TKernel::ENullMode::Default;

    static TOutput Do(TOutput left, TOutput right)
    {
        return left / right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return BinaryOperator::CreateFDiv(left, right, "div", block);
    }
#endif
};

template <typename TLeft, typename TRight, typename TOutput>
struct TIntegralDiv {
    static_assert(std::is_integral<TOutput>::value, "integral type expected");

    static constexpr auto NullMode = TKernel::ENullMode::AlwaysNull;

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = static_cast<TOutput>(left.template Get<TLeft>());
        const auto rv = static_cast<TOutput>(right.template Get<TRight>());

        if (rv == 0 ||
            (std::is_signed<TOutput>::value && sizeof(TOutput) <= sizeof(TLeft) && rv == TOutput(-1) && lv == Min<TOutput>()))
        {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(lv / rv);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lv = StaticCast<TLeft, TOutput>(GetterFor<TLeft>(left, context, block), context, block);
        const auto rv = StaticCast<TRight, TOutput>(GetterFor<TRight>(right, context, block), context, block);
        const auto type = Type::getInt128Ty(context);
        const auto zero = ConstantInt::get(type, 0);
        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, rv, ConstantInt::get(rv->getType(), 0), "check", block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto result = PHINode::Create(type, 2, "result", done);
        result->addIncoming(zero, block);

        if constexpr (std::is_signed<TOutput>() && sizeof(TOutput) <= sizeof(TLeft)) {
            const auto min = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, lv, ConstantInt::get(lv->getType(), Min<TOutput>()), "min", block);
            const auto one = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, rv, ConstantInt::get(rv->getType(), -1), "one", block);
            const auto two = BinaryOperator::CreateAnd(min, one, "two", block);
            const auto all = BinaryOperator::CreateOr(check, two, "all", block);
            BranchInst::Create(done, good, all, block);
        } else {
            BranchInst::Create(done, good, check, block);
        }

        block = good;
        const auto div = std::is_signed<TOutput>() ? BinaryOperator::CreateSDiv(lv, rv, "div", block) : BinaryOperator::CreateUDiv(lv, rv, "div", block);
        const auto full = SetterFor<TOutput>(div, context, block);
        result->addIncoming(full, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
};

template <typename TLeft, typename TRight, typename TOutput>
struct TNumDivInterval {
    static_assert(TLeft::Features & NYql::NUdf::TimeIntervalType, "Left must be interval type");
    static_assert(TRight::Features & NYql::NUdf::IntegralType, "Right must be integral type");
    static_assert(TOutput::Features & NYql::NUdf::TimeIntervalType, "Output must be interval type");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = static_cast<typename TOutput::TLayout>(left.template Get<typename TLeft::TLayout>());
        const auto rv = static_cast<typename TOutput::TLayout>(right.template Get<typename TRight::TLayout>());

        if (rv == 0) {
            return NUdf::TUnboxedValuePod();
        }

        const auto ret = lv / rv;
        return IsBadInterval<TOutput>(ret) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(ret);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lv = StaticCast<typename TLeft::TLayout, typename TOutput::TLayout>(
                GetterFor<typename TLeft::TLayout>(left, context, block), context, block);
        const auto rv = StaticCast<typename TRight::TLayout, typename TOutput::TLayout>(
                GetterFor<typename TRight::TLayout>(right, context, block), context, block);
        const auto type = Type::getInt128Ty(context);
        const auto zero = ConstantInt::get(type, 0);
        const auto check = CmpInst::Create(
                Instruction::ICmp, ICmpInst::ICMP_EQ, rv, ConstantInt::get(rv->getType(), 0), "check", block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto result = PHINode::Create(type, 2, "result", done);
        result->addIncoming(zero, block);

        BranchInst::Create(done, good, check, block);

        block = good;
        const auto div = BinaryOperator::CreateSDiv(lv, rv, "div", block);
        const auto full = SetterFor<typename TOutput::TLayout>(div, context, block);
        const auto bad = GenIsBadInterval<TOutput>(div, context, block);
        const auto sel = SelectInst::Create(bad, zero, full, "sel", block);
        result->addIncoming(sel, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
};

}

template <typename TInterval>
void RegisterIntegralDiv(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<ui8>,
        TInterval, TNumDivInterval, TBinaryArgsOptWithNullableResult>(registry, "Div");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<i8>,
        TInterval, TNumDivInterval, TBinaryArgsOptWithNullableResult>(registry, "Div");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<ui16>,
        TInterval, TNumDivInterval, TBinaryArgsOptWithNullableResult>(registry, "Div");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<i16>,
        TInterval, TNumDivInterval, TBinaryArgsOptWithNullableResult>(registry, "Div");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<ui32>,
        TInterval, TNumDivInterval, TBinaryArgsOptWithNullableResult>(registry, "Div");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<i32>,
        TInterval, TNumDivInterval, TBinaryArgsOptWithNullableResult>(registry, "Div");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<ui64>,
        TInterval, TNumDivInterval, TBinaryArgsOptWithNullableResult>(registry, "Div");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<i64>,
        TInterval, TNumDivInterval, TBinaryArgsOptWithNullableResult>(registry, "Div");
}

void RegisterDiv(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryRealFunctionOpt<TDiv, TBinaryArgsOpt>(registry, "Div");
    RegisterBinaryIntegralFunctionOpt<TIntegralDiv, TBinaryArgsOptWithNullableResult>(registry, "Div");

    RegisterIntegralDiv<NUdf::TDataType<NUdf::TInterval>>(registry);
    RegisterIntegralDiv<NUdf::TDataType<NUdf::TInterval64>>(registry);
}

void RegisterDiv(TKernelFamilyMap& kernelFamilyMap) {
    kernelFamilyMap["Div"] = std::make_unique<TBinaryNumericKernelFamily<TIntegralDiv, TDiv>>();
}

} // namespace NMiniKQL
} // namespace NKikimr
