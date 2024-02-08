#include "mkql_builtins_impl.h"  // Y_IGNORE

#include <cmath>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TMod : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TMod<TLeft, TRight, TOutput>> {
    static_assert(std::is_floating_point<TOutput>::value, "expected floating point");

    static constexpr auto NullMode = TKernel::ENullMode::Default;

    static TOutput Do(TOutput left, TOutput right)
    {
        return std::fmod(left, right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        ctx.Codegen.AddGlobalMapping("fmod", reinterpret_cast<const void*>(static_cast<double(*)(double, double)>(&std::fmod)));
        ctx.Codegen.AddGlobalMapping("fmodf", reinterpret_cast<const void*>(static_cast<float(*)(float, float)>(&std::fmod)));
        return BinaryOperator::CreateFRem(left, right, "frem", block);
    }
#endif
};

template <typename TLeft, typename TRight, typename TOutput>
struct TIntegralMod {
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

        return NUdf::TUnboxedValuePod(lv % rv);
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
        const auto rem = std::is_signed<TOutput>() ? BinaryOperator::CreateSRem(lv, rv, "rem", block) : BinaryOperator::CreateURem(lv, rv, "rem", block);
        const auto full = SetterFor<TOutput>(rem, context, block);
        result->addIncoming(full, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
};

}

void RegisterMod(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryRealFunctionOpt<TMod, TBinaryArgsOpt>(registry, "Mod");
    RegisterBinaryIntegralFunctionOpt<TIntegralMod, TBinaryArgsOptWithNullableResult>(registry, "Mod");
}

void RegisterMod(TKernelFamilyMap& kernelFamilyMap) {
    kernelFamilyMap["Mod"] = std::make_unique<TBinaryNumericKernelFamily<TIntegralMod, TMod>>();
}

} // namespace NMiniKQL
} // namespace NKikimr
