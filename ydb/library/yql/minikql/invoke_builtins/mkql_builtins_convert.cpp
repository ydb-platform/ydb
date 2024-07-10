#include "mkql_builtins_decimal.h" // Y_IGNORE

#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <util/generic/ylimits.h>
#include <util/generic/ymath.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/binary_json/write.h>
#include <ydb/library/binary_json/read.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename TIn, typename TOut>
struct TFloatToIntegralImpl {
    static constexpr TIn MinValue = static_cast<TIn>(std::numeric_limits<TOut>::min());
    static constexpr TIn MaxValue = std::is_same<TIn, double>::value ? MaxFloor<TOut>() : static_cast<TIn>(std::numeric_limits<TOut>::max());

    static NUdf::TUnboxedValuePod Do(TIn val) {
        switch (std::fpclassify(val)) {
        case FP_NORMAL:
            break;
        case FP_ZERO:
        case FP_SUBNORMAL:
            return NUdf::TUnboxedValuePod::Zero();
        default:
            return NUdf::TUnboxedValuePod();
        }

        if (val < MinValue || val > MaxValue) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(static_cast<TOut>(val));
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        auto& module = ctx.Codegen.GetModule();
        const auto val = GetterFor<TIn>(arg, context, block);
        const auto type = Type::getInt32Ty(context);
        const auto fnType = FunctionType::get(type, {val->getType()}, false);
        const auto name = std::is_same<TIn, float>() ? "MyFloatClassify" : "MyDoubleClassify";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(static_cast<int(*)(TIn)>(&std::fpclassify)));
        const auto func = module.getOrInsertFunction(name, fnType).getCallee();
        const auto classify = CallInst::Create(fnType, func, {val}, "fpclassify", block);

        const auto none = BasicBlock::Create(context, "none", ctx.Func);
        const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto result = PHINode::Create(arg->getType(), 3, "result", done);
        result->addIncoming(GetFalse(context), zero);
        result->addIncoming(ConstantInt::get(arg->getType(), 0), none);

        const auto choise = SwitchInst::Create(classify, none, 5, block);
        choise->addCase(ConstantInt::get(type, FP_NAN), none);
        choise->addCase(ConstantInt::get(type, FP_INFINITE), none);
        BranchInst::Create(done, none);

        choise->addCase(ConstantInt::get(type, FP_ZERO), zero);
        choise->addCase(ConstantInt::get(type, FP_SUBNORMAL), zero);
        BranchInst::Create(done, zero);

        choise->addCase(ConstantInt::get(type, FP_NORMAL), good);
        block = good;

        const auto less = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OLT, val, ConstantFP::get(val->getType(), MinValue), "less", block);
        const auto greater = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_OGT, val, ConstantFP::get(val->getType(), MaxValue), "greater", block);
        const auto bad = BinaryOperator::CreateOr(less, greater, "or", block);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        BranchInst::Create(none, make, bad, block);

        block = make;
        const auto cast = StaticCast<TIn, TOut>(val, context, block);
        const auto wide = SetterFor<TOut>(cast, context, block);
        result->addIncoming(wide, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
};

template <typename TIn>
struct TFloatToIntegralImpl<TIn, bool> {
    static NUdf::TUnboxedValuePod Do(TIn val) {
        switch (std::fpclassify(val)) {
        case FP_NORMAL:
        case FP_INFINITE:
            return NUdf::TUnboxedValuePod(true);
            break;
        case FP_ZERO:
        case FP_SUBNORMAL:
            return NUdf::TUnboxedValuePod(false);
        default:
            return NUdf::TUnboxedValuePod();
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        auto& module = ctx.Codegen.GetModule();
        const auto val = GetterFor<TIn>(arg, context, block);
        const auto type = Type::getInt32Ty(context);
        const auto fnType = FunctionType::get(type, {val->getType()}, false);
        const auto name = std::is_same<TIn, float>() ? "MyFloatClassify" : "MyDoubleClassify";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(static_cast<int(*)(TIn)>(&std::fpclassify)));
        const auto func = module.getOrInsertFunction(name, fnType).getCallee();
        const auto classify = CallInst::Create(fnType, func, {val}, "fpclassify", block);

        const auto none = BasicBlock::Create(context, "none", ctx.Func);
        const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto result = PHINode::Create(arg->getType(), 3, "result", done);
        result->addIncoming(GetTrue(context), good);
        result->addIncoming(GetFalse(context), zero);
        result->addIncoming(ConstantInt::get(arg->getType(), 0), none);

        const auto choise = SwitchInst::Create(classify, none, 5, block);
        choise->addCase(ConstantInt::get(type, FP_NAN), none);
        BranchInst::Create(done, none);

        choise->addCase(ConstantInt::get(type, FP_ZERO), zero);
        choise->addCase(ConstantInt::get(type, FP_SUBNORMAL), zero);
        BranchInst::Create(done, zero);

        choise->addCase(ConstantInt::get(type, FP_INFINITE), good);
        choise->addCase(ConstantInt::get(type, FP_NORMAL), good);
        BranchInst::Create(done, good);

        block = done;
        return result;
    }
#endif
};

template <typename TInput, typename TOutput>
struct TFloatToIntegral : public TArithmeticConstraintsUnary<TInput, TOutput> {
    static_assert(std::is_floating_point<TInput>::value, "Input type must be floating point!");
    static_assert(std::is_integral<TOutput>::value, "Output type must be integral!");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg)
    {
        return TFloatToIntegralImpl<TInput, TOutput>::Do(arg.template Get<TInput>());
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return TFloatToIntegralImpl<TInput, TOutput>::Gen(arg, ctx, block);
    }
#endif
};

#ifndef MKQL_DISABLE_CODEGEN
Value* GenInBounds(Value* val, Constant* low, Constant* high, BasicBlock* block) {
    const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, val, high, "lt", block);
    const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, low, "gt", block);
    const auto good = BinaryOperator::CreateAnd(lt, gt, "and", block);
    return good;
}
#endif

template <typename TInput, typename TOutput,
    TOutput MaxVal = std::numeric_limits<TOutput>::max(),
    TOutput MinVal = std::numeric_limits<TOutput>::min()>
struct TWideToShort : public TArithmeticConstraintsUnary<TInput, TOutput> {
    static_assert(std::is_integral_v<TInput>, "Input type must be integral!");
    static_assert(std::is_integral_v<TOutput>, "Output type must be integral!");

    static constexpr auto LowerBound = static_cast<TInput>(MinVal);
    static constexpr auto UpperBound = static_cast<TInput>(MaxVal);

    static constexpr bool SkipLower = std::is_unsigned_v<TInput> ||
        (sizeof(TInput) < sizeof(TOutput) && std::is_signed_v<TOutput>);
    static constexpr bool SkipUpper = sizeof(TInput) < sizeof(TOutput) ||
        (sizeof(TInput) == sizeof(TOutput) && std::is_signed_v<TInput> && std::is_unsigned_v<TOutput> && UpperBound < 0);

    static_assert(!(SkipLower && SkipUpper), "Only for cut input digits!");


    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg)
    {
        const auto val = arg.template Get<TInput>();
        const bool ok = (SkipLower || val >= LowerBound) && (SkipUpper || val <= UpperBound);
        return ok ? NUdf::TUnboxedValuePod(static_cast<TOutput>(val)) : NUdf::TUnboxedValuePod();
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<TInput>(arg, context, block);
        const auto lb = ConstantInt::get(val->getType(), LowerBound);
        const auto ub = ConstantInt::get(val->getType(), UpperBound);
        const auto good = SkipLower ?
            CmpInst::Create(Instruction::ICmp, std::is_signed_v<TInput> ? ICmpInst::ICMP_SLE : ICmpInst::ICMP_ULE, val, ub, "ok", block):
            SkipUpper ?
                CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, lb, "ok", block):
                GenInBounds(val, lb, ub, block);
        const auto full = SetterFor<TOutput>(StaticCast<TInput, TOutput>(val, context, block), context, block);
        const auto res = SelectInst::Create(good, full, ConstantInt::get(arg->getType(), 0), "result", block);
        return res;
    }
#endif
};

template <typename TInput, typename TOutput>
struct TConvert : public TArithmeticConstraintsUnary<TInput, TOutput> {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(static_cast<TOutput>(arg.template Get<TInput>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<TInput>(arg, context, block);
        const auto res = StaticCast<TInput, TOutput>(val, context, block);
        const auto wide = SetterFor<TOutput>(res, context, block);
        return wide;
    }
#endif
};

template <typename TInput, typename TOutput, TOutput Multiplier>
struct TScaleUp : public TArithmeticConstraintsUnary<TInput, TOutput> {
    static_assert(sizeof(TInput) < sizeof(TOutput), "Output should be wider than input.");
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(Multiplier * static_cast<TOutput>(arg.template Get<TInput>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<TInput>(arg, context, block);
        const auto cast = StaticCast<TInput, TOutput>(val, context, block);
        const auto mul = BinaryOperator::CreateMul(ConstantInt::get(cast->getType(), Multiplier), cast, "mul", block);
        const auto wide = SetterFor<TOutput>(mul, context, block);
        return wide;
    }
#endif
};

template <typename TInput, typename TOutput>
struct TDatetimeScale;

template <>
struct TDatetimeScale<ui16, ui32> {
    static constexpr ui32 Modifier = 86400U;
};

template <>
struct TDatetimeScale<ui32, ui64> {
    static constexpr ui64 Modifier = 1000000ULL;
};

template <>
struct TDatetimeScale<ui16, ui64> {
    static constexpr ui64 Modifier = 86400000000ULL;
};

template <typename TInput, typename TOutput>
struct TBigDateScale;

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TDatetime64>> {
    static constexpr i64 Modifier = 86400LL;
};

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime64>> {
    static constexpr i64 Modifier = 86400LL;
};

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime>> {
    static constexpr i64 Modifier = 86400LL;
};

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTimestamp64>> {
    static constexpr i64 Modifier = 1000000LL;
};

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp64>> {
    static constexpr i64 Modifier = 1000000LL;
};

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp>> {
    static constexpr i64 Modifier = 1000000LL;
};

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTimestamp64>> {
    static constexpr i64 Modifier = 86400000000LL;
};

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp64>> {
    static constexpr i64 Modifier = 86400000000LL;
};

template <>
struct TBigDateScale<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp>> {
    static constexpr i64 Modifier = 86400000000LL;
};

template <typename TInput, typename TOutput>
struct TBigDateScaleUp : public TArithmeticConstraintsUnary<typename TInput::TLayout, typename TOutput::TLayout> {
    static_assert(
            sizeof(typename TInput::TLayout) <= sizeof(typename TOutput::TLayout),
            "Output size should be greater or equal than input size.");
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(
                TBigDateScale<TInput, TOutput>::Modifier
                * static_cast<typename TOutput::TLayout>(arg.template Get<typename TInput::TLayout>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<typename TInput::TLayout>(arg, context, block);
        const auto cast = StaticCast<typename TInput::TLayout, typename TOutput::TLayout>(val, context, block);
        const auto mul = BinaryOperator::CreateMul(ConstantInt::get(cast->getType(), TBigDateScale<TInput, TOutput>::Modifier), cast, "mul", block);
        return SetterFor<typename TOutput::TLayout>(mul, context, block);
    }
#endif
};

template <typename TInput, typename TOutput, i64 UpperBound>
struct TBigDateToNarrowScaleUp : public TArithmeticConstraintsUnary<typename TInput::TLayout, typename TOutput::TLayout> {
    static_assert(
            sizeof(typename TInput::TLayout) <= sizeof(typename TOutput::TLayout),
            "Output size should be greater or equal than input size.");
    static_assert(std::is_signed_v<typename TInput::TLayout>, "Expect signed input type");
    static_assert(std::is_unsigned_v<typename TOutput::TLayout>, "Expect unsigned output type");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        auto result = TBigDateScale<TInput, TOutput>::Modifier * arg.template Get<typename TInput::TLayout>();
        if (result < 0 || result > UpperBound) {
            return NUdf::TUnboxedValuePod();
        }
        return NUdf::TUnboxedValuePod(static_cast<typename TOutput::TLayout>(result));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<typename TInput::TLayout>(arg, context, block);
        const auto val64 = StaticCast<typename TInput::TLayout, i64>(val, context, block);
        const auto mul = BinaryOperator::CreateMul(val64, ConstantInt::get(val64->getType(), TBigDateScale<TInput, TOutput>::Modifier), "mul", block);
        const auto cast = StaticCast<i64, typename TOutput::TLayout>(mul, context, block);
        const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, ConstantInt::get(val->getType(), 0), "gt", block);
        const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, mul, ConstantInt::get(mul->getType(), UpperBound), "lt", block);
        const auto good = BinaryOperator::CreateAnd(gt, lt, "and", block);
        const auto full = SetterFor<typename TOutput::TLayout>(cast, context, block);
        return SelectInst::Create(good, full, ConstantInt::get(arg->getType(), 0), "result", block);
    }
#endif
};

template <typename TInput, typename TOutput>
struct TNarrowToBigDateScaleDown : public TArithmeticConstraintsUnary<typename TInput::TLayout, typename TOutput::TLayout> {
    static_assert(
            sizeof(typename TInput::TLayout) >= sizeof(typename TOutput::TLayout),
            "Output size should be smaller or equal than input size.");
    static_assert(std::is_unsigned_v<typename TInput::TLayout>, "Input type must be unsigned.");
    static_assert(std::is_signed_v<typename TOutput::TLayout>, "Output type must be signed.");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(static_cast<typename TOutput::TLayout>(
                    arg.template Get<typename TInput::TLayout>() / TBigDateScale<TOutput, TInput>::Modifier));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<typename TInput::TLayout>(arg, context, block);
        const auto div = BinaryOperator::CreateUDiv(val, ConstantInt::get(val->getType(), TBigDateScale<TOutput, TInput>::Modifier), "div", block);
        const auto cast = StaticCast<typename TInput::TLayout, typename TOutput::TLayout>(div, context, block);
        return SetterFor<typename TOutput::TLayout>(cast, context, block);
    }
#endif
};

template <typename TInput, typename TOutput>
struct TBigDateScaleDown : public TArithmeticConstraintsUnary<typename TInput::TLayout, typename TOutput::TLayout> {
    static_assert(
            sizeof(typename TInput::TLayout) >= sizeof(typename TOutput::TLayout),
            "Output size should be smaller or equal than input size.");
    static_assert(
            std::is_signed_v<typename TInput::TLayout> &&
            std::is_signed_v<typename TOutput::TLayout>,
            "Only for signed layout types.");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(static_cast<typename TOutput::TLayout>(
                    arg.template Get<typename TInput::TLayout>() / TBigDateScale<TOutput, TInput>::Modifier));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<typename TInput::TLayout>(arg, context, block);
        const auto div = BinaryOperator::CreateSDiv(val, ConstantInt::get(val->getType(), TBigDateScale<TOutput, TInput>::Modifier), "div", block);
        const auto cast = StaticCast<typename TInput::TLayout, typename TOutput::TLayout>(div, context, block);
        return SetterFor<typename TOutput::TLayout>(cast, context, block);
    }
#endif
};

template <typename TInput, typename TOutput, i64 UpperBound>
struct TBigDateToNarrowScaleDown : public TArithmeticConstraintsUnary<typename TInput::TLayout, typename TOutput::TLayout> {
    static_assert(
            sizeof(typename TInput::TLayout) > sizeof(typename TOutput::TLayout),
            "Output size should be smaller than input size.");
    static_assert(std::is_same_v<i64, typename TInput::TLayout>, "Expect i64 input type");
    static_assert(std::is_unsigned_v<typename TOutput::TLayout>, "Expect unsigned output type");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        auto inputValue = arg.template Get<typename TInput::TLayout>();
        if (inputValue < 0) {
            return NUdf::TUnboxedValuePod();
        }
        auto result = inputValue / TBigDateScale<TOutput, TInput>::Modifier;
        if (result > UpperBound) {
            return NUdf::TUnboxedValuePod();
        }
        return NUdf::TUnboxedValuePod(static_cast<typename TOutput::TLayout>(result));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<typename TInput::TLayout>(arg, context, block);
        const auto div = BinaryOperator::CreateSDiv(val, ConstantInt::get(val->getType(), TBigDateScale<TOutput, TInput>::Modifier), "div", block);
        const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, ConstantInt::get(val->getType(), 0), "gt", block);
        const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, div, ConstantInt::get(div->getType(), UpperBound), "lt", block);
        const auto good = BinaryOperator::CreateAnd(gt, lt, "and", block);
        const auto cast = StaticCast<typename TInput::TLayout, typename TOutput::TLayout>(div, context, block);
        const auto full = SetterFor<typename TOutput::TLayout>(cast, context, block);
        return SelectInst::Create(good, full, ConstantInt::get(arg->getType(), 0), "result", block);
    }
#endif
};

template <typename TInput, typename TOutput, bool Tz = false>
struct TDatetimeScaleUp : public TArithmeticConstraintsUnary<TInput, TOutput> {
    static_assert(sizeof(TInput) < sizeof(TOutput), "Output size should be wider than input size.");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        auto result = NUdf::TUnboxedValuePod(TDatetimeScale<TInput, TOutput>::Modifier * static_cast<TOutput>(arg.template Get<TInput>()));
        if constexpr (Tz) {
            result.SetTimezoneId(arg.GetTimezoneId());
        }
        return result;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<TInput>(arg, context, block);
        const auto cast = StaticCast<TInput, TOutput>(val, context, block);
        const auto mul = BinaryOperator::CreateMul(ConstantInt::get(cast->getType(), TDatetimeScale<TInput, TOutput>::Modifier), cast, "mul", block);
        const auto wide = SetterFor<TOutput>(mul, context, block);
        if constexpr (Tz) {
            const uint64_t init[] = {0ULL, 0xFFFFULL};
            const auto mask = ConstantInt::get(arg->getType(), APInt(128, 2, init));
            const auto tzid = BinaryOperator::CreateAnd(arg, mask, "tzid",  block);
            const auto full = BinaryOperator::CreateOr(wide, tzid, "full",  block);
            return full;
        } else {
            return wide;
        }
    }
#endif
};

template <typename TInput, typename TOutput, bool Tz = false>
struct TDatetimeScaleDown : public TArithmeticConstraintsUnary<TInput, TOutput> {
    static_assert(sizeof(TInput) > sizeof(TOutput), "Output size should be narrower than input size.");
    static_assert(std::is_unsigned_v<TInput> && std::is_unsigned_v<TOutput>, "Only for unsigned.");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        auto result = NUdf::TUnboxedValuePod(static_cast<TOutput>(arg.template Get<TInput>() / TDatetimeScale<TOutput, TInput>::Modifier));
        if constexpr (Tz) {
            result.SetTimezoneId(arg.GetTimezoneId());
        }
        return result;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterFor<TInput>(arg, context, block);
        const auto div = BinaryOperator::CreateUDiv(val, ConstantInt::get(val->getType(), TDatetimeScale<TOutput, TInput>::Modifier), "div", block);
        const auto cast = StaticCast<TInput, TOutput>(div, context, block);
        const auto wide = SetterFor<TOutput>(cast, context, block);
        if constexpr (Tz) {
            const uint64_t init[] = {0ULL, 0xFFFFULL};
            const auto mask = ConstantInt::get(arg->getType(), APInt(128, 2, init));
            const auto tzid = BinaryOperator::CreateAnd(arg, mask, "tzid",  block);
            const auto full = BinaryOperator::CreateOr(wide, tzid, "full",  block);
            return full;
        } else {
            return wide;
        }
    }
#endif
};

template <typename TInput, typename TOutput, bool Tz>
using TDatetimeRescale = std::conditional_t<sizeof(TInput) < sizeof(TOutput),
    TDatetimeScaleUp<TInput, TOutput, Tz>,
    TDatetimeScaleDown<TInput, TOutput, Tz>
>;

template <bool Cleanup = false>
struct TDatetimeTzStub {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        auto result = arg;
        if (Cleanup) {
            result.SetTimezoneId(0);
        }
        return result;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext&, BasicBlock*& block)
    {
        if constexpr (Cleanup) {
            const uint64_t init[] = {0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFF0000ULL};
            const auto mask = ConstantInt::get(arg->getType(), APInt(128, 2, init));
            return BinaryOperator::CreateAnd(arg, mask, "clean",  block);
        } else {
            return arg;
        }
    }
#endif
};

struct TStringConvert {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg)
    {
        Y_DEBUG_ABORT_UNLESS(!arg.IsBoxed(), "Expected unboxed arg in String::Convert()");
        return arg; // handle optional args as well
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext&, BasicBlock*&)
    {
        return arg;
    }
#endif
};

NUdf::TUnboxedValuePod JsonToJsonDocument(const NUdf::TUnboxedValuePod value) {
    auto binaryJson = NKikimr::NBinaryJson::SerializeToBinaryJson(value.AsStringRef());
    if (!binaryJson.Defined()) {
        // JSON parse error happened, return NULL
        return NUdf::TUnboxedValuePod();
    }
    return MakeString(TStringBuf(binaryJson->Data(), binaryJson->Size()));
}

struct TJsonToJsonDocumentConvert {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg)
    {
        return JsonToJsonDocument(arg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* json, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto functionAddress = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(JsonToJsonDocument));
        const auto functionType = FunctionType::get(json->getType(), {json->getType()}, /* isVarArg */ false);
        const auto functionPtr = CastInst::Create(Instruction::IntToPtr, functionAddress, PointerType::getUnqual(functionType), "func", block);
        return CallInst::Create(functionType, functionPtr, {json}, "jsonToJsonDocument", block);
    }
#endif
};

NUdf::TUnboxedValuePod JsonDocumentToJson(const NUdf::TUnboxedValuePod value) {
    auto json = NKikimr::NBinaryJson::SerializeToJson(value.AsStringRef());
    return MakeString(json);
}

struct TJsonDocumentToJsonConvert {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg)
    {
        return JsonDocumentToJson(arg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* jsonDocument, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto functionAddress = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(JsonDocumentToJson));
        const auto functionType = FunctionType::get(jsonDocument->getType(), {jsonDocument->getType()}, /* isVarArg */ false);
        const auto functionPtr = CastInst::Create(Instruction::IntToPtr, functionAddress, PointerType::getUnqual(functionType), "func", block);
        return CallInst::Create(functionType, functionPtr, {jsonDocument}, "jsonDocumentToJson", block);
    }
#endif
};

}

namespace NDecimal {

template <typename TInput>
struct TConvertFromIntegral {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(NYql::NDecimal::TInt128(arg.Get<TInput>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        const auto val = GetterFor<TInput>(arg, ctx.Codegen.GetContext(), block);
        const auto ext = CastInst::Create(std::is_signed<TInput>() ? Instruction::SExt : Instruction::ZExt, val, arg->getType(), "ext", block);
        return SetterForInt128(ext, block);
    }
#endif
    static_assert(std::is_arithmetic<TInput>::value, "Input type must be arithmetic!");
};

template <typename TOutput>
struct TConvertToIntegral {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        const auto v = arg.GetInt128();
        return v >= std::numeric_limits<TOutput>::min() && v <= std::numeric_limits<TOutput>::max() ?
            NUdf::TUnboxedValuePod(static_cast<TOutput>(arg.GetInt128())) : NUdf::TUnboxedValuePod();
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterForInt128(arg, block);
        const auto cut = CastInst::Create(Instruction::Trunc, val, GetTypeFor<TOutput>(context), "cut", block);
        const auto full = SetterFor<TOutput>(cut, context, block);
        const auto good = GenInBounds<true>(val, GenConstant(std::numeric_limits<TOutput>::min(), context), GenConstant(std::numeric_limits<TOutput>::max(), context), block);
        const auto res = SelectInst::Create(good, full, ConstantInt::get(arg->getType(), 0), "result", block);
        return res;
    }
#endif
    static_assert(std::is_arithmetic<TOutput>::value, "Output type must be arithmetic!");
};

template<typename TOutput, ui8 Scale>
TOutput GetFP(NYql::NDecimal::TInt128 v);

template<>
float GetFP<float, 0>(NYql::NDecimal::TInt128 v) {
    return static_cast<float>(v);
}

template<>
double GetFP<double, 0>(NYql::NDecimal::TInt128 v) {
    return static_cast<double>(v);
}

template<typename TOutput, ui8 Scale>
TOutput GetFP(NYql::NDecimal::TInt128 v) {
    if (v % 10)
        return static_cast<TOutput>(v) / static_cast<TOutput>(NYql::NDecimal::GetDivider<Scale>());
    else
        return GetFP<TOutput, Scale - 1>(v / 10);
}

#ifndef MKQL_DISABLE_CODEGEN
template<typename TOutput, ui8 Scale>
void GenFP(PHINode* result, Value* val, const TCodegenContext& ctx, BasicBlock* done, BasicBlock*& block);

template<>
void GenFP<float, 0>(PHINode* result, Value* val, const TCodegenContext& ctx, BasicBlock* done, BasicBlock*& block) {
    const auto cast = CastInst::Create(Instruction::SIToFP, val, GetTypeFor<float>(ctx.Codegen.GetContext()), "cast", block);
    result->addIncoming(cast, block);
    BranchInst::Create(done, block);
}

template<>
void GenFP<double, 0>(PHINode* result, Value* val, const TCodegenContext& ctx, BasicBlock* done, BasicBlock*& block) {
    const auto cast = CastInst::Create(Instruction::SIToFP, val, GetTypeFor<double>(ctx.Codegen.GetContext()), "cast", block);
    result->addIncoming(cast, block);
    BranchInst::Create(done, block);
}

template<typename TOutput, ui8 Scale>
void GenFP(PHINode* result, Value* val, const TCodegenContext& ctx, BasicBlock* done, BasicBlock*& block) {
    auto& context = ctx.Codegen.GetContext();
    const auto& str = ToString(Scale);
    const auto stop = BasicBlock::Create(context, (TString("stop_") += str).c_str(), ctx.Func);
    const auto step = BasicBlock::Create(context, (TString("step_") += str).c_str(), ctx.Func);

    const auto ten = ConstantInt::get(val->getType(), 10U);

    const auto rem = BinaryOperator::CreateSRem(val, ten, "rem", block);
    const auto nul = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, rem, ConstantInt::get(val->getType(), 0U), "nul", block);

    BranchInst::Create(step, stop, nul, block);

    block = stop;
    const auto cast = CastInst::Create(Instruction::SIToFP, val, GetTypeFor<TOutput>(ctx.Codegen.GetContext()), "cast", block);
    const auto divf = BinaryOperator::CreateFDiv(cast, ConstantFP::get(GetTypeFor<TOutput>(context), static_cast<TOutput>(NYql::NDecimal::GetDivider<Scale>())), "divf", block);
    result->addIncoming(divf, block);
    BranchInst::Create(done, block);

    block = step;

    const auto div = BinaryOperator::CreateSDiv(val, ten, "div", block);
    GenFP<TOutput, Scale - 1>(result, div, ctx, done, block);
}
#endif

template <typename TOutput, ui8 Scale>
struct TToFP {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        const auto v = arg.GetInt128();
        if (v == +NYql::NDecimal::Inf())
            return NUdf::TUnboxedValuePod(+std::numeric_limits<TOutput>::infinity());
        if (v == -NYql::NDecimal::Inf())
            return NUdf::TUnboxedValuePod(-std::numeric_limits<TOutput>::infinity());
        if (v == +NYql::NDecimal::Nan())
            return NUdf::TUnboxedValuePod(+std::numeric_limits<TOutput>::quiet_NaN());
        if (v == -NYql::NDecimal::Nan())
            return NUdf::TUnboxedValuePod(-std::numeric_limits<TOutput>::quiet_NaN());

        return NUdf::TUnboxedValuePod(GetFP<TOutput, Scale>(v));
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();

        const auto val = GetterForInt128(arg, block);

        const auto pnan = BasicBlock::Create(context, "pnan", ctx.Func);
        const auto pinf = BasicBlock::Create(context, "pinf", ctx.Func);
        const auto mnan = BasicBlock::Create(context, "mnan", ctx.Func);
        const auto minf = BasicBlock::Create(context, "minf", ctx.Func);
        const auto norm = BasicBlock::Create(context, "norm", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(GetTypeFor<TOutput>(context), 5U + Scale, "result", done);

        const auto choise = SwitchInst::Create(val, norm, 4U, block);
        choise->addCase(GenConstant(+NYql::NDecimal::Nan(), context), pnan);
        choise->addCase(GenConstant(-NYql::NDecimal::Nan(), context), mnan);
        choise->addCase(GenConstant(+NYql::NDecimal::Inf(), context), pinf);
        choise->addCase(GenConstant(-NYql::NDecimal::Inf(), context), minf);

        block = pnan;
        result->addIncoming(ConstantFP::get(GetTypeFor<TOutput>(context), +std::numeric_limits<TOutput>::quiet_NaN()), block);
        BranchInst::Create(done, block);

        block = mnan;
        result->addIncoming(ConstantFP::get(GetTypeFor<TOutput>(context), -std::numeric_limits<TOutput>::quiet_NaN()), block);
        BranchInst::Create(done, block);

        block = pinf;
        result->addIncoming(ConstantFP::get(GetTypeFor<TOutput>(context), +std::numeric_limits<TOutput>::infinity()), block);
        BranchInst::Create(done, block);

        block = minf;
        result->addIncoming(ConstantFP::get(GetTypeFor<TOutput>(context), -std::numeric_limits<TOutput>::infinity()), block);
        BranchInst::Create(done, block);

        block = norm;
        GenFP<TOutput, Scale>(result, val, ctx, done, block);

        block = done;
        return SetterFor<TOutput>(result, context, block);
    }
#endif
    static_assert(std::is_floating_point<TOutput>::value, "Output type must be floating point!");
    static_assert(Scale <= NYql::NDecimal::MaxPrecision, "Too large scale!");
};

template <ui8 Scale> using TToFloat = TToFP<float, Scale>;
template <ui8 Scale> using TToDouble = TToFP<double, Scale>;

template <ui8 Scale>
struct TScaleUp {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(NYql::NDecimal::Mul(arg.GetInt128(), NYql::NDecimal::GetDivider<Scale>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterForInt128(arg, block);
        const auto mul = BinaryOperator::CreateMul(val, GenConstant(NYql::NDecimal::GetDivider<Scale>(), context), "mul", block);
        const auto res = SelectInst::Create(GenIsNormal(val, context, block), mul, val, "result", block);
        return SetterForInt128(res, block);
    }
#endif
    static_assert(Scale <= NYql::NDecimal::MaxPrecision, "Too large scale!");
};

template <ui8 Scale>
struct TScaleDown {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(NYql::NDecimal::Div(arg.GetInt128(), NYql::NDecimal::GetDivider<Scale>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto val = GetterForInt128(arg, block);
        const auto divider = GenConstant(NYql::NDecimal::GetDivider<Scale>() >> 1, context);

        const auto nul = ConstantInt::get(val->getType(), 0);
        const auto one = ConstantInt::get(val->getType(), 1);

        const auto div = BinaryOperator::CreateSDiv(val, divider, "div", block);
        const auto ashr = BinaryOperator::CreateAShr(div, one, "ashr", block);
        const auto bit = CastInst::Create(Instruction::Trunc, div, Type::getInt1Ty(context), "bit", block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto round = BasicBlock::Create(context, "round", ctx.Func);
        const auto result = PHINode::Create(val->getType(), 2, "result", done);
        result->addIncoming(ashr, block);

        BranchInst::Create(round, done, bit, block);

        block = round;

        const auto mod = BinaryOperator::CreateSRem(val, divider, "mod", block);
        const auto zero = CmpInst::Create(Instruction::ICmp, FCmpInst::ICMP_EQ, mod, nul, "zero", block);
        const auto plus = CmpInst::Create(Instruction::ICmp, FCmpInst::ICMP_SGT, mod, nul, "plus", block);

        const auto test = CastInst::Create(Instruction::Trunc, ashr, Type::getInt1Ty(context), "test", block);
        const auto even = BinaryOperator::CreateAnd(test, zero, "even", block);
        const auto up = BinaryOperator::CreateOr(plus, even, "up", block);

        const auto inc = BinaryOperator::CreateAdd(ashr, one, "inc", block);
        const auto rounded = SelectInst::Create(up, inc, ashr, "result", block);
        result->addIncoming(rounded, block);
        BranchInst::Create(done, block);

        block = done;

        const auto res = SelectInst::Create(GenIsNormal(val, context, block), result, val, "res", block);
        return SetterForInt128(res, block);
    }
#endif
    static_assert(Scale <= NYql::NDecimal::MaxPrecision, "Too large scale!");
};

template <ui8 Precision>
struct TCheckBounds {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        const auto v = arg.GetInt128();

        using namespace NYql::NDecimal;

        if (IsNormal<Precision>(v))
            return arg;

        return NUdf::TUnboxedValuePod(IsNan(v) ? Nan() : (v > 0 ? +Inf() : -Inf()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();

        const auto val = GetterForInt128(arg, block);

        const auto& bounds = GenBounds<Precision>(context);
        const auto good = GenInBounds(val, bounds.first, bounds.second, block);

        const auto nan = GenIsNonComparable(val, context, block);

        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, ConstantInt::get(val->getType(), 0), "plus", block);
        const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);

        const auto bad = SelectInst::Create(nan, GetDecimalNan(context), inf, "bad", block);
        const auto res = SelectInst::Create(good, arg, SetterForInt128(bad, block), "res", block);
        return res;
    }
#endif
    static_assert(Precision <= NYql::NDecimal::MaxPrecision, "Too large precision!");
};

}

namespace {

constexpr auto convert = "Convert";
constexpr auto integral = "ToIntegral";
constexpr auto decimal = "ToDecimal";

template <typename TInput, typename TOutput>
void RegisterConvert(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionUnOpt<TInput, TOutput, TConvert, TUnaryArgsOpt>(registry, convert);
}

template <typename TInput, typename TOutput>
void RegisterStringConvert(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionOpt<TInput, TOutput, TStringConvert, TUnaryArgsOpt>(registry, convert);
}

template <typename TOutput>
void RegisterIntegralCasts(IBuiltinFunctionRegistry& registry) {
    RegisterConvert<NUdf::TDataType<i8>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<i16>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<i32>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<ui32>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<i64>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<ui64>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<bool>, TOutput>(registry);
}

template <typename TInput>
void RegisterDecimalConvertFromIntegral(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionOpt<NUdf::TDataType<TInput>, NUdf::TDataType<NUdf::TDecimal>, NDecimal::TConvertFromIntegral<TInput>, TUnaryArgsOpt>(registry, decimal);
}

template <typename TOutput>
void RegisterDecimalConvertToIntegral(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<NDecimal::TConvertToIntegral<TOutput>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<TOutput>, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<NDecimal::TConvertToIntegral<TOutput>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<TOutput>, true>, TUnaryWrap>(registry, integral);
}

void RegisterDecimalConvert(IBuiltinFunctionRegistry& registry) {
    RegisterDecimalConvertFromIntegral<i8>(registry);
    RegisterDecimalConvertFromIntegral<ui8>(registry);
    RegisterDecimalConvertFromIntegral<i16>(registry);
    RegisterDecimalConvertFromIntegral<ui16>(registry);
    RegisterDecimalConvertFromIntegral<i32>(registry);
    RegisterDecimalConvertFromIntegral<ui32>(registry);
    RegisterDecimalConvertFromIntegral<i64>(registry);
    RegisterDecimalConvertFromIntegral<ui64>(registry);

    RegisterDecimalConvertToIntegral<i8>(registry);
    RegisterDecimalConvertToIntegral<ui8>(registry);
    RegisterDecimalConvertToIntegral<i16>(registry);
    RegisterDecimalConvertToIntegral<ui16>(registry);
    RegisterDecimalConvertToIntegral<i32>(registry);
    RegisterDecimalConvertToIntegral<ui32>(registry);
    RegisterDecimalConvertToIntegral<i64>(registry);
    RegisterDecimalConvertToIntegral<ui64>(registry);

    NDecimal::RegisterUnaryFunctionForAllPrecisions<NDecimal::TScaleUp, TUnaryArgsOpt>(registry, "ScaleUp_");
    NDecimal::RegisterUnaryFunctionForAllPrecisions<NDecimal::TScaleDown, TUnaryArgsOpt>(registry, "ScaleDown_");
    NDecimal::RegisterUnaryFunctionForAllPrecisions<NDecimal::TCheckBounds, TUnaryArgsOpt>(registry, "CheckBounds_");

    NDecimal::RegisterCastFunctionForAllPrecisions<NDecimal::TToFloat, TUnaryArgsOpt, NUdf::TDataType<float>>(registry, "ToFloat_");
    NDecimal::RegisterCastFunctionForAllPrecisions<NDecimal::TToDouble, TUnaryArgsOpt, NUdf::TDataType<double>>(registry, "ToDouble_");
}

template <typename TOutput>
void RegisterRealCasts(IBuiltinFunctionRegistry& registry) {
    RegisterConvert<NUdf::TDataType<float>, TOutput>(registry);
    RegisterConvert<NUdf::TDataType<double>, TOutput>(registry);
}

template <typename TInput, typename TOutput>
void RegisterRealToIntegralCastsImpl(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TFloatToIntegral<typename TInput::TLayout, typename TOutput::TLayout>, TUnaryArgsWithNullableResultOpt<TInput, TOutput, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TFloatToIntegral<typename TInput::TLayout, typename TOutput::TLayout>, TUnaryArgsWithNullableResultOpt<TInput, TOutput, true>, TUnaryWrap>(registry, integral);
}

template <typename TInput>
void RegisterRealToIntegralCasts(IBuiltinFunctionRegistry& registry) {
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<i8>>(registry);
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<ui8>>(registry);
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<i16>>(registry);
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<ui16>>(registry);
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<i32>>(registry);
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<ui32>>(registry);
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<i64>>(registry);
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<ui64>>(registry);
    RegisterRealToIntegralCastsImpl<TInput, NUdf::TDataType<bool>>(registry);
}

template <typename TInput, typename TOutput>
void RegisterWideToShortCastsImpl(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TWideToShort<typename TInput::TLayout, typename TOutput::TLayout>, TUnaryArgsWithNullableResultOpt<TInput, TOutput, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<typename TInput::TLayout, typename TOutput::TLayout>, TUnaryArgsWithNullableResultOpt<TInput, TOutput, true>, TUnaryWrap>(registry, integral);
}

template <typename TInput, typename TOutput,
    typename TInput::TLayout UpperBound = std::numeric_limits<typename TInput::TLayout>::max()>
void RegisterWideToDateCastsImpl(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TWideToShort<typename TInput::TLayout, typename TOutput::TLayout, UpperBound>, TUnaryArgsWithNullableResultOpt<TInput, TOutput, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<typename TInput::TLayout, typename TOutput::TLayout, UpperBound>, TUnaryArgsWithNullableResultOpt<TInput, TOutput, true>, TUnaryWrap>(registry, integral);
}

template <typename TInput, typename TOutput,
    typename TInput::TLayout UpperBound = std::numeric_limits<typename TInput::TLayout>::max(),
    typename TInput::TLayout LowerBound = std::numeric_limits<typename TInput::TLayout>::min()>
void RegisterWideToBigDateCastsImpl(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TWideToShort<typename TInput::TLayout, typename TOutput::TLayout, UpperBound, LowerBound>, TUnaryArgsWithNullableResultOpt<TInput, TOutput, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<typename TInput::TLayout, typename TOutput::TLayout, UpperBound, LowerBound>, TUnaryArgsWithNullableResultOpt<TInput, TOutput, true>, TUnaryWrap>(registry, integral);
}

void RegisterWideToIntervalCasts(IBuiltinFunctionRegistry& registry) {
    constexpr auto TimestampLimit = static_cast<i64>(NUdf::MAX_TIMESTAMP - 1ULL);
    RegisterFunctionImpl<TWideToShort<i64, i64, TimestampLimit, -TimestampLimit>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TInterval>, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<i64, i64, TimestampLimit, -TimestampLimit>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TInterval>, true>, TUnaryWrap>(registry, integral);
    RegisterFunctionImpl<TWideToShort<ui64, i64, TimestampLimit>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TInterval>, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<ui64, i64, TimestampLimit>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TInterval>, true>, TUnaryWrap>(registry, integral);
}

template <typename TInput>
void RegisterWideToUnsignedCasts(IBuiltinFunctionRegistry& registry) {
    RegisterWideToShortCastsImpl<TInput, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<TInput, NUdf::TDataType<ui16>>(registry);
    RegisterWideToShortCastsImpl<TInput, NUdf::TDataType<ui32>>(registry);
    RegisterWideToShortCastsImpl<TInput, NUdf::TDataType<ui64>>(registry);
}

void RegisterWideToDateCasts(IBuiltinFunctionRegistry& registry) {
    RegisterWideToDateCastsImpl<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>(registry);

    RegisterWideToDateCastsImpl<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TTzDate>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TTzDate>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TTzDate>, NUdf::MAX_DATE - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TTzDate>, NUdf::MAX_DATE - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TTzDate>, NUdf::MAX_DATE - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TTzDate>, NUdf::MAX_DATE - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TTzDate>, NUdf::MAX_DATE - 1U>(registry);
}

void RegisterWideToDatetimeCasts(IBuiltinFunctionRegistry& registry) {
    RegisterWideToDateCastsImpl<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TDatetime>, NUdf::MAX_DATETIME - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TDatetime>, NUdf::MAX_DATETIME - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TDatetime>, NUdf::MAX_DATETIME - 1U>(registry);

    RegisterWideToDateCastsImpl<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TTzDatetime>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TTzDatetime>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TTzDatetime>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::MAX_DATETIME - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::MAX_DATETIME - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TTzDatetime>, NUdf::MAX_DATETIME - 1U>(registry);
}

void RegisterWideToBigDateCasts(IBuiltinFunctionRegistry& registry) {
    RegisterWideToBigDateCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TDate32>, NUdf::MAX_DATE32, NUdf::MIN_DATE32>(registry);
    RegisterWideToBigDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TDate32>, NUdf::MAX_DATE32, NUdf::MIN_DATE32>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TDate32>, NUdf::MAX_DATE32>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TDate32>, NUdf::MAX_DATE32>(registry);

    RegisterWideToBigDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TDatetime64>, NUdf::MAX_DATETIME64, NUdf::MIN_DATETIME64>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TDatetime64>, NUdf::MAX_DATETIME64>(registry);

    RegisterWideToBigDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TTimestamp64>, NUdf::MAX_TIMESTAMP64, NUdf::MIN_TIMESTAMP64>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TTimestamp64>, NUdf::MAX_TIMESTAMP64>(registry);

    RegisterWideToBigDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TInterval64>, NUdf::MAX_INTERVAL64, -NUdf::MAX_INTERVAL64>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TInterval64>, NUdf::MAX_INTERVAL64>(registry);
}

void RegisterWideToTimestampCasts(IBuiltinFunctionRegistry& registry) {
    RegisterWideToDateCastsImpl<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TTimestamp>, NUdf::MAX_TIMESTAMP - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TTimestamp>, NUdf::MAX_TIMESTAMP - 1U>(registry);

    RegisterWideToDateCastsImpl<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::MAX_TIMESTAMP - 1U>(registry);
    RegisterWideToDateCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::MAX_TIMESTAMP - 1U>(registry);
}

void RegisterWideToShortIntegralCasts(IBuiltinFunctionRegistry& registry) {
    RegisterWideToUnsignedCasts<NUdf::TDataType<i8>>(registry);
    RegisterWideToUnsignedCasts<NUdf::TDataType<i16>>(registry);
    RegisterWideToUnsignedCasts<NUdf::TDataType<i32>>(registry);
    RegisterWideToUnsignedCasts<NUdf::TDataType<i64>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<ui8>, NUdf::TDataType<i8>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<i16>, NUdf::TDataType<i8>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<ui16>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui16>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui16>, NUdf::TDataType<i16>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<i32>, NUdf::TDataType<i16>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<ui16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui32>, NUdf::TDataType<i32>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<i64>, NUdf::TDataType<i32>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<ui16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<i32>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<ui32>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<ui64>, NUdf::TDataType<i64>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<i16>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<i16>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<ui16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<i32>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<ui16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<i32>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<ui16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<i32>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<ui32>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<ui8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<ui16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<i32>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<ui32>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<i32>>(registry);
    RegisterWideToUnsignedCasts<NUdf::TDataType<NUdf::TInterval>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<i32>>(registry);
    RegisterWideToUnsignedCasts<NUdf::TDataType<NUdf::TDate32>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<i32>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<i64>>(registry);
    RegisterWideToUnsignedCasts<NUdf::TDataType<NUdf::TDatetime64>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<i32>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<i64>>(registry);
    RegisterWideToUnsignedCasts<NUdf::TDataType<NUdf::TTimestamp64>>(registry);

    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<i8>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<i16>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<i32>>(registry);
    RegisterWideToShortCastsImpl<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<i64>>(registry);
    RegisterWideToUnsignedCasts<NUdf::TDataType<NUdf::TInterval64>>(registry);

    RegisterWideToDateCasts(registry);
    RegisterWideToBigDateCasts(registry);
    RegisterWideToDatetimeCasts(registry);
    RegisterWideToTimestampCasts(registry);
    RegisterWideToIntervalCasts(registry);
}

template <typename TDate>
void RegisterFromDateConvert(IBuiltinFunctionRegistry& registry) {
    RegisterConvert<TDate, NUdf::TDataType<i8>>(registry);
    RegisterConvert<TDate, NUdf::TDataType<ui8>>(registry);
    RegisterConvert<TDate, NUdf::TDataType<i16>>(registry);
    RegisterConvert<TDate, NUdf::TDataType<ui16>>(registry);
    RegisterConvert<TDate, NUdf::TDataType<i32>>(registry);
    RegisterConvert<TDate, NUdf::TDataType<ui32>>(registry);
    RegisterConvert<TDate, NUdf::TDataType<i64>>(registry);
    RegisterConvert<TDate, NUdf::TDataType<ui64>>(registry);

    RegisterConvert<TDate, NUdf::TDataType<float>>(registry);
    RegisterConvert<TDate, NUdf::TDataType<double>>(registry);
}

void RegisterToDateConvert(IBuiltinFunctionRegistry& registry) {
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TTzDate>>(registry);

    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TTzDatetime>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TTzDatetime>>(registry);

    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);
    RegisterConvert<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterConvert<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);

    RegisterConvert<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TInterval>>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TInterval>>(registry);
    RegisterConvert<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TInterval>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TInterval>>(registry);
    RegisterConvert<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TInterval>>(registry);
    RegisterConvert<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TInterval>>(registry);

    // Unsafe converts from layout type. Only for internal use.
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterConvert<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterConvert<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterConvert<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TInterval>>(registry);
}

void RegisterToBigDateConvert(IBuiltinFunctionRegistry& registry) {
    RegisterConvert<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TDate32>>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TDate32>>(registry);
    RegisterConvert<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TDate32>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TDate32>>(registry);

    RegisterConvert<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterConvert<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterConvert<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterConvert<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TDatetime64>>(registry);

    RegisterConvert<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterConvert<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterConvert<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterConvert<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);

    RegisterConvert<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TInterval64>>(registry);
    RegisterConvert<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TInterval64>>(registry);
    RegisterConvert<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TInterval64>>(registry);
    RegisterConvert<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TInterval64>>(registry);
    RegisterConvert<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TInterval64>>(registry);
    RegisterConvert<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TInterval64>>(registry);
}

template <typename TInput, typename TOutput>
void RegisterBigDateScaleUp(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TBigDateScaleUp<TInput, TOutput>, TUnaryArgsOpt<TInput, TOutput, false>, TUnaryStub>(registry, convert);
    RegisterFunctionImpl<TBigDateScaleUp<TInput, TOutput>, TUnaryArgsOpt<TInput, TOutput, true>, TUnaryWrap>(registry, convert);
}

template <typename TInput, typename TOutput>
void RegisterBigDateScaleDown(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TBigDateScaleDown<TInput, TOutput>, TUnaryArgsOpt<TInput, TOutput, false>, TUnaryStub>(registry, convert);
    RegisterFunctionImpl<TBigDateScaleDown<TInput, TOutput>, TUnaryArgsOpt<TInput, TOutput, true>, TUnaryWrap>(registry, convert);
}

void RegisterBigDateRescale(IBuiltinFunctionRegistry& registry) {
    RegisterBigDateScaleUp<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterBigDateScaleUp<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterBigDateScaleUp<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);

    RegisterBigDateScaleDown<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDate32>>(registry);
    RegisterBigDateScaleDown<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDate32>>(registry);
    RegisterBigDateScaleDown<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
}

void RegisterNarrowToBigDateCasts(IBuiltinFunctionRegistry& registry) {
    RegisterConvert<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval64>>(registry);
    RegisterConvert<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterConvert<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterConvert<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TDate32>>(registry);

    RegisterBigDateScaleUp<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterBigDateScaleUp<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterBigDateScaleUp<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTimestamp64>>(registry);

    RegisterFunctionImpl<TNarrowToBigDateScaleDown<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDate32>>, TUnaryArgsOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDate32>, false>, TUnaryStub>(registry, convert);
    RegisterFunctionImpl<TNarrowToBigDateScaleDown<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDate32>>, TUnaryArgsOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDate32>, true>, TUnaryWrap>(registry, convert);

    RegisterFunctionImpl<TNarrowToBigDateScaleDown<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDate32>>, TUnaryArgsOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDate32>, false>, TUnaryStub>(registry, convert);
    RegisterFunctionImpl<TNarrowToBigDateScaleDown<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDate32>>, TUnaryArgsOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDate32>, true>, TUnaryWrap>(registry, convert);

    RegisterFunctionImpl<TNarrowToBigDateScaleDown<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDatetime64>>, TUnaryArgsOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDatetime64>, false>, TUnaryStub>(registry, convert);
    RegisterFunctionImpl<TNarrowToBigDateScaleDown<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDatetime64>>, TUnaryArgsOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDatetime64>, true>, TUnaryWrap>(registry, convert);
}

void RegisterBigDateToNarrowCasts(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TWideToShort<i32, ui16, NYql::NUdf::MAX_DATE - 1U>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDate>, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<i32, ui16, NYql::NUdf::MAX_DATE - 1U>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDate>, true>, TUnaryWrap>(registry, integral);

    RegisterFunctionImpl<TWideToShort<i64, ui32, NYql::NUdf::MAX_DATETIME - 1U>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDatetime>, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<i64, ui32, NYql::NUdf::MAX_DATETIME - 1U>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDatetime>, true>, TUnaryWrap>(registry, integral);

    RegisterFunctionImpl<TWideToShort<i64, ui64, NYql::NUdf::MAX_TIMESTAMP - 1ULL>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTimestamp>, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<i64, ui64, NYql::NUdf::MAX_TIMESTAMP - 1ULL>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTimestamp>, true>, TUnaryWrap>(registry, integral);

    constexpr auto TimestampLimit = static_cast<i64>(NUdf::MAX_TIMESTAMP - 1ULL);
    RegisterFunctionImpl<TWideToShort<i64, i64, TimestampLimit, -TimestampLimit>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>, false>, TUnaryStub>(registry, integral);
    RegisterFunctionImpl<TWideToShort<i64, i64, TimestampLimit, -TimestampLimit>, TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>, true>, TUnaryWrap>(registry, integral);

    RegisterFunctionImpl<
        TBigDateToNarrowScaleUp<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime>, NUdf::MAX_DATETIME - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime>, false>,
        TUnaryStub>(registry, integral);
    RegisterFunctionImpl<
        TBigDateToNarrowScaleUp<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime>, NUdf::MAX_DATETIME - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime>, true>,
        TUnaryWrap>(registry, integral);

    RegisterFunctionImpl<
        TBigDateToNarrowScaleUp<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp>, NUdf::MAX_TIMESTAMP - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp>, false>,
        TUnaryStub>(registry, integral);
    RegisterFunctionImpl<
        TBigDateToNarrowScaleUp<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp>, NUdf::MAX_TIMESTAMP - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp>, true>,
        TUnaryWrap>(registry, integral);

    RegisterFunctionImpl<
        TBigDateToNarrowScaleUp<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp>, NUdf::MAX_TIMESTAMP - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp>, false>,
        TUnaryStub>(registry, integral);
    RegisterFunctionImpl<
        TBigDateToNarrowScaleUp<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp>, NUdf::MAX_TIMESTAMP - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp>, true>,
        TUnaryWrap>(registry, integral);

    RegisterFunctionImpl<
        TBigDateToNarrowScaleDown<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDate>, false>,
        TUnaryStub>(registry, integral);
    RegisterFunctionImpl<
        TBigDateToNarrowScaleDown<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDate>, true>,
        TUnaryWrap>(registry, integral);

    RegisterFunctionImpl<
        TBigDateToNarrowScaleDown<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDate>, false>,
        TUnaryStub>(registry, integral);
    RegisterFunctionImpl<
        TBigDateToNarrowScaleDown<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDate>, NUdf::MAX_DATE - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDate>, true>,
        TUnaryWrap>(registry, integral);

    RegisterFunctionImpl<
        TBigDateToNarrowScaleDown<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDatetime>, NUdf::MAX_DATETIME - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDatetime>, false>,
        TUnaryStub>(registry, integral);
    RegisterFunctionImpl<
        TBigDateToNarrowScaleDown<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDatetime>, NUdf::MAX_DATETIME - 1U>,
        TUnaryArgsWithNullableResultOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDatetime>, true>,
        TUnaryWrap>(registry, integral);
}

template <typename TInput, typename TOutput, bool Tz = false>
void RegisterRescaleOpt(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TDatetimeRescale<typename TInput::TLayout, typename TOutput::TLayout, Tz>, TUnaryArgsOpt<TInput, TOutput, false>, TUnaryStub>(registry, convert);
    RegisterFunctionImpl<TDatetimeRescale<typename TInput::TLayout, typename TOutput::TLayout, Tz>, TUnaryArgsOpt<TInput, TOutput, true>, TUnaryWrap>(registry, convert);
}

void RegisterDatetimeRescale(IBuiltinFunctionRegistry& registry) {
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzDatetime>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);

    RegisterRescaleOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzDate>>(registry);

    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzDate>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzDatetime>>(registry);

    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTzDatetime>, true>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTzTimestamp>, true>(registry);

    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTzDate>, true>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTzTimestamp>, true>(registry);

    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTzDate>, true>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterRescaleOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTzDatetime>, true>(registry);
}

template <typename TInput, typename TOutput, bool Clenup = false>
void RegisterTzDateimeOpt(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionImpl<TDatetimeTzStub<Clenup>, TUnaryArgsOpt<TInput, TOutput, false>, TUnaryStub>(registry, convert);
    RegisterFunctionImpl<TDatetimeTzStub<Clenup>, TUnaryArgsOpt<TInput, TOutput, true>, TUnaryWrap>(registry, convert);
}

void RegisterTzDateimeConvert(IBuiltinFunctionRegistry& registry) {
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzDate>>(registry);
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzDatetime>>(registry);
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzTimestamp>>(registry);

    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>, true>(registry);
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>, true>(registry);
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>, true>(registry);

    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTzDate32>>(registry);
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTzDatetime64>>(registry);
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTzTimestamp64>>(registry);

    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TDate32>, true>(registry);
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TDatetime64>, true>(registry);
    RegisterTzDateimeOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTimestamp64>, true>(registry);
}

void RegisterJsonDocumentConvert(IBuiltinFunctionRegistry& registry) {
    // String/Utf8 -> JsonDocument and JsonDocument -> String/Utf8 conversions. TStringConvert is used as a placeholder because
    // actual conversions are handled by ValueFromString and ValueToString in mkql_type_ops.cpp
    RegisterFunctionOpt<NUdf::TDataType<char*>, NUdf::TDataType<NUdf::TJsonDocument>, TStringConvert, TUnaryArgsOpt>(registry, convert);
    RegisterFunctionOpt<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<NUdf::TJsonDocument>, TStringConvert, TUnaryArgsOpt>(registry, convert);
    RegisterFunctionOpt<NUdf::TDataType<NUdf::TJsonDocument>, NUdf::TDataType<char*>, TStringConvert, TUnaryArgsOpt>(registry, convert);
    RegisterFunctionOpt<NUdf::TDataType<NUdf::TJsonDocument>, NUdf::TDataType<NUdf::TUtf8>, TStringConvert, TUnaryArgsOpt>(registry, convert);

    // Json -> JsonDocument and JsonDocument -> Json conversions
    RegisterFunctionOpt<NUdf::TDataType<NUdf::TJson>, NUdf::TDataType<NUdf::TJsonDocument>, TJsonToJsonDocumentConvert, TUnaryArgsOpt>(registry, convert);
    RegisterFunctionOpt<NUdf::TDataType<NUdf::TJsonDocument>, NUdf::TDataType<NUdf::TJson>, TJsonDocumentToJsonConvert, TUnaryArgsOpt>(registry, convert);
}

}

void RegisterConvert(IBuiltinFunctionRegistry& registry) {
    RegisterIntegralCasts<NUdf::TDataType<i32>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<ui32>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<i64>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<ui64>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<ui8>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<i8>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<ui16>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<i16>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<bool>>(registry);

    RegisterIntegralCasts<NUdf::TDataType<float>>(registry);
    RegisterIntegralCasts<NUdf::TDataType<double>>(registry);

    RegisterWideToShortIntegralCasts(registry);

    RegisterRealCasts<NUdf::TDataType<i8>>(registry);
    RegisterRealCasts<NUdf::TDataType<ui8>>(registry);
    RegisterRealCasts<NUdf::TDataType<i16>>(registry);
    RegisterRealCasts<NUdf::TDataType<ui16>>(registry);
    RegisterRealCasts<NUdf::TDataType<i32>>(registry);
    RegisterRealCasts<NUdf::TDataType<ui32>>(registry);
    RegisterRealCasts<NUdf::TDataType<i64>>(registry);
    RegisterRealCasts<NUdf::TDataType<ui64>>(registry);

    RegisterRealCasts<NUdf::TDataType<float>>(registry);
    RegisterRealCasts<NUdf::TDataType<double>>(registry);

    RegisterRealToIntegralCasts<NUdf::TDataType<float>>(registry);
    RegisterRealToIntegralCasts<NUdf::TDataType<double>>(registry);

    RegisterStringConvert<NUdf::TDataType<char*>, NUdf::TDataType<char*>>(registry);
    RegisterStringConvert<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<char*>>(registry);
    RegisterStringConvert<NUdf::TDataType<NUdf::TYson>, NUdf::TDataType<char*>>(registry);
    RegisterStringConvert<NUdf::TDataType<NUdf::TJson>, NUdf::TDataType<char*>>(registry);
    RegisterStringConvert<NUdf::TDataType<NUdf::TJson>, NUdf::TDataType<NUdf::TUtf8>>(registry);

    RegisterFromDateConvert<NUdf::TDataType<NUdf::TDate>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TDatetime>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TTimestamp>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TInterval>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TTzDate>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TTzDatetime>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TTzTimestamp>>(registry);

    RegisterFromDateConvert<NUdf::TDataType<NUdf::TDate32>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TDatetime64>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TTimestamp64>>(registry);
    RegisterFromDateConvert<NUdf::TDataType<NUdf::TInterval64>>(registry);

    RegisterTzDateimeConvert(registry);
    RegisterDatetimeRescale(registry);
    RegisterToDateConvert(registry);
    RegisterToBigDateConvert(registry);
    RegisterBigDateRescale(registry);
    RegisterNarrowToBigDateCasts(registry);
    RegisterBigDateToNarrowCasts(registry);

    RegisterDecimalConvert(registry);

    RegisterJsonDocumentConvert(registry);
}

} // namespace NMiniKQL
} // namespace NKikimr
