#include "mkql_builtins_decimal.h" // Y_IGNORE
#include "mkql_builtins_compare.h"

#include <cmath>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline T Min(T l, T r) {
    return std::min(l, r);
}

template <typename T, std::enable_if_t<std::is_floating_point<T>::value>* = nullptr>
inline T Min(T l, T r) {
    return std::fmin(l, r);
}

template<typename TLeft, typename TRight, typename TOutput>
struct TMin : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TMin<TLeft, TRight, TOutput>> {
    static TOutput Do(TLeft left, TRight right)
    {
        return Min<TOutput>(left, right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        if constexpr (std::is_floating_point<TOutput>()) {
            auto& context = ctx.Codegen.GetContext();
            auto& module = ctx.Codegen.GetModule();
            const auto fnType = FunctionType::get(GetTypeFor<TOutput>(context), {left->getType(), right->getType()}, false);
            const auto& name = GetFuncNameForType<TOutput>("llvm.minnum");
            const auto func = module.getOrInsertFunction(name, fnType).getCallee();
            const auto res = CallInst::Create(fnType, func, {left, right}, "minnum", block);
            return res;
        } else {
            const auto check = CmpInst::Create(Instruction::ICmp, std::is_signed<TOutput>() ? ICmpInst::ICMP_SGT : ICmpInst::ICMP_UGT, left, right, "greater", block);
            const auto res = SelectInst::Create(check, right, left, "min", block);
            return res;
        }
    }
#endif
};

template<typename TType>
struct TFloatAggrMin : public TSimpleArithmeticBinary<TType, TType, TType, TFloatAggrMin<TType>> {
    static TType Do(TType left, TType right)
    {
        return  left < right || std::isnan(right) ? left : right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        const auto ult = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ULT, left, right, "less", block);
        const auto ord = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ORD, ConstantFP::get(left->getType(), 0.0), left, "ordered", block);
        const auto both = BinaryOperator::CreateAnd(ult, ord, "and", block);
        return SelectInst::Create(both, left, right, "min", block);
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TDecimalMin {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto lv = left.GetInt128();
        if (!NYql::NDecimal::IsComparable(lv))
            return right;
        const auto rv = right.GetInt128();
        if (!NYql::NDecimal::IsComparable(rv))
            return left;
        return NUdf::TUnboxedValuePod(lv > rv ? rv : lv);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();

        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto result = PHINode::Create(Type::getInt128Ty(context), 3, "result", done);

        const auto l = GetterForInt128(left, block);
        const auto lok = NDecimal::GenIsComparable(l, context, block);
        result->addIncoming(right, block);
        BranchInst::Create(next, done, lok, block);

        block = next;

        const auto r = GetterForInt128(right, block);
        const auto rok = NDecimal::GenIsComparable(r, context, block);
        result->addIncoming(left, block);
        BranchInst::Create(good, done, rok, block);

        block = good;

        const auto greater = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, l, r, "greater", block);
        const auto res = SelectInst::Create(greater, right, left, "min", block);
        result->addIncoming(res, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TDecimalAggrMin {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto lv = left.GetInt128();
        const auto rv = right.GetInt128();
        return lv < rv ? left : right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto less = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, l, r, "less", block);
        return SelectInst::Create(less, left, right, "min", block);
    }
#endif
};

template<typename TType>
using TAggrMin = std::conditional_t<std::is_floating_point<TType>::value, TFloatAggrMin<TType>, TMin<TType, TType, TType>>;

template<typename TType>
struct TTzMin : public TSelectArithmeticBinaryCopyTimezone<TType, TTzMin<TType>> {
    static bool Do(TType left, TType right)
    {
        return left <= right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, left, right, "less_or_equal", block);
    }
#endif
};

template<typename TType>
struct TAggrTzMin : public TSelectArithmeticBinaryWithTimezone<TType, TAggrTzMin<TType>> {
    static bool Do(TType left, TType right)
    {
        return left <= right;
    }

    static bool DoTz(ui16 left, ui16 right)
    {
        return left <= right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, left, right, "less_value", block);
    }

    static Value* GenTz(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, left, right, "less_timezone", block);
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TCustomMin {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const bool r = CompareCustoms<Slot>(left, right) > 0;
        (r ? left : right).DeleteUnreferenced();
        return r ? right : left;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto res = CallBinaryUnboxedValueFunction(&CompareCustoms<Slot>, Type::getInt32Ty(context), left, right, ctx.Codegen, block);
        const auto comp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, res, ConstantInt::get(res->getType(), 0), "greater", block);
        const auto max = SelectInst::Create(comp, left, right, "max", block);
        ValueCleanup(EValueRepresentation::String, max, ctx, block);
        const auto min = SelectInst::Create(comp, right, left, "min", block);
        return min;
    }
#endif
};

}

void RegisterMin(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNumericFunctionOpt<TMin, TBinaryArgsOpt>(registry, "Min");
    RegisterBooleanSameTypesFunction<TAggrMin, TBinaryArgsOpt>(registry, "Min");
    RegisterDatetimeSameTypesFunction<TAggrMin, TBinaryArgsOpt>(registry, "Min");
    RegisterTzDatetimeSameTypesFunction<TTzMin, TBinaryArgsOpt>(registry, "Min");

    RegisterCustomSameTypesFunction<NUdf::TDataType<NUdf::TDecimal>, TDecimalMin, TBinaryArgsOpt>(registry, "Min");

    RegisterCustomSameTypesFunction<NUdf::TDataType<char*>, TCustomMin, TBinaryArgsOpt>(registry, "Min");
    RegisterCustomSameTypesFunction<NUdf::TDataType<NUdf::TUtf8>, TCustomMin, TBinaryArgsOpt>(registry, "Min");
}

void RegisterAggrMin(IBuiltinFunctionRegistry& registry) {
    RegisterNumericAggregateFunction<TAggrMin, TBinaryArgsSameOpt>(registry, "AggrMin");
    RegisterBooleanAggregateFunction<TAggrMin, TBinaryArgsSameOpt>(registry, "AggrMin");
    RegisterDatetimeAggregateFunction<TAggrMin, TBinaryArgsSameOpt>(registry, "AggrMin");
    RegisterBigDateAggregateFunction<TAggrMin, TBinaryArgsSameOpt>(registry, "AggrMin");
    RegisterTzDatetimeAggregateFunction<TAggrTzMin, TBinaryArgsSameOpt>(registry, "AggrMin");

    RegisterCustomAggregateFunction<NUdf::TDataType<NUdf::TDecimal>, TDecimalAggrMin, TBinaryArgsSameOpt>(registry, "AggrMin");

    RegisterCustomAggregateFunction<NUdf::TDataType<char*>, TCustomMin, TBinaryArgsSameOpt>(registry, "AggrMin");
    RegisterCustomAggregateFunction<NUdf::TDataType<NUdf::TUtf8>, TCustomMin, TBinaryArgsSameOpt>(registry, "AggrMin");
}

} // namespace NMiniKQL
} // namespace NKikimr
