#include "mkql_builtins_decimal.h" // Y_IGNORE
#include "mkql_builtins_compare.h"

#include <cmath>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline T Max(T l, T r) {
    return std::max(l, r);
}

template <typename T, std::enable_if_t<std::is_floating_point<T>::value>* = nullptr>
inline T Max(T l, T r) {
    return std::fmax(l, r);
}

template<typename TLeft, typename TRight, typename TOutput>
struct TMax : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TMax<TLeft, TRight, TOutput>> {
    static TOutput Do(TLeft left, TRight right)
    {
        return Max<TOutput>(left, right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        if constexpr (std::is_floating_point<TOutput>()) {
            auto& context = ctx.Codegen.GetContext();
            auto& module = ctx.Codegen.GetModule();
            const auto fnType = FunctionType::get(GetTypeFor<TOutput>(context), {left->getType(), right->getType()}, false);
            const auto& name = GetFuncNameForType<TOutput>("llvm.maxnum");
            const auto func = module.getOrInsertFunction(name, fnType).getCallee();
            const auto res = CallInst::Create(fnType, func, {left, right}, "maxnum", block);
            return res;
        } else {
            const auto check = CmpInst::Create(Instruction::ICmp, std::is_signed<TOutput>() ? ICmpInst::ICMP_SLT : ICmpInst::ICMP_ULT, left, right, "less", block);
            const auto res = SelectInst::Create(check, right, left, "max", block);
            return res;
        }
    }
#endif
};

template<typename TType>
struct TFloatAggrMax : public TSimpleArithmeticBinary<TType, TType, TType, TFloatAggrMax<TType>> {
    static TType Do(TType left, TType right)
    {
        return  left > right || std::isnan(left) ? left : right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        const auto ugt = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_UGT, left, right, "greater", block);
        const auto ord = CmpInst::Create(Instruction::FCmp, FCmpInst::FCMP_ORD, ConstantFP::get(right->getType(), 0.0), right, "ordered", block);
        const auto both = BinaryOperator::CreateAnd(ugt, ord, "and", block);
        return SelectInst::Create(both, left, right, "max", block);
    }
#endif
};

template<typename TType>
using TAggrMax = std::conditional_t<std::is_floating_point<TType>::value, TFloatAggrMax<TType>, TMax<TType, TType, TType>>;

template<typename TType>
struct TTzMax : public TSelectArithmeticBinaryCopyTimezone<TType, TTzMax<TType>> {
    static bool Do(TType left, TType right)
    {
        return left >= right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, left, right, "greater_or_equal", block);
    }
#endif
};

template<typename TType>
struct TAggrTzMax : public TSelectArithmeticBinaryWithTimezone<TType, TAggrTzMax<TType>> {
    static bool Do(TType left, TType right)
    {
        return left >= right;
    }

    static bool DoTz(ui16 left, ui16 right)
    {
        return left >= right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, left, right, "greater_value", block);
    }

    static Value* GenTz(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, left, right, "greater_timezone", block);
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TDecimalMax {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto rv = right.GetInt128();
        if (!NYql::NDecimal::IsComparable(rv))
            return left;
        const auto lv = left.GetInt128();
        if (!NYql::NDecimal::IsComparable(lv))
            return right;
        return NUdf::TUnboxedValuePod(lv < rv ? rv : lv);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();

        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto result = PHINode::Create(Type::getInt128Ty(context), 3, "result", done);

        const auto r = GetterForInt128(right, block);
        const auto rok = NDecimal::GenIsComparable(r, context, block);
        result->addIncoming(left, block);
        BranchInst::Create(next, done, rok, block);

        block = next;

        const auto l = GetterForInt128(left, block);
        const auto lok = NDecimal::GenIsComparable(l, context, block);
        result->addIncoming(right, block);
        BranchInst::Create(good, done, lok, block);

        block = good;

        const auto less = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, l, r, "less", block);
        const auto res = SelectInst::Create(less, right, left, "max", block);
        result->addIncoming(res, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TDecimalAggrMax {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto lv = left.GetInt128();
        const auto rv = right.GetInt128();
        return lv > rv ? left : right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        const auto l = GetterForInt128(left, block);
        const auto r = GetterForInt128(right, block);
        const auto greater = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, l, r, "greater", block);
        const auto res = SelectInst::Create(greater, left, right, "max", block);
        return res;
    }
#endif
};

template<NUdf::EDataSlot Slot>
struct TCustomMax {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod left, NUdf::TUnboxedValuePod right) {
        const bool r = CompareCustoms<Slot>(left, right) < 0;
        (r ? left : right).DeleteUnreferenced();
        return r ? right : left;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto res = CallBinaryUnboxedValueFunction(&CompareCustoms<Slot>, Type::getInt32Ty(context), left, right, ctx.Codegen, block);
        const auto comp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, res, ConstantInt::get(res->getType(), 0), "less", block);
        const auto min = SelectInst::Create(comp, left, right, "min", block);
        ValueCleanup(EValueRepresentation::String, min, ctx, block);
        const auto max = SelectInst::Create(comp, right, left, "max", block);
        return max;
    }
#endif
};

}

void RegisterMax(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNumericFunctionOpt<TMax, TBinaryArgsOpt>(registry, "Max");
    RegisterBooleanSameTypesFunction<TAggrMax, TBinaryArgsOpt>(registry, "Max");
    RegisterDatetimeSameTypesFunction<TAggrMax, TBinaryArgsOpt>(registry, "Max");
    RegisterTzDatetimeSameTypesFunction<TTzMax, TBinaryArgsOpt>(registry, "Max");

    RegisterCustomSameTypesFunction<NUdf::TDataType<NUdf::TDecimal>, TDecimalMax, TBinaryArgsOpt>(registry, "Max");

    RegisterCustomSameTypesFunction<NUdf::TDataType<char*>, TCustomMax, TBinaryArgsOpt>(registry, "Max");
    RegisterCustomSameTypesFunction<NUdf::TDataType<NUdf::TUtf8>, TCustomMax, TBinaryArgsOpt>(registry, "Max");
}

void RegisterAggrMax(IBuiltinFunctionRegistry& registry) {
    RegisterNumericAggregateFunction<TAggrMax, TBinaryArgsSameOpt>(registry, "AggrMax");
    RegisterBooleanAggregateFunction<TAggrMax, TBinaryArgsSameOpt>(registry, "AggrMax");
    RegisterDatetimeAggregateFunction<TAggrMax, TBinaryArgsSameOpt>(registry, "AggrMax");
    RegisterBigDateAggregateFunction<TAggrMax, TBinaryArgsSameOpt>(registry, "AggrMax");
    RegisterTzDatetimeAggregateFunction<TAggrTzMax, TBinaryArgsSameOpt>(registry, "AggrMax");

    RegisterCustomAggregateFunction<NUdf::TDataType<NUdf::TDecimal>, TDecimalAggrMax, TBinaryArgsSameOpt>(registry, "AggrMax");

    RegisterCustomAggregateFunction<NUdf::TDataType<char*>, TCustomMax, TBinaryArgsSameOpt>(registry, "AggrMax");
    RegisterCustomAggregateFunction<NUdf::TDataType<NUdf::TUtf8>, TCustomMax, TBinaryArgsSameOpt>(registry, "AggrMax");
}

} // namespace NMiniKQL
} // namespace NKikimr
