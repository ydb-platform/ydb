#pragma once

#include "mkql_builtins_impl.h"  // Y_IGNORE // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {


template <typename TLeft, typename TRight, class TImpl>
struct TCompareArithmeticBinary : public TArithmeticConstraintsBinary<TLeft, TRight, bool> {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        return NUdf::TUnboxedValuePod(TImpl::Do(left.template Get<TLeft>(), right.template Get<TRight>()));
    }

    static void DoPtr(
        const typename TPrimitiveDataType<TLeft>::TLayout* left,
        const typename TPrimitiveDataType<TRight>::TLayout* right,
        typename TPrimitiveDataType<bool>::TLayout* res) {
        *res = TImpl::Do(*left, *right);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<TLeft>(left, context, block);
        const auto rhs = GetterFor<TRight>(right, context, block);
        const auto res = TImpl::Gen(lhs, rhs, ctx, block);
        const auto wide = MakeBoolean(res, context, block);
        return wide;
    }
#endif
};

template <typename TLeft, typename TRight, class TImpl>
struct TCompareArithmeticBinaryWithTimezone : public TArithmeticConstraintsBinary<TLeft, TRight, bool> {
    static_assert(std::is_same<TLeft, TRight>::value, "Must be same type.");
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.template Get<TLeft>();
        const auto r = right.template Get<TRight>();
        return NUdf::TUnboxedValuePod(l == r ? TImpl::DoTz(left.GetTimezoneId(), right.GetTimezoneId()) : TImpl::Do(left.template Get<TLeft>(), right.template Get<TRight>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<TLeft>(left, context, block);
        const auto rhs = GetterFor<TRight>(right, context, block);
        const auto equals = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, lhs, rhs, "equals", block);
        const auto ltz = GetterForTimezone(context, left, block);
        const auto rtz = GetterForTimezone(context, right, block);
        const auto result = SelectInst::Create(equals, TImpl::GenTz(ltz, rtz, ctx, block), TImpl::Gen(lhs, rhs, ctx, block), "result", block);
        const auto wide = MakeBoolean(result, context, block);
        return wide;
    }
#endif
};

template <typename TType, class TImpl>
struct TSelectArithmeticBinaryCopyTimezone : public TArithmeticConstraintsBinary<TType, TType, bool> {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        return TImpl::Do(left.template Get<TType>(), right.template Get<TType>()) ? left : right;
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<TType>(left, context, block);
        const auto rhs = GetterFor<TType>(right, context, block);
        const auto result = SelectInst::Create(TImpl::Gen(lhs, rhs, ctx, block), left, right, "result", block);
        return result;
    }
#endif
};

template <typename TType, class TImpl>
struct TSelectArithmeticBinaryWithTimezone : public TArithmeticConstraintsBinary<TType, TType, bool> {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        const auto l = left.template Get<TType>();
        const auto r = right.template Get<TType>();
        const bool choise = l == r ? TImpl::DoTz(left.GetTimezoneId(), right.GetTimezoneId()) : TImpl::Do(l, r);
        return choise ? left : right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = GetterFor<TType>(left, context, block);
        const auto rhs = GetterFor<TType>(right, context, block);
        const auto equals = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, lhs, rhs, "equals", block);
        const auto ltz = GetterForTimezone(context, left, block);
        const auto rtz = GetterForTimezone(context, right, block);
        const auto choise = SelectInst::Create(equals, TImpl::GenTz(ltz, rtz, ctx, block), TImpl::Gen(lhs, rhs, ctx, block), "choise", block);
        const auto result = SelectInst::Create(choise, left, right, "result", block);
        return result;
    }
#endif
};

template<NUdf::EDataSlot Slot>
int CompareCustoms(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) {
    const TStringBuf lhsBuf = lhs.AsStringRef();
    const TStringBuf rhsBuf = rhs.AsStringRef();
    return lhsBuf.compare(rhsBuf);
}

template<NUdf::EDataSlot Slot>
int CompareCustomsWithCleanup(NUdf::TUnboxedValuePod left, NUdf::TUnboxedValuePod right) {
    const auto c = CompareCustoms<Slot>(left, right);
    left.DeleteUnreferenced();
    right.DeleteUnreferenced();
    return c;
}


template <typename TInput1, typename TInput2, bool IsLeftOptional, bool IsRightOptional, bool IsResultOptional>
struct TCompareArgsOpt {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput1, typename TInput2, bool IsLeftOptional, bool IsRightOptional, bool IsResultOptional>
const TFunctionParamMetadata TCompareArgsOpt<TInput1, TInput2, IsLeftOptional, IsRightOptional, IsResultOptional>::Value[4] = {
    { NUdf::TDataType<bool>::Id, IsResultOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput1::Id, IsLeftOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput2::Id, IsRightOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { 0, 0 }
};

template <
    typename TInput1, typename TInput2,
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, false>, TArgs<TInput1, TInput2, false, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, false>, TArgs<TInput1, TInput2, false, true, true>, TBinaryWrap<false, true>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, false>, TArgs<TInput1, TInput2, true, false, true>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, false>, TArgs<TInput1, TInput2, true, true, true>, TBinaryWrap<true, true>>(registry, name);
}

template <
    typename TInput,
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrCompareOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<typename TInput::TLayout, typename TInput::TLayout, true>, TArgs<TInput, TInput, false, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput::TLayout, typename TInput::TLayout, true>, TArgs<TInput, TInput, true, true, false>, TAggrCompareWrap>(registry, name);
}

template <
    typename TType1, typename TType2,
    class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareCustomOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc, TArgs<TType1, TType2, false, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc, TArgs<TType1, TType2, false, true, true>, TBinaryWrap<false, true>>(registry, name);
    RegisterFunctionImpl<TFunc, TArgs<TType1, TType2, true, false, true>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TFunc, TArgs<TType1, TType2, true, true, true>, TBinaryWrap<true, true>>(registry, name);
}

template <
    typename TType1, typename TType2,
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterComparePolyOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<TType1, TType2, false>, TArgs<TType1, TType2, false, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<TType1, TType2, false>, TArgs<TType1, TType2, false, true, true>, TBinaryWrap<false, true>>(registry, name);
    RegisterFunctionImpl<TFunc<TType1, TType2, false>, TArgs<TType1, TType2, true, false, true>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TFunc<TType1, TType2, false>, TArgs<TType1, TType2, true, true, true>, TBinaryWrap<true, true>>(registry, name);
}


template <
    typename TType,
    class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrCompareCustomOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc, TArgs<TType, TType, false, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc, TArgs<TType, TType, true, true, false>, TAggrCompareWrap>(registry, name);
}

template <
    typename TInput,
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrComparePolyOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<TInput, TInput, true>, TArgs<TInput, TInput, false, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<TInput, TInput, true>, TArgs<TInput, TInput, true, true, false>, TAggrCompareWrap>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareUnsigned(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareSigned(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareSignedAndUnsigned(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareUnsignedAndSigned(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool>  class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareReal(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i8>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui8>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i16>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui16>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i32>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui32>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<i64>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<ui64>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<float>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterCompareOpt<NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool>  class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareBool(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareOpt<NUdf::TDataType<bool>, NUdf::TDataType<bool>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool>  class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareIntegral(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareSigned<TFunc, TArgs>(registry, name);
    RegisterCompareUnsigned<TFunc, TArgs>(registry, name);
    RegisterCompareSignedAndUnsigned<TFunc, TArgs>(registry, name);
    RegisterCompareUnsignedAndSigned<TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool>  class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterComparePrimitive(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareBool<TFunc, TArgs>(registry, name);
    RegisterCompareReal<TFunc, TArgs>(registry, name);
    RegisterCompareIntegral<TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool>  class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrComparePrimitive(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggrCompareOpt<NUdf::TDataType<bool>, TFunc, TArgs>(registry, name);

    RegisterAggrCompareOpt<NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);

    RegisterAggrCompareOpt<NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);

    RegisterAggrCompareOpt<NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);

    RegisterAggrCompareOpt<NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterAggrCompareOpt<NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareDatetime(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterCompareBigDatetime(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval64>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);    
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);    
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDate32>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzDatetime64>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);

    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval64>, TFunc, TArgs>(registry, name);
    RegisterComparePolyOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrCompareDatetime(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggrComparePolyOpt<NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterAggrComparePolyOpt<NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterAggrComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterAggrComparePolyOpt<NUdf::TDataType<NUdf::TInterval>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrCompareTzDatetime(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggrCompareOpt<NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrCompareBigDatetime(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggrComparePolyOpt<NUdf::TDataType<NUdf::TDate32>, TFunc, TArgs>(registry, name);
    RegisterAggrComparePolyOpt<NUdf::TDataType<NUdf::TDatetime64>, TFunc, TArgs>(registry, name);
    RegisterAggrComparePolyOpt<NUdf::TDataType<NUdf::TTimestamp64>, TFunc, TArgs>(registry, name);
    RegisterAggrComparePolyOpt<NUdf::TDataType<NUdf::TInterval64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, bool> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrCompareBigTzDatetime(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggrCompareOpt<NUdf::TDataType<NUdf::TTzDate32>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<NUdf::TTzDatetime64>, TFunc, TArgs>(registry, name);
    RegisterAggrCompareOpt<NUdf::TDataType<NUdf::TTzTimestamp64>, TFunc, TArgs>(registry, name);
}

template <
    template<NUdf::EDataSlot> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs,
    bool WithSpecial = true
>
void RegisterCompareStrings(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterCompareCustomOpt<NUdf::TDataType<char*>, NUdf::TDataType<char*>, TFunc<NUdf::EDataSlot::String>, TArgs>(registry, name);
    RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<NUdf::TUtf8>, TFunc<NUdf::EDataSlot::Utf8>, TArgs>(registry, name);
    RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<char*>, TFunc<NUdf::EDataSlot::Utf8>, TArgs>(registry, name);
    RegisterCompareCustomOpt<NUdf::TDataType<char*>, NUdf::TDataType<NUdf::TUtf8>, TFunc<NUdf::EDataSlot::Utf8>, TArgs>(registry, name);
    if constexpr (WithSpecial) {
        RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TUuid>, NUdf::TDataType<NUdf::TUuid>, TFunc<NUdf::EDataSlot::Uuid>, TArgs>(registry, name);
        RegisterCompareCustomOpt<NUdf::TDataType<NUdf::TDyNumber>, NUdf::TDataType<NUdf::TDyNumber>, TFunc<NUdf::EDataSlot::DyNumber>, TArgs>(registry, name);
    }
}

template <
    template<NUdf::EDataSlot> class TFunc,
    template<typename, typename, bool, bool, bool> class TArgs
>
void RegisterAggrCompareStrings(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggrCompareCustomOpt<NUdf::TDataType<char*>, TFunc<NUdf::EDataSlot::String>, TArgs>(registry, name);
    RegisterAggrCompareCustomOpt<NUdf::TDataType<NUdf::TUtf8>, TFunc<NUdf::EDataSlot::Utf8>, TArgs>(registry, name);
    RegisterAggrCompareCustomOpt<NUdf::TDataType<NUdf::TUuid>, TFunc<NUdf::EDataSlot::Uuid>, TArgs>(registry, name);
    RegisterAggrCompareCustomOpt<NUdf::TDataType<NUdf::TDyNumber>, TFunc<NUdf::EDataSlot::DyNumber>, TArgs>(registry, name);
}

void RegisterEquals(IBuiltinFunctionRegistry& registry);
void RegisterEquals(TKernelFamilyMap& kernelFamilyMap);
void RegisterNotEquals(IBuiltinFunctionRegistry& registry);
void RegisterNotEquals(TKernelFamilyMap& kernelFamilyMap);
void RegisterLess(IBuiltinFunctionRegistry& registry);
void RegisterLess(TKernelFamilyMap& kernelFamilyMap);
void RegisterLessOrEqual(IBuiltinFunctionRegistry& registry);
void RegisterLessOrEqual(TKernelFamilyMap& kernelFamilyMap);
void RegisterGreater(IBuiltinFunctionRegistry& registry);
void RegisterGreater(TKernelFamilyMap& kernelFamilyMap);
void RegisterGreaterOrEqual(IBuiltinFunctionRegistry& registry);
void RegisterGreaterOrEqual(TKernelFamilyMap& kernelFamilyMap);

}
}
