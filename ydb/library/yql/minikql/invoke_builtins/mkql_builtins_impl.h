#pragma once

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/minikql/mkql_function_metadata.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <util/string/cast.h>

#include "mkql_builtins.h"
#include "mkql_builtins_codegen.h"

#include <arrow/compute/function.h>
#include <arrow/scalar.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap.h>
#include <arrow/util/bitmap_ops.h>

namespace NKikimr {
namespace NMiniKQL {

struct TUnaryStub {
    template<typename TFunc>
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod* args) {
        return TFunc::Execute(*args);
    }

#ifndef MKQL_DISABLE_CODEGEN
    template<typename TFunc>
    static Value* Generate(Value *const * args, const TCodegenContext& ctx, BasicBlock*& block) {
        return GenerateUnaryWithoutCheck(*args, ctx, block, &TFunc::Generate);
    }
#endif
};

struct TUnaryWrap {
    template<typename TFunc>
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod* args) {
        return *args ? TFunc::Execute(*args) : NUdf::TUnboxedValuePod();
    }

#ifndef MKQL_DISABLE_CODEGEN
    template<typename TFunc>
    static Value* Generate(Value *const * args, const TCodegenContext& ctx, BasicBlock*& block) {
        return GenerateUnaryWithCheck(*args, ctx, block, &TFunc::Generate);
    }
#endif
};

template<bool CheckLeft, bool CheckRight>
struct TBinaryWrap {
    template<typename TFunc>
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod* args) {
        if (CheckLeft && !args[0])
            return NUdf::TUnboxedValuePod();
        if (CheckRight && !args[1])
            return NUdf::TUnboxedValuePod();
        return TFunc::Execute(args[0], args[1]);
    }

#ifndef MKQL_DISABLE_CODEGEN
    template<typename TFunc>
    static Value* Generate(Value *const * args, const TCodegenContext& ctx, BasicBlock*& block) {
        return GenerateBinary<CheckLeft, CheckRight>(args[0], args[1], ctx, block, &TFunc::Generate);
    }
#endif
};

struct TAggregateWrap {
    template<typename TFunc>
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod* args) {
        if (!args[0])
            return args[1];
        if (!args[1])
            return args[0];
        return TFunc::Execute(args[0], args[1]);
    }

#ifndef MKQL_DISABLE_CODEGEN
    template<typename TFunc>
    static Value* Generate(Value *const * args, const TCodegenContext& ctx, BasicBlock*& block) {
        return GenerateAggregate(args[0], args[1], ctx, block, &TFunc::Generate);
    }
#endif
};

struct TAggrCompareWrap {
    template<typename TFunc>
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod* args) {
        const bool a0(args[0]), a1(args[1]);
        return (a0 && a1) ?
            TFunc::Execute(args[0], args[1]) : NUdf::TUnboxedValuePod(TFunc::Simple(a0, a1));
    }

#ifndef MKQL_DISABLE_CODEGEN
    template<typename TFunc>
    static Value* Generate(Value *const * args, const TCodegenContext& ctx, BasicBlock*& block) {
        return GenerateCompareAggregate(args[0], args[1], ctx, block, &TFunc::Generate, TFunc::SimplePredicate);
    }
#endif
};

template<bool CheckFirst>
struct TTernaryWrap {
    template<typename TFunc>
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod* args) {
        if (CheckFirst && !*args)
            return NUdf::TUnboxedValuePod();
        return TFunc::Execute(args[0], args[1], args[2]);
    }

#ifndef MKQL_DISABLE_CODEGEN
    template<typename TFunc>
    static Value* Generate(Value *const * args, const TCodegenContext& ctx, BasicBlock*& block) {
        return GenerateTernary<CheckFirst>(args[0], args[1], args[2], ctx, block, &TFunc::Generate);
    }
#endif
};

template <typename TInput, typename TOutput>
struct TArithmeticConstraintsUnary {
    static_assert(std::is_arithmetic<TInput>::value, "Input type must be arithmetic!");
    static_assert(std::is_arithmetic<TOutput>::value, "Output type must be arithmetic!");
};

template <typename TInput, typename TOutput>
struct TArithmeticConstraintsSame {
    static_assert(std::is_arithmetic<TInput>::value, "Input type must be arithmetic!");
    static_assert(std::is_same<TInput, TOutput>::value, "Input and output must be same types!");
};

template <typename TLeft, typename TRight, typename TOutput>
struct TArithmeticConstraintsBinary {
    static_assert(std::is_arithmetic<TLeft>::value, "Left type must be arithmetic!");
    static_assert(std::is_arithmetic<TRight>::value, "Right type must be arithmetic!");
    static_assert(std::is_arithmetic<TOutput>::value, "Output type must be arithmetic!");
};

template <typename TInput, typename TOutput, class TImpl>
struct TSimpleArithmeticUnary : public TArithmeticConstraintsSame<TInput, TOutput> {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return NUdf::TUnboxedValuePod(TImpl::Do(arg.template Get<TInput>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen->GetContext();
        const auto val = GetterFor<TInput>(arg, context, block);
        const auto res = TImpl::Gen(val, ctx, block);
        const auto wide = SetterFor<TOutput>(res, context, block);
        return wide;
    }
#endif
};

template <typename TLeft, typename TRight, typename TOutput, class TImpl>
struct TSimpleArithmeticBinary : public TArithmeticConstraintsBinary<TLeft, TRight, TOutput> {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        return NUdf::TUnboxedValuePod(TImpl::Do(left.template Get<TLeft>(), right.template Get<TRight>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen->GetContext();
        const auto lhs = StaticCast<TLeft, TOutput>(GetterFor<TLeft>(left, context, block), context, block);
        const auto rhs = StaticCast<TRight, TOutput>(GetterFor<TRight>(right, context, block), context, block);
        const auto res = TImpl::Gen(lhs, rhs, ctx, block);
        const auto wide = SetterFor<TOutput>(res, context, block);
        return wide;
    }
#endif
};

template <typename TInput, typename TOutput, class TImpl>
struct TShiftArithmeticBinary : public TArithmeticConstraintsSame<TInput, TOutput> {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
        return NUdf::TUnboxedValuePod(TImpl::Do(left.template Get<TInput>(), right.Get<ui8>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen->GetContext();
        const auto lhs = GetterFor<TInput>(left, context, block);
        const auto rhs = CastInst::Create(Instruction::Trunc, right, Type::getInt8Ty(context), "bits", block);
        const auto res = TImpl::Gen(lhs, rhs, ctx, block);
        const auto wide = SetterFor<TOutput>(res, context, block);
        return wide;
    }
#endif
};

template <typename TInput, typename TOutput>
struct TUnaryArgs {
    static const TFunctionParamMetadata Value[3];
};

template <typename TInput, typename TOutput>
const TFunctionParamMetadata TUnaryArgs<TInput, TOutput>::Value[3] = {
    { TOutput::Id, 0 },
    { TInput::Id, 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput, bool IsOptional>
struct TUnaryArgsOpt {
    static const TFunctionParamMetadata Value[3];
};

template <typename TInput, typename TOutput, bool IsOptional>
const TFunctionParamMetadata TUnaryArgsOpt<TInput, TOutput, IsOptional>::Value[3] = {
    { TOutput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput>
struct TUnaryArgsWithNullableResult {
    static const TFunctionParamMetadata Value[3];
};

template <typename TInput, typename TOutput>
const TFunctionParamMetadata TUnaryArgsWithNullableResult<TInput, TOutput>::Value[3] = {
    { TOutput::Id, TFunctionParamMetadata::FlagIsNullable },
    { TInput::Id, 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput, bool IsOptional>
struct TUnaryArgsWithNullableResultOpt {
    static const TFunctionParamMetadata Value[3];
};

template <typename TInput, typename TOutput, bool IsOptional>
const TFunctionParamMetadata TUnaryArgsWithNullableResultOpt<TInput, TOutput, IsOptional>::Value[3] = {
    { TOutput::Id, TFunctionParamMetadata::FlagIsNullable },
    { TInput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput>
struct TBinaryArgs {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput, typename TOutput>
const TFunctionParamMetadata TBinaryArgs<TInput, TOutput>::Value[4] = {
    { TOutput::Id, 0 },
    { TInput::Id, 0 },
    { TInput::Id, 0 },
    { 0, 0 }
};

template <typename TInput1, typename TInput2, typename TOutput, bool IsLeftOptional, bool IsRightOptional>
struct TBinaryArgsOpt {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput1, typename TInput2, typename TOutput, bool IsLeftOptional, bool IsRightOptional>
const TFunctionParamMetadata TBinaryArgsOpt<TInput1, TInput2, TOutput, IsLeftOptional, IsRightOptional>::Value[4] = {
    { TOutput::Id, (IsLeftOptional || IsRightOptional) ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput1::Id, IsLeftOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput2::Id, IsRightOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput, bool IsOptional>
struct TBinaryArgsSameOpt {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput, typename TOutput, bool IsOptional>
const TFunctionParamMetadata TBinaryArgsSameOpt<TInput, TOutput, IsOptional>::Value[4] = {
    { TOutput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput, bool IsOptional>
struct TBinaryArgsSameOptArgsWithNullableResult {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput, typename TOutput, bool IsOptional>
const TFunctionParamMetadata TBinaryArgsSameOptArgsWithNullableResult<TInput, TOutput, IsOptional>::Value[4] = {
    { TOutput::Id, TFunctionParamMetadata::FlagIsNullable },
    { TInput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput>
struct TBinaryShiftArgs {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput, typename TOutput>
const TFunctionParamMetadata TBinaryShiftArgs<TInput, TOutput>::Value[4] = {
    { TOutput::Id, 0 },
    { TInput::Id, 0 },
    { NUdf::TDataType<ui8>::Id, 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput, bool IsOptional>
struct TBinaryShiftArgsOpt {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput, typename TOutput, bool IsOptional>
const TFunctionParamMetadata TBinaryShiftArgsOpt<TInput, TOutput, IsOptional>::Value[4] = {
    { TOutput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { NUdf::TDataType<ui8>::Id, 0 },
    { 0, 0 }
};

template <typename TInput, typename TOutput>
struct TBinaryArgsWithNullableResult {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput, typename TOutput>
const TFunctionParamMetadata TBinaryArgsWithNullableResult<TInput, TOutput>::Value[4] = {
    { TOutput::Id, TFunctionParamMetadata::FlagIsNullable },
    { TInput::Id, 0 },
    { TInput::Id, 0 },
    { 0, 0 }
};

template <typename TOutput, typename TInput1, typename TInput2, typename TInput3, bool IsFirstOptional, bool IsSecondOptional, bool IsThirdOptional, bool IsResultOptional = IsFirstOptional>
struct TTernaryArgs {
    static const TFunctionParamMetadata Value[5];
};

template <typename TOutput, typename TInput1, typename TInput2, typename TInput3, bool IsFirstOptional, bool IsSecondOptional, bool IsThirdOptional, bool IsResultOptional>
const TFunctionParamMetadata TTernaryArgs<TOutput, TInput1, TInput2, TInput3, IsFirstOptional, IsSecondOptional, IsThirdOptional, IsResultOptional>::Value[5] = {
    { TOutput::Id, IsResultOptional ? TFunctionParamMetadata::FlagIsNullable : 0},
    { TInput1::Id, IsFirstOptional  ? TFunctionParamMetadata::FlagIsNullable : 0},
    { TInput2::Id, IsSecondOptional ? TFunctionParamMetadata::FlagIsNullable : 0},
    { TInput3::Id, IsThirdOptional  ? TFunctionParamMetadata::FlagIsNullable : 0},
    { 0, 0 }
};

template <typename TInput1, typename TInput2, typename TOutput, bool IsLeftOptional, bool IsRightOptional>
struct TBinaryArgsOptWithNullableResult {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput1, typename TInput2, typename TOutput, bool IsLeftOptional, bool IsRightOptional>
const TFunctionParamMetadata TBinaryArgsOptWithNullableResult<TInput1, TInput2, TOutput, IsLeftOptional, IsRightOptional>::Value[4] = {
    { TOutput::Id, TFunctionParamMetadata::FlagIsNullable },
    { TInput1::Id, IsLeftOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { TInput2::Id, IsRightOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { 0, 0 }
};

template <typename TFunc, typename TArgs, typename TWrap>
void RegisterFunctionImpl(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
#ifndef MKQL_DISABLE_CODEGEN
    const TFunctionDescriptor description(TArgs::Value, &TWrap::template Execute<TFunc>, reinterpret_cast<void*>(&TWrap::template Generate<TFunc>));
#else
    const TFunctionDescriptor description(TArgs::Value, &TWrap::template Execute<TFunc>);
#endif
    registry.Register(name, description);
}

template <
    typename TInput, typename TOutput,
    template<typename, typename> class TFunc,
    template<typename, typename> class TArgs
>
void RegisterFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<TInput, TOutput>, TArgs<TInput, TOutput>, TUnaryStub>(registry, name);
}

template <
    typename TInput, typename TOutput,
    class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc, TArgs<TInput, TOutput, false>, TUnaryStub>(registry, name);
    RegisterFunctionImpl<TFunc, TArgs<TInput, TOutput, true>, TUnaryWrap>(registry, name);
}

template <
    typename TType,
    template<NUdf::EDataSlot> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterCustomAggregateFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<TType::Slot>, TArgs<TType, TType, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<TType::Slot>, TArgs<TType, TType, true>, TAggregateWrap>(registry, name);
}

template <
    typename TType,
    template<NUdf::EDataSlot> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterCustomSameTypesFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<TType::Slot>, TArgs<TType, TType, TType, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<TType::Slot>, TArgs<TType, TType, TType, true, false>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TFunc<TType::Slot>, TArgs<TType, TType, TType, false, true>, TBinaryWrap<false, true>>(registry, name);
    RegisterFunctionImpl<TFunc<TType::Slot>, TArgs<TType, TType, TType, true, true>, TBinaryWrap<true, true>>(registry, name);
}

template <
    typename TType,
    template<typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterAggregateFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<typename TType::TLayout>, TArgs<TType, TType, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TType::TLayout>, TArgs<TType, TType, true>, TAggregateWrap>(registry, name);
}

template <
    typename TType,
    template<typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterSameTypesFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<typename TType::TLayout>, TArgs<TType, TType, TType, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TType::TLayout>, TArgs<TType, TType, TType, true, false>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TType::TLayout>, TArgs<TType, TType, TType, false, true>, TBinaryWrap<false, true>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TType::TLayout>, TArgs<TType, TType, TType, true, true>, TBinaryWrap<true, true>>(registry, name);
}

template <
    typename TInput, typename TOutput,
    template<typename, typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterFunctionUnOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<typename TInput::TLayout, typename TOutput::TLayout>, TArgs<TInput, TOutput, false>, TUnaryStub>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput::TLayout, typename TOutput::TLayout>, TArgs<TInput, TOutput, true>, TUnaryWrap>(registry, name);
}

template <
    typename TInput1, typename TInput2, typename TOutput,
    template<typename, typename, typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterFunctionBinOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, typename TOutput::TLayout>, TArgs<TInput1, TInput2, TOutput, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, typename TOutput::TLayout>, TArgs<TInput1, TInput2, TOutput, false, true>, TBinaryWrap<false, true>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, typename TOutput::TLayout>, TArgs<TInput1, TInput2, TOutput, true, false>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, typename TOutput::TLayout>, TArgs<TInput1, TInput2, TOutput, true, true>, TBinaryWrap<true, true>>(registry, name);
}

template <
    template<typename, typename, typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBinaryUnsignedFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui8>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui16>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui8>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui16>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui8>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui16>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui8>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui16>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui32>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
}

template <
    typename TInput, typename TOutput,
    template<typename, typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterShiftFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionImpl<TFunc<typename TInput::TLayout, typename TOutput::TLayout>, TArgs<TInput, TOutput, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput::TLayout, typename TOutput::TLayout>, TArgs<TInput, TOutput, true>, TBinaryWrap<true, false>>(registry, name);
}

template <
    template<typename, typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterUnsignedShiftFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterShiftFunctionOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterShiftFunctionOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterShiftFunctionOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterShiftFunctionOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterUnaryUnsignedFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionUnOpt<NUdf::TDataType<ui8>, NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterFunctionUnOpt<NUdf::TDataType<ui16>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterFunctionUnOpt<NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionUnOpt<NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterUnaryIntegralFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterUnaryUnsignedFunctionOpt<TFunc, TArgs>(registry, name);

    RegisterFunctionUnOpt<NUdf::TDataType<i8>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterFunctionUnOpt<NUdf::TDataType<i16>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterFunctionUnOpt<NUdf::TDataType<i32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionUnOpt<NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterUnaryNumericFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterUnaryIntegralFunctionOpt<TFunc, TArgs>(registry, name);

    RegisterFunctionUnOpt<NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionUnOpt<NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBinaryIntegralToUnsignedFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<ui16>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<i8>, NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<i8>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<i16>, NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<ui64>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<i8>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<i16>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<i32>, NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBinaryIntegralToSignedFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<i8>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<i16>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<i32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<i8>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<ui8>, NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<i16>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<i32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<i16>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<i32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<i8>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<ui8>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<i16>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<ui16>, NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<i32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<i32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<i8>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<ui8>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<i16>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<ui16>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<i32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<ui32>, NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<i8>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<ui8>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<i16>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<ui16>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<i32>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<ui32>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<i64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<ui64>, NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBinaryIntegralFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterBinaryUnsignedFunctionOpt<TFunc, TArgs>(registry, name);
    RegisterBinaryIntegralToUnsignedFunctionOpt<TFunc, TArgs>(registry, name);
    RegisterBinaryIntegralToSignedFunctionOpt<TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBinaryRealFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<i8>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<ui8>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<i16>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<ui16>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<i32>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<ui32>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<i64>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<ui64>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);

    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<i8>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<ui8>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<i16>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<ui16>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<i32>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<ui32>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<i64>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<ui64>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<float>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBinaryNumericFunctionOpt(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterBinaryIntegralFunctionOpt<TFunc, TArgs>(registry, name);
    RegisterBinaryRealFunctionOpt<TFunc, TArgs>(registry, name);
}

template <
    template<typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterNumericAggregateFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggregateFunction<NUdf::TDataType<i8>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<ui8>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<i16>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<ui16>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<i32>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<ui32>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<i64>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<ui64>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
}

template <
    template<typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterDatetimeAggregateFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggregateFunction<NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);

    RegisterAggregateFunction<NUdf::TDataType<NUdf::TInterval>, TFunc, TArgs>(registry, name);
}

template <
    template<typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterTzDatetimeAggregateFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggregateFunction<NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterAggregateFunction<NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
}

template <
    template<typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterDatetimeSameTypesFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterSameTypesFunction<NUdf::TDataType<NUdf::TDate>, TFunc, TArgs>(registry, name);
    RegisterSameTypesFunction<NUdf::TDataType<NUdf::TDatetime>, TFunc, TArgs>(registry, name);
    RegisterSameTypesFunction<NUdf::TDataType<NUdf::TTimestamp>, TFunc, TArgs>(registry, name);
    RegisterSameTypesFunction<NUdf::TDataType<NUdf::TInterval>, TFunc, TArgs>(registry, name);
}

template <
    template<typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterTzDatetimeSameTypesFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterSameTypesFunction<NUdf::TDataType<NUdf::TTzDate>, TFunc, TArgs>(registry, name);
    RegisterSameTypesFunction<NUdf::TDataType<NUdf::TTzDatetime>, TFunc, TArgs>(registry, name);
    RegisterSameTypesFunction<NUdf::TDataType<NUdf::TTzTimestamp>, TFunc, TArgs>(registry, name);
}

template <
    template<typename> class TFunc,
    template<typename, typename, bool> class TArgs
>
void RegisterBooleanAggregateFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterAggregateFunction<NUdf::TDataType<bool>, TFunc, TArgs>(registry, name);
}

template <
    template<typename> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBooleanSameTypesFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterSameTypesFunction<NUdf::TDataType<bool>, TFunc, TArgs>(registry, name);
}

template <
    template<typename, typename, typename, bool, bool> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBinaryRealFunction(IBuiltinFunctionRegistry& registry, const std::string_view& name) {
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<float>, NUdf::TDataType<float>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<float>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<float>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<double>, NUdf::TDataType<double>, NUdf::TDataType<double>, TFunc, TArgs>(registry, name);
}

void RegisterAdd(IBuiltinFunctionRegistry& registry);
void RegisterAdd(arrow::compute::FunctionRegistry& registry);
void RegisterAggrAdd(IBuiltinFunctionRegistry& registry);
void RegisterSub(IBuiltinFunctionRegistry& registry);
void RegisterSub(arrow::compute::FunctionRegistry& registry);
void RegisterMul(IBuiltinFunctionRegistry& registry);
void RegisterMul(arrow::compute::FunctionRegistry& registry);
void RegisterDiv(IBuiltinFunctionRegistry& registry);
void RegisterDiv(arrow::compute::FunctionRegistry& registry);
void RegisterMod(IBuiltinFunctionRegistry& registry);
void RegisterMod(arrow::compute::FunctionRegistry& registry);
void RegisterIncrement(IBuiltinFunctionRegistry& registry);
void RegisterDecrement(IBuiltinFunctionRegistry& registry);
void RegisterBitAnd(IBuiltinFunctionRegistry& registry);
void RegisterBitOr(IBuiltinFunctionRegistry& registry);
void RegisterBitXor(IBuiltinFunctionRegistry& registry);
void RegisterShiftLeft(IBuiltinFunctionRegistry& registry);
void RegisterShiftRight(IBuiltinFunctionRegistry& registry);
void RegisterRotLeft(IBuiltinFunctionRegistry& registry);
void RegisterRotRight(IBuiltinFunctionRegistry& registry);
void RegisterPlus(IBuiltinFunctionRegistry& registry);
void RegisterMinus(IBuiltinFunctionRegistry& registry);
void RegisterBitNot(IBuiltinFunctionRegistry& registry);
void RegisterCountBits(IBuiltinFunctionRegistry& registry);
void RegisterAbs(IBuiltinFunctionRegistry& registry);
void RegisterConvert(IBuiltinFunctionRegistry& registry);
void RegisterConcat(IBuiltinFunctionRegistry& registry);
void RegisterSubstring(IBuiltinFunctionRegistry& registry);
void RegisterFind(IBuiltinFunctionRegistry& registry);
void RegisterInversePresortString(IBuiltinFunctionRegistry& registry);
void RegisterInverseString(IBuiltinFunctionRegistry& registry);
void RegisterNanvl(IBuiltinFunctionRegistry& registry);
void RegisterByteAt(IBuiltinFunctionRegistry& registry);
void RegisterMax(IBuiltinFunctionRegistry& registry);
void RegisterMin(IBuiltinFunctionRegistry& registry);
void RegisterAggrMax(IBuiltinFunctionRegistry& registry);
void RegisterAggrMin(IBuiltinFunctionRegistry& registry);
void RegisterWith(IBuiltinFunctionRegistry& registry);

void AddFunction(arrow::compute::FunctionRegistry& registry, const std::shared_ptr<arrow::compute::ScalarFunction>& f);

inline arrow::internal::Bitmap GetBitmap(const arrow::ArrayData& arr, int index) {
    return arrow::internal::Bitmap{ arr.buffers[index], arr.offset, arr.length };
}

template <typename T>
std::shared_ptr<arrow::DataType> GetPrimitiveDataType();

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<bool>() {
    return arrow::boolean();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i8>() {
    return arrow::int8();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui8>() {
    return arrow::uint8();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i16>() {
    return arrow::int16();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui16>() {
    return arrow::uint16();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i32>() {
    return arrow::int32();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui32>() {
    return arrow::uint32();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i64>() {
    return arrow::int64();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui64>() {
    return arrow::uint64();
}

template <typename T>
arrow::compute::InputType GetPrimitiveInputArrowType(bool isScalar) {
    return arrow::compute::InputType(GetPrimitiveDataType<T>(), isScalar ? arrow::ValueDescr::SCALAR : arrow::ValueDescr::ARRAY);
}

template <typename T>
arrow::compute::OutputType GetPrimitiveOutputArrowType() {
    return arrow::compute::OutputType(GetPrimitiveDataType<T>());
}

template <typename T>
T GetPrimitiveScalarValue(const arrow::Scalar& scalar) {
    return *static_cast<const T*>(dynamic_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data());
}

template <typename T>
arrow::Datum MakeScalarDatum(T value);

template <>
inline arrow::Datum MakeScalarDatum<bool>(bool value) {
    return arrow::Datum(std::make_shared<arrow::BooleanScalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<i8>(i8 value) {
    return arrow::Datum(std::make_shared<arrow::Int8Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<ui8>(ui8 value) {
    return arrow::Datum(std::make_shared<arrow::UInt8Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<i16>(i16 value) {
    return arrow::Datum(std::make_shared<arrow::Int16Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<ui16>(ui16 value) {
    return arrow::Datum(std::make_shared<arrow::UInt16Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<i32>(i32 value) {
    return arrow::Datum(std::make_shared<arrow::Int32Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<ui32>(ui32 value) {
    return arrow::Datum(std::make_shared<arrow::UInt32Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<i64>(i64 value) {
    return arrow::Datum(std::make_shared<arrow::Int64Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<ui64>(ui64 value) {
    return arrow::Datum(std::make_shared<arrow::UInt64Scalar>(value));
}

template<typename TInput1, typename TInput2, typename TOutput,
    template<typename, typename, typename> class TFunc, bool DefaultNulls>
struct TBinaryKernelExecs;

template<typename TInput1, typename TInput2, typename TOutput,
        template<typename, typename, typename> class TFunc>
struct TBinaryKernelExecs<TInput1, TInput2, TOutput, TFunc, true>
{
    using TFuncInstance = TFunc<TInput1, TInput2, TOutput>;

    static arrow::Status ExecScalarScalar(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        if (!arg1.scalar()->is_valid || !arg2.scalar()->is_valid) {
            *res = arrow::MakeNullScalar(GetPrimitiveDataType<TOutput>());
        } else {
            const auto val1 = GetPrimitiveScalarValue<TInput1>(*arg1.scalar());
            const auto val2 = GetPrimitiveScalarValue<TInput2>(*arg2.scalar());
            *res = MakeScalarDatum<TOutput>(TFuncInstance::Do(val1, val2));
        }

        return arrow::Status::OK();
    }

    static arrow::Status ExecScalarArray(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        auto& resArr = *res->array();
        if (arg1.scalar()->is_valid) {
            const auto val1 = GetPrimitiveScalarValue<TInput1>(*arg1.scalar());
            const auto& arr2 = *arg2.array();
            auto length = arr2.length;
            const auto values2 = arr2.GetValues<TInput2>(1);
            auto resValues = resArr.GetMutableValues<TOutput>(1);
            for (int64_t i = 0; i < length; ++i) {
                resValues[i] = TFuncInstance::Do(val1, values2[i]);
            }
        }

        return arrow::Status::OK();
    }

    static arrow::Status ExecArrayScalar(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        auto& resArr = *res->array();
        if (arg2.scalar()->is_valid) {
            const auto& arr1 = *arg1.array();
            auto length = arr1.length;
            const auto values1 = arr1.GetValues<TInput1>(1);
            auto resValues = resArr.GetMutableValues<TOutput>(1);
            const auto val2 = GetPrimitiveScalarValue<TInput2>(*arg2.scalar());
            for (int64_t i = 0; i < length; ++i) {
                resValues[i] = TFuncInstance::Do(values1[i], val2);
            }
        }

        return arrow::Status::OK();
    }

    static arrow::Status ExecArrayArray(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        const auto& arr1 = *arg1.array();
        const auto& arr2 = *arg2.array();
        auto& resArr = *res->array();
        MKQL_ENSURE(arr1.length == arr2.length, "Expected same length");
        auto length = arr1.length;
        const auto values1 = arr1.GetValues<TInput1>(1);
        const auto values2 = arr2.GetValues<TInput2>(1);
        auto resValues = resArr.GetMutableValues<TOutput>(1);
        for (int64_t i = 0; i < length; ++i) {
            resValues[i] = TFuncInstance::Do(values1[i], values2[i]);
        }

        return arrow::Status::OK();
    }
};

template<typename TInput1, typename TInput2, typename TOutput,
        template<typename, typename, typename> class TFunc>
struct TBinaryKernelExecs<TInput1, TInput2, TOutput, TFunc, false>
{
    using TFuncInstance = TFunc<TInput1, TInput2, TOutput>;

    static arrow::Status ExecScalarScalar(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        if (!arg1.scalar()->is_valid || !arg2.scalar()->is_valid) {
            *res = arrow::MakeNullScalar(GetPrimitiveDataType<TOutput>());
        } else {
            const auto val1 = GetPrimitiveScalarValue<TInput1>(*arg1.scalar());
            const auto val2 = GetPrimitiveScalarValue<TInput2>(*arg2.scalar());
            auto podRes = TFuncInstance::Execute(NUdf::TUnboxedValuePod(val1), NUdf::TUnboxedValuePod(val2));
            if (!podRes) {
                *res = arrow::MakeNullScalar(GetPrimitiveDataType<TOutput>());
            } else {
                *res = MakeScalarDatum<TOutput>(podRes.template Get<TOutput>());
            }
        }

        return arrow::Status::OK();
    }

    static arrow::Status ExecScalarArray(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        const auto& arr2 = *arg2.array();
        auto& resArr = *res->array();
        if (arg1.scalar()->is_valid) {
            const auto val1 = GetPrimitiveScalarValue<TInput1>(*arg1.scalar());
            auto length = arr2.length;
            const auto values2 = arr2.GetValues<TInput2>(1);
            const auto valid2 = arr2.GetValues<uint8_t>(0);
            const auto nullCount2 = arr2.GetNullCount();
            auto resValues = resArr.GetMutableValues<TOutput>(1);
            auto resValid = resArr.GetMutableValues<uint8_t>(0);

            for (int64_t i = 0; i < length; ++i) {
                if (nullCount2 == 0 || arrow::BitUtil::GetBit(valid2, i + arr2.offset)) {
                    auto podRes = TFuncInstance::Execute(NUdf::TUnboxedValuePod(val1), NUdf::TUnboxedValuePod(values2[i]));
                    if (podRes) {
                        resValues[i] = podRes.template Get<TOutput>();
                        arrow::BitUtil::SetBit(resValid, i + resArr.offset);
                        continue;
                    }
                }
                
                arrow::BitUtil::ClearBit(resValid, i + resArr.offset);
            }
        } else {
            GetBitmap(resArr, 0).SetBitsTo(false);
        }

        return arrow::Status::OK();
    }

    static arrow::Status ExecArrayScalar(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        const auto& arr1 = *arg1.array();
        auto& resArr = *res->array();
        if (arg2.scalar()->is_valid) {
            const auto val2 = GetPrimitiveScalarValue<TInput1>(*arg2.scalar());
            auto length = arr1.length;
            const auto values1 = arr1.GetValues<TInput2>(1);
            const auto valid1 = arr1.GetValues<uint8_t>(0);
            const auto nullCount1 = arr1.GetNullCount();
            auto resValues = resArr.GetMutableValues<TOutput>(1);
            auto resValid = resArr.GetMutableValues<uint8_t>(0);

            for (int64_t i = 0; i < length; ++i) {
                if (nullCount1 == 0 || arrow::BitUtil::GetBit(valid1, i + arr1.offset)) {
                    auto podRes = TFuncInstance::Execute(NUdf::TUnboxedValuePod(values1[i]), NUdf::TUnboxedValuePod(val2));
                    if (podRes) {
                        resValues[i] = podRes.template Get<TOutput>();
                        arrow::BitUtil::SetBit(resValid, i + resArr.offset);
                        continue;
                    }
                }

                arrow::BitUtil::ClearBit(resValid, i + resArr.offset);
            }
        } else {
            GetBitmap(resArr, 0).SetBitsTo(false);
        }

        return arrow::Status::OK();
    }

    static arrow::Status ExecArrayArray(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        const auto& arr1 = *arg1.array();
        const auto& arr2 = *arg2.array();
        MKQL_ENSURE(arr1.length == arr2.length, "Expected same length");
        auto length = arr1.length;
        const auto values1 = arr1.GetValues<TInput1>(1);
        const auto valid1 = arr1.GetValues<uint8_t>(0);
        const auto nullCount1 = arr1.GetNullCount();
        const auto values2 = arr2.GetValues<TInput2>(1);
        const auto valid2 = arr2.GetValues<uint8_t>(0);
        const auto nullCount2 = arr2.GetNullCount();
        auto& resArr = *res->array();
        auto resValues = resArr.GetMutableValues<TOutput>(1);
        auto resValid = resArr.GetMutableValues<uint8_t>(0);
        for (int64_t i = 0; i < length; ++i) {
            if ((nullCount1 == 0 || arrow::BitUtil::GetBit(valid1, i + arr1.offset)) &&
                (nullCount2 == 0 || arrow::BitUtil::GetBit(valid2, i + arr2.offset))) {
                auto podRes = TFuncInstance::Execute(NUdf::TUnboxedValuePod(values1[i]), NUdf::TUnboxedValuePod(values2[i]));
                if (podRes) {
                    resValues[i] = podRes.template Get<TOutput>();
                    arrow::BitUtil::SetBit(resValid, i + resArr.offset);
                    continue;
                }
            }

            arrow::BitUtil::ClearBit(resValid, i + resArr.offset);
        }

        return arrow::Status::OK();
    }
};

template<typename TInput1, typename TInput2, typename TOutput,
    template<typename, typename, typename> class TFunc>
void AddBinaryKernel(arrow::compute::ScalarFunction& function) {
    using TFuncInstance = TFunc<TInput1, TInput2, TOutput>;
    using TExecs = TBinaryKernelExecs<TInput1, TInput2, TOutput, TFunc, TFuncInstance::DefaultNulls>;
    auto nullHandling = TFuncInstance::DefaultNulls ? arrow::compute::NullHandling::INTERSECTION : arrow::compute::NullHandling::COMPUTED_PREALLOCATE;

    arrow::compute::ScalarKernel ss({GetPrimitiveInputArrowType<TInput1>(true), GetPrimitiveInputArrowType<TInput2>(true) }, GetPrimitiveOutputArrowType<TOutput>(), &TExecs::ExecScalarScalar);
    ss.null_handling = nullHandling;
    ARROW_OK(function.AddKernel(ss));

    arrow::compute::ScalarKernel sa({ GetPrimitiveInputArrowType<TInput1>(true), GetPrimitiveInputArrowType<TInput2>(false) }, GetPrimitiveOutputArrowType<TOutput>(), &TExecs::ExecScalarArray);
    sa.null_handling = nullHandling;
    ARROW_OK(function.AddKernel(sa));

    arrow::compute::ScalarKernel as({ GetPrimitiveInputArrowType<TInput1>(false), GetPrimitiveInputArrowType<TInput2>(true) }, GetPrimitiveOutputArrowType<TOutput>(), &TExecs::ExecArrayScalar);
    as.null_handling = nullHandling;
    ARROW_OK(function.AddKernel(as));

    arrow::compute::ScalarKernel aa({ GetPrimitiveInputArrowType<TInput1>(false), GetPrimitiveInputArrowType<TInput2>(false) }, GetPrimitiveOutputArrowType<TOutput>(), &TExecs::ExecArrayArray);
    aa.null_handling = nullHandling;
    ARROW_OK(function.AddKernel(aa));
}

template<template<typename, typename, typename> class TFunc>
void AddBinaryIntegralKernels(arrow::compute::ScalarFunction& function) {
    AddBinaryKernel<ui8, ui8, ui8, TFunc>(function);
    AddBinaryKernel<ui8, i8, i8, TFunc>(function);
    AddBinaryKernel<ui8, ui16, ui16, TFunc>(function);
    AddBinaryKernel<ui8, i16, i16, TFunc>(function);
    AddBinaryKernel<ui8, ui32, ui32, TFunc>(function);
    AddBinaryKernel<ui8, i32, i32, TFunc>(function);
    AddBinaryKernel<ui8, ui64, ui64, TFunc>(function);
    AddBinaryKernel<ui8, i64, i64, TFunc>(function);

    AddBinaryKernel<i8, ui8, i8, TFunc>(function);
    AddBinaryKernel<i8, i8, i8, TFunc>(function);
    AddBinaryKernel<i8, ui16, ui16, TFunc>(function);
    AddBinaryKernel<i8, i16, i16, TFunc>(function);
    AddBinaryKernel<i8, ui32, ui32, TFunc>(function);
    AddBinaryKernel<i8, i32, i32, TFunc>(function);
    AddBinaryKernel<i8, ui64, ui64, TFunc>(function);
    AddBinaryKernel<i8, i64, i64, TFunc>(function);

    AddBinaryKernel<ui16, ui8, ui16, TFunc>(function);
    AddBinaryKernel<ui16, i8, ui16, TFunc>(function);
    AddBinaryKernel<ui16, ui16, ui16, TFunc>(function);
    AddBinaryKernel<ui16, i16, i16, TFunc>(function);
    AddBinaryKernel<ui16, ui32, ui32, TFunc>(function);
    AddBinaryKernel<ui16, i32, i32, TFunc>(function);
    AddBinaryKernel<ui16, ui64, ui64, TFunc>(function);
    AddBinaryKernel<ui16, i64, i64, TFunc>(function);

    AddBinaryKernel<i16, ui8, i16, TFunc>(function);
    AddBinaryKernel<i16, i8, i16, TFunc>(function);
    AddBinaryKernel<i16, ui16, i16, TFunc>(function);
    AddBinaryKernel<i16, i16, i16, TFunc>(function);
    AddBinaryKernel<i16, ui32, ui32, TFunc>(function);
    AddBinaryKernel<i16, i32, i32, TFunc>(function);
    AddBinaryKernel<i16, ui64, ui64, TFunc>(function);
    AddBinaryKernel<i16, i64, i64, TFunc>(function);

    AddBinaryKernel<ui32, ui8, ui32, TFunc>(function);
    AddBinaryKernel<ui32, i8, ui32, TFunc>(function);
    AddBinaryKernel<ui32, ui16, ui32, TFunc>(function);
    AddBinaryKernel<ui32, i16, ui32, TFunc>(function);
    AddBinaryKernel<ui32, ui32, ui32, TFunc>(function);
    AddBinaryKernel<ui32, i32, i32, TFunc>(function);
    AddBinaryKernel<ui32, ui64, ui64, TFunc>(function);
    AddBinaryKernel<ui32, i64, i64, TFunc>(function);

    AddBinaryKernel<i32, ui8, i32, TFunc>(function);
    AddBinaryKernel<i32, i8, i32, TFunc>(function);
    AddBinaryKernel<i32, ui16, i32, TFunc>(function);
    AddBinaryKernel<i32, i16, i32, TFunc>(function);
    AddBinaryKernel<i32, ui32, i32, TFunc>(function);
    AddBinaryKernel<i32, i32, i32, TFunc>(function);
    AddBinaryKernel<i32, ui64, ui64, TFunc>(function);
    AddBinaryKernel<i32, i64, i64, TFunc>(function);

    AddBinaryKernel<ui64, ui8, ui64, TFunc>(function);
    AddBinaryKernel<ui64, i8, ui64, TFunc>(function);
    AddBinaryKernel<ui64, ui16, ui64, TFunc>(function);
    AddBinaryKernel<ui64, i16, ui64, TFunc>(function);
    AddBinaryKernel<ui64, ui32, ui64, TFunc>(function);
    AddBinaryKernel<ui64, i32, ui64, TFunc>(function);
    AddBinaryKernel<ui64, ui64, ui64, TFunc>(function);
    AddBinaryKernel<ui64, i64, i64, TFunc>(function);

    AddBinaryKernel<i64, ui8, i64, TFunc>(function);
    AddBinaryKernel<i64, i8, i64, TFunc>(function);
    AddBinaryKernel<i64, ui16, i64, TFunc>(function);
    AddBinaryKernel<i64, i16, i64, TFunc>(function);
    AddBinaryKernel<i64, ui32, i64, TFunc>(function);
    AddBinaryKernel<i64, i32, i64, TFunc>(function);
    AddBinaryKernel<i64, ui64, i64, TFunc>(function);
    AddBinaryKernel<i64, i64, i64, TFunc>(function);
}

template<template<typename, typename, typename> class TFunc>
class TBinaryNumericFunction : public arrow::compute::ScalarFunction {
public:
    TBinaryNumericFunction(const std::string& name)
        : ScalarFunction(name, arrow::compute::Arity::Binary(), nullptr)
    {
        AddBinaryIntegralKernels<TFunc>(*this);
    }
};

}
}
