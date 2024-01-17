#include "mkql_builtins_impl.h"  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool StartOptional, bool CountOptional>
struct TSubString {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod string, NUdf::TUnboxedValuePod start, NUdf::TUnboxedValuePod count)
    {
        return SubString(string,
            StartOptional && !start ? std::numeric_limits<ui32>::min() : start.Get<ui32>(),
            CountOptional && !count ? std::numeric_limits<ui32>::max() : count.Get<ui32>()
        );
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* string, Value* st, Value* cn, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(SubString));
        const auto start = StartOptional ?
            SelectInst::Create(
                IsEmpty(st, block),
                ConstantInt::get(GetTypeFor<ui32>(context), std::numeric_limits<ui32>::min()),
                GetterFor<ui32>(st, context, block), "start", block
            ):
            GetterFor<ui32>(st, context, block);
        const auto count = CountOptional ?
            SelectInst::Create(
                IsEmpty(cn, block),
                ConstantInt::get(GetTypeFor<ui32>(context), std::numeric_limits<ui32>::max()),
                GetterFor<ui32>(cn, context, block), "count", block
            ):
            GetterFor<ui32>(cn, context, block);
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(string->getType(), {string->getType(), start->getType(), count->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "func", block);
            const auto result = CallInst::Create(funType, funcPtr, {string, start, count}, "substring", block);
            return result;
        } else {
            const auto ptrArg = new AllocaInst(string->getType(), 0U, "arg", block);
            const auto ptrResult = new AllocaInst(string->getType(), 0U, "result", block);
            new StoreInst(string, ptrArg, block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {ptrResult->getType(), ptrArg->getType(), start->getType(), count->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "func", block);
            CallInst::Create(funType, funcPtr, {ptrResult, ptrArg, start, count}, "", block);
            const auto result = new LoadInst(string->getType(), ptrResult, "substring", block);
            return result;
        }
    }
#endif
};

template <typename TInput>
void RegisterSubstringnOpt(IBuiltinFunctionRegistry& registry, const char* name) {
    RegisterFunctionImpl<TSubString<false, false>, TTernaryArgs<TInput, TInput, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, false, false, false>, TTernaryWrap<false>>(registry, name);
    RegisterFunctionImpl<TSubString<false, false>, TTernaryArgs<TInput, TInput, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, true, false, false>, TTernaryWrap<true>>(registry, name);

    RegisterFunctionImpl<TSubString<true, false>, TTernaryArgs<TInput, TInput, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, false, true, false>, TTernaryWrap<false>>(registry, name);
    RegisterFunctionImpl<TSubString<true, false>, TTernaryArgs<TInput, TInput, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, true, true, false>, TTernaryWrap<true>>(registry, name);

    RegisterFunctionImpl<TSubString<false, true>, TTernaryArgs<TInput, TInput, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, false, false, true>, TTernaryWrap<false>>(registry, name);
    RegisterFunctionImpl<TSubString<false, true>, TTernaryArgs<TInput, TInput, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, true, false, true>, TTernaryWrap<true>>(registry, name);

    RegisterFunctionImpl<TSubString<true, true>, TTernaryArgs<TInput, TInput, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, false, true, true>, TTernaryWrap<false>>(registry, name);
    RegisterFunctionImpl<TSubString<true, true>, TTernaryArgs<TInput, TInput, NUdf::TDataType<ui32>, NUdf::TDataType<ui32>, true, true, true>, TTernaryWrap<true>>(registry, name);
}

}

void RegisterSubstring(IBuiltinFunctionRegistry& registry) {
    RegisterSubstringnOpt<NUdf::TDataType<char*>>(registry, "Substring");
    RegisterSubstringnOpt<NUdf::TDataType<NUdf::TUtf8>>(registry, "Substring");
}

} // namespace NMiniKQL
} // namespace NKikimr
