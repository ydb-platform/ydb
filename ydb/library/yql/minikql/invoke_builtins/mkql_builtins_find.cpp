#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool Reverse>
NUdf::TUnboxedValuePod Find(const NUdf::TUnboxedValuePod haystack, const NUdf::TUnboxedValuePod needle, std::string_view::size_type pos) {
    const std::string_view& one(haystack.AsStringRef());
    const std::string_view& two(needle.AsStringRef());
    const auto f = Reverse ? one.rfind(two, pos): one.find(two, pos);
    return std::string_view::npos == f ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(ui32(f));
}

template <bool PosOptional, bool Reverse>
struct TFind {
    static constexpr std::string_view::size_type DefaultPos = Reverse ? std::string_view::npos : 0;

    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod string, NUdf::TUnboxedValuePod sub, NUdf::TUnboxedValuePod pos)
    {
        return Find<Reverse>(string, sub, PosOptional && !pos ? DefaultPos : std::string_view::size_type(pos.Get<ui32>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* string, Value* sub, Value* p, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(Find<Reverse>));
        const auto pos = PosOptional ?
            SelectInst::Create(
                IsEmpty(p, block),
                ConstantInt::get(GetTypeFor<std::string_view::size_type>(context), DefaultPos),
                StaticCast<ui32, std::string_view::size_type>(GetterFor<ui32>(p, context, block), context, block),
            "pos", block):
            StaticCast<ui32, std::string_view::size_type>(GetterFor<ui32>(p, context, block), context, block);
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(string->getType(), {string->getType(), sub->getType(), pos->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "func", block);
            const auto result = CallInst::Create(funType, funcPtr, {string, sub, pos}, "find", block);
            return result;
        } else {
            const auto ptrArg = new AllocaInst(string->getType(), 0U, "arg", block);
            const auto ptrSub = new AllocaInst(sub->getType(), 0U, "sub", block);
            const auto ptrResult = new AllocaInst(string->getType(), 0U, "result", block);
            new StoreInst(string, ptrArg, block);
            new StoreInst(sub, ptrSub, block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {ptrResult->getType(), ptrArg->getType(), ptrSub->getType(), pos->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "func", block);
            CallInst::Create(funType, funcPtr, {ptrResult, ptrArg, ptrSub, pos}, "", block);
            const auto result = new LoadInst(string->getType(), ptrResult, "find", block);
            return result;
        }
    }
#endif
};

template <typename TInput, bool Reverse>
void RegisterFindOpt(IBuiltinFunctionRegistry& registry, const char* name) {
    RegisterFunctionImpl<TFind<false, Reverse>, TTernaryArgs<NUdf::TDataType<ui32>, TInput, TInput, NUdf::TDataType<ui32>, false, false, false, true>, TTernaryWrap<false>>(registry, name);
    RegisterFunctionImpl<TFind<false, Reverse>, TTernaryArgs<NUdf::TDataType<ui32>, TInput, TInput, NUdf::TDataType<ui32>, true, false, false, true>, TTernaryWrap<true>>(registry, name);

    RegisterFunctionImpl<TFind<true, Reverse>, TTernaryArgs<NUdf::TDataType<ui32>, TInput, TInput, NUdf::TDataType<ui32>, false, false, true, true>, TTernaryWrap<false>>(registry, name);
    RegisterFunctionImpl<TFind<true, Reverse>, TTernaryArgs<NUdf::TDataType<ui32>, TInput, TInput, NUdf::TDataType<ui32>, true, false, true, true>, TTernaryWrap<true>>(registry, name);
}

}

void RegisterFind(IBuiltinFunctionRegistry& registry) {
    RegisterFindOpt<NUdf::TDataType<char*>, false>(registry, "Find");
    RegisterFindOpt<NUdf::TDataType<NUdf::TUtf8>, false>(registry, "Find");

    RegisterFindOpt<NUdf::TDataType<char*>, true>(registry, "RFind");
    RegisterFindOpt<NUdf::TDataType<NUdf::TUtf8>, true>(registry, "RFind");
}

} // namespace NMiniKQL
} // namespace NKikimr
