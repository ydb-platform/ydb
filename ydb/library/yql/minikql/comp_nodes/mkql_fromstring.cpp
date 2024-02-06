#include "mkql_fromstring.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins_decimal.h> // Y_IGNORE

#include <ydb/library/yql/public/udf/udf_terminator.h>

#ifndef MKQL_DISABLE_CODEGEN
Y_PRAGMA_DIAGNOSTIC_PUSH
Y_PRAGMA("GCC diagnostic ignored \"-Wreturn-type-c-linkage\"")
extern "C" NKikimr::NUdf::TUnboxedValuePod DataFromString(const NKikimr::NUdf::TUnboxedValuePod data, NKikimr::NUdf::EDataSlot slot) {
    return NKikimr::NMiniKQL::ValueFromString(slot, data.AsStringRef());
}

extern "C" NYql::NDecimal::TInt128 DecimalFromString(const NKikimr::NUdf::TUnboxedValuePod decimal, ui8 precision, ui8 scale) {
    return NYql::NDecimal::FromStringEx(decimal.AsStringRef(), precision, scale);
}
Y_PRAGMA_DIAGNOSTIC_POP
#endif

namespace NKikimr {
namespace NMiniKQL {

namespace {

const unsigned ERROR_FRAGMENT_LIMIT = 5000;

[[noreturn]]
void ThrowConvertError(NYql::NUdf::TStringRef data, TStringBuf type) {
    TStringBuilder builder;
    builder << "could not convert \"";
    if (data.Size() < ERROR_FRAGMENT_LIMIT) {
        builder << data << "\"";
    } else {
        builder << TStringBuf(data.Data(), ERROR_FRAGMENT_LIMIT) << "\" (truncated)";
    }
    builder << " to " << type;
    UdfTerminate(builder.data());
}

template <bool IsStrict, bool IsOptional>
class TDecimalFromStringWrapper : public TMutableCodegeneratorNode<TDecimalFromStringWrapper<IsStrict, IsOptional>> {
    typedef TMutableCodegeneratorNode<TDecimalFromStringWrapper<IsStrict, IsOptional>> TBaseComputation;
public:
    TDecimalFromStringWrapper(TComputationMutables& mutables, IComputationNode* data, ui8 precision, ui8 scale)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Data(data)
        , Precision(precision)
        , Scale(scale)
    {
        MKQL_ENSURE(precision > 0 && precision <= NYql::NDecimal::MaxPrecision, "Wrong precision.");
        MKQL_ENSURE(scale <= precision, "Wrong scale.");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& data = Data->GetValue(ctx);
        if (IsOptional && !data) {
            return NUdf::TUnboxedValuePod();
        }

        if (const auto v = NYql::NDecimal::FromStringEx(data.AsStringRef(), Precision, Scale); !NYql::NDecimal::IsError(v)) {
            return NUdf::TUnboxedValuePod(v);
        }

        if constexpr (IsStrict) {
            Throw(data, Precision, Scale);
        } else {
            return NUdf::TUnboxedValuePod();
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);
        const auto psType = Type::getInt8Ty(context);
        const auto valTypePtr = PointerType::getUnqual(valType);

        const auto name = "DecimalFromString";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DecimalFromString));
        const auto fnType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(valType, { valType, psType, psType }, false):
            FunctionType::get(Type::getVoidTy(context), { valTypePtr, valTypePtr, psType, psType }, false);
        const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);

        const auto zero = ConstantInt::get(valType, 0ULL);
        const auto precision = ConstantInt::get(psType, Precision);
        const auto scale = ConstantInt::get(psType, Scale);
        const auto value = GetNodeValue(Data, ctx, block);

        const auto fail = BasicBlock::Create(context, "fail", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        const auto ways = (IsOptional ? 1U : 0U) + (IsStrict ? 0U : 1U);
        const auto last = ways > 0U ? BasicBlock::Create(context, "last", ctx.Func) : nullptr;
        const auto phi = last ? PHINode::Create(valType, ways + 1U, "result", last) : nullptr;

        if constexpr (IsOptional) {
            phi->addIncoming(zero, block);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, zero, "check", block);

            const auto call = BasicBlock::Create(context, "call", ctx.Func);
            BranchInst::Create(last, call, check, block);
            block = call;
        }

        Value* decimal;
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            decimal = CallInst::Create(func, { value, precision, scale }, "from_string", block);
        } else {
            const auto retPtr = new AllocaInst(valType, 0U, "ret_ptr", block);
            new StoreInst(value, retPtr, block);
            CallInst::Create(func, { retPtr, retPtr, precision, scale }, "", block);
            decimal = new LoadInst(valType, retPtr, "res", block);
        }

        if (Data->IsTemporaryValue())
            ValueCleanup(Data->GetRepresentation(), value, ctx, block);

        const auto test = NDecimal::GenIsError(decimal, context, block);
        BranchInst::Create(fail, good, test, block);

        {
            block = fail;
            if constexpr (IsStrict) {
                const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TDecimalFromStringWrapper::Throw));
                const auto doFuncType = FunctionType::get(Type::getVoidTy(context), {valType, psType, psType}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(doFuncType), "thrower", block);
                CallInst::Create(doFuncType, doFuncPtr, { value, precision, scale }, "", block);
                new UnreachableInst(context, block);
            } else {
                phi->addIncoming(zero, block);
                BranchInst::Create(last, block);
            }
        }

        block = good;

        if constexpr (IsOptional || !IsStrict) {
            phi->addIncoming(SetterForInt128(decimal, block), block);
            BranchInst::Create(last, block);

            block = last;
            return phi;
        } else {
            return SetterForInt128(decimal, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    [[noreturn]] static void Throw(const NUdf::TUnboxedValuePod data, ui8 precision, ui8 scale) {
        const TString type = TStringBuilder() << "Decimal(" << unsigned(precision) << ", " << unsigned(scale) << ")";
        ThrowConvertError(data.AsStringRef(), type);
    }

    IComputationNode* const Data;
    const ui8 Precision, Scale;
};

template <bool IsStrict, bool IsOptional>
class TFromStringWrapper : public TMutableCodegeneratorNode<TFromStringWrapper<IsStrict, IsOptional>> {
    typedef TMutableCodegeneratorNode<TFromStringWrapper<IsStrict, IsOptional>> TBaseComputation;
public:
    TFromStringWrapper(TComputationMutables& mutables, IComputationNode* data, NUdf::TDataTypeId schemeType)
        : TBaseComputation(mutables, GetValueRepresentation(schemeType))
        , Data(data)
        , SchemeType(NUdf::GetDataSlot(schemeType))
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        const auto& data = Data->GetValue(ctx);
        if (IsOptional && !data) {
            return NUdf::TUnboxedValuePod();
        }

        if (const auto out = ValueFromString(SchemeType, data.AsStringRef())) {
            return out;
        }

        if constexpr (IsStrict) {
            Throw(data, SchemeType);
        } else {
            return NUdf::TUnboxedValuePod();
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);
        const auto slotType = Type::getInt32Ty(context);
        const auto valTypePtr = PointerType::getUnqual(valType);

        const auto name = "DataFromString";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DataFromString));
        const auto fnType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(valType, { valType, slotType }, false):
            FunctionType::get(Type::getVoidTy(context), { valTypePtr, valTypePtr, slotType }, false);
        const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);

        const auto zero = ConstantInt::get(valType, 0ULL);
        const auto slot = ConstantInt::get(slotType, static_cast<ui32>(SchemeType));
        const auto value = GetNodeValue(Data, ctx, block);

        const auto fail = IsStrict ? BasicBlock::Create(context, "fail", ctx.Func) : nullptr;
        const auto last = IsOptional || fail ? BasicBlock::Create(context, "last", ctx.Func) : nullptr;
        const auto phi = IsOptional ? PHINode::Create(valType, 2U, "result", last) : nullptr;

        if constexpr (IsOptional) {
            phi->addIncoming(zero, block);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, zero, "check", block);

            const auto call = BasicBlock::Create(context, "call", ctx.Func);
            BranchInst::Create(last, call, check, block);
            block = call;
        }

        Value* data;
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            data = CallInst::Create(func, { value, slot }, "from_string", block);
        } else {
            const auto retPtr = new AllocaInst(valType, 0U, "ret_ptr", block);
            new StoreInst(value, retPtr, block);
            CallInst::Create(func, { retPtr, retPtr, slot }, "", block);
            data = new LoadInst(valType, retPtr, "res", block);
        }

        if (Data->IsTemporaryValue())
            ValueCleanup(Data->GetRepresentation(), value, ctx, block);

        if constexpr (IsOptional) {
            phi->addIncoming(data, block);
        }

        if constexpr (IsStrict) {
            const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, data, zero, "test", block);
            BranchInst::Create(fail, last, test, block);

            block = fail;
            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TFromStringWrapper::Throw));
            const auto doFuncType = FunctionType::get(Type::getVoidTy(context), {valType, slotType}, false);
            const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(doFuncType), "thrower", block);
            CallInst::Create(doFuncType, doFuncPtr, { value, slot }, "", block);
            new UnreachableInst(context, block);
        } else if constexpr (IsOptional) {
            BranchInst::Create(last, block);
        }

        if constexpr (IsOptional || IsStrict) {
            block = last;
        }

        return IsOptional ? phi : data;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    [[noreturn]] static void Throw(const NUdf::TUnboxedValuePod data, NUdf::EDataSlot slot) {
        ThrowConvertError(data.AsStringRef(), NUdf::GetDataTypeInfo(slot).Name);
    }

    IComputationNode* const Data;
    const NUdf::EDataSlot SchemeType;
};

}

IComputationNode* WrapFromString(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 2, "Expected 2 args");

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<char*>::Id || dataType->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id, "Expected String");

    const auto schemeTypeData = AS_VALUE(TDataLiteral, callable.GetInput(1));
    const auto schemeType = schemeTypeData->AsValue().Get<ui32>();

    const auto data = LocateNode(ctx.NodeLocator, callable, 0);
    if (NUdf::TDataType<NUdf::TDecimal>::Id == schemeType) {
        MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
        const auto precision = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().Get<ui8>();
        const auto scale = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui8>();

        if (isOptional) {
            return new TDecimalFromStringWrapper<false, true>(ctx.Mutables, data, precision, scale);
        } else {
            return new TDecimalFromStringWrapper<false, false>(ctx.Mutables, data, precision, scale);
        }
    } else {
        MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
        if (isOptional) {
            return new TFromStringWrapper<false, true>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType));
        } else {
            return new TFromStringWrapper<false, false>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType));
        }
    }
}

IComputationNode* WrapStrictFromString(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 2, "Expected 2 args");

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<char*>::Id || dataType->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id, "Expected String");

    const auto schemeTypeData = AS_VALUE(TDataLiteral, callable.GetInput(1));
    const auto schemeType = schemeTypeData->AsValue().Get<ui32>();

    const auto data = LocateNode(ctx.NodeLocator, callable, 0);
    if (NUdf::TDataType<NUdf::TDecimal>::Id == schemeType) {
        MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
        const auto precision = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().Get<ui8>();
        const auto scale = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui8>();

        if (isOptional) {
            return new TDecimalFromStringWrapper<true, true>(ctx.Mutables, data, precision, scale);
        } else {
            return new TDecimalFromStringWrapper<true, false>(ctx.Mutables, data, precision, scale);
        }
    } else {
        MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

        if (isOptional) {
            return new TFromStringWrapper<true, true>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType));
        } else {
            return new TFromStringWrapper<true, false>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType));
        }
    }
}

}
}
