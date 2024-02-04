#include "mkql_tostring.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <ydb/library/yql/public/udf/udf_terminator.h>

#ifndef MKQL_DISABLE_CODEGEN
Y_PRAGMA_DIAGNOSTIC_PUSH
Y_PRAGMA("GCC diagnostic ignored \"-Wreturn-type-c-linkage\"")
extern "C" NYql::NUdf::TUnboxedValuePod DataToString(NYql::NUdf::TUnboxedValuePod data, NYql::NUdf::EDataSlot slot) {
    return NKikimr::NMiniKQL::ValueToString(slot, data);
}

extern "C" NYql::NUdf::TUnboxedValuePod DecimalToString(NYql::NDecimal::TInt128 decimal, ui8 precision, ui8 scale) {
    if (const auto str = NYql::NDecimal::ToString(decimal, precision, scale)) {
        return NKikimr::NMiniKQL::MakeString(NYql::NUdf::TStringRef(str, std::strlen(str)));
    }
    return NYql::NUdf::TUnboxedValuePod();
}
Y_PRAGMA_DIAGNOSTIC_POP
#endif

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsOptional>
class TDecimalToStringWrapper : public TMutableCodegeneratorNode<TDecimalToStringWrapper<IsOptional>> {
    typedef TMutableCodegeneratorNode<TDecimalToStringWrapper<IsOptional>> TBaseComputation;
public:
    TDecimalToStringWrapper(TComputationMutables& mutables, IComputationNode* data, ui8 precision, ui8 scale)
        : TBaseComputation(mutables, EValueRepresentation::String)
        , Data(data)
        , Precision(precision)
        , Scale(scale)
    {
        MKQL_ENSURE(precision > 0 && precision <= NYql::NDecimal::MaxPrecision, "Wrong precision.");
        MKQL_ENSURE(scale <= precision, "Wrong scale.");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& dataValue = Data->GetValue(ctx);
        if (IsOptional && !dataValue) {
            return NUdf::TUnboxedValuePod();
        }

        if (const auto str = NYql::NDecimal::ToString(dataValue.GetInt128(), Precision, Scale)) {
            return MakeString(NUdf::TStringRef(str, std::strlen(str)));
        }

        Throw();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);
        const auto psType = Type::getInt8Ty(context);
        const auto valTypePtr = PointerType::getUnqual(valType);

        const auto name = "DecimalToString";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DecimalToString));
        const auto fnType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(valType, { valType, psType, psType }, false):
            FunctionType::get(Type::getVoidTy(context), { valTypePtr, valTypePtr, psType, psType }, false);
        const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);

        const auto fail = BasicBlock::Create(context, "fail", ctx.Func);
        const auto nice = BasicBlock::Create(context, "nice", ctx.Func);

        const auto zero = ConstantInt::get(valType, 0ULL);
        const auto precision = ConstantInt::get(psType, Precision);
        const auto scale = ConstantInt::get(psType, Scale);
        const auto value = GetNodeValue(Data, ctx, block);

        Value* result;
        if constexpr (IsOptional) {
            const auto call = BasicBlock::Create(context, "call", ctx.Func);
            const auto res = PHINode::Create(valType, 2, "result", nice);
            res->addIncoming(zero, block);
            result = res;

            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, zero, "check", block);
            BranchInst::Create(nice, call, check, block);

            block = call;

            Value* string;
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                string = CallInst::Create(func, { GetterForInt128(value, block), precision, scale }, "to_string", block);
            } else {
                const auto retPtr = new AllocaInst(valType, 0U, "ret_ptr", block);
                new StoreInst(GetterForInt128(value, block), retPtr, block);
                CallInst::Create(func, { retPtr, retPtr, precision, scale }, "", block);
                string = new LoadInst(valType, retPtr, "res", block);
            }

            res->addIncoming(string, block);

            const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, string, zero, "test", block);
            BranchInst::Create(fail, nice, test, block);
        } else {
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                result = CallInst::Create(func, { GetterForInt128(value, block), precision, scale }, "to_string", block);
            } else {
                const auto retPtr = new AllocaInst(valType, 0U, "ret_ptr", block);
                new StoreInst(GetterForInt128(value, block), retPtr, block);
                CallInst::Create(func, { retPtr, retPtr, precision, scale }, "", block);
                result = new LoadInst(valType, retPtr, "res", block);
            }

            const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, result, zero, "test", block);
            BranchInst::Create(fail, nice, test, block);
        }

        block = fail;
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TDecimalToStringWrapper::Throw));
        const auto doFuncType = FunctionType::get(Type::getVoidTy(context), {}, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(doFuncType), "thrower", block);
        CallInst::Create(doFuncType, doFuncPtr, {}, "", block);
        new UnreachableInst(context, block);

        block = nice;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    [[noreturn]] static void Throw() {
        UdfTerminate("Ivalid Decimal value.");
    }

    IComputationNode* const Data;
    const ui8 Precision, Scale;
};

template <bool IsOptional>
class TToStringWrapper : public TMutableCodegeneratorNode<TToStringWrapper<IsOptional>> {
    typedef TMutableCodegeneratorNode<TToStringWrapper<IsOptional>> TBaseComputation;
public:
    TToStringWrapper(TComputationMutables& mutables, IComputationNode* data, NUdf::TDataTypeId schemeType)
        : TBaseComputation(mutables, EValueRepresentation::String)
        , Data(data)
        , SchemeType(NUdf::GetDataSlot(schemeType))
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        const auto& dataValue = Data->GetValue(ctx);
        if (IsOptional && !dataValue) {
            return NUdf::TUnboxedValuePod();
        }

        return ValueToString(SchemeType, dataValue);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);
        const auto slotType = Type::getInt32Ty(context);
        const auto valTypePtr = PointerType::getUnqual(valType);

        const auto name = "DataToString";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DataToString));
        const auto fnType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(valType, { valType, slotType }, false):
            FunctionType::get(Type::getVoidTy(context), { valTypePtr, valTypePtr, slotType }, false);
        const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);

        const auto zero = ConstantInt::get(valType, 0ULL);
        const auto slot = ConstantInt::get(slotType, static_cast<ui32>(SchemeType));
        const auto value = GetNodeValue(Data, ctx, block);

        if constexpr (IsOptional) {
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto call = BasicBlock::Create(context, "call", ctx.Func);
            const auto result = PHINode::Create(valType, 2, "result", done);
            result->addIncoming(zero, block);

            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, zero, "check", block);
            BranchInst::Create(done, call, check, block);

            block = call;

            Value* string;
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                string = CallInst::Create(func, { value, slot }, "to_string", block);
            } else {
                const auto retPtr = new AllocaInst(valType, 0U, "ret_ptr", block);
                new StoreInst(value, retPtr, block);
                CallInst::Create(func, { retPtr, retPtr, slot }, "", block);
                string = new LoadInst(valType, retPtr, "res", block);
            }

            if (Data->IsTemporaryValue())
                ValueCleanup(Data->GetRepresentation(), value, ctx, block);

            result->addIncoming(string, block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            Value* string;
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                string = CallInst::Create(func, { value, slot }, "to_string", block);
            } else {
                const auto retPtr = new AllocaInst(valType, 0U, "ret_ptr", block);
                new StoreInst(value, retPtr, block);
                CallInst::Create(func, { retPtr, retPtr, slot}, "", block);
                string = new LoadInst(valType, retPtr, "res", block);
            }

            if (Data->IsTemporaryValue())
                ValueCleanup(Data->GetRepresentation(), value, ctx, block);

            return string;
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    IComputationNode* const Data;
    const NUdf::EDataSlot SchemeType;
};

class TAsIsWrapper : public TDecoratorCodegeneratorNode<TAsIsWrapper> {
using TBaseComputation = TDecoratorCodegeneratorNode<TAsIsWrapper>;
public:
    TAsIsWrapper(IComputationNode* optional)
        : TBaseComputation(optional)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const { return value; }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext&, Value* value, BasicBlock*&) const { return value; }
#endif
};

}

IComputationNode* WrapToString(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    const auto schemeType = dataType->GetSchemeType();

    const auto data = LocateNode(ctx.NodeLocator, callable, 0);
    if (NUdf::EDataTypeFeatures::StringType & NUdf::GetDataTypeInfo(NUdf::GetDataSlot(schemeType)).Features) {
        return new TAsIsWrapper(data);
    } else if (NUdf::TDataType<NUdf::TDecimal>::Id == schemeType) {
        const auto& params = static_cast<TDataDecimalType*>(dataType)->GetParams();
        if (isOptional) {
            return new TDecimalToStringWrapper<true>(ctx.Mutables, data, params.first, params.second);
        } else {
            return new TDecimalToStringWrapper<false>(ctx.Mutables, data, params.first, params.second);
        }
    } else {
        if (isOptional) {
            return new TToStringWrapper<true>(ctx.Mutables, data, schemeType);
        } else {
            return new TToStringWrapper<false>(ctx.Mutables, data, schemeType);
        }
    }
}

}
}
