#pragma once

#include "mkql_computation_node_impl.h"
#include "mkql_custom_list.h"

#include <ydb/library/yql/minikql/codegen/codegen.h>
#include <type_traits>

#ifdef MKQL_DISABLE_CODEGEN
namespace NKikimr {
namespace NMiniKQL {

using TUnboxedImmutableCodegeneratorNode = TUnboxedImmutableComputationNode;
using TUnboxedImmutableRunCodegeneratorNode = TUnboxedImmutableComputationNode;
using TExternalCodegeneratorNode = TExternalComputationNode;
using TWideFlowProxyCodegeneratorNode = TWideFlowProxyComputationNode;

template <typename TDerived>
using TDecoratorCodegeneratorNode = TDecoratorComputationNode<TDerived>;

template <typename TDerived>
using TBinaryCodegeneratorNode = TBinaryComputationNode<TDerived>;

template <typename TDerived>
using TMutableCodegeneratorNode = TMutableComputationNode<TDerived>;

template <typename TDerived>
using TMutableCodegeneratorPtrNode = TMutableComputationNode<TDerived>;

template <typename TDerived>
using TMutableCodegeneratorFallbackNode = TMutableComputationNode<TDerived>;

template <typename TDerived>
using TMutableCodegeneratorRootNode = TMutableComputationNode<TDerived>;

template <typename TDerived>
using TCustomValueCodegeneratorNode = TMutableComputationNode<TDerived>;

template <typename TDerived>
using TBothWaysCodegeneratorNode = TMutableComputationNode<TDerived>;

template <typename TDerived>
using TStatelessFlowCodegeneratorNode = TStatelessFlowComputationNode<TDerived>;

template <typename TDerived>
using TStatelessWideFlowCodegeneratorNode = TStatelessWideFlowComputationNode<TDerived>;

template <typename TDerived, bool SerializableState = false>
using TStatefulWideFlowCodegeneratorNode = TStatefulWideFlowComputationNode<TDerived, SerializableState>;

template <typename TDerived>
using TPairStateWideFlowCodegeneratorNode = TPairStateWideFlowComputationNode<TDerived>;

template <typename TDerived>
using TStatelessFlowCodegeneratorRootNode = TStatelessFlowComputationNode<TDerived>;

template <typename TDerived, bool SerializableState = false>
using TStatefulFlowCodegeneratorNode = TStatefulFlowComputationNode<TDerived, SerializableState>;

template <typename TDerived>
using TPairStateFlowCodegeneratorNode = TPairStateFlowComputationNode<TDerived>;

template <typename TDerived>
using TFlowSourceCodegeneratorNode = TFlowSourceComputationNode<TDerived>;

}
}
#else
#include <llvm/IR/Value.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Constants.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace llvm;

Type* GetCompContextType(LLVMContext &context);

struct TCodegenContext {
    TCodegenContext(const NYql::NCodegen::ICodegen::TPtr& codegen) : Codegen(codegen) {}

    const NYql::NCodegen::ICodegen::TPtr& Codegen;
    Function* Func = nullptr;
    Argument* Ctx = nullptr;
    bool AlwaysInline = false;

    Value* GetFactory() const;
    Value* GetStat() const;
    Value* GetMutables() const;
    Value* GetBuilder() const;

private:
    Value * Factory = nullptr;
    Value * Stat = nullptr;
    Value * Mutables = nullptr;
    Value * Builder = nullptr;
};

using TGeneratorPtr = Value* (*)(Value *const * args, const TCodegenContext& ctx, BasicBlock*& block);

class ICodegeneratorRootNode {
public:
    virtual ~ICodegeneratorRootNode() {}
    virtual void GenerateFunctions(const NYql::NCodegen::ICodegen::TPtr& codegen) = 0;
    virtual void FinalizeFunctions(const NYql::NCodegen::ICodegen::TPtr& codegen) = 0;
};

class ICodegeneratorInlineNode {
public:
    virtual ~ICodegeneratorInlineNode() {}
    virtual Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const = 0;
};

class ICodegeneratorInlineWideNode {
public:
    virtual ~ICodegeneratorInlineWideNode() {}

    using TGettersList = std::vector<std::function<Value*(const TCodegenContext& ctx, BasicBlock*&)>>;
    using TGenerateResult = std::pair<Value*, TGettersList>;

    virtual TGenerateResult GenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const = 0;
};

class IWideFlowProxyCodegeneratorNode: public ICodegeneratorInlineWideNode
{
public:
    using TGenerator = std::function<TGenerateResult(const TCodegenContext&, BasicBlock*&)>;

    virtual void SetGenerator(TGenerator&& generator) = 0;

    virtual void CreateInvalidate(const TCodegenContext& ctx, BasicBlock*& block) const = 0;
};

class ICodegeneratorExternalNode : public ICodegeneratorInlineNode {
public:
    virtual Value* CreateRefValue(const TCodegenContext& ctx, BasicBlock*& block) const = 0;
    virtual void CreateSetValue(const TCodegenContext& ctx, BasicBlock*& block, Value* value) const = 0;
    virtual Value* CreateSwapValue(const TCodegenContext& ctx, BasicBlock*& block, Value* value) const = 0;
    virtual void CreateInvalidate(const TCodegenContext& ctx, BasicBlock*& block) const = 0;
    virtual void SetTemporaryValue(Value* value) = 0;
    virtual void SetValueGetter(Function* value) = 0;
};

class ICodegeneratorRunNode {
public:
    virtual ~ICodegeneratorRunNode() {}
    virtual void CreateRun(const TCodegenContext& ctx, BasicBlock*& block, Value* result, Value* args) const = 0;
};

size_t GetMethodPtrIndex(uintptr_t ptr);

template<typename Method>
inline size_t GetMethodIndex(Method method) {
    uintptr_t ptr;
    std::memcpy(&ptr, &method, sizeof(uintptr_t));
    return GetMethodPtrIndex(ptr);
}

template<typename Method>
inline uintptr_t GetMethodPtr(Method method) {
    uintptr_t ptr;
    std::memcpy(&ptr, &method, sizeof(uintptr_t));
    return ptr;
}

Value* WrapArgumentForWindows(Value* arg, const TCodegenContext& ctx, BasicBlock* block);

template<NUdf::TBoxedValueAccessor::EMethod Method>
Value* CallBoxedValueVirtualMethod(Type* returnType, Value* value, const NYql::NCodegen::ICodegen::TPtr& codegen, BasicBlock* block) {
    auto& context = codegen->GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = FunctionType::get(returnType, {boxed->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(NUdf::TBoxedValueAccessor::GetMethodPtr<Method>()))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    const auto call = CallInst::Create(funType, func, {boxed}, returnType->isVoidTy() ? "" : "return", block);
    return call;
}

template<NUdf::TBoxedValueAccessor::EMethod Method>
void CallBoxedValueVirtualMethod(Value* output, Value* value, const NYql::NCodegen::ICodegen::TPtr& codegen, BasicBlock* block) {
    auto& context = codegen->GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = (codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) ?
        FunctionType::get(Type::getVoidTy(context), {output->getType(), boxed->getType()}, false):
        FunctionType::get(Type::getVoidTy(context), {boxed->getType(), output->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(NUdf::TBoxedValueAccessor::GetMethodPtr<Method>()))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    if (codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        CallInst::Create(funType, func, {output, boxed}, "", block);
    } else {
        CallInst::Create(funType, func, {boxed, output}, "", block);
    }
}

template<NUdf::TBoxedValueAccessor::EMethod Method>
void CallBoxedValueVirtualMethod(Value* output, Value* value, const NYql::NCodegen::ICodegen::TPtr& codegen, BasicBlock* block, Value* argument) {
    auto& context = codegen->GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = (codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) ?
        FunctionType::get(Type::getVoidTy(context), {output->getType(), boxed->getType(), argument->getType()}, false):
        FunctionType::get(Type::getVoidTy(context), {boxed->getType(), output->getType(), argument->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(NUdf::TBoxedValueAccessor::GetMethodPtr<Method>()))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    if (codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        CallInst::Create(funType, func, {output, boxed, argument}, "", block);
    } else {
        CallInst::Create(funType, func, {boxed, output, argument}, "", block);
    }
}

template<NUdf::TBoxedValueAccessor::EMethod Method>
Value* CallBoxedValueVirtualMethod(Type* returnType, Value* value, const NYql::NCodegen::ICodegen::TPtr& codegen, BasicBlock* block, Value* argument) {
    auto& context = codegen->GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = FunctionType::get(returnType, {boxed->getType(), argument->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(NUdf::TBoxedValueAccessor::GetMethodPtr<Method>()))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    const auto call = CallInst::Create(funType, func, {boxed, argument}, returnType->isVoidTy() ? "" : "return", block);
    return call;
}

template<NUdf::TBoxedValueAccessor::EMethod Method>
void CallBoxedValueVirtualMethod(Value* output, Value* value, const NYql::NCodegen::ICodegen::TPtr& codegen, BasicBlock* block, Value* arg1, Value* arg2) {
    auto& context = codegen->GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = (codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) ?
        FunctionType::get(Type::getVoidTy(context), {output->getType(), boxed->getType(), arg1->getType(), arg2->getType()}, false):
        FunctionType::get(Type::getVoidTy(context), {boxed->getType(), output->getType(), arg1->getType(), arg2->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(NUdf::TBoxedValueAccessor::GetMethodPtr<Method>()))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    if (codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
        CallInst::Create(funType, func, {output, boxed, arg1, arg2}, "", block);
    } else {
        CallInst::Create(funType, func, {boxed, output, arg1, arg2}, "", block);
    }
}

template<NUdf::TBoxedValueAccessor::EMethod Method>
Value* CallBoxedValueVirtualMethod(Type* returnType, Value* value, const NYql::NCodegen::ICodegen::TPtr& codegen, BasicBlock* block, Value* arg1, Value* arg2) {
    auto& context = codegen->GetContext();

    const auto data = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "data", block);
    const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
    const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);

    const auto funType = FunctionType::get(returnType, {boxed->getType(), arg1->getType(), arg2->getType()}, false);
    const auto ptrFunType = PointerType::getUnqual(funType);
    const auto tableType = PointerType::getUnqual(ptrFunType);
    const auto vTable = CastInst::Create(Instruction::IntToPtr, data, PointerType::getUnqual(tableType), "vtable", block);

    const auto table = new LoadInst(tableType, vTable, "table", false, block);
    const auto elem = GetElementPtrInst::CreateInBounds(ptrFunType, table, {ConstantInt::get(Type::getInt64Ty(context), GetMethodPtrIndex(NUdf::TBoxedValueAccessor::GetMethodPtr<Method>()))}, "element", block);
    const auto func = new LoadInst(ptrFunType, elem, "func", false, block);

    const auto call = CallInst::Create(funType, func, {boxed, arg1, arg2}, returnType->isVoidTy() ? "" : "return", block);
    return call;
}

template<typename Method>
Value* CallUnaryUnboxedValueFunction(Method method, Type* result, Value* arg, const NYql::NCodegen::ICodegen::TPtr& codegen, BasicBlock* block)
{
    auto& context = codegen->GetContext();
    const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(method));
    if (NYql::NCodegen::ETarget::Windows != codegen->GetEffectiveTarget()) {
        const auto funType = FunctionType::get(result, {arg->getType()}, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
        const auto call = CallInst::Create(funType, funcPtr, {arg}, "call", block);
        return call;
    } else {
        const auto ptrArg = new AllocaInst(arg->getType(), 0U, "arg", block);
        new StoreInst(arg, ptrArg, block);

        if (Type::getInt128Ty(context) == result) {
            const auto ptrResult = new AllocaInst(result, 0U, "result", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {ptrResult->getType(), ptrArg->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
            CallInst::Create(funType, funcPtr, {ptrResult, ptrArg}, "", block);
            const auto res = new LoadInst(result, ptrResult, "res", block);
            return res;
        } else {
            const auto funType = FunctionType::get(result, {ptrArg->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
            const auto call = CallInst::Create(funType, funcPtr, {ptrArg}, "call", block);
            return call;
        }
    }
}

template<typename Method>
Value* CallBinaryUnboxedValueFunction(Method method, Type* result, Value* left, Value* right, const NYql::NCodegen::ICodegen::TPtr& codegen, BasicBlock* block)
{
    auto& context = codegen->GetContext();
    const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(method));
    if (NYql::NCodegen::ETarget::Windows != codegen->GetEffectiveTarget()) {
        const auto funType = FunctionType::get(result, {left->getType(), right->getType()}, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
        const auto call = CallInst::Create(funType, funcPtr, {left, right}, "call", block);
        return call;
    } else {
        const auto ptrLeft = new AllocaInst(left->getType(), 0U, "left", block);
        const auto ptrRight = new AllocaInst(right->getType(), 0U, "right", block);
        new StoreInst(left, ptrLeft, block);
        new StoreInst(right, ptrRight, block);

        if (Type::getInt128Ty(context) == result) {
            const auto ptrResult = new AllocaInst(result, 0U, "result", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {ptrResult->getType(), ptrLeft->getType(), ptrRight->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
            CallInst::Create(funType, funcPtr, {ptrResult, ptrLeft, ptrRight}, "", block);
            const auto res = new LoadInst(result, ptrResult, "res", block);
            return res;
        } else {
            const auto funType = FunctionType::get(result, {ptrLeft->getType(), ptrRight->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "ptr", block);
            const auto call = CallInst::Create(funType, funcPtr, {ptrLeft, ptrRight}, "call", block);
            return call;
        }
    }
}

void AddRefBoxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block);
void UnRefBoxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block);
void CleanupBoxed(Value* value, const TCodegenContext& ctx, BasicBlock*& block);

void SafeUnRefUnboxed(Value* pointer, const TCodegenContext& ctx, BasicBlock*& block);

void ValueAddRef(EValueRepresentation kind, Value* pointer, const TCodegenContext& ctx, BasicBlock*& block);
void ValueUnRef(EValueRepresentation kind, Value* pointer, const TCodegenContext& ctx, BasicBlock*& block);
void ValueCleanup(EValueRepresentation kind, Value* pointer, const TCodegenContext& ctx, BasicBlock*& block);
void ValueRelease(EValueRepresentation kind, Value* pointer, const TCodegenContext& ctx, BasicBlock*& block);

std::pair<Value*, Value*> GetVariantParts(Value* variant, const TCodegenContext& ctx, BasicBlock*& block);
Value* MakeVariant(Value* item, Value* variant, const TCodegenContext& ctx, BasicBlock*& block);

Value* GetNodeValue(IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block);
void GetNodeValue(Value* value, IComputationNode* node, const TCodegenContext& ctx, BasicBlock*& block);

struct TNoCodegen {};
ICodegeneratorInlineWideNode::TGenerateResult GetNodeValues(IComputationWideFlowNode* node, const TCodegenContext& ctx, BasicBlock*& block);

Function* GenerateCompareFunction(
    const NYql::NCodegen::ICodegen::TPtr& codegen,
    const TString& name,
    IComputationExternalNode* left,
    IComputationExternalNode* right,
    IComputationNode* compare
);

Function* GenerateEqualsFunction(const NYql::NCodegen::ICodegen::TPtr& codegen, const TString& name, bool isTuple, const TKeyTypes& types);
Function* GenerateHashFunction(const NYql::NCodegen::ICodegen::TPtr& codegen, const TString& name, bool isTuple, const TKeyTypes& types);

Function* GenerateEqualsFunction(const NYql::NCodegen::ICodegen::TPtr& codegen, const TString& name, const TKeyTypes& types);
Function* GenerateHashFunction(const NYql::NCodegen::ICodegen::TPtr& codegen, const TString& name, const TKeyTypes& types);
Function* GenerateCompareFunction(const NYql::NCodegen::ICodegen::TPtr& codegen, const TString& name, const TKeyTypes& types);

template <typename TDerived>
class TDecoratorCodegeneratorNode: public TDecoratorComputationNode<TDerived>, public ICodegeneratorInlineNode
{
using TBase = TDecoratorComputationNode<TDerived>;
protected:
    TDecoratorCodegeneratorNode(IComputationNode* node, EValueRepresentation kind)
        : TBase(node, kind)
    {}

    TDecoratorCodegeneratorNode(IComputationNode* node)
        : TBase(node)
    {}

    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final {
        const auto arg = GetNodeValue(this->Node, ctx, block);
        const auto value = static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, arg, block);
        if (value->getType()->isPointerTy()) {
            const auto load = new LoadInst(Type::getInt128Ty(ctx.Codegen->GetContext()), value, "load", block);
            ValueRelease(this->Node->GetRepresentation(), load, ctx, block);
            return load;
        } else {
            return value;
        }
    }
};

template <typename TDerived>
class TStatelessFlowCodegeneratorNode: public TStatelessFlowComputationNode<TDerived>, public ICodegeneratorInlineNode
{
using TBase = TStatelessFlowComputationNode<TDerived>;
protected:
    TStatelessFlowCodegeneratorNode(const IComputationNode* source, EValueRepresentation kind)
        : TBase(source, kind)
    {}

    TStatelessFlowCodegeneratorNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation kind)
        : TBase(mutables, source, kind)
    {}

    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final {
        const auto value = static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, block);
        if (value->getType()->isPointerTy()) {
            const auto load = new LoadInst(Type::getInt128Ty(ctx.Codegen->GetContext()), value, "load", block);
            ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), load, ctx, block);
            return load;
        } else {
            return value;
        }
    }
};

template <typename TDerived>
class TStatelessWideFlowCodegeneratorNode: public TStatelessWideFlowComputationNode<TDerived>, public ICodegeneratorInlineWideNode
{
using TBase = TStatelessWideFlowCodegeneratorNode<TDerived>;
protected:
    TStatelessWideFlowCodegeneratorNode(const IComputationNode* source)
        : TStatelessWideFlowComputationNode<TDerived>(source)
    {}

    TGenerateResult GenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const final {
        return static_cast<const TDerived*>(this)->DoGenGetValues(ctx, block);
    }
};

template <typename TDerived>
class TFlowSourceCodegeneratorNode: public TFlowSourceComputationNode<TDerived>, public ICodegeneratorInlineNode
{
using TBase = TFlowSourceComputationNode<TDerived>;
protected:
    TFlowSourceCodegeneratorNode(TComputationMutables& mutables, EValueRepresentation kind, EValueRepresentation stateKind)
        : TBase(mutables, kind, stateKind)
    {}

    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final {
        auto& context = ctx.Codegen->GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), static_cast<const IComputationNode*>(this)->GetIndex())}, "state_ptr", block);

        const auto value = static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, statePtr, block);
        if (value->getType()->isPointerTy()) {
            const auto load = new LoadInst(valueType, value, "load", block);
            ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), load, ctx, block);
            return load;
        } else {
            return value;
        }
    }
};

template <typename TDerived, bool SerializableState = false>
class TStatefulFlowCodegeneratorNode: public TStatefulFlowComputationNode<TDerived, SerializableState>, public ICodegeneratorInlineNode
{
using TBase = TStatefulFlowComputationNode<TDerived, SerializableState>;
protected:
    TStatefulFlowCodegeneratorNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation kind, EValueRepresentation stateKind = EValueRepresentation::Any)
        : TBase(mutables, source, kind, stateKind)
    {}

    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final {
        auto& context = ctx.Codegen->GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), static_cast<const IComputationNode*>(this)->GetIndex())}, "state_ptr", block);

        const auto value = static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, statePtr, block);
        if (value->getType()->isPointerTy()) {
            const auto load = new LoadInst(valueType, value, "load", block);
            ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), load, ctx, block);
            return load;
        } else {
            return value;
        }
    }
};

template <typename TDerived, bool SerializableState = false>
class TStatefulWideFlowCodegeneratorNode: public TStatefulWideFlowComputationNode<TDerived, SerializableState>, public ICodegeneratorInlineWideNode
{
using TBase = TStatefulWideFlowComputationNode<TDerived, SerializableState>;
protected:
    TStatefulWideFlowCodegeneratorNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation stateKind)
        : TBase(mutables, source, stateKind)
    {}

    TStatefulWideFlowCodegeneratorNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation, EValueRepresentation stateKind)
        : TBase(mutables, source, stateKind)
    {}

    TGenerateResult GenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const final {
        auto& context = ctx.Codegen->GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), static_cast<const IComputationNode*>(this)->GetIndex())}, "state_ptr", block);
        return static_cast<const TDerived*>(this)->DoGenGetValues(ctx, statePtr, block);
    }
};

template <typename TDerived>
class TPairStateWideFlowCodegeneratorNode: public TPairStateWideFlowComputationNode<TDerived>, public ICodegeneratorInlineWideNode
{
using TBase = TPairStateWideFlowComputationNode<TDerived>;
protected:
    TPairStateWideFlowCodegeneratorNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation firstKind, EValueRepresentation secondKind)
        : TBase(mutables, source, firstKind, secondKind)
    {}

    TGenerateResult GenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const final {
        auto& context = ctx.Codegen->GetContext();
        auto idx = static_cast<const IComputationNode*>(this)->GetIndex();
        const auto valueType = Type::getInt128Ty(context);
        const auto firstPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), idx)}, "first_ptr", block);
        const auto secondPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), ++idx)}, "second_ptr", block);
        return static_cast<const TDerived*>(this)->DoGenGetValues(ctx, firstPtr, secondPtr, block);
    }
};

template <typename TDerived>
class TPairStateFlowCodegeneratorNode: public TPairStateFlowComputationNode<TDerived>, public ICodegeneratorInlineNode
{
using TBase = TPairStateFlowComputationNode<TDerived>;
protected:
    TPairStateFlowCodegeneratorNode(TComputationMutables& mutables, const IComputationNode* source, EValueRepresentation kind, EValueRepresentation firstKind = EValueRepresentation::Any, EValueRepresentation secondKind = EValueRepresentation::Any)
        : TBase(mutables, source, kind, firstKind, secondKind)
    {}

    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final {
        auto& context = ctx.Codegen->GetContext();
        auto idx = static_cast<const IComputationNode*>(this)->GetIndex();
        const auto valueType = Type::getInt128Ty(context);
        const auto firstPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), idx)}, "first_ptr", block);
        const auto secondPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), ++idx)}, "second_ptr", block);

        const auto value = static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, firstPtr, secondPtr, block);
        if (value->getType()->isPointerTy()) {
            const auto load = new LoadInst(valueType, value, "load", block);
            ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), load, ctx, block);
            return load;
        } else {
            return value;
        }
    }
};

template <typename TDerived>
class TBinaryCodegeneratorNode: public TBinaryComputationNode<TDerived>, public ICodegeneratorInlineNode
{
using TBase = TBinaryComputationNode<TDerived>;
protected:
    TBinaryCodegeneratorNode(IComputationNode* left, IComputationNode* right, const EValueRepresentation kind)
        : TBase(left, right, kind)
    {}

    TBinaryCodegeneratorNode(TComputationMutables&, EValueRepresentation kind, IComputationNode* left, IComputationNode* right)
        : TBase(left, right, kind)
    {}

    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final {
        const auto value = static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, block);
        if (value->getType()->isPointerTy()) {
            ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), value, ctx, block);
            const auto load = new LoadInst(Type::getInt128Ty(ctx.Codegen->GetContext()), value, "load", block);
            return load;
        } else {
            return value;
        }
    }
};

template <typename TDerived>
class TMutableCodegeneratorNode: public TMutableComputationNode<TDerived>, public ICodegeneratorInlineNode
{
using TBase = TMutableComputationNode<TDerived>;
protected:
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::internal_Get_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    TMutableCodegeneratorNode(TComputationMutables& mutables, EValueRepresentation kind)
        : TBase(mutables, kind)
    {}

    Function* GenerateInternalGetValue(const NYql::NCodegen::ICodegen::TPtr& codegen) const {
        auto& module = codegen->GetModule();
        auto& context = codegen->GetContext();
        const auto& name = MakeName();
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto funcType = FunctionType::get(Type::getInt128Ty(context), {PointerType::getUnqual(GetCompContextType(context))}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        auto main = BasicBlock::Create(context, "main", ctx.Func);
        ctx.Ctx = &*ctx.Func->arg_begin();
        ctx.Ctx->addAttr(Attribute::NonNull);

        const auto get = this->MakeGetValueBody(ctx, main);

        ReturnInst::Create(context, get, main);
        return ctx.Func;
    }

    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final {
        if (*this->Stateless) {
            const auto newValue = static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, block);
            if (newValue->getType()->isPointerTy()) {
                ValueRelease(this->GetRepresentation(), newValue, ctx, block);
                const auto load = new LoadInst(Type::getInt128Ty(ctx.Codegen->GetContext()), newValue, "load", block);
                return load;
            } else {
                return newValue;
            }
        }

        return ctx.AlwaysInline ? MakeGetValueBody(ctx, block) :
            CallInst::Create(GenerateInternalGetValue(ctx.Codegen), {ctx.Ctx}, "getter", block);
    }

    Value* MakeGetValueBody(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen->GetContext();
        const auto indexType = Type::getInt32Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, this->ValueIndex)}, "value_ptr", block);
        const auto value = new LoadInst(valueType, valuePtr, "value", block);

        const auto invv = ConstantInt::get(value->getType(), 0xFFFFFFFFFFFFFFFFULL);

        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, invv, "check", block);

        const auto comp = BasicBlock::Create(context, "comp", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(comp, done, check, block);

        block = comp;

        const auto newValue = static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, block);

        if (newValue->getType()->isPointerTy()) {
            const auto load = new LoadInst(valueType, newValue, "value", block);
            new StoreInst(load, valuePtr, block);
            new StoreInst(ConstantInt::get(load->getType(), 0), newValue, block);
        } else {
            new StoreInst(newValue, valuePtr, block);
            ValueAddRef(this->RepresentationKind, valuePtr, ctx, block);
        }

        BranchInst::Create(done, block);
        block = done;

        const auto result = new LoadInst(valueType, valuePtr, "result", false, block);
        return result;
    }
};

template <typename TDerived>
class TMutableCodegeneratorPtrNode: public TMutableComputationNode<TDerived>, public ICodegeneratorInlineNode
{
using TBase = TMutableComputationNode<TDerived>;
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::internal_Get_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

protected:
    TMutableCodegeneratorPtrNode(TComputationMutables& mutables, EValueRepresentation kind)
        : TBase(mutables, kind)
    {}

    Function* GenerateInternalGetValue(const NYql::NCodegen::ICodegen::TPtr& codegen) const {
        auto& module = codegen->GetModule();
        auto& context = codegen->GetContext();
        const auto& name = MakeName();
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto contextType = GetCompContextType(context);

        const auto funcType = FunctionType::get(Type::getInt128Ty(context), {PointerType::getUnqual(contextType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        auto main = BasicBlock::Create(context, "main", ctx.Func);
        ctx.Ctx = &*ctx.Func->arg_begin();
        ctx.Ctx->addAttr(Attribute::NonNull);

        const auto get = this->MakeGetValueBody(ctx, main);

        ReturnInst::Create(context, get, main);
        return ctx.Func;
    }

    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final {
        if (*this->Stateless) {
            const auto type = Type::getInt128Ty(ctx.Codegen->GetContext());
            const auto pointer = ctx.Func->getEntryBlock().empty() ?
                new AllocaInst(type, 0U, "output", &ctx.Func->getEntryBlock()):
                new AllocaInst(type, 0U, "output", &ctx.Func->getEntryBlock().back());

            static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, pointer, block);
            ValueRelease(this->GetRepresentation(), pointer, ctx, block);
            const auto load = new LoadInst(type, pointer, "load", block);
            return load;
        }

        return ctx.AlwaysInline ? MakeGetValueBody(ctx, block) :
            CallInst::Create(GenerateInternalGetValue(ctx.Codegen), {ctx.Ctx}, "getter", block);
    }

    Value* MakeGetValueBody(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen->GetContext();
        const auto indexType = Type::getInt32Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, this->ValueIndex)}, "value_ptr", block);
        const auto value = new LoadInst(valueType, valuePtr, "value", block);

        const auto invv = ConstantInt::get(value->getType(), 0xFFFFFFFFFFFFFFFFULL);

        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, invv, "check", block);

        const auto comp = BasicBlock::Create(context, "comp", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(comp, done, check, block);

        block = comp;

        static_cast<const TDerived*>(this)->DoGenerateGetValue(ctx, valuePtr, block);

        BranchInst::Create(done, block);
        block = done;

        const auto result = new LoadInst(valueType, valuePtr, "result", false, block);
        return result;
    }
};

template <typename TDerived>
class TMutableCodegeneratorFallbackNode: public TMutableCodegeneratorNode<TDerived>
{
using TBase = TMutableCodegeneratorNode<TDerived>;

protected:
    TMutableCodegeneratorFallbackNode(TComputationMutables& mutables, EValueRepresentation kind)
        : TBase(mutables, kind)
    {}

public:
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen->GetContext();
        const auto type = Type::getInt128Ty(context);
        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        static_assert(std::is_same<std::invoke_result_t<decltype(&TDerived::DoCalculate), TDerived, TComputationContext&>, NUdf::TUnboxedValuePod>(), "DoCalculate must return pod!");
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TDerived::DoCalculate));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen->GetEffectiveTarget()) {
            const auto funType = FunctionType::get(type, {self->getType(), ctx.Ctx->getType()}, false);
            const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
            const auto value = CallInst::Create(funType, doFuncPtr, {self, ctx.Ctx}, "value", block);
            return value;
        } else {
            const auto resultPtr = new AllocaInst(type, 0U, "return", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), resultPtr->getType(), ctx.Ctx->getType()}, false);
            const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, doFuncPtr, {self, resultPtr, ctx.Ctx}, "", block);
            const auto value = new LoadInst(type, resultPtr, "value", block);
            return value;
        }
    }
};

template <typename TDerived, bool PreventReGeneration = false>
class TCodegeneratorRootNode: public TDerived, public ICodegeneratorRootNode
{
using TBase = TDerived;
public:
    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final {
        if (compCtx.ExecuteLLVM && GetFunction)
            return GetFunction(&compCtx);

        return TBase::GetValue(compCtx);
    }

protected:
    TCodegeneratorRootNode(EValueRepresentation kind)
        : TBase(kind)
    {}

    TCodegeneratorRootNode(const IComputationNode* source, EValueRepresentation kind)
        : TBase(source, kind)
    {}

    TCodegeneratorRootNode(TComputationMutables& mutables, EValueRepresentation kind)
        : TBase(mutables, kind)
    {}

private:
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::Get_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    Function* GenerateGetValue(const NYql::NCodegen::ICodegen::TPtr& codegen) {
        auto& module = codegen->GetModule();
        auto& context = codegen->GetContext();

        if constexpr (PreventReGeneration) {
            if (const auto& name = TDerived::MakeName(); module.getFunction(name.c_str()))
                return nullptr;
        }

        const auto& name = MakeName();
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto contextType = GetCompContextType(context);

        const auto funcType = codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
            FunctionType::get(valueType, {PointerType::getUnqual(contextType)}, false):
            FunctionType::get(Type::getVoidTy(context) , {PointerType::getUnqual(valueType), PointerType::getUnqual(contextType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        auto args = ctx.Func->arg_begin();
        if (codegen->GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows) {
            auto& firstArg = *args++;
            firstArg.addAttr(Attribute::StructRet);
            firstArg.addAttr(Attribute::NoAlias);
        }

        auto main = BasicBlock::Create(context, "main", ctx.Func);
        ctx.Ctx = &*args;
        ctx.Ctx->addAttr(Attribute::NonNull);

        const auto get = this->CreateGetValue(ctx, main);

        if (codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows) {
            ReturnInst::Create(context, get, main);
        } else {
            new StoreInst(get, &*--args, main);
            ReturnInst::Create(context, main);
        }

        return ctx.Func;
    }
protected:
    void FinalizeFunctions(const NYql::NCodegen::ICodegen::TPtr& codegen) override {
        if (GetFunc)
            GetFunction = reinterpret_cast<TGetPtr>(codegen->GetPointerToFunction(GetFunc));
    }

    void GenerateFunctions(const NYql::NCodegen::ICodegen::TPtr& codegen) override {
        if (GetFunc = GenerateGetValue(codegen))
            codegen->ExportSymbol(GetFunc);
    }
private:
    using TGetPtr = NUdf::TUnboxedValuePod (*)(TComputationContext* ctx);

    Function* GetFunc = nullptr;
    TGetPtr GetFunction = nullptr;
};

template <typename TDerived>
using TMutableCodegeneratorRootNode = TCodegeneratorRootNode<TMutableCodegeneratorNode<TDerived>, true>;

template <typename TDerived>
using TStatelessFlowCodegeneratorRootNode = TCodegeneratorRootNode<TStatelessFlowCodegeneratorNode<TDerived>>;

class TUnboxedImmutableCodegeneratorNode: public TUnboxedImmutableComputationNode, public ICodegeneratorInlineNode
{
public:
    TUnboxedImmutableCodegeneratorNode(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value);

private:
    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*&) const final;
};

class TUnboxedImmutableRunCodegeneratorNode: public TUnboxedImmutableComputationNode, public ICodegeneratorRunNode
{
public:
    TUnboxedImmutableRunCodegeneratorNode(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value);
};

class TExternalCodegeneratorNode: public TExternalComputationNode, public ICodegeneratorExternalNode
{
public:
    TExternalCodegeneratorNode(TComputationMutables& mutables, EValueRepresentation kind);
protected:
    Value* CreateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const final;
    Value* CreateRefValue(const TCodegenContext& ctx, BasicBlock*& block) const final;
    void CreateSetValue(const TCodegenContext& ctx, BasicBlock*& block, Value* value) const final;
    Value* CreateSwapValue(const TCodegenContext& ctx, BasicBlock*& block, Value* value) const final;
    void CreateInvalidate(const TCodegenContext& ctx, BasicBlock*& block) const final;
    void SetTemporaryValue(Value* value) final;
    void SetValueGetter(Function* getter) final;
private:
    Function* ValueGetter = nullptr;
    Value* TemporaryValue = nullptr;
};

class TExternalCodegeneratorRootNode: public TExternalCodegeneratorNode, public ICodegeneratorRootNode
{
public:
    TExternalCodegeneratorRootNode(TComputationMutables& mutables, EValueRepresentation kind);

private:
    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const final;

    void SetValue(TComputationContext& compCtx, NUdf::TUnboxedValue&& newValue) const final;

    TString MakeName(const TString& method) const;

    void FinalizeFunctions(const NYql::NCodegen::ICodegen::TPtr& codegen) final;
    void GenerateFunctions(const NYql::NCodegen::ICodegen::TPtr& codegen) final;

    Function* GenerateGetValue(const NYql::NCodegen::ICodegen::TPtr& codegen);
    Function* GenerateSetValue(const NYql::NCodegen::ICodegen::TPtr& codegen);

    using TGetPtr = NUdf::TUnboxedValuePod (*)(TComputationContext* ctx);

    Function* GetValueFunc = nullptr;
    TGetPtr GetFunction = nullptr;

    using TSetPtr = void (*)(TComputationContext* ctx, NUdf::TUnboxedValuePod);

    Function* SetValueFunc = nullptr;
    TSetPtr SetFunction = nullptr;
};


class TWideFlowProxyCodegeneratorNode: public TWideFlowProxyComputationNode, public IWideFlowProxyCodegeneratorNode
{
public:
    TWideFlowProxyCodegeneratorNode() = default;
private:
    void SetGenerator(TGenerator&& generator) final;

    void CreateInvalidate(const TCodegenContext& ctx, BasicBlock*& block) const final;

    TGenerateResult GenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const final;

    TGenerator Generator;
};

class TCodegenIterator : public TComputationValue<TCodegenIterator> {
public:
    using TNextPtr = bool (*)(TComputationContext*, NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod&);

    TCodegenIterator(TMemoryUsageInfo* memInfo, TNextPtr next, TComputationContext* ctx, NUdf::TUnboxedValue&& iterator)
        : TComputationValue<TCodegenIterator>(memInfo)
        , Ctx(ctx)
        , NextFunc(next)
        , Iterator(std::move(iterator))
    {}

protected:
    bool Next(NUdf::TUnboxedValue& value) override {
        return NextFunc(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Iterator), value);
    }

    TComputationContext* const Ctx;
    const TNextPtr NextFunc;
    const NUdf::TUnboxedValue Iterator;
};

template <typename TState = NUdf::TUnboxedValue>
class TCodegenStatefulIterator : public TComputationValue<TCodegenStatefulIterator<TState>> {
public:
    using TStateType = TState;
    using TNextPtr = bool (*)(TComputationContext*, NUdf::TUnboxedValuePod, TStateType&, NUdf::TUnboxedValuePod&);

    TCodegenStatefulIterator(TMemoryUsageInfo* memInfo, TNextPtr next, TComputationContext* ctx, NUdf::TUnboxedValue&& iterator, const TStateType& init = TStateType())
        : TComputationValue<TCodegenStatefulIterator>(memInfo)
        , Ctx(ctx)
        , NextFunc(next)
        , Iterator(std::move(iterator))
        , State(init)
    {}

protected:
    bool Next(NUdf::TUnboxedValue& value) override {
        return NextFunc(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Iterator), State, value);
    }

    TComputationContext* const Ctx;
    const TNextPtr NextFunc;
    const NUdf::TUnboxedValue Iterator;
    TStateType State;
};

class TCustomListCodegenValue : public TCustomListValue {
public:
    using TIterator = TCodegenIterator;
    using TNextPtr = typename TIterator::TNextPtr;

    TCustomListCodegenValue(TMemoryUsageInfo* memInfo, TNextPtr next, TComputationContext* ctx, NUdf::TUnboxedValue&& list)
        : TCustomListValue(memInfo)
        , Ctx(ctx)
        , NextFunc(next)
        , List(std::move(list))
    {}

private:
    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), NextFunc, Ctx, List.GetListIterator()));
    }

    TComputationContext* const Ctx;
    const TNextPtr NextFunc;
    const NUdf::TUnboxedValue List;
};

template <class TIterator = TCodegenStatefulIterator<>>
class TCustomListCodegenStatefulValueT : public TCustomListValue {
public:
    using TStateType = typename TIterator::TStateType;
    using TNextPtr = typename TIterator::TNextPtr;

    TCustomListCodegenStatefulValueT(TMemoryUsageInfo* memInfo, TNextPtr next, TComputationContext* ctx, NUdf::TUnboxedValue&& list, TStateType&& init = TStateType())
        : TCustomListValue(memInfo)
        , Ctx(ctx)
        , NextFunc(next)
        , List(std::move(list))
        , Init(std::move(init))
    {}

private:
    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), NextFunc, Ctx, List.GetListIterator(), Init));
    }

    TComputationContext* const Ctx;
    const TNextPtr NextFunc;
    const NUdf::TUnboxedValue List;
    const TStateType Init;
};

using TCustomListCodegenStatefulValue = TCustomListCodegenStatefulValueT<>;

class TListCodegenValue : public TComputationValue<TListCodegenValue> {
public:
    using TNextPtr = TCodegenIterator::TNextPtr;

    TListCodegenValue(TMemoryUsageInfo* memInfo, TNextPtr next, TComputationContext* ctx, NUdf::TUnboxedValue&& list)
        : TComputationValue<TListCodegenValue>(memInfo)
        , Ctx(ctx)
        , NextFunc(next)
        , List(std::move(list))
    {}

private:
    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TUnboxedValuePod(new TCodegenIterator(GetMemInfo(), NextFunc, Ctx, List.GetListIterator()));
    }

    ui64 GetListLength() const final {
        return List.GetListLength();
    }

    bool HasListItems() const final {
        return List.HasListItems();
    }

    bool HasFastListLength() const final {
        return List.HasFastListLength();
    }

    TComputationContext* const Ctx;
    const TNextPtr NextFunc;
    const NUdf::TUnboxedValue List;
};

class TCodegenIteratorOne : public TComputationValue<TCodegenIteratorOne> {
public:
    using TNextPtr = bool (*)(TComputationContext*, NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod&);

    TCodegenIteratorOne(TMemoryUsageInfo* memInfo, TNextPtr nextOne, TNextPtr nextTwo, TComputationContext* ctx, NUdf::TUnboxedValue&& iterator)
        : TComputationValue<TCodegenIteratorOne>(memInfo)
        , NextFuncOne(nextOne)
        , NextFuncTwo(nextTwo)
        , Ctx(ctx)
        , Iterator(std::move(iterator))
    {}

private:
    bool Next(NUdf::TUnboxedValue& value) override {
        if (FirstCall) {
            FirstCall = false;
            return NextFuncOne(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Iterator), value);
        } else {
            return NextFuncTwo(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Iterator), value);
        }
    }

    const TNextPtr NextFuncOne;
    const TNextPtr NextFuncTwo;
    TComputationContext* const Ctx;
    const NUdf::TUnboxedValue Iterator;
    bool FirstCall = true;
};

class TListCodegenValueOne : public TComputationValue<TListCodegenValueOne> {
public:
    using TNextPtr = TCodegenIteratorOne::TNextPtr;

    TListCodegenValueOne(TMemoryUsageInfo* memInfo, TNextPtr nextOne, TNextPtr nextTwo, TComputationContext* ctx, NUdf::TUnboxedValue&& list)
        : TComputationValue<TListCodegenValueOne>(memInfo)
        , NextFuncOne(nextOne)
        , NextFuncTwo(nextTwo)
        , Ctx(ctx)
        , List(std::move(list))
    {}

private:
    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TUnboxedValuePod(new TCodegenIteratorOne(GetMemInfo(), NextFuncOne, NextFuncTwo, Ctx, List.GetListIterator()));
    }

    ui64 GetListLength() const final {
        return List.GetListLength();
    }

    bool HasListItems() const final {
        return List.HasListItems();
    }

    bool HasFastListLength() const final {
        return List.HasFastListLength();
    }

    const TNextPtr NextFuncOne;
    const TNextPtr NextFuncTwo;
    TComputationContext* const Ctx;
    const NUdf::TUnboxedValue List;
};

class TStreamCodegenValueStateless : public TComputationValue<TStreamCodegenValueStateless> {
public:
    using TBase = TComputationValue<TStreamCodegenValueStateless>;

    using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod&);

    TStreamCodegenValueStateless(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, NUdf::TUnboxedValue&& stream)
        : TBase(memInfo)
        , FetchFunc(fetch)
        , Ctx(ctx)
        , Stream(std::move(stream))
    {}

protected:
    ui32 GetTraverseCount() const final {
        return 1;
    }

    NUdf::TUnboxedValue GetTraverseItem(ui32) const final {
        return Stream;
    }

    NUdf::TUnboxedValue Save() const override {
        return NUdf::TUnboxedValue::Zero();
    }

    void Load(const NUdf::TStringRef& state) override {
        Y_UNUSED(state);
    }

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        return FetchFunc(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Stream), result);
    }

    const TFetchPtr FetchFunc;
    TComputationContext* const Ctx;
    const NUdf::TUnboxedValue Stream;
};

class TStreamCodegenValueOne : public TComputationValue<TStreamCodegenValueOne> {
public:
    using TBase = TComputationValue<TStreamCodegenValueOne>;

    using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod&);

    TStreamCodegenValueOne(TMemoryUsageInfo* memInfo, TFetchPtr fetchOne, TFetchPtr fetchTwo, TComputationContext* ctx, NUdf::TUnboxedValue&& stream)
        : TBase(memInfo)
        , FetchFuncOne(fetchOne)
        , FetchFuncTwo(fetchTwo)
        , Ctx(ctx)
        , Stream(std::move(stream))
    {}

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
        if (FirstCall) {
            FirstCall = false;
            return FetchFuncOne(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Stream), result);
        } else {
            return FetchFuncTwo(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Stream), result);
        }
    }

    const TFetchPtr FetchFuncOne;
    const TFetchPtr FetchFuncTwo;
    TComputationContext* const Ctx;
    const NUdf::TUnboxedValue Stream;
    bool FirstCall = true;
};

template <typename TState = NUdf::TUnboxedValue>
class TStreamCodegenStatefulValueT : public TComputationValue<TStreamCodegenStatefulValueT<TState>> {
public:
    using TBase = TComputationValue<TStreamCodegenStatefulValueT<TState>>;

    using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod, TState&, NUdf::TUnboxedValuePod&);

    TStreamCodegenStatefulValueT(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, NUdf::TUnboxedValue&& stream, TState&& init = TState())
        : TBase(memInfo)
        , FetchFunc(fetch)
        , Ctx(ctx)
        , Stream(std::move(stream))
        , State(std::move(init))
    {}

protected:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        return FetchFunc(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Stream), State, result);
    }

    const TFetchPtr FetchFunc;
    TComputationContext* const Ctx;
    const NUdf::TUnboxedValue Stream;
    TState State;
};

using TStreamCodegenStatefulValue = TStreamCodegenStatefulValueT<>;

template <class StateType>
class TStreamCodegenSelfStateValue : public StateType {
using TState = StateType;
public:
    using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod, TState* state, NUdf::TUnboxedValuePod&);

    template <typename...TArgs>
    TStreamCodegenSelfStateValue(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, NUdf::TUnboxedValue&& stream, TArgs&&...args)
        : TState(memInfo, std::forward<TArgs>(args)...)
        , FetchFunc(fetch)
        , Ctx(ctx)
        , Stream(std::move(stream))
    {}

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        return FetchFunc(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Stream), this, result);
    }

    const TFetchPtr FetchFunc;
    TComputationContext* const Ctx;
    const NUdf::TUnboxedValue Stream;
};

template <class StateType>
class TStreamCodegenSelfStatePlusValue : public StateType {
using TState = StateType;
public:
    using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod, TState* state, NUdf::TUnboxedValuePod&, NUdf::TUnboxedValuePod&);

    template <typename...TArgs>
    TStreamCodegenSelfStatePlusValue(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, NUdf::TUnboxedValue&& stream, TArgs&&...args)
        : TState(memInfo, std::forward<TArgs>(args)...)
        , FetchFunc(fetch)
        , Ctx(ctx)
        , Stream(std::move(stream))
    {}

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        return FetchFunc(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Stream), this, State, result);
    }

    const TFetchPtr FetchFunc;
    TComputationContext* const Ctx;
    const NUdf::TUnboxedValue Stream;
    NUdf::TUnboxedValue State;
};

template <typename TDerived>
class TCustomValueCodegeneratorNode: public TMutableCodegeneratorFallbackNode<TDerived>, public ICodegeneratorRootNode
{
using TBase = TMutableCodegeneratorFallbackNode<TDerived>;

protected:
    TCustomValueCodegeneratorNode(TComputationMutables& mutables)
        : TBase(mutables, EValueRepresentation::Boxed)
    {}

    TString MakeName(const TString& method) const {
        TStringStream out;
        out << this->DebugString() << "::" << method << "_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }
};

template <typename TDerived>
class TBothWaysCodegeneratorNode: public TMutableCodegeneratorRootNode<TDerived>
{
using TBase = TMutableCodegeneratorRootNode<TDerived>;

protected:
    TBothWaysCodegeneratorNode(TComputationMutables& mutables)
        : TBase(mutables, EValueRepresentation::Boxed)
    {}

    TString MakeName(const TString& method) const {
        TStringStream out;
        out << this->DebugString() << "::" << method << "_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }
};

template<typename T> Type* GetTypeFor(LLVMContext &context);

template<typename T> inline Value* GetterFor(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto trunc = CastInst::Create(Instruction::Trunc, value, std::is_same<T, bool>() ? Type::getInt1Ty(context) : IntegerType::get(context, sizeof(T) << 3U), "trunc", block);
    if  (std::is_integral<T>::value)
        return trunc;
    return CastInst::Create(Instruction::BitCast, trunc, GetTypeFor<T>(context), "bitcast", block);
}

template<typename T> inline
Value* SetterFor(Value* value, LLVMContext &context, BasicBlock* block) {
    if  (value->getType()->isFloatingPointTy())
        value = CastInst::Create(Instruction::BitCast, value, IntegerType::get(context, sizeof(T) << 3U), "bitcast", block);

    const auto type = Type::getInt128Ty(context);
    const auto zext = CastInst::Create(Instruction::ZExt, value, type, "zext", block);
    const uint64_t init[] = {0ULL, 0x100000000000000ULL}; // Embedded
    const auto meta = ConstantInt::get(context, APInt(128, 2, init));
    const auto full = BinaryOperator::CreateOr(zext, meta, "or", block);
    return full;
}

Value* SetterForInt128(Value* value, BasicBlock* block);
Value* GetterForInt128(Value* value, BasicBlock* block);

Value* GetterForTimezone(LLVMContext& context, Value* value, BasicBlock* block);

template<typename TSrc, typename TDst> inline
Value* StaticCast(Value* value, LLVMContext &context, BasicBlock* block) {
    if (std::is_same<TSrc, TDst>())
        return value;

    if (std::is_integral<TSrc>() == std::is_integral<TDst>()) {
        if (std::is_integral<TSrc>()) {
            if (sizeof(TSrc) > sizeof(TDst)) {
                if (std::is_same<TDst, bool>())
                    return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, value, ConstantInt::get(value->getType(), 0), "test", block);
                else
                    return CastInst::Create(Instruction::Trunc, value, GetTypeFor<TDst>(context), "trunc", block);
            } else if ((sizeof(TSrc) < sizeof(TDst))) {
                return CastInst::Create(std::is_signed<TSrc>() ? Instruction::SExt : Instruction::ZExt, value, GetTypeFor<TDst>(context), "ext", block);
            } else {
                return value;
            }
        } else {
            if (sizeof(TSrc) > sizeof(TDst)) {
                return CastInst::Create(Instruction::FPTrunc, value, GetTypeFor<TDst>(context), "fptrunc", block);
            } else if ((sizeof(TSrc) < sizeof(TDst))) {
                return CastInst::Create(Instruction::FPExt, value, GetTypeFor<TDst>(context), "fpext", block);
            } else {
                return value;
            }
        }
    } else {
        constexpr auto instruction = std::is_integral<TSrc>() ?
            std::is_signed<TSrc>() ? Instruction::SIToFP : Instruction::UIToFP:
            std::is_signed<TDst>() ? Instruction::FPToSI : Instruction::FPToUI;
        return CastInst::Create(instruction, value, GetTypeFor<TDst>(context), std::is_integral<TSrc>() ? "int_to_float" : "float_to_int", block);
    }
}

Value* GetOptionalValue(LLVMContext& context, Value* value, BasicBlock* block);
Value* MakeOptional(LLVMContext& context, Value* value, BasicBlock* block);

Value* MakeBoolean(Value* boolean, LLVMContext &context, BasicBlock* block);

ConstantInt* GetEmpty(LLVMContext &context);

ConstantInt* GetTrue(LLVMContext &context);
ConstantInt* GetFalse(LLVMContext &context);

ConstantInt* GetDecimalPlusInf(LLVMContext &context);
ConstantInt* GetDecimalMinusInf(LLVMContext &context);

ConstantInt* GetDecimalNan(LLVMContext &context);
ConstantInt* GetDecimalMinusNan(LLVMContext &context);

ConstantInt* GetInvalid(LLVMContext &context);
ConstantInt* GetFinish(LLVMContext &context);
ConstantInt* GetYield(LLVMContext &context);

ConstantInt* GetConstant(ui64 value, LLVMContext &context);

Value* IsExists(Value* value, BasicBlock* block);
Value* IsEmpty(Value* value, BasicBlock* block);
Value* IsInvalid(Value* value, BasicBlock* block);
Value* IsValid(Value* value, BasicBlock* block);
Value* IsFinish(Value* value, BasicBlock* block);
Value* IsYield(Value* value, BasicBlock* block);
Value* IsSpecial(Value* value, BasicBlock* block);
Value* HasValue(Value* value, BasicBlock* block);

Value* GenNewArray(const TCodegenContext& ctx, Value* size, Value* items, BasicBlock* block);

Value* GetMemoryUsed(ui64 limit, const TCodegenContext& ctx, BasicBlock* block);

template <bool TrackRss>
Value* CheckAdjustedMemLimit(ui64 limit, Value* init, const TCodegenContext& ctx, BasicBlock*& block);

}
}
#endif
