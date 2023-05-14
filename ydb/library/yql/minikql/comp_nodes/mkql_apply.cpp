#include "mkql_apply.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <library/cpp/containers/stack_array/stack_array.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TApplyWrapper: public TMutableCodegeneratorPtrNode<TApplyWrapper> {
    typedef TMutableCodegeneratorPtrNode<TApplyWrapper> TBaseComputation;
public:
    TApplyWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* callableNode,
        TComputationNodePtrVector&& argNodes, ui32 usedArgs, const NUdf::TSourcePosition& pos)
        : TBaseComputation(mutables, kind)
        , CallableNode(callableNode)
        , ArgNodes(std::move(argNodes))
        , UsedArgs(usedArgs)
        , Position(pos)
    {
        Stateless = false;
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        NStackArray::TStackArray<NUdf::TUnboxedValue> values(ALLOC_ON_STACK(NUdf::TUnboxedValue, UsedArgs));
        for (size_t i = 0; i < UsedArgs; ++i) {
            if (const auto valueNode = ArgNodes[i]) {
                values[i] = valueNode->GetValue(ctx);
            }
        }

        const auto callable = CallableNode->GetValue(ctx);
        const auto prev = ctx.CalleePosition;
        ctx.CalleePosition = &Position;
        const auto ret = callable.Run(ctx.Builder, values.data());
        ctx.CalleePosition = prev;
        return ret;
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        auto& context = ctx.Codegen->GetContext();

        const auto idxType = Type::getInt32Ty(context);
        const auto valType = Type::getInt128Ty(context);
        const auto arrayType = ArrayType::get(valType, ArgNodes.size());
        const auto args = *Stateless || ctx.AlwaysInline ?
            new AllocaInst(arrayType, 0U, "args", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(arrayType, 0U, "args", block);

        ui32 i = 0;
        std::vector<std::pair<Value*, EValueRepresentation>> argsv;
        argsv.reserve(ArgNodes.size());
        for (const auto node : ArgNodes) {
            const auto argPtr = GetElementPtrInst::CreateInBounds(arrayType, args, {ConstantInt::get(idxType, 0), ConstantInt::get(idxType, i++)}, "arg_ptr", block);
            if (node) {
                GetNodeValue(argPtr, node, ctx, block);
                argsv.emplace_back(argPtr, node->GetRepresentation());
            } else {
                new StoreInst(ConstantInt::get(valType, 0), argPtr, block);
            }
        }

        if (const auto codegen = dynamic_cast<ICodegeneratorRunNode*>(CallableNode)) {
            codegen->CreateRun(ctx, block, pointer, args);
        } else {
            const auto callable = GetNodeValue(CallableNode, ctx, block);
            const auto calleePtr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), ctx.Ctx, {ConstantInt::get(idxType, 0), ConstantInt::get(idxType, 6)}, "callee_ptr", block);
            const auto previous = new LoadInst(calleePtr->getType()->getPointerElementType(), calleePtr, "previous", block);
            const auto callee = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), ui64(&Position)), previous->getType(), "callee", block);
            new StoreInst(callee, calleePtr, block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Run>(pointer, callable, ctx.Codegen, block, ctx.GetBuilder(), args);
            new StoreInst(previous, calleePtr, block);
            if (CallableNode->IsTemporaryValue()) {
                CleanupBoxed(callable, ctx, block);
            }
        }
        for (const auto& arg : argsv) {
            ValueUnRef(arg.second, arg.first, ctx, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(CallableNode);
        for (const auto node : ArgNodes) {
            if (node) {
                DependsOn(node);
            }
        }
    }

    IComputationNode *const CallableNode;
    const TComputationNodePtrVector ArgNodes;
    const ui32 UsedArgs;
    const NUdf::TSourcePosition Position;
};

}

IComputationNode* WrapApply(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const bool withPos = callable.GetType()->GetName() == "Apply2";
    const ui32 deltaArgs = withPos ? 3 : 0;
    MKQL_ENSURE(callable.GetInputsCount() >= 2 + deltaArgs, "Expected at least " << (2 + deltaArgs) << " arguments");

    const auto function = callable.GetInput(0);
    MKQL_ENSURE(!function.IsImmediate() && function.GetNode()->GetType()->IsCallable(),
                "First argument of Apply must be a callable");

    const auto functionCallable = static_cast<TCallable*>(function.GetNode());
    const auto returnType = functionCallable->GetType()->GetReturnType();
    MKQL_ENSURE(returnType->IsCallable(), "Expected callable as return type");

    const TStringBuf file = withPos ? AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef() : NUdf::TStringRef();
    const ui32 row = withPos ? AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>() : 0;
    const ui32 column = withPos ? AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<ui32>() : 0;

    const ui32 inputsCount = callable.GetInputsCount() - deltaArgs;
    const ui32 argsCount = inputsCount - 2;

    const ui32 dependentCount = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    MKQL_ENSURE(dependentCount <= argsCount, "Too many dependent nodes");
    const ui32 usedArgs = argsCount - dependentCount;

    auto callableType = static_cast<TCallableType*>(returnType);
    MKQL_ENSURE(usedArgs <= callableType->GetArgumentsCount(), "Too many arguments");
    MKQL_ENSURE(usedArgs >= callableType->GetArgumentsCount() - callableType->GetOptionalArgumentsCount(), "Too few arguments");

    TComputationNodePtrVector argNodes(callableType->GetArgumentsCount() + dependentCount);
    for (ui32 i = 2; i < 2 + usedArgs; ++i) {
        argNodes[i - 2] = LocateNode(ctx.NodeLocator, callable, i + deltaArgs);
    }

    for (ui32 i = 2 + usedArgs; i < inputsCount; ++i) {
        argNodes[callableType->GetArgumentsCount() + i - 2 - usedArgs] = LocateNode(ctx.NodeLocator, callable, i + deltaArgs);
    }

    auto functionNode = LocateNode(ctx.NodeLocator, callable, 0);
    return new TApplyWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), functionNode, std::move(argNodes),
        callableType->GetArgumentsCount(), NUdf::TSourcePosition(row, column, file));
}

}
}
