#include "mkql_invoke.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsOptional>
class TUnaryArgInvokeBase {
protected:
    TUnaryArgInvokeBase(TStringBuf name, const TFunctionDescriptor& descr)
        : Name(name), Descriptor(descr)
    {}

    NUdf::TUnboxedValuePod DoCalc(const NUdf::TUnboxedValuePod& arg) const {
        if (IsOptional && !arg) {
            return {};
        }
        return Descriptor.Function(&arg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenGetValue(const TCodegenContext& ctx, Value* arg, BasicBlock*& block) const {
        if (IsOptional) {
            auto& context = ctx.Codegen.GetContext();

            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto result = PHINode::Create(arg->getType(), 2U, "result", done);

            result->addIncoming(arg, block);
            BranchInst::Create(good, done, IsExists(arg, block), block);

            block = good;
            const auto out = reinterpret_cast<TGeneratorPtr>(Descriptor.Generator)(&arg, ctx, block);

            result->addIncoming(out, block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            return reinterpret_cast<TGeneratorPtr>(Descriptor.Generator)(&arg, ctx, block);
        }
    }
#endif
    const TStringBuf Name;
    const TFunctionDescriptor Descriptor;
};

template<bool IsOptional>
class TSimpleUnaryArgInvokeWrapper : public TDecoratorCodegeneratorNode<TSimpleUnaryArgInvokeWrapper<IsOptional>>, private TUnaryArgInvokeBase<IsOptional> {
    typedef TDecoratorCodegeneratorNode<TSimpleUnaryArgInvokeWrapper<IsOptional>> TBaseComputation;
public:
    TSimpleUnaryArgInvokeWrapper(TStringBuf name, const TFunctionDescriptor& descr, IComputationNode* arg)
        : TBaseComputation(arg), TUnaryArgInvokeBase<IsOptional>(name, descr)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& arg) const {
        return this->DoCalc(arg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* arg, BasicBlock*& block) const {
        return this->DoGenGetValue(ctx, arg, block);
    }
#endif

private:
    TString DebugString() const final {
        return TBaseComputation::DebugString() + "(" + this->Name + ")" ;
    }
};

template<bool IsOptional>
class TDefaultUnaryArgInvokeWrapper : public TMutableCodegeneratorNode<TDefaultUnaryArgInvokeWrapper<IsOptional>>, private TUnaryArgInvokeBase<IsOptional> {
    typedef TMutableCodegeneratorNode<TDefaultUnaryArgInvokeWrapper<IsOptional>> TBaseComputation;
public:
    TDefaultUnaryArgInvokeWrapper(TComputationMutables& mutables, EValueRepresentation kind, TStringBuf name, const TFunctionDescriptor& descr, IComputationNode* arg)
        : TBaseComputation(mutables, kind), TUnaryArgInvokeBase<IsOptional>(name, descr), Arg(arg)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return this->DoCalc(Arg->GetValue(ctx));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        const auto arg = GetNodeValue(Arg, ctx, block);
        return this->DoGenGetValue(ctx, arg, block);
    }
#endif

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    TString DebugString() const final {
        return TBaseComputation::DebugString() + "(" + this->Name + ")" ;
    }

    IComputationNode *const Arg;
};

class TBinaryInvokeWrapper : public TBinaryCodegeneratorNode<TBinaryInvokeWrapper> {
    typedef TBinaryCodegeneratorNode<TBinaryInvokeWrapper> TBaseComputation;
public:
    TBinaryInvokeWrapper(TStringBuf name, const TFunctionDescriptor& descr, IComputationNode* left, IComputationNode* right, EValueRepresentation kind = EValueRepresentation::Embedded)
        : TBaseComputation(left, right, kind), Name(name), Descriptor(descr)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const std::array<NUdf::TUnboxedValue, 2U> args {{Left->GetValue(compCtx), Right->GetValue(compCtx)}};
        return Descriptor.Function(args.data());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        const std::array<Value*, 2U> args {{GetNodeValue(Left, ctx, block), GetNodeValue(Right, ctx, block)}};
        return reinterpret_cast<TGeneratorPtr>(Descriptor.Generator)(args.data(), ctx, block);
    }
#endif

private:
    TString DebugString() const final {
        return TBaseComputation::DebugString() + "(" + Name + ")" ;
    }

    const TStringBuf Name;
    const TFunctionDescriptor Descriptor;
};

template<size_t Size>
class TInvokeWrapper : public TMutableCodegeneratorNode<TInvokeWrapper<Size>> {
    typedef TMutableCodegeneratorNode<TInvokeWrapper<Size>> TBaseComputation;
public:
    TInvokeWrapper(TComputationMutables& mutables, EValueRepresentation kind, TStringBuf name, const TFunctionDescriptor& descr, TComputationNodePtrVector&& argNodes)
        : TBaseComputation(mutables, kind)
        , Name(name), Descriptor(descr)
        , ArgNodes(std::move(argNodes))
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        std::array<NUdf::TUnboxedValue, Size> values;
        std::transform(ArgNodes.cbegin(), ArgNodes.cend(), values.begin(),
            std::bind(&IComputationNode::GetValue, std::placeholders::_1, std::ref(ctx))
        );
        return Descriptor.Function(values.data());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        std::array<Value*, Size> values;
        std::transform(ArgNodes.cbegin(), ArgNodes.cend(), values.begin(),
            [&](IComputationNode* node) { return GetNodeValue(node, ctx, block); }
        );
        return reinterpret_cast<TGeneratorPtr>(Descriptor.Generator)(values.data(), ctx, block);
    }
#endif

private:
    void RegisterDependencies() const final {
        std::for_each(ArgNodes.cbegin(), ArgNodes.cend(), std::bind(&TInvokeWrapper::DependsOn, this, std::placeholders::_1));
    }

    TString DebugString() const final {
        return TBaseComputation::DebugString() + "(" + Name + ")" ;
    }

    const TStringBuf Name;
    const TFunctionDescriptor Descriptor;
    const TComputationNodePtrVector ArgNodes;
};

}

IComputationNode* WrapInvoke(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 2U && callable.GetInputsCount() <= 4U, "Expected from one to three arguments.");
    const auto returnType = callable.GetType()->GetReturnType();

    const auto inputsCount = callable.GetInputsCount();
    std::array<TArgType, 4U> argsTypes;
    TComputationNodePtrVector argNodes;
    argNodes.reserve(inputsCount - 1U);
    argsTypes.front().first = UnpackOptionalData(returnType, argsTypes.front().second)->GetSchemeType();
    for (ui32 i = 1U; i < inputsCount; ++i) {
        argsTypes[i].first = UnpackOptionalData(callable.GetInput(i), argsTypes[i].second)->GetSchemeType();
        argNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
    }

    const auto funcName = AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef();
    const auto funcDesc = ctx.FunctionRegistry.GetBuiltins()->GetBuiltin(funcName, argsTypes.data(), inputsCount);

    const auto returnKind = GetValueRepresentation(returnType);
    switch (argNodes.size()) {
    case 1U:
        if (EValueRepresentation::Embedded == returnKind) {
            return new TSimpleUnaryArgInvokeWrapper<false>(funcName, funcDesc, argNodes.front());
        } else {
            return new TDefaultUnaryArgInvokeWrapper<false>(ctx.Mutables, returnKind, funcName, funcDesc, argNodes.front());
        }
    case 2U:
        if (EValueRepresentation::Embedded == returnKind) {
            return new TBinaryInvokeWrapper(funcName, funcDesc, argNodes.front(), argNodes.back());
        }
        return new TInvokeWrapper<2U>(ctx.Mutables, returnKind, funcName, funcDesc, std::move(argNodes));
    case 3U:
        return new TInvokeWrapper<3U>(ctx.Mutables, returnKind, funcName, funcDesc, std::move(argNodes));
    default:
        Y_ABORT("Too wide invoke.");
    }
}

}
}
