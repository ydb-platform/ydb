#include "mkql_udf.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/computation/mkql_validate.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TUdfRunCodegeneratorNode: public TUnboxedImmutableRunCodegeneratorNode {
public:
    TUdfRunCodegeneratorNode(TMemoryUsageInfo* memInfo,
        NUdf::TUnboxedValue&& value, const TString& moduleIRUniqID, const TString& moduleIR, const TString& functionName)
        : TUnboxedImmutableRunCodegeneratorNode(memInfo, std::move(value))
        , ModuleIRUniqID(moduleIRUniqID)
        , ModuleIR(moduleIR)
        , FunctionName(functionName)
    {
    }

#ifndef MKQL_DISABLE_CODEGEN
    void CreateRun(const TCodegenContext& ctx, BasicBlock*& block, Value* result, Value* args) const final {
        ctx.Codegen.LoadBitCode(ModuleIR, ModuleIRUniqID);

        auto& context = ctx.Codegen.GetContext();

        const auto type = Type::getInt128Ty(context);
        YQL_ENSURE(result->getType() == PointerType::getUnqual(type));

        const auto data = ConstantInt::get(Type::getInt64Ty(context), reinterpret_cast<ui64>(UnboxedValue.AsBoxed().Get()));
        const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
        const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);
        const auto builder = ctx.GetBuilder();

        const auto funType = FunctionType::get(Type::getVoidTy(context), {boxed->getType(), result->getType(), builder->getType(), args->getType()}, false);
        const auto runFunc = ctx.Codegen.GetModule().getOrInsertFunction(llvm::StringRef(FunctionName.data(), FunctionName.size()), funType);
        CallInst::Create(runFunc, {boxed, result, builder, args}, "", block);
    }
#endif
private:
    const TString ModuleIRUniqID;
    const TString ModuleIR;
    const TString FunctionName;
};


template<class TValidatePolicy, class TValidateMode>
class TUdfWrapper: public TMutableCodegeneratorPtrNode<TUdfWrapper<TValidatePolicy,TValidateMode>> {
    typedef TMutableCodegeneratorPtrNode<TUdfWrapper<TValidatePolicy,TValidateMode>> TBaseComputation;
public:
    TUdfWrapper(
            TComputationMutables& mutables,
            IComputationNode* functionImpl,
            TString&& functionName,
            IComputationNode* runConfigNode,
            const TCallableType* callableType)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , FunctionImpl(functionImpl)
        , FunctionName(std::move(functionName))
        , RunConfigNode(runConfigNode)
        , CallableType(callableType)
    {
        this->Stateless = false;
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        const auto runConfig = RunConfigNode->GetValue(ctx);
        auto callable = FunctionImpl->GetValue(ctx).Run(ctx.Builder, &runConfig);
        Wrap(callable);
        return callable;
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        GetNodeValue(pointer, RunConfigNode, ctx, block);
        const auto conf = new LoadInst(Type::getInt128Ty(context), pointer, "conf", block);

        const auto func = GetNodeValue(FunctionImpl, ctx, block);

        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Run>(pointer, func, ctx.Codegen, block, ctx.GetBuilder(), pointer);

        ValueUnRef(RunConfigNode->GetRepresentation(), conf, ctx, block);

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto wrap = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TUdfWrapper::Wrap));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), pointer->getType()}, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, wrap, PointerType::getUnqual(funType), "function", block);
        CallInst::Create(funType, doFuncPtr, {self, pointer}, "", block);
    }
#endif
private:
    void Wrap(NUdf::TUnboxedValue& callable) const {
        MKQL_ENSURE(bool(callable), "Returned empty value in function: " << FunctionName);
        TValidate<TValidatePolicy,TValidateMode>::WrapCallable(CallableType, callable, TStringBuilder() << "FunctionWithConfig<" << FunctionName << ">");
    }

    void RegisterDependencies() const final {
        this->DependsOn(FunctionImpl);
        this->DependsOn(RunConfigNode);
    }

    IComputationNode* const FunctionImpl;
    const TString FunctionName;
    IComputationNode* const RunConfigNode;
    const TCallableType* const CallableType;
};

inline IComputationNode* CreateUdfWrapper(
    const TComputationNodeFactoryContext& ctx,
    NUdf::TUnboxedValue&& functionImpl,
    TString&& functionName,
    IComputationNode* runConfigNode,
    const TCallableType* callableType)
{
    const auto node = ctx.NodeFactory.CreateImmutableNode(std::move(functionImpl));
    ctx.NodePushBack(node);
    switch (ctx.ValidateMode) {
        case NUdf::EValidateMode::None:
            return new TUdfWrapper<TValidateErrorPolicyNone,TValidateModeLazy<TValidateErrorPolicyNone>>(ctx.Mutables, std::move(node), std::move(functionName), runConfigNode, callableType);
        case NUdf::EValidateMode::Lazy:
            if (ctx.ValidatePolicy == NUdf::EValidatePolicy::Fail) {
                return new TUdfWrapper<TValidateErrorPolicyFail,TValidateModeLazy<TValidateErrorPolicyFail>>(ctx.Mutables, std::move(node), std::move(functionName), runConfigNode, callableType);
            } else {
                return new TUdfWrapper<TValidateErrorPolicyThrow,TValidateModeLazy<TValidateErrorPolicyThrow>>(ctx.Mutables, std::move(node), std::move(functionName), runConfigNode, callableType);
            }
        case NUdf::EValidateMode::Greedy:
            if (ctx.ValidatePolicy == NUdf::EValidatePolicy::Fail) {
                return new TUdfWrapper<TValidateErrorPolicyFail,TValidateModeGreedy<TValidateErrorPolicyFail>>(ctx.Mutables, std::move(node), std::move(functionName), runConfigNode, callableType);
            } else {
                return new TUdfWrapper<TValidateErrorPolicyThrow,TValidateModeGreedy<TValidateErrorPolicyThrow>>(ctx.Mutables, std::move(node), std::move(functionName), runConfigNode, callableType);
            }
        default:
            Y_ABORT("Unexpected validate mode: %u", static_cast<unsigned>(ctx.ValidateMode));
    };
}

}

IComputationNode* WrapUdf(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4 || callable.GetInputsCount() == 7, "Expected 4 or 7 arguments");

    const auto funcNameNode = callable.GetInput(0);
    const auto userTypeNode = callable.GetInput(1);
    const auto typeCfgNode = callable.GetInput(2);
    const auto runCfgNode = callable.GetInput(3);

    MKQL_ENSURE(userTypeNode.IsImmediate(), "Expected immediate node");
    MKQL_ENSURE(userTypeNode.GetStaticType()->IsType(), "Expected type");

    TString funcName(AS_VALUE(TDataLiteral, funcNameNode)->AsValue().AsStringRef());
    TStringBuf typeConfig(AS_VALUE(TDataLiteral, typeCfgNode)->AsValue().AsStringRef());

    NUdf::TSourcePosition pos;
    if (callable.GetInputsCount() == 7) {
        pos.File_ = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().AsStringRef();
        pos.Row_ = AS_VALUE(TDataLiteral, callable.GetInput(5))->AsValue().Get<ui32>();
        pos.Column_ = AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<ui32>();
    }

    ui32 flags = 0;
    TFunctionTypeInfo funcInfo;
    const auto userType = static_cast<TType*>(userTypeNode.GetNode());
    const auto status = ctx.FunctionRegistry.FindFunctionTypeInfo(
        ctx.Env, ctx.TypeInfoHelper, ctx.CountersProvider, funcName, userType->IsVoid() ? nullptr : userType,
        typeConfig, flags, pos, ctx.SecureParamsProvider, &funcInfo);

    MKQL_ENSURE(status.IsOk(), status.GetError());
    MKQL_ENSURE(funcInfo.FunctionType->IsConvertableTo(*callable.GetType()->GetReturnType(), true),
                "Function '" << funcName << "' type mismatch, expected return type: " << PrintNode(callable.GetType()->GetReturnType(), true) <<
                ", actual:" << PrintNode(funcInfo.FunctionType, true));
    MKQL_ENSURE(funcInfo.Implementation, "UDF implementation is not set");

    const auto runConfigType = funcInfo.RunConfigType;
    const bool typesMatch = runConfigType->IsSameType(*runCfgNode.GetStaticType());
    MKQL_ENSURE(typesMatch, "RunConfig '" << funcName << "' type mismatch, expected: " << PrintNode(runCfgNode.GetStaticType(), true) <<
                ", actual: " << PrintNode(runConfigType, true));

    NUdf::TUnboxedValue impl(NUdf::TUnboxedValuePod(funcInfo.Implementation.Release()));
    if (runConfigType->IsVoid()) {
        // use function implementation as is

        if (ctx.ValidateMode == NUdf::EValidateMode::None && funcInfo.ModuleIR && funcInfo.IRFunctionName) {
            return new TUdfRunCodegeneratorNode(&ctx.HolderFactory.GetMemInfo(), std::move(impl), funcInfo.ModuleIRUniqID, funcInfo.ModuleIR, funcInfo.IRFunctionName);
        }

        if (ctx.ValidateMode != NUdf::EValidateMode::None) {
            if (ctx.ValidateMode == NUdf::EValidateMode::Lazy) {
                if (ctx.ValidatePolicy == NUdf::EValidatePolicy::Fail) {
                    TValidate<TValidateErrorPolicyFail, TValidateModeLazy<TValidateErrorPolicyFail>>::WrapCallable(funcInfo.FunctionType, impl, TStringBuilder() << "FunctionWrapper<" << funcName << ">");
                } else {
                    TValidate<TValidateErrorPolicyThrow, TValidateModeLazy<TValidateErrorPolicyThrow>>::WrapCallable(funcInfo.FunctionType, impl, TStringBuilder() << "FunctionWrapper<" << funcName << ">");
                }
            } else {
                if (ctx.ValidatePolicy == NUdf::EValidatePolicy::Fail) {
                    TValidate<TValidateErrorPolicyFail, TValidateModeGreedy<TValidateErrorPolicyFail>>::WrapCallable(funcInfo.FunctionType, impl, TStringBuilder() << "FunctionWrapper<" << funcName << ">");
                } else {
                    TValidate<TValidateErrorPolicyThrow, TValidateModeGreedy<TValidateErrorPolicyThrow>>::WrapCallable(funcInfo.FunctionType, impl, TStringBuilder() << "FunctionWrapper<" << funcName << ">");
                }
            }
        }
        return ctx.NodeFactory.CreateImmutableNode(std::move(impl));
    }

    // use function factory to get implementation by runconfig in runtime
    const auto runCfgCompNode = LocateNode(ctx.NodeLocator, *runCfgNode.GetNode());
    return CreateUdfWrapper(ctx, std::move(impl), std::move(funcName), runCfgCompNode, funcInfo.FunctionType);
}

IComputationNode* WrapScriptUdf(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4 || callable.GetInputsCount() == 7, "Expected 4 or 7 arguments");

    const auto funcNameNode = callable.GetInput(0);
    const auto userTypeNode = callable.GetInput(1);
    const auto typeConfigNode = callable.GetInput(2);
    const auto programNode = callable.GetInput(3);
    MKQL_ENSURE(userTypeNode.IsImmediate() && userTypeNode.GetStaticType()->IsType(), "Expected immediate type");

    TString funcName(AS_VALUE(TDataLiteral, funcNameNode)->AsValue().AsStringRef());
    TStringBuf typeConfig(AS_VALUE(TDataLiteral, typeConfigNode)->AsValue().AsStringRef());

    NUdf::TSourcePosition pos;
    if (callable.GetInputsCount() == 7) {
        pos.File_ = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().AsStringRef();
        pos.Row_ = AS_VALUE(TDataLiteral, callable.GetInput(5))->AsValue().Get<ui32>();
        pos.Column_ = AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<ui32>();
    }

    ui32 flags = 0;
    TFunctionTypeInfo funcInfo;
    const auto status = ctx.FunctionRegistry.FindFunctionTypeInfo(
        ctx.Env, ctx.TypeInfoHelper, ctx.CountersProvider, funcName, static_cast<TType*>(userTypeNode.GetNode()),
        typeConfig, flags, pos, ctx.SecureParamsProvider, &funcInfo);
    MKQL_ENSURE(status.IsOk(), status.GetError());
    MKQL_ENSURE(funcInfo.Implementation, "UDF implementation is not set");

    MKQL_ENSURE(!funcInfo.FunctionType, "Function type info is exist for same kind script, it's better use it");
    const auto callableType = callable.GetType();
    MKQL_ENSURE(callableType->GetKind() == TType::EKind::Callable, "Expected callable type in callable type info");
    const auto callableResultType = callableType->GetReturnType();
    MKQL_ENSURE(callableResultType->GetKind() == TType::EKind::Callable, "Expected callable type in result of script wrapper");
    const auto funcTypeInfo = static_cast<TCallableType*>(callableResultType);

    const auto programCompNode = LocateNode(ctx.NodeLocator, *programNode.GetNode());
    return CreateUdfWrapper(ctx, NUdf::TUnboxedValuePod(funcInfo.Implementation.Release()), std::move(funcName), programCompNode, funcTypeInfo);
}

}
}
