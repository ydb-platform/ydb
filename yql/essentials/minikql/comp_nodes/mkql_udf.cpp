#include "mkql_udf.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_validate.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/containers/stack_array/stack_array.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr size_t TypeDiffLimit = 1000;

TString TruncateTypeDiff(const TString& s) {
    if (s.size() < TypeDiffLimit) {
        return s;
    }

    return s.substr(0,TypeDiffLimit) + "...";
}

template<class TValidatePolicy, class TValidateMode>
class TSimpleUdfWrapper: public TMutableComputationNode<TSimpleUdfWrapper<TValidatePolicy,TValidateMode>> {
using TBaseComputation = TMutableComputationNode<TSimpleUdfWrapper<TValidatePolicy,TValidateMode>>;
public:
    TSimpleUdfWrapper(
            TComputationMutables& mutables,
            TString&& functionName,
            TString&& typeConfig,
            NUdf::TSourcePosition pos,
            const TCallableType* callableType,
            const TCallableType* functionType,
            TType* userType)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , FunctionName(std::move(functionName))
        , TypeConfig(std::move(typeConfig))
        , Pos(pos)
        , CallableType(callableType)
        , FunctionType(functionType)
        , UserType(userType)
    {
        this->Stateless = false;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        ui32 flags = 0;
        TFunctionTypeInfo funcInfo;
        const auto status = ctx.HolderFactory.GetFunctionRegistry()->FindFunctionTypeInfo(
            ctx.LangVer, ctx.TypeEnv, ctx.TypeInfoHelper, ctx.CountersProvider, FunctionName, UserType->IsVoid() ? nullptr : UserType,
            TypeConfig, flags, Pos, ctx.SecureParamsProvider, ctx.LogProvider, &funcInfo);

        if (!status.IsOk()) {
            UdfTerminate((TStringBuilder() << Pos << " Failed to find UDF function " << FunctionName << ", reason: "
                << status.GetError()).c_str());
        }

        if (!funcInfo.Implementation) {
            UdfTerminate((TStringBuilder() << Pos << " UDF implementation is not set for function " << FunctionName).c_str());
        }

        NUdf::TUnboxedValue udf(NUdf::TUnboxedValuePod(funcInfo.Implementation.Release()));
        TValidate<TValidatePolicy,TValidateMode>::WrapCallable(FunctionType, udf, TStringBuilder() << "FunctionWithConfig<" << FunctionName << ">");
        ExtendArgs(udf, CallableType, funcInfo.FunctionType);
        return udf.Release();
    }
private:
    // xXX: This class implements the wrapper to properly handle
    // the case when the signature of the emitted callable (i.e.
    // callable type) requires less arguments than the actual
    // function (i.e. function type). It wraps the unboxed value
    // with the resolved UDF to introduce the bridge in the
    // Run chain, preparing the valid argument vector for the
    // chosen UDF implementation.
    class TExtendedArgsWrapper: public NUdf::TBoxedValue {
    public:
        TExtendedArgsWrapper(NUdf::TUnboxedValue&& callable, size_t usedArgs, size_t requiredArgs)
            : Callable_(callable)
            , UsedArgs_(usedArgs)
            , RequiredArgs_(requiredArgs)
        {};

    private:
        NUdf::TUnboxedValue Run(const NUdf::IValueBuilder* valueBuilder, const NUdf::TUnboxedValuePod* args) const final {
            NStackArray::TStackArray<NUdf::TUnboxedValue> values(ALLOC_ON_STACK(NUdf::TUnboxedValue, RequiredArgs_));
            for (size_t i = 0; i < UsedArgs_; i++) {
                values[i] = args[i];
            }
            return Callable_.Run(valueBuilder, values.data());
        }

        const NUdf::TUnboxedValue Callable_;
        const size_t UsedArgs_;
        const size_t RequiredArgs_;
    };

    void ExtendArgs(NUdf::TUnboxedValue& callable, const TCallableType* callableType, const TCallableType* functionType) const {
        const auto callableArgc = callableType->GetArgumentsCount();
        const auto functionArgc = functionType->GetArgumentsCount();
        if (callableArgc < functionArgc) {
            callable = NUdf::TUnboxedValuePod(new TExtendedArgsWrapper(std::move(callable), callableArgc, functionArgc));
        }
    }

    void RegisterDependencies() const final {}

    const TString FunctionName;
    const TString TypeConfig;
    const NUdf::TSourcePosition Pos;
    const TCallableType *const CallableType;
    const TCallableType *const FunctionType;
    TType *const UserType;
};

class TUdfRunCodegeneratorNode: public TSimpleUdfWrapper<TValidateErrorPolicyNone, TValidateModeLazy<TValidateErrorPolicyNone>>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRunNode
#endif
{
public:
    TUdfRunCodegeneratorNode(
            TComputationMutables& mutables,
            TString&& functionName,
            TString&& typeConfig,
            NUdf::TSourcePosition pos,
            const TCallableType* callableType,
            const TCallableType* functionType,
            TType* userType,
            TString&& moduleIRUniqID,
            TString&& moduleIR,
            TString&& fuctioNameIR,
            NUdf::TUniquePtr<NUdf::IBoxedValue>&& impl)
        : TSimpleUdfWrapper(mutables, std::move(functionName), std::move(typeConfig), pos, callableType, functionType, userType)
        , ModuleIRUniqID(std::move(moduleIRUniqID))
        , ModuleIR(std::move(moduleIR))
        , IRFunctionName(std::move(fuctioNameIR))
        , Impl(std::move(impl))
    {}
#ifndef MKQL_DISABLE_CODEGEN
    void CreateRun(const TCodegenContext& ctx, BasicBlock*& block, Value* result, Value* args) const final {
        ctx.Codegen.LoadBitCode(ModuleIR, ModuleIRUniqID);

        auto& context = ctx.Codegen.GetContext();

        const auto type = Type::getInt128Ty(context);
        YQL_ENSURE(result->getType() == PointerType::getUnqual(type));

        const auto data = ConstantInt::get(Type::getInt64Ty(context), reinterpret_cast<ui64>(Impl.Get()));
        const auto ptrStructType = PointerType::getUnqual(StructType::get(context));
        const auto boxed = CastInst::Create(Instruction::IntToPtr, data, ptrStructType, "boxed", block);
        const auto builder = ctx.GetBuilder();

        const auto funType = FunctionType::get(Type::getVoidTy(context), {boxed->getType(), result->getType(), builder->getType(), args->getType()}, false);
        const auto runFunc = ctx.Codegen.GetModule().getOrInsertFunction(llvm::StringRef(IRFunctionName.data(), IRFunctionName.size()), funType);
        CallInst::Create(runFunc, {boxed, result, builder, args}, "", block);
    }
#endif
private:
    const TString ModuleIRUniqID;
    const TString ModuleIR;
    const TString IRFunctionName;
    const NUdf::TUniquePtr<NUdf::IBoxedValue> Impl;
};

template<class TValidatePolicy, class TValidateMode>
class TUdfWrapper: public TMutableCodegeneratorPtrNode<TUdfWrapper<TValidatePolicy,TValidateMode>> {
using TBaseComputation = TMutableCodegeneratorPtrNode<TUdfWrapper<TValidatePolicy,TValidateMode>>;
public:
    TUdfWrapper(
            TComputationMutables& mutables,
            TString&& functionName,
            TString&& typeConfig,
            NUdf::TSourcePosition pos,
            IComputationNode* runConfigNode,
            ui32 runConfigArgs,
            const TCallableType* callableType,
            TType* userType)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , FunctionName(std::move(functionName))
        , TypeConfig(std::move(typeConfig))
        , Pos(pos)
        , RunConfigNode(runConfigNode)
        , RunConfigArgs(runConfigArgs)
        , CallableType(callableType)
        , UserType(userType)
        , UdfIndex(mutables.CurValueIndex++)
    {
        this->Stateless = false;
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto& udf = ctx.MutableValues[UdfIndex];
        if (!udf.HasValue()) {
            MakeUdf(ctx, udf);
        }
        NStackArray::TStackArray<NUdf::TUnboxedValue> args(ALLOC_ON_STACK(NUdf::TUnboxedValue, RunConfigArgs));
        args[0] = RunConfigNode->GetValue(ctx);
        auto callable = udf.Run(ctx.Builder, args.data());
        Wrap(callable);
        return callable;
    }
#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto indexType = Type::getInt32Ty(context);
        const auto valueType = Type::getInt128Ty(context);

        const auto udfPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), UdfIndex)}, "udf_ptr", block);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);

        BranchInst::Create(main, make, HasValue(udfPtr, block, context), block);

        block = make;

        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<&TUdfWrapper::MakeUdf>());
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), udfPtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, udfPtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto argsType = ArrayType::get(valueType, RunConfigArgs);
        const auto args = new AllocaInst(argsType, 0U, "args", block);
        const auto zero = ConstantInt::get(indexType, 0);
        Value* runConfigValue;
        for (ui32 i = 0; i < RunConfigArgs; i++) {
            const auto argIndex = ConstantInt::get(indexType, i);
            const auto argSlot = GetElementPtrInst::CreateInBounds(argsType, args, {zero, argIndex}, "arg", block);
            if (i == 0) {
                GetNodeValue(argSlot, RunConfigNode, ctx, block);
                runConfigValue = new LoadInst(valueType, argSlot, "runconfig", block);
            } else {
                new StoreInst(ConstantInt::get(valueType, 0U), argSlot, block);
            }
        }
        const auto udf = new LoadInst(valueType, udfPtr, "udf", block);

        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Run>(pointer, udf, ctx.Codegen, block, ctx.GetBuilder(), args);

        ValueUnRef(RunConfigNode->GetRepresentation(), runConfigValue, ctx, block);

        const auto wrap = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<&TUdfWrapper::Wrap>());
        const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), pointer->getType()}, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, wrap, PointerType::getUnqual(funType), "function", block);
        CallInst::Create(funType, doFuncPtr, {self, pointer}, "", block);
    }
#endif
private:
    void MakeUdf(TComputationContext& ctx, NUdf::TUnboxedValue& udf) const {
        ui32 flags = 0;
        TFunctionTypeInfo funcInfo;
        const auto status = ctx.HolderFactory.GetFunctionRegistry()->FindFunctionTypeInfo(
            ctx.LangVer, ctx.TypeEnv, ctx.TypeInfoHelper, ctx.CountersProvider, FunctionName, UserType->IsVoid() ? nullptr : UserType,
            TypeConfig, flags, Pos, ctx.SecureParamsProvider, ctx.LogProvider, &funcInfo);

        if (!status.IsOk()) {
            UdfTerminate((TStringBuilder() << Pos << " Failed to find UDF function " << FunctionName << ", reason: "
                << status.GetError()).c_str());
        }

        if (!funcInfo.Implementation) {
            UdfTerminate((TStringBuilder() << Pos << " UDF implementation is not set for function " << FunctionName).c_str());
        }

        udf =  NUdf::TUnboxedValuePod(funcInfo.Implementation.Release());
    }

    void Wrap(NUdf::TUnboxedValue& callable) const {
        TValidate<TValidatePolicy,TValidateMode>::WrapCallable(CallableType, callable, TStringBuilder() << "FunctionWithConfig<" << FunctionName << ">");
    }

    void RegisterDependencies() const final {
        this->DependsOn(RunConfigNode);
    }

    const TString FunctionName;
    const TString TypeConfig;
    const NUdf::TSourcePosition Pos;
    IComputationNode* const RunConfigNode;
    const ui32 RunConfigArgs;
    const TCallableType* CallableType;
    TType* const UserType;
    const ui32 UdfIndex;
};

template<bool Simple, class TValidatePolicy, class TValidateMode>
using TWrapper = std::conditional_t<Simple, TSimpleUdfWrapper<TValidatePolicy, TValidateMode>, TUdfWrapper<TValidatePolicy, TValidateMode>>;

template<bool Simple, typename...TArgs>
inline IComputationNode* CreateUdfWrapper(const TComputationNodeFactoryContext& ctx, TArgs&&...args)
{
    switch (ctx.ValidateMode) {
        case NUdf::EValidateMode::None:
            return new TWrapper<Simple, TValidateErrorPolicyNone,TValidateModeLazy<TValidateErrorPolicyNone>>(ctx.Mutables, std::forward<TArgs>(args)...);
        case NUdf::EValidateMode::Lazy:
            if (ctx.ValidatePolicy == NUdf::EValidatePolicy::Fail) {
                return new TWrapper<Simple, TValidateErrorPolicyFail,TValidateModeLazy<TValidateErrorPolicyFail>>(ctx.Mutables, std::forward<TArgs>(args)...);
            } else {
                return new TWrapper<Simple, TValidateErrorPolicyThrow,TValidateModeLazy<TValidateErrorPolicyThrow>>(ctx.Mutables, std::forward<TArgs>(args)...);
            }
        case NUdf::EValidateMode::Greedy:
            if (ctx.ValidatePolicy == NUdf::EValidatePolicy::Fail) {
                return new TWrapper<Simple, TValidateErrorPolicyFail,TValidateModeGreedy<TValidateErrorPolicyFail>>(ctx.Mutables, std::forward<TArgs>(args)...);
            } else {
                return new TWrapper<Simple, TValidateErrorPolicyThrow,TValidateModeGreedy<TValidateErrorPolicyThrow>>(ctx.Mutables, std::forward<TArgs>(args)...);
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
    TString typeConfig(AS_VALUE(TDataLiteral, typeCfgNode)->AsValue().AsStringRef());

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
        ctx.LangVer, ctx.Env, ctx.TypeInfoHelper, ctx.CountersProvider, funcName, userType->IsVoid() ? nullptr : userType,
        typeConfig, flags, pos, ctx.SecureParamsProvider, ctx.LogProvider, &funcInfo);

    if (!status.IsOk()) {
        UdfTerminate((TStringBuilder() << pos << " Failed to find UDF function " << funcName << ", reason: "
            << status.GetError()).c_str());
    }

    const auto callableFuncType = AS_TYPE(TCallableType, funcInfo.FunctionType);
    const auto callableNodeType = AS_TYPE(TCallableType, callable.GetType()->GetReturnType());
    const auto runConfigFuncType = funcInfo.RunConfigType;
    const auto runConfigNodeType = runCfgNode.GetStaticType();

    if (!runConfigFuncType->IsSameType(*runConfigNodeType)) {
        // It's only legal, when the compiled UDF declares its
        // signature using run config at compilation phase, but then
        // omits it in favor to function currying at execution phase.
        // And vice versa for the forward compatibility.
        if (!runConfigNodeType->IsVoid() && !runConfigFuncType->IsVoid()) {
            TString diff = TStringBuilder()
                << "run config type mismatch, expected: "
                << PrintNode((runConfigNodeType), true)
                << ", actual: "
                << PrintNode(runConfigFuncType, true);
            UdfTerminate((TStringBuilder() << pos
                                           << " UDF Function '"
                                           << funcName
                                           << "' "
                                           << TruncateTypeDiff(diff)).c_str());
        }

        const auto callableType = runConfigNodeType->IsVoid()
                                ? callableNodeType : callableFuncType;
        const auto runConfigType = runConfigNodeType->IsVoid()
                                 ? runConfigFuncType : runConfigNodeType;

        // If so, check the following invariants:
        // * The first argument of the head function in the sequence
        //   of the curried functions has to be the same as the
        //   run config type.
        // * All other arguments of the head function in the sequence
        //   of the curried function have to be optional.
        // * The type of the resulting callable has to be the same
        //   as the function type.
        if (callableType->GetArgumentsCount() - callableType->GetOptionalArgumentsCount() != 1U) {
            UdfTerminate((TStringBuilder() << pos
                                           << " Udf Function '"
                                           << funcName
                                           << "' wrapper has more than one required argument: "
                                           << PrintNode(callableType)).c_str());
        }
        const auto firstArgType = callableType->GetArgumentType(0);
        if (!runConfigType->IsSameType(*firstArgType)) {
            TString diff = TStringBuilder()
                << "type mismatch, expected run config type: "
                << PrintNode(runConfigType, true)
                << ", actual: "
                << PrintNode(firstArgType, true);
            UdfTerminate((TStringBuilder() << pos
                                           << " Udf Function '"
                                           << funcName
                                           << "' "
                                           << TruncateTypeDiff(diff)).c_str());
        }
        const auto closureFuncType = runConfigNodeType->IsVoid()
                                   ? callableFuncType
                                   : AS_TYPE(TCallableType, callableFuncType)->GetReturnType();
        const auto closureNodeType = runConfigNodeType->IsVoid()
                                   ? AS_TYPE(TCallableType, callableNodeType)->GetReturnType()
                                   : callableNodeType;
        if (!closureNodeType->IsConvertableTo(*closureFuncType)) {
            TString diff = TStringBuilder()
                << "type mismatch, expected return type: "
                << PrintNode(closureNodeType, true)
                << ", actual: "
                << PrintNode(closureFuncType, true);
            UdfTerminate((TStringBuilder() << pos
                                           << " Udf Function '"
                                           << funcName
                                           << "' "
                                           << TruncateTypeDiff(diff)).c_str());
        }

        const auto runConfigCompNode = LocateNode(ctx.NodeLocator, *runCfgNode.GetNode());
        const auto runConfigArgs = funcInfo.FunctionType->GetArgumentsCount();
        return runConfigNodeType->IsVoid()
            ? CreateUdfWrapper<true>(ctx, std::move(funcName), std::move(typeConfig), pos, callableNodeType, callableFuncType, userType)
            : CreateUdfWrapper<false>(ctx, std::move(funcName), std::move(typeConfig), pos, runConfigCompNode, runConfigArgs, callableNodeType, userType);
    }

    if (!callableFuncType->IsConvertableTo(*callableNodeType, true)) {
        TString diff = TStringBuilder() << "type mismatch, expected return type: " << PrintNode(callableNodeType, true) <<
                ", actual:" << PrintNode(callableFuncType, true);
        UdfTerminate((TStringBuilder() << pos << " UDF Function '" << funcName << "' " << TruncateTypeDiff(diff)).c_str());
    }

    if (!funcInfo.Implementation) {
        UdfTerminate((TStringBuilder() << pos << " UDF implementation is not set for function " << funcName).c_str());
    }

    if (runConfigFuncType->IsVoid()) {
        if (ctx.ValidateMode == NUdf::EValidateMode::None && funcInfo.ModuleIR && funcInfo.IRFunctionName) {
            return new TUdfRunCodegeneratorNode(
                ctx.Mutables, std::move(funcName), std::move(typeConfig), pos, callableNodeType, callableFuncType, userType,
                std::move(funcInfo.ModuleIRUniqID), std::move(funcInfo.ModuleIR), std::move(funcInfo.IRFunctionName), std::move(funcInfo.Implementation)
            );
        }
        return CreateUdfWrapper<true>(ctx, std::move(funcName), std::move(typeConfig), pos, callableNodeType, callableFuncType, userType);
    }

    const auto runCfgCompNode = LocateNode(ctx.NodeLocator, *runCfgNode.GetNode());
    return CreateUdfWrapper<false>(ctx, std::move(funcName), std::move(typeConfig), pos, runCfgCompNode, 1U, callableNodeType, userType);
}

IComputationNode* WrapScriptUdf(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4 || callable.GetInputsCount() == 7, "Expected 4 or 7 arguments");

    const auto funcNameNode = callable.GetInput(0);
    const auto userTypeNode = callable.GetInput(1);
    const auto typeConfigNode = callable.GetInput(2);
    const auto programNode = callable.GetInput(3);
    MKQL_ENSURE(userTypeNode.IsImmediate() && userTypeNode.GetStaticType()->IsType(), "Expected immediate type");

    TString funcName(AS_VALUE(TDataLiteral, funcNameNode)->AsValue().AsStringRef());
    TString typeConfig(AS_VALUE(TDataLiteral, typeConfigNode)->AsValue().AsStringRef());

    NUdf::TSourcePosition pos;
    if (callable.GetInputsCount() == 7) {
        pos.File_ = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().AsStringRef();
        pos.Row_ = AS_VALUE(TDataLiteral, callable.GetInput(5))->AsValue().Get<ui32>();
        pos.Column_ = AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<ui32>();
    }

    const auto userType = static_cast<TType*>(userTypeNode.GetNode());
    ui32 flags = 0;
    TFunctionTypeInfo funcInfo;
    const auto status = ctx.FunctionRegistry.FindFunctionTypeInfo(
        ctx.LangVer, ctx.Env, ctx.TypeInfoHelper, ctx.CountersProvider, funcName, userType,
        typeConfig, flags, pos, ctx.SecureParamsProvider, ctx.LogProvider, &funcInfo);

    if (!status.IsOk()) {
        UdfTerminate((TStringBuilder() << pos << " Failed to find UDF function " << funcName << ", reason: "
            << status.GetError()).c_str());
    }

    if (!funcInfo.Implementation) {
        UdfTerminate((TStringBuilder() << pos << " UDF implementation is not set for function " << funcName).c_str());
    }

    if (funcInfo.FunctionType) {
        UdfTerminate((TStringBuilder() << pos << " UDF function type exists for function " << funcName).c_str());
    }

    const auto callableType = callable.GetType();
    MKQL_ENSURE(callableType->GetKind() == TType::EKind::Callable, "Expected callable type in callable type info");
    const auto callableResultType = callableType->GetReturnType();
    MKQL_ENSURE(callableResultType->GetKind() == TType::EKind::Callable, "Expected callable type in result of script wrapper");
    const auto funcTypeInfo = static_cast<TCallableType*>(callableResultType);

    const auto programCompNode = LocateNode(ctx.NodeLocator, *programNode.GetNode());
    return CreateUdfWrapper<false>(ctx, std::move(funcName), std::move(typeConfig), pos, programCompNode, 1U, funcTypeInfo, userType);
}

}
}
