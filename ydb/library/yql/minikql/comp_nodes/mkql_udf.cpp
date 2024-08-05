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
            TType* userType)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , FunctionName(std::move(functionName))
        , TypeConfig(std::move(typeConfig))
        , Pos(pos)
        , CallableType(callableType)
        , UserType(userType)
    {
        this->Stateless = false;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        ui32 flags = 0;
        TFunctionTypeInfo funcInfo;
        const auto status = ctx.HolderFactory.GetFunctionRegistry()->FindFunctionTypeInfo(
            ctx.TypeEnv, ctx.TypeInfoHelper, ctx.CountersProvider, FunctionName, UserType,
            TypeConfig, flags, Pos, ctx.SecureParamsProvider, &funcInfo);

        MKQL_ENSURE(status.IsOk(), status.GetError());
        MKQL_ENSURE(funcInfo.Implementation, "UDF implementation is not set for function " << FunctionName);
        NUdf::TUnboxedValue udf(NUdf::TUnboxedValuePod(funcInfo.Implementation.Release()));
        TValidate<TValidatePolicy,TValidateMode>::WrapCallable(CallableType, udf, TStringBuilder() << "FunctionWithConfig<" << FunctionName << ">");
        return udf.Release();
    }
private:
    void RegisterDependencies() const final {}

    const TString FunctionName;
    const TString TypeConfig;
    const NUdf::TSourcePosition Pos;
    const TCallableType *const CallableType;
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
            TType* userType,
            TString&& moduleIRUniqID,
            TString&& moduleIR,
            TString&& fuctioNameIR,
            NUdf::TUniquePtr<NUdf::IBoxedValue>&& impl)
        : TSimpleUdfWrapper(mutables, std::move(functionName), std::move(typeConfig), pos, callableType, userType)
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
            const TCallableType* callableType,
            TType* userType)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , FunctionName(std::move(functionName))
        , TypeConfig(std::move(typeConfig))
        , Pos(pos)
        , RunConfigNode(runConfigNode)
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
        const auto runConfig = RunConfigNode->GetValue(ctx);
        auto callable = udf.Run(ctx.Builder, &runConfig);
        Wrap(callable);
        return runConfig;
    }
#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        const auto udfPtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), UdfIndex)}, "udf_ptr", block);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(main, make, HasValue(udfPtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TUdfWrapper::MakeUdf));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), udfPtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, udfPtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto udf = new LoadInst(valueType, udfPtr, "udf", block);

        GetNodeValue(pointer, RunConfigNode, ctx, block);
        const auto conf = new LoadInst(valueType, pointer, "conf", block);

        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Run>(pointer, udf, ctx.Codegen, block, ctx.GetBuilder(), pointer);

        ValueUnRef(RunConfigNode->GetRepresentation(), conf, ctx, block);

        const auto wrap = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TUdfWrapper::Wrap));
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
            ctx.TypeEnv, ctx.TypeInfoHelper, ctx.CountersProvider, FunctionName, UserType,
            TypeConfig, flags, Pos, ctx.SecureParamsProvider, &funcInfo);

        MKQL_ENSURE(status.IsOk(), status.GetError());
        MKQL_ENSURE(funcInfo.Implementation, "UDF implementation is not set for function " << FunctionName);
        udf =  NUdf::TUnboxedValuePod(funcInfo.Implementation.Release());
    }

    void Wrap(NUdf::TUnboxedValue& callable) const {
        MKQL_ENSURE(bool(callable), "Returned empty value in function: " << FunctionName);
        TValidate<TValidatePolicy,TValidateMode>::WrapCallable(CallableType, callable, TStringBuilder() << "FunctionWithConfig<" << FunctionName << ">");
    }

    void RegisterDependencies() const final {
        this->DependsOn(RunConfigNode);
    }

    const TString FunctionName;
    const TString TypeConfig;
    const NUdf::TSourcePosition Pos;
    IComputationNode* const RunConfigNode;
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
        ctx.Env, ctx.TypeInfoHelper, ctx.CountersProvider, funcName, userType->IsVoid() ? nullptr : userType,
        typeConfig, flags, pos, ctx.SecureParamsProvider, &funcInfo);

    MKQL_ENSURE(status.IsOk(), status.GetError());
    MKQL_ENSURE(funcInfo.FunctionType->IsConvertableTo(*callable.GetType()->GetReturnType(), true),
                "Function '" << funcName << "' type mismatch, expected return type: " << PrintNode(callable.GetType()->GetReturnType(), true) <<
                ", actual:" << PrintNode(funcInfo.FunctionType, true));
    MKQL_ENSURE(funcInfo.Implementation, "UDF implementation is not set for function " << funcName);

    const auto runConfigType = funcInfo.RunConfigType;
    const bool typesMatch = runConfigType->IsSameType(*runCfgNode.GetStaticType());
    MKQL_ENSURE(typesMatch, "RunConfig '" << funcName << "' type mismatch, expected: " << PrintNode(runCfgNode.GetStaticType(), true) <<
                ", actual: " << PrintNode(runConfigType, true));

    if (runConfigType->IsVoid()) {
        if (ctx.ValidateMode == NUdf::EValidateMode::None && funcInfo.ModuleIR && funcInfo.IRFunctionName) {
            return new TUdfRunCodegeneratorNode(
                ctx.Mutables, std::move(funcName), std::move(typeConfig), pos, funcInfo.FunctionType, userType,
                std::move(funcInfo.ModuleIRUniqID), std::move(funcInfo.ModuleIR), std::move(funcInfo.IRFunctionName), std::move(funcInfo.Implementation)
            );
        }
        return CreateUdfWrapper<true>(ctx, std::move(funcName), std::move(typeConfig), pos, funcInfo.FunctionType, userType);
    }

    const auto runCfgCompNode = LocateNode(ctx.NodeLocator, *runCfgNode.GetNode());
    return CreateUdfWrapper<false>(ctx, std::move(funcName), std::move(typeConfig), pos, runCfgCompNode, funcInfo.FunctionType, userType);
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
        ctx.Env, ctx.TypeInfoHelper, ctx.CountersProvider, funcName, userType,
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
    return CreateUdfWrapper<false>(ctx, std::move(funcName), std::move(typeConfig), pos, programCompNode, funcTypeInfo, userType);
}

}
}
