#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <arrow/memory_pool.h>

#ifndef MKQL_DISABLE_CODEGEN

namespace NKikimr::NMiniKQL {
namespace {

using namespace llvm;
using namespace NYql::NCodegen;

// Deliberately has no codegenerator interface: GetNodeValue must call C++.
class TNativeNode final: public TExternalComputationNode {
public:
    explicit TNativeNode(TComputationMutables& mutables)
        : TExternalComputationNode(mutables, EValueRepresentation::Embedded)
    {
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& ctx) const override {
        UNIT_ASSERT_VALUES_EQUAL(&ctx, ExpectedContext);
        return NUdf::TUnboxedValuePod(Payload);
    }

    TComputationContext* ExpectedContext = nullptr;
    ui64 Payload = 0x123456789abcdef0ULL;
};

void CheckNodeReturn(bool storeResult) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TMemoryUsageInfo memInfo("NativeReturnTest");
    THolderFactory factory(alloc.Ref(), memInfo);
    TDefaultValueBuilder builder(factory, NUdf::EValidatePolicy::Exception);
    auto random = CreateDeterministicRandomProvider(1);
    auto time = CreateDeterministicTimeProvider(1);
    auto settings = NYql::MakeRuntimeSettings();
    TComputationOptsFull opts(nullptr, alloc.Ref(), env, *random, *time,
                              NUdf::EValidatePolicy::Exception, nullptr, nullptr, nullptr,
                              NYql::UnknownLangVersion, settings, NUdf::EBridgeMode::None, {});
    TComputationMutables mutables;
    TNativeNode node(mutables);
    TMaybe<NUdf::TSourcePosition> notConsumed;
    TComputationContext compCtx(factory, &builder, opts, mutables,
                                *arrow::default_memory_pool(), notConsumed, settings);
    node.ExpectedContext = &compCtx;

    auto codegen = ICodegen::Make(ETarget::Native);
    auto& context = codegen->GetContext();
    auto* valueType = Type::getInt128Ty(context);
    auto* ptrType = PointerType::getUnqual(valueType);
    auto* type = FunctionType::get(Type::getVoidTy(context),
                                   {PointerType::getUnqual(GetCompContextType(context)), ptrType}, false);
    auto* function = Function::Create(type, Function::ExternalLinkage, "node_return", &codegen->GetModule());
    auto* entry = BasicBlock::Create(context, "entry", function);
    auto* block = BasicBlock::Create(context, "body", function);
    BranchInst::Create(block, entry);
    TCodegenContext ctx(*codegen);
    ctx.Func = function;
    ctx.Ctx = function->getArg(0);
    if (storeResult) {
        GetNodeValue(function->getArg(1), &node, ctx, block);
    } else {
        new StoreInst(GetNodeValue(&node, ctx, block), function->getArg(1), block);
    }
    ReturnInst::Create(context, block);
    codegen->ExportSymbol(function);
    codegen->Verify();
    codegen->Compile();
    using TCall = void (*)(TComputationContext*, NUdf::TUnboxedValue*);
    const auto call = reinterpret_cast<TCall>(codegen->GetPointerToFunction(function));
    NUdf::TUnboxedValue result;
    call(&compCtx, &result);
    UNIT_ASSERT_VALUES_EQUAL(result.Get<ui64>(), node.Payload);
}

class TNativeBoxed final: public NUdf::TBoxedValue {
public:
    NUdf::TUnboxedValue GetVariantItem() const override {
        return NUdf::TUnboxedValuePod(Payload);
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const override {
        return NUdf::TUnboxedValuePod(Payload + index);
    }

    NUdf::TUnboxedValue Run(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod* args) const override {
        UNIT_ASSERT_VALUES_EQUAL(builder, ExpectedBuilder);
        return NUdf::TUnboxedValuePod(Payload + args[0].Get<ui64>());
    }

    const NUdf::IValueBuilder* ExpectedBuilder = nullptr;
    ui64 Payload = 0x123456789abcdef0ULL;
};

void CheckBoxedReturn(unsigned arguments) {
    TScopedAlloc alloc(__LOCATION__);
    TMemoryUsageInfo memInfo("BoxedReturnTest");
    THolderFactory factory(alloc.Ref(), memInfo);
    TDefaultValueBuilder builder(factory, NUdf::EValidatePolicy::Exception);
    auto* boxed = new TNativeBoxed();
    NUdf::TUnboxedValue value{NUdf::TUnboxedValuePod(boxed)};
    boxed->ExpectedBuilder = &builder;
    auto codegen = ICodegen::Make(ETarget::Native);
    auto& context = codegen->GetContext();
    auto* valueType = Type::getInt128Ty(context);
    auto* ptrType = PointerType::getUnqual(valueType);
    auto* type = FunctionType::get(Type::getVoidTy(context), {ptrType, ptrType, ptrType, ptrType}, false);
    auto* function = Function::Create(type, Function::ExternalLinkage, "boxed_return", &codegen->GetModule());
    auto* block = BasicBlock::Create(context, "entry", function);
    auto* input = new LoadInst(valueType, function->getArg(0), "input", block);
    auto* output = function->getArg(1);
    using EMethod = NUdf::TBoxedValueAccessor::EMethod;
    if (arguments == 0) {
        CallBoxedValueVirtualMethod<EMethod::GetVariantItem>(output, input, *codegen, block);
    } else if (arguments == 1) {
        CallBoxedValueVirtualMethod<EMethod::GetElement>(output, input, *codegen, block,
                                                         ConstantInt::get(Type::getInt32Ty(context), 7));
    } else {
        CallBoxedValueVirtualMethod<EMethod::Run>(output, input, *codegen, block,
                                                  function->getArg(2), function->getArg(3));
    }
    ReturnInst::Create(context, block);
    codegen->ExportSymbol(function);
    codegen->Verify();
    codegen->Compile();
    using TCall = void (*)(const NUdf::TUnboxedValue*, NUdf::TUnboxedValue*, const NUdf::IValueBuilder*, const NUdf::TUnboxedValuePod*);
    const auto call = reinterpret_cast<TCall>(codegen->GetPointerToFunction(function));
    NUdf::TUnboxedValue result;
    NUdf::TUnboxedValuePod argument(ui64(7));
    call(&value, &result, &builder, &argument);
    UNIT_ASSERT_VALUES_EQUAL(result.Get<ui64>(), boxed->Payload + (arguments ? 7 : 0));
}

} // namespace

Y_UNIT_TEST_SUITE(TNativeValueReturnCodegen) {
Y_UNIT_TEST(NodeValue) {
    CheckNodeReturn(false);
}
Y_UNIT_TEST(NodeValueToPointer) {
    CheckNodeReturn(true);
}
Y_UNIT_TEST(BoxedValue) {
    CheckBoxedReturn(0);
}
Y_UNIT_TEST(BoxedValueOneArgument) {
    CheckBoxedReturn(1);
}
Y_UNIT_TEST(BoxedValueTwoArguments) {
    CheckBoxedReturn(2);
}
} // Y_UNIT_TEST_SUITE(TNativeValueReturnCodegen)

} // namespace NKikimr::NMiniKQL

#endif
