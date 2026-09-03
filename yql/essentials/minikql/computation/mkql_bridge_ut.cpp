#include "mkql_bridge.h"
#include "mkql_bridge_inprocess.h"
#include "mkql_value_builder.h"

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/public/udf/udf_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

namespace {

SIMPLE_UDF(TAddOne, ui32(ui32)) {
    Y_UNUSED(valueBuilder);
    return NUdf::TUnboxedValuePod(args[0].Get<ui32>() + 1);
}

SIMPLE_UDF(TSeqList, NUdf::TListType<ui32>(ui32)) {
    const ui32 size = args[0].Get<ui32>();
    std::vector<NUdf::TUnboxedValue> res(size);
    for (ui32 i = 0; i < size; ++i) {
        res[i] = NUdf::TUnboxedValuePod(i);
    }
    return valueBuilder->NewList(res.data(), res.size());
}

SIMPLE_UDF(TSumList, ui32(NUdf::TListType<ui32>)) {
    Y_UNUSED(valueBuilder);
    ui32 sum = 0;
    auto iterator = args[0].GetListIterator();
    for (NUdf::TUnboxedValue item; iterator.Next(item);) {
        sum += item.Get<ui32>();
    }
    return NUdf::TUnboxedValuePod(sum);
}

SIMPLE_UDF(TMaybeAddOne, NUdf::TOptional<ui32>(ui32, bool)) {
    Y_UNUSED(valueBuilder);
    if (!args[1].Get<bool>()) {
        return NUdf::TUnboxedValuePod();
    }
    return NUdf::TUnboxedValuePod(args[0].Get<ui32>() + 1).MakeOptional();
}

SIMPLE_UDF(TMaybeSeqList, NUdf::TOptional<NUdf::TListType<ui32>>(ui32, bool)) {
    if (!args[1].Get<bool>()) {
        return NUdf::TUnboxedValuePod();
    }
    const ui32 size = args[0].Get<ui32>();
    std::vector<NUdf::TUnboxedValue> res(size);
    for (ui32 i = 0; i < size; ++i) {
        res[i] = NUdf::TUnboxedValuePod(i);
    }
    return valueBuilder->NewList(res.data(), res.size()).MakeOptional();
}

SIMPLE_UDF(TEcho, char*(char*)) {
    Y_UNUSED(valueBuilder);
    return NUdf::TUnboxedValuePod(args[0]);
}

extern const char BridgeUtResourceTag[] = "BridgeUtResource";
using TBridgeUtResource = NUdf::TBoxedResource<int, BridgeUtResourceTag>;

SIMPLE_UDF(TMakeResource, NUdf::TResource<BridgeUtResourceTag>(ui32)) {
    Y_UNUSED(valueBuilder);
    return NUdf::TUnboxedValuePod(new TBridgeUtResource(static_cast<int>(args[0].Get<ui32>())));
}

SIMPLE_UDF(TReadResource, ui32(NUdf::TResource<BridgeUtResourceTag>)) {
    Y_UNUSED(valueBuilder);
    return NUdf::TUnboxedValuePod(*static_cast<ui32*>(args[0].GetResource()));
}

SIMPLE_UDF(TTerminate, ui64(char*)) {
    Y_UNUSED(valueBuilder);
    MKQLTerminate(TString(args[0].AsStringRef()).c_str());
}

using TMakeDictSignature = NUdf::TDict<char*, ui32>(ui32);
SIMPLE_UDF(TMakeDict, TMakeDictSignature) {
    const ui32 size = args[0].Get<ui32>();
    auto dictBuilder = valueBuilder->NewDict(ReturnType_, 0);
    for (ui32 i = 0; i < size; ++i) {
        const TString key(1, static_cast<char>('a' + i));
        dictBuilder->Add(valueBuilder->NewString(key), NUdf::TUnboxedValuePod(i * 10));
    }
    return dictBuilder->Build();
}

using TAddWithDefaultSignature = ui32(ui32, NUdf::TOptional<ui32>);
SIMPLE_UDF_WITH_OPTIONAL_ARGS(TAddWithDefault, TAddWithDefaultSignature, 1) {
    Y_UNUSED(valueBuilder);
    const ui32 base = args[0].Get<ui32>();
    const ui32 add = args[1] ? args[1].Get<ui32>() : 1;
    return NUdf::TUnboxedValuePod(base + add);
}

SIMPLE_MODULE(TBridgeUtModule, TAddOne, TSeqList, TSumList, TMaybeAddOne, TMaybeSeqList, TEcho, TMakeResource, TReadResource, TTerminate, TMakeDict, TAddWithDefault)

TIntrusivePtr<IFunctionRegistry> CreateBridgeTestRegistry() {
    auto freg = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
    freg->AddModule("", "BridgeUt", new TBridgeUtModule());
    return freg;
}

TFunctionTypeInfo ResolveDirect(const IFunctionRegistry& registry, const TTypeEnvironment& env, const TString& funcName) {
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
    NYql::TRuntimeSettings runtimeSettings;
    TFunctionTypeInfo funcInfo;
    const auto status = registry.FindFunctionTypeInfo(
        NYql::UnknownLangVersion, runtimeSettings, env, typeInfoHelper, /* countersProvider */ nullptr,
        funcName, /* userType */ nullptr, TStringBuf(), /* flags */ 0, NUdf::TSourcePosition(),
        /* secureParamsProvider */ nullptr, /* logProvider */ nullptr, &funcInfo);
    MKQL_ENSURE(status.IsOk(), status.GetError());
    return funcInfo;
}

struct TTestContext {
    explicit TTestContext(const IFunctionRegistry& registry)
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , MemInfo("BridgeUt")
        , HolderFactory(Alloc.Ref(), MemInfo, &registry)
        , ValueBuilder(HolderFactory, NUdf::EValidatePolicy::Exception)
    {
    }

    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder ValueBuilder;
};

TIntrusivePtr<TBridgeChannel> MakeTestChannel(const IFunctionRegistry& registry, TTestContext& ctx) {
    return CreateInProcessBridgeChannel(registry, ctx.HolderFactory, &ctx.ValueBuilder, TBridgeNamespaceId(1), NYql::MakeRuntimeSettings());
}

NUdf::TUnboxedValue ResolveTestFunction(TBridgeChannel& channel, const TString& funcName, const TCallableType* funcType) {
    TBridgeUdfSpec spec;
    spec.FunctionName = funcName;
    return channel.ResolveFunction(spec, funcType);
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBridgeTest) {
Y_UNIT_TEST(ScalarCallRoundTrip) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.AddOne");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.AddOne", funcInfo.FunctionType);

    const NUdf::TUnboxedValuePod arg(ui32(41));
    const auto result = proxy.Run(&ctx.ValueBuilder, &arg);
    UNIT_ASSERT_VALUES_EQUAL(result.Get<ui32>(), 42U);

    UNIT_ASSERT_VALUES_EQUAL(channel->DebugNodeTableSize(), 0U);
}

Y_UNIT_TEST(OptionalTrailingArgumentRoundTrips) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.AddWithDefault");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.AddWithDefault", funcInfo.FunctionType);

    {
        std::array<NUdf::TUnboxedValuePod, 2> args = {NUdf::TUnboxedValuePod(ui32(10)), NUdf::TUnboxedValuePod(ui32(5)).MakeOptional()};
        const auto result = proxy.Run(&ctx.ValueBuilder, args.data());
        UNIT_ASSERT_VALUES_EQUAL(result.Get<ui32>(), 15U);
    }
    {
        std::array<NUdf::TUnboxedValuePod, 2> args = {NUdf::TUnboxedValuePod(ui32(10)), NUdf::TUnboxedValuePod()};
        const auto result = proxy.Run(&ctx.ValueBuilder, args.data());
        UNIT_ASSERT_VALUES_EQUAL(result.Get<ui32>(), 11U);
    }
}

Y_UNIT_TEST(LazyListIsNotEagerlyMaterialized) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.SeqList");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.SeqList", funcInfo.FunctionType);

    const NUdf::TUnboxedValuePod arg(ui32(5));
    auto result = proxy.Run(&ctx.ValueBuilder, &arg);

    UNIT_ASSERT(result.IsBoxed());
    UNIT_ASSERT(!NUdf::TBoxedValueAccessor::HasFastListLength(*result.AsBoxed()));

    std::vector<ui32> items;
    auto iterator = result.GetListIterator();
    for (NUdf::TUnboxedValue item; iterator.Next(item);) {
        items.push_back(item.Get<ui32>());
    }
    UNIT_ASSERT_VALUES_EQUAL(items.size(), 5U);
    for (ui32 i = 0; i < items.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(items[i], i);
    }

    iterator = NUdf::TUnboxedValue();
    result = NUdf::TUnboxedValue();

    const NUdf::TUnboxedValuePod secondArg(ui32(2));
    auto secondResult = proxy.Run(&ctx.ValueBuilder, &secondArg);
    ui64 count = 0;
    auto secondIterator = secondResult.GetListIterator();
    for (NUdf::TUnboxedValue item; secondIterator.Next(item); ++count) {
    }
    UNIT_ASSERT_VALUES_EQUAL(count, 2U);
}

Y_UNIT_TEST(LiteralListArgumentIsIteratedViaNestedRequest) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.SumList");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.SumList", funcInfo.FunctionType);

    std::vector<NUdf::TUnboxedValue> items(5);
    for (ui32 i = 0; i < items.size(); ++i) {
        items[i] = NUdf::TUnboxedValuePod(i);
    }
    const NUdf::TUnboxedValue arg = ctx.ValueBuilder.NewList(items.data(), items.size());
    const auto result = proxy.Run(&ctx.ValueBuilder, &arg);
    UNIT_ASSERT_VALUES_EQUAL(result.Get<ui32>(), 0U + 1U + 2U + 3U + 4U);
}

Y_UNIT_TEST(DictIsProxiedAndAllThreeIteratorsWork) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.MakeDict");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.MakeDict", funcInfo.FunctionType);

    const NUdf::TUnboxedValuePod arg(ui32(3));
    auto result = proxy.Run(&ctx.ValueBuilder, &arg);
    UNIT_ASSERT(result.IsBoxed());

    {
        ui64 count = 0;
        ui32 sum = 0;
        auto iterator = result.GetDictIterator();
        for (NUdf::TUnboxedValue key, payload; iterator.NextPair(key, payload); ++count) {
            UNIT_ASSERT_VALUES_EQUAL(TString(key.AsStringRef()).size(), 1U);
            sum += payload.Get<ui32>();
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 3U);
        UNIT_ASSERT_VALUES_EQUAL(sum, 0U + 10U + 20U);
    }

    {
        ui64 count = 0;
        auto iterator = result.GetKeysIterator();
        for (NUdf::TUnboxedValue key; iterator.Next(key); ++count) {
            UNIT_ASSERT_VALUES_EQUAL(TString(key.AsStringRef()).size(), 1U);
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 3U);
    }

    {
        ui64 count = 0;
        ui32 sum = 0;
        auto iterator = result.GetPayloadsIterator();
        for (NUdf::TUnboxedValue payload; iterator.Next(payload); ++count) {
            sum += payload.Get<ui32>();
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 3U);
        UNIT_ASSERT_VALUES_EQUAL(sum, 0U + 10U + 20U);
    }
}

Y_UNIT_TEST(OptionalOfPlainTypeRoundTrips) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.MaybeAddOne");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.MaybeAddOne", funcInfo.FunctionType);

    {
        std::array<NUdf::TUnboxedValuePod, 2> args = {NUdf::TUnboxedValuePod(ui32(41)), NUdf::TUnboxedValuePod(true)};
        auto result = proxy.Run(&ctx.ValueBuilder, args.data());
        UNIT_ASSERT(result.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(result.GetOptionalValue().Get<ui32>(), 42U);
    }
    {
        std::array<NUdf::TUnboxedValuePod, 2> args = {NUdf::TUnboxedValuePod(ui32(41)), NUdf::TUnboxedValuePod(false)};
        auto result = proxy.Run(&ctx.ValueBuilder, args.data());
        UNIT_ASSERT(!result.HasValue());
    }
}

Y_UNIT_TEST(OptionalOfProxiedTypeIsHandledCorrectly) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.MaybeSeqList");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.MaybeSeqList", funcInfo.FunctionType);

    {
        std::array<NUdf::TUnboxedValuePod, 2> args = {NUdf::TUnboxedValuePod(ui32(3)), NUdf::TUnboxedValuePod(true)};
        auto result = proxy.Run(&ctx.ValueBuilder, args.data());
        UNIT_ASSERT(result.HasValue());
        auto list = result.GetOptionalValue();
        UNIT_ASSERT(list.IsBoxed());
        UNIT_ASSERT(!NUdf::TBoxedValueAccessor::HasFastListLength(*list.AsBoxed()));
        std::vector<ui32> items;
        auto iterator = list.GetListIterator();
        for (NUdf::TUnboxedValue item; iterator.Next(item);) {
            items.push_back(item.Get<ui32>());
        }
        UNIT_ASSERT_VALUES_EQUAL(items.size(), 3U);
    }
    {
        std::array<NUdf::TUnboxedValuePod, 2> args = {NUdf::TUnboxedValuePod(ui32(3)), NUdf::TUnboxedValuePod(false)};
        auto result = proxy.Run(&ctx.ValueBuilder, args.data());
        UNIT_ASSERT(!result.HasValue());
    }
}

Y_UNIT_TEST(StringRoundTripsIncludingLongBoxedStrings) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.Echo");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.Echo", funcInfo.FunctionType);

    for (const auto& s : {TString("short"), TString(20, 'x'), TString(500, 'y')}) {
        const NUdf::TUnboxedValue arg = MakeString(TStringBuf(s));
        auto result = proxy.Run(&ctx.ValueBuilder, &arg);
        UNIT_ASSERT_VALUES_EQUAL(TString(result.AsStringRef()), s);
    }
}

Y_UNIT_TEST(ResourceIsProxiedNotDereferenceable) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.MakeResource");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.MakeResource", funcInfo.FunctionType);

    const NUdf::TUnboxedValuePod arg(ui32(7));
    auto result = proxy.Run(&ctx.ValueBuilder, &arg);

    UNIT_ASSERT(result.IsBoxed());
    UNIT_ASSERT_VALUES_EQUAL(TString(result.GetResourceTag()), TString(BridgeUtResourceTag));
    UNIT_ASSERT_EXCEPTION(result.GetResource(), yexception);
}

Y_UNIT_TEST(ResourceProducedByOneFunctionIsConsumedByAnother) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto makeInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.MakeResource");
    const auto readInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.ReadResource");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto makeProxy = ResolveTestFunction(*channel, "BridgeUt.MakeResource", makeInfo.FunctionType);
    const auto readProxy = ResolveTestFunction(*channel, "BridgeUt.ReadResource", readInfo.FunctionType);

    const NUdf::TUnboxedValuePod makeArg(ui32(123));
    const auto resource = makeProxy.Run(&ctx.ValueBuilder, &makeArg);
    UNIT_ASSERT(resource.IsBoxed());

    const auto content = readProxy.Run(&ctx.ValueBuilder, &resource);
    UNIT_ASSERT_VALUES_EQUAL(content.Get<ui32>(), 123U);
}

Y_UNIT_TEST(UdfTerminateIsReraisedAsTerminateNotGenericError) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.Terminate");
    const auto channel = MakeTestChannel(*registry, ctx);
    const auto proxy = ResolveTestFunction(*channel, "BridgeUt.Terminate", funcInfo.FunctionType);

    const TOnlyThrowingBindTerminator terminator;

    const NUdf::TUnboxedValue arg = MakeString(TStringBuf("boom"));
    UNIT_ASSERT_EXCEPTION_CONTAINS(proxy.Run(&ctx.ValueBuilder, &arg), TTerminateException, "boom");
}

Y_UNIT_TEST(WorkerResolveFailureSurfacesAsRealError) {
    auto registry = CreateBridgeTestRegistry();
    TTestContext ctx(*registry);

    const auto funcInfo = ResolveDirect(*registry, ctx.Env, "BridgeUt.AddOne");
    const auto channel = MakeTestChannel(*registry, ctx);

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        ResolveTestFunction(*channel, "BridgeUt.NoSuchFunction", funcInfo.FunctionType), yexception, "NoSuchFunction");
}
} // Y_UNIT_TEST_SUITE(TMiniKQLBridgeTest)

} // namespace NKikimr::NMiniKQL
