#include "mkql_spiller_adapter.h"
#include "mock_spiller_factory_ut.h"

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL {

namespace {

struct TMemoryCounters {
    ui64 Allocated = 0;
    ui64 Freed = 0;
};

TMultiType* CreateTestMultiType(TTypeEnvironment& typeEnv) {
    TTypeBuilder builder(typeEnv);
    std::vector<TType*> types = {
        builder.NewDataType(NUdf::TDataType<ui32>::Id),
        builder.NewDataType(NUdf::TDataType<ui64>::Id),
        builder.NewDataType(NUdf::TDataType<char*>::Id)};
    return TMultiType::Create(types.size(), types.data(), typeEnv);
}

std::string MakeStringOfSize(ui64 size) {
    return std::string(size, 'x');
}

std::vector<NUdf::TUnboxedValue> CreateTestWideItem(ui32 val1, ui64 val2, ui64 stringLen) {
    return {
        NUdf::TUnboxedValuePod(val1),
        NUdf::TUnboxedValuePod(val2),
        NMiniKQL::MakeString(MakeStringOfSize(stringLen))};
}

void VerifyWideItem(const TArrayRef<NUdf::TUnboxedValue>& wideItem, ui32 expectedVal1, ui64 expectedVal2, const TString& expectedStr) {
    UNIT_ASSERT_VALUES_EQUAL(wideItem.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(wideItem[0].Get<ui32>(), expectedVal1);
    UNIT_ASSERT_VALUES_EQUAL(wideItem[1].Get<ui64>(), expectedVal2);
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(wideItem[2].AsStringRef()), expectedStr);
}

void VerifyCounters(TMemoryCounters& counters, ui64 spilledData) {
    UNIT_ASSERT_VALUES_EQUAL(counters.Allocated, counters.Freed);
    // estimated packer size is reported. This is enough for spilling purposes.
    // So, the size of really spilled data may be slightly different from the reported value.
    ui64 delta = 1000;
    ui64 lower = 0;
    if (spilledData > delta) {
        lower = spilledData - delta;
    }
    ui64 upper = spilledData + delta;

    UNIT_ASSERT_GE(counters.Allocated, lower);
    UNIT_ASSERT_LE(counters.Allocated, upper);
}

std::shared_ptr<TMockSpillerFactory> MakeFactoryWithCounters(TMemoryCounters& counters) {
    auto factory = std::make_shared<TMockSpillerFactory>();
    factory->SetMemoryReportingCallbacks(
        [&counters](ui64 sz) { counters.Allocated += sz; },
        [&counters](ui64 sz) { counters.Freed += sz; });
    return factory;
}

ui64 RunScenario(
    TTypeEnvironment& typeEnv,
    THolderFactory& holderFactory,
    ui64 flushThreshold,
    ui64 reportMemoryThreshold,
    ui64 numItems,
    TMemoryCounters& counters) {
    auto factory = MakeFactoryWithCounters(counters);
    auto spiller = factory->CreateSpiller();
    UNIT_ASSERT(spiller != nullptr);

    auto multiType = CreateTestMultiType(typeEnv);
    TWideUnboxedValuesSpillerAdapter adapter(spiller, multiType, flushThreshold, reportMemoryThreshold);

    for (ui64 i = 0; i < numItems; ++i) {
        auto future = adapter.WriteWideItem(CreateTestWideItem(static_cast<ui32>(i), 100 * i, i * 100));

        if (future.has_value()) {
            UNIT_ASSERT(future->HasValue());
            adapter.AsyncWriteCompleted(future->GetValue());
        }
    }

    auto finishFuture = adapter.FinishWriting();
    if (finishFuture.has_value()) {
        adapter.AsyncWriteCompleted(finishFuture->ExtractValue());
    }

    ui64 item = 0;
    while (!adapter.Empty()) {
        std::vector<NUdf::TUnboxedValue> readItem(multiType->GetElementsCount());
        auto readFuture = adapter.ExtractWideItem(readItem);
        if (readFuture.has_value()) {
            adapter.AsyncReadCompleted(*readFuture->ExtractValue(), holderFactory);
            continue;
        }
        VerifyWideItem(readItem, static_cast<ui32>(item), 100 * item, MakeStringOfSize(item * 100));
        ++item;
    }
    UNIT_ASSERT_VALUES_EQUAL(item, numItems);

    const auto mock_spiller = std::dynamic_pointer_cast<TMockSpiller>(spiller);
    return mock_spiller->GetTotalSpilled();
}

} // namespace

Y_UNIT_TEST_SUITE(TWideUnboxedValuesSpillerAdapterTest) {

Y_UNIT_TEST(TestBasicReadWrite) {
    TScopedAlloc alloc(__LOCATION__);
    TMemoryUsageInfo memInfo("test");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    TTypeEnvironment env(alloc);

    TMemoryCounters counters;
    const ui64 flushThreshold = 100;
    const ui64 reportMemoryThreshold = 10;
    const ui64 numItems = 10;

    const ui64 totalSpilled = RunScenario(
        env, holderFactory,
        flushThreshold, reportMemoryThreshold,
        numItems, counters);

    VerifyCounters(counters, totalSpilled);
}

Y_UNIT_TEST(TestLargeLimits) {
    TScopedAlloc Alloc(__LOCATION__);
    TTypeEnvironment TypeEnv(Alloc);
    TMemoryUsageInfo memInfo("test");
    auto holderFactory = THolderFactory(Alloc.Ref(), memInfo);

    TMemoryCounters counters;
    const ui64 flushThreshold = 1_MB;
    const ui64 reportMemoryThreshold = 64_KB;
    const ui64 numItems = 20;

    const ui64 totalSpilled = RunScenario(
        TypeEnv, holderFactory,
        flushThreshold, reportMemoryThreshold,
        numItems, counters);

    VerifyCounters(counters, totalSpilled);
}

Y_UNIT_TEST(TestFrequentMemoryReports) {
    TScopedAlloc Alloc(__LOCATION__);
    TTypeEnvironment TypeEnv(Alloc);
    TMemoryUsageInfo memInfo("test");
    auto holderFactory = THolderFactory(Alloc.Ref(), memInfo);

    TMemoryCounters counters;
    const ui64 flushThreshold = 256_KB;
    const ui64 reportMemoryThreshold = 1;
    const ui64 numItems = 200;

    const ui64 totalSpilled = RunScenario(
        TypeEnv, holderFactory,
        flushThreshold, reportMemoryThreshold,
        numItems, counters);

    VerifyCounters(counters, totalSpilled);
}

Y_UNIT_TEST(TestZeroItems) {
    TScopedAlloc Alloc(__LOCATION__);
    TTypeEnvironment TypeEnv(Alloc);
    TMemoryUsageInfo memInfo("test");
    auto holderFactory = THolderFactory(Alloc.Ref(), memInfo);

    TMemoryCounters counters;
    const ui64 flushThreshold = 128;
    const ui64 reportMemoryThreshold = 64;
    const ui64 numItems = 0;

    const ui64 estimatedPackedSize = RunScenario(
        TypeEnv, holderFactory,
        flushThreshold, reportMemoryThreshold,
        numItems, counters);

    UNIT_ASSERT_EQUAL(estimatedPackedSize, 0);
    UNIT_ASSERT_EQUAL(counters.Allocated, 0);
    UNIT_ASSERT_EQUAL(counters.Freed, 0);
}

} // Y_UNIT_TEST_SUITE(TWideUnboxedValuesSpillerAdapterTest)

} // namespace NKikimr::NMiniKQL
