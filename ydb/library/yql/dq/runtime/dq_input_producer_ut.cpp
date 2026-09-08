#include "dq_async_input.h"
#include "dq_input_producer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {
using namespace NKikimr::NMiniKQL;

Y_UNIT_TEST_SUITE(TDqInputMergeTiming) {
    Y_UNIT_TEST(WideMergeRecordsFirstRowOrEmptyCompletion) {
        for (const bool empty : {false, true}) {
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TMemoryUsageInfo memory("test");
            THolderFactory factory(alloc.Ref(), memory);
            TType* itemType = TDataType::Create(NUdf::TDataType<ui64>::Id, env);
            auto* type = TMultiType::Create(1, &itemType, env);
            auto buffer = CreateDqAsyncInputBuffer(0, "test", type, 1024, TCollectStatsLevel::Basic);
            TSortColumnInfo column(TColumnInfo("key", 0, itemType, {}));
            column.Ascending = true;
            TInstant start;
            ui64 consumed = 0;
            auto input = CreateInputMergeValue(type, {buffer}, {column}, factory, {}, start, consumed);
            NUdf::TUnboxedValue row;
            UNIT_ASSERT(input.WideFetch(&row, 1) == NUdf::EFetchStatus::Yield);
            UNIT_ASSERT(!start);

            if (empty) {
                buffer->Finish();
                UNIT_ASSERT(input.WideFetch(&row, 1) == NUdf::EFetchStatus::Finish);
                UNIT_ASSERT(start);
                UNIT_ASSERT_VALUES_EQUAL(consumed, 0);
            } else {
                TUnboxedValueBatch batch(type);
                batch.PushRow([](ui32) { return NUdf::TUnboxedValuePod(ui64(1)); });
                batch.PushRow([](ui32) { return NUdf::TUnboxedValuePod(ui64(2)); });
                buffer->Push(std::move(batch), 16);
                buffer->Finish();
                UNIT_ASSERT(input.WideFetch(&row, 1) == NUdf::EFetchStatus::Ok);
                UNIT_ASSERT_VALUES_EQUAL(row.Get<ui64>(), 1);
                UNIT_ASSERT(start);
                const auto firstRowTime = start;
                UNIT_ASSERT(input.WideFetch(&row, 1) == NUdf::EFetchStatus::Ok);
                UNIT_ASSERT_VALUES_EQUAL(row.Get<ui64>(), 2);
                UNIT_ASSERT_VALUES_EQUAL(start, firstRowTime);
                UNIT_ASSERT(input.WideFetch(&row, 1) == NUdf::EFetchStatus::Finish);
                UNIT_ASSERT_VALUES_EQUAL(start, firstRowTime);
                UNIT_ASSERT_VALUES_EQUAL(consumed, 2);
            }
        }
    }
}

} // namespace NYql::NDq
