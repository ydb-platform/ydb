#include "mkql_spiller_adapter.h"
#include "mock_spiller_ut.h"
#include "mock_spiller_factory_ut.h"

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {

namespace {

    THolderFactory CreateTestHolderFactory() {
        TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("test");
        return THolderFactory(alloc.Ref(), memInfo);
    }

    TMultiType* CreateTestMultiType(TTypeEnvironment& typeEnv) {
        TTypeBuilder builder(typeEnv);
        std::vector<TType*> types = {
            builder.NewDataType(NUdf::TDataType<ui32>::Id),
            builder.NewDataType(NUdf::TDataType<ui64>::Id),
            builder.NewDataType(NUdf::TDataType<char*>::Id)
        };
        return TMultiType::Create(types.size(), types.data(), typeEnv);
    }

    std::vector<NUdf::TUnboxedValuePod> CreateTestWideItem(ui32 val1, ui64 val2, const TString& str) {
        return {
            NUdf::TUnboxedValuePod(val1),
            NUdf::TUnboxedValuePod(val2),
            NUdf::TUnboxedValuePod::Embedded(str.substr(0, 23)) // Ограничиваем размер для Embedded
        };
    }

    void VerifyWideItem(const TArrayRef<NUdf::TUnboxedValue>& wideItem, ui32 expectedVal1, ui64 expectedVal2, const TString& expectedStr) {
        UNIT_ASSERT_VALUES_EQUAL(wideItem.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(wideItem[0].Get<ui32>(), expectedVal1);
        UNIT_ASSERT_VALUES_EQUAL(wideItem[1].Get<ui64>(), expectedVal2);
        UNIT_ASSERT_VALUES_EQUAL(TString(wideItem[2].AsStringRef()), expectedStr);
    }

} // namespace

Y_UNIT_TEST_SUITE(TWideUnboxedValuesSpillerAdapterTest) {

    Y_UNIT_TEST(TestBasicWriteAndRead) {
        auto holderFactory = CreateTestHolderFactory();
        
        TScopedAlloc Alloc(__LOCATION__);
        TTypeEnvironment TypeEnv(Alloc);
        
        auto multiType = CreateTestMultiType(TypeEnv);
        auto spiller = CreateMockSpiller();
        
        TWideUnboxedValuesSpillerAdapter adapter(spiller, multiType, 1000);
        
        // Write some items
        auto item1 = CreateTestWideItem(1, 100, "test1");
        auto item2 = CreateTestWideItem(2, 200, "test2");
        
        auto future1 = adapter.WriteWideItem(item1);
        UNIT_ASSERT(!future1.has_value()); // Should not spill yet
        
        auto future2 = adapter.WriteWideItem(item2);
        UNIT_ASSERT(!future2.has_value()); // Should not spill yet
        
        // Finish writing
        auto finishFuture = adapter.FinishWriting();
        UNIT_ASSERT(finishFuture.has_value());
        
        // Wait for async operation
        auto key = finishFuture->GetValueSync();
        adapter.AsyncWriteCompleted(key);
        
        // Read items back
        std::vector<NUdf::TUnboxedValue> readItem1(3);
        auto readFuture1 = adapter.ExtractWideItem(readItem1);
        UNIT_ASSERT(readFuture1.has_value());
        
        // Wait for async read
        auto rope = readFuture1->GetValueSync();
        UNIT_ASSERT(rope.has_value());
        adapter.AsyncReadCompleted(std::move(*rope), holderFactory);
        
        // Extract the first item
        std::vector<NUdf::TUnboxedValue> extractedItem1(3);
        auto extractFuture1 = adapter.ExtractWideItem(extractedItem1);
        UNIT_ASSERT(!extractFuture1.has_value());
        VerifyWideItem(extractedItem1, 1, 100, "test1");
        
        // Extract the second item (should be in the same batch)
        std::vector<NUdf::TUnboxedValue> extractedItem2(3);
        auto extractFuture2 = adapter.ExtractWideItem(extractedItem2);
        UNIT_ASSERT(!extractFuture2.has_value());
        VerifyWideItem(extractedItem2, 2, 200, "test2");
        
        // Should be empty now
        UNIT_ASSERT(adapter.Empty());
    }

    Y_UNIT_TEST(TestReportAllocAndReportFree) {
        auto holderFactory = CreateTestHolderFactory();
        
        TScopedAlloc Alloc(__LOCATION__);
        TTypeEnvironment TypeEnv(Alloc);
        
        auto multiType = CreateTestMultiType(TypeEnv);
        auto spiller = CreateMockSpiller();
        auto mockSpiller = static_cast<TMockSpiller*>(spiller.get());
        
        TWideUnboxedValuesSpillerAdapter adapter(spiller, multiType, 100); // Small size limit to force spilling
        
        // Write items that will exceed the size limit
        auto item1 = CreateTestWideItem(1, 100, "test1");
        auto item2 = CreateTestWideItem(2, 200, "test2");
        auto item3 = CreateTestWideItem(3, 300, "test3");
        
        // First item should not spill
        auto future1 = adapter.WriteWideItem(item1);
        UNIT_ASSERT(!future1.has_value());
        
        // Check that ReportAlloc was called
        UNIT_ASSERT_VALUES_EQUAL(mockSpiller->GetAllocCalls().size(), 1);
        UNIT_ASSERT(mockSpiller->GetAllocatedMemory() > 0);
        
        // Second item might cause spilling depending on size
        auto future2 = adapter.WriteWideItem(item2);
        // Note: spilling might not happen if items are small enough
        
        // Wait for async operation if spilling happened
        if (future2.has_value()) {
            auto key = future2->GetValueSync();
            adapter.AsyncWriteCompleted(key);
            
            // Check that ReportFree was called after AsyncWriteCompleted
            UNIT_ASSERT_VALUES_EQUAL(mockSpiller->GetFreeCalls().size(), 1);
            UNIT_ASSERT(mockSpiller->GetFreedMemory() > 0);
        }
        
        // Write third item
        auto future3 = adapter.WriteWideItem(item3);
        UNIT_ASSERT(!future3.has_value());
        
        // Finish writing
        auto finishFuture = adapter.FinishWriting();
        UNIT_ASSERT(finishFuture.has_value());
        
        auto finishKey = finishFuture->GetValueSync();
        adapter.AsyncWriteCompleted(finishKey);
        
        // Verify memory tracking
        UNIT_ASSERT(mockSpiller->GetAllocCalls().size() >= 1);
        // Free calls depend on whether spilling happened
        if (mockSpiller->GetFreeCalls().size() > 0) {
            UNIT_ASSERT(mockSpiller->GetFreedMemory() > 0);
        }
        
        // Read all items back
        for (int i = 0; i < 3; ++i) {
            std::vector<NUdf::TUnboxedValue> readItem(3);
            auto readFuture = adapter.ExtractWideItem(readItem);
            
            if (readFuture.has_value()) {
                // Data is in spiller, need to read asynchronously
                auto rope = readFuture->GetValueSync();
                UNIT_ASSERT(rope.has_value());
                adapter.AsyncReadCompleted(std::move(*rope), holderFactory);
                
                std::vector<NUdf::TUnboxedValue> extractedItem(3);
                auto extractFuture = adapter.ExtractWideItem(extractedItem);
                UNIT_ASSERT(!extractFuture.has_value());
            } else {
                // Data is already in CurrentBatch, no need for async read
                // The data is already in readItem
            }
        }
        
        UNIT_ASSERT(adapter.Empty());
    }

    Y_UNIT_TEST(TestEmptyAdapter) {
        TScopedAlloc Alloc(__LOCATION__);
        TTypeEnvironment TypeEnv(Alloc);
        
        auto multiType = CreateTestMultiType(TypeEnv);
        auto spiller = CreateMockSpiller();
        
        TWideUnboxedValuesSpillerAdapter adapter(spiller, multiType, 1000);
        
        // Should be empty initially
        UNIT_ASSERT(adapter.Empty());
        
        // Finish writing without any data
        auto finishFuture = adapter.FinishWriting();
        UNIT_ASSERT(!finishFuture.has_value());
        
        // Should still be empty
        UNIT_ASSERT(adapter.Empty());
    }

    Y_UNIT_TEST(TestLargeItems) {
        auto holderFactory = CreateTestHolderFactory();
        
        TScopedAlloc Alloc(__LOCATION__);
        TTypeEnvironment TypeEnv(Alloc);
        
        auto multiType = CreateTestMultiType(TypeEnv);
        auto spiller = CreateMockSpiller();
        auto mockSpiller = static_cast<TMockSpiller*>(spiller.get());
        
        TWideUnboxedValuesSpillerAdapter adapter(spiller, multiType, 50); // Very small size limit
        
        // Create a large string (but not too large for Embedded)
        TString largeString(20, 'x');
        auto item = CreateTestWideItem(1, 100, largeString);
        
        // This should immediately spill due to size
        auto future = adapter.WriteWideItem(item);
        UNIT_ASSERT(future.has_value());
        
        // Wait for async operation
        auto key = future->GetValueSync();
        adapter.AsyncWriteCompleted(key);
        
        // Verify memory tracking
        UNIT_ASSERT(mockSpiller->GetAllocCalls().size() >= 1);
        UNIT_ASSERT(mockSpiller->GetFreeCalls().size() >= 1);
        
        // Read back
        std::vector<NUdf::TUnboxedValue> readItem(3);
        auto readFuture = adapter.ExtractWideItem(readItem);
        UNIT_ASSERT(readFuture.has_value());
        
        auto rope = readFuture->GetValueSync();
        UNIT_ASSERT(rope.has_value());
        adapter.AsyncReadCompleted(std::move(*rope), holderFactory);
        
        std::vector<NUdf::TUnboxedValue> extractedItem(3);
        auto extractFuture = adapter.ExtractWideItem(extractedItem);
        UNIT_ASSERT(!extractFuture.has_value());
        VerifyWideItem(extractedItem, 1, 100, largeString);
    }

    Y_UNIT_TEST(TestMockSpillerFactory) {
        TScopedAlloc Alloc(__LOCATION__);
        TTypeEnvironment TypeEnv(Alloc);
        
        auto factory = std::make_shared<TMockSpillerFactory>();
        
        // Create spiller through factory
        auto spiller = factory->CreateSpiller();
        UNIT_ASSERT(spiller != nullptr);
        
        // Verify spiller was tracked
        UNIT_ASSERT_VALUES_EQUAL(factory->GetCreatedSpillers().size(), 1);
        UNIT_ASSERT_EQUAL(factory->GetCreatedSpillers()[0], spiller);
        
        // Create another spiller
        auto spiller2 = factory->CreateSpiller();
        UNIT_ASSERT(spiller2 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(factory->GetCreatedSpillers().size(), 2);
        
        // Test memory reporting callbacks
        bool allocCalled = false;
        bool freeCalled = false;
        
        factory->SetMemoryReportingCallbacks(
            [&allocCalled](ui64 /*size*/) { allocCalled = true; return true; },
            [&freeCalled](ui64 /*size*/) { freeCalled = true; }
        );
        
        UNIT_ASSERT(factory->GetReportAllocCallback());
        UNIT_ASSERT(factory->GetReportFreeCallback());
    }

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NMiniKQL
