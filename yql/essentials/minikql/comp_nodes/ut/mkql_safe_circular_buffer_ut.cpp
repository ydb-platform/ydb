#include <yql/essentials/minikql/comp_nodes/mkql_safe_circular_buffer.h>

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <library/cpp/testing/unittest/registar.h>

#include <deque>

namespace NKikimr {
using namespace NUdf;
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLSafeCircularBuffer) {

using TBufUnboxed = TSafeCircularBuffer<TUnboxedValue>;

Y_UNIT_TEST(TestUnboxedNoFailOnEmpty) {
    TBufUnboxed bufferOptional(1, TUnboxedValuePod());
    TBufUnboxed buffer(1, TUnboxedValue::Void());
    UNIT_ASSERT(buffer.Get(0));
    UNIT_ASSERT(buffer.Get(1));
    UNIT_ASSERT(buffer.Get(3));
    UNIT_ASSERT(buffer.Get(-1));
    for (auto i = 0; i < 5; ++i) {
        buffer.PopFront();
    }
}

Y_UNIT_TEST(TestUnboxedNormalUsage) {
    TBufUnboxed buffer(5, TUnboxedValuePod());
    buffer.PushBack(TUnboxedValue::Embedded("It"));
    UNIT_ASSERT_EQUAL(buffer.Get(0).AsStringRef(), "It");
    buffer.PushBack(TUnboxedValue::Embedded("is"));
    UNIT_ASSERT_EQUAL(buffer.Get(0).AsStringRef(), "It");
    buffer.PushBack(TUnboxedValue::Embedded("funny"));
    UNIT_ASSERT_EQUAL(buffer.Size(), 3);
    UNIT_ASSERT_EQUAL(buffer.Get(0).AsStringRef(), "It");
    UNIT_ASSERT_EQUAL(buffer.Get(2).AsStringRef(), "funny");
    UNIT_ASSERT(!buffer.Get(3));
    buffer.PopFront();
    UNIT_ASSERT_EQUAL(buffer.Get(0).AsStringRef(), "is");
    UNIT_ASSERT_EQUAL(buffer.Get(1).AsStringRef(), "funny");
    buffer.PushBack(TUnboxedValue::Embedded("bunny"));
    UNIT_ASSERT_EQUAL(buffer.Get(1).AsStringRef(), "funny");
    UNIT_ASSERT_EQUAL(buffer.Get(2).AsStringRef(), "bunny");
    UNIT_ASSERT(!buffer.Get(3));
    buffer.PopFront();
    UNIT_ASSERT_EQUAL(buffer.Get(0).AsStringRef(), "funny");
    UNIT_ASSERT_EQUAL(buffer.Get(1).AsStringRef(), "bunny");
    UNIT_ASSERT_EQUAL(buffer.Size(), 2);
    buffer.PopFront();
    UNIT_ASSERT_EQUAL(buffer.Get(0).AsStringRef(), "bunny");
    UNIT_ASSERT_EQUAL(buffer.Size(), 1);
    for (auto i = 0; i < 3; ++i) {
        buffer.PopFront();
        UNIT_ASSERT_EQUAL(buffer.Size(), 0);
        UNIT_ASSERT(!buffer.Get(0));
    }
}

Y_UNIT_TEST(TestOverflowNoInitSize) {
    TBufUnboxed buffer(3, TUnboxedValuePod(), 0);
    buffer.PushBack(TUnboxedValue::Embedded("1"));
    buffer.PushBack(TUnboxedValue::Embedded("2"));
    buffer.PushBack(TUnboxedValue::Embedded("3"));
    UNIT_ASSERT_EXCEPTION(buffer.PushBack(TUnboxedValue::Embedded("4")), yexception);
}

Y_UNIT_TEST(TestOverflowWithInitSize) {
    TBufUnboxed buffer(3, TUnboxedValuePod(), 3);
    buffer.PopFront();
    buffer.PushBack(TUnboxedValue::Embedded("1"));
    buffer.PopFront();
    buffer.PushBack(TUnboxedValue::Embedded("2"));
    UNIT_ASSERT_EQUAL(buffer.Size(), 3);
    UNIT_ASSERT_EQUAL(buffer.Get(1).AsStringRef(), "1");
    UNIT_ASSERT_EXCEPTION(buffer.PushBack(TUnboxedValue::Embedded("3")), yexception);
}

Y_UNIT_TEST(TestOverflowOnEmpty) {
    TBufUnboxed buffer(0, TUnboxedValuePod());
    buffer.PopFront();
    UNIT_ASSERT_EXCEPTION(buffer.PushBack(TUnboxedValue::Embedded("1")), yexception);
}

Y_UNIT_TEST(TestUnbounded) {
    TBufUnboxed buffer({}, TUnboxedValuePod(), 3);
    for (size_t i = 0; i < 100; ++i) {
        buffer.PushBack(TUnboxedValue::Embedded(ToString(i)));
    }

    UNIT_ASSERT(!buffer.Get(0));
    UNIT_ASSERT(!buffer.Get(1));
    UNIT_ASSERT(!buffer.Get(2));

    for (size_t i = 0; i < 100; ++i) {
        UNIT_ASSERT_EQUAL(TStringBuf(buffer.Get(i + 3).AsStringRef()), ToString(i));
    }

    for (size_t i = 0; i < 100; ++i) {
        buffer.PopFront();
    }

    UNIT_ASSERT_EQUAL(buffer.Size(), 3);
    UNIT_ASSERT_EQUAL(TStringBuf(buffer.Get(0).AsStringRef()), ToString(97));
    UNIT_ASSERT_EQUAL(TStringBuf(buffer.Get(1).AsStringRef()), ToString(98));
    UNIT_ASSERT_EQUAL(TStringBuf(buffer.Get(2).AsStringRef()), ToString(99));

    buffer.PopFront();
    buffer.PopFront();
    buffer.PopFront();
    UNIT_ASSERT_EQUAL(buffer.Size(), 0);
}

Y_UNIT_TEST(TestUnboxedFewCycles) {
    const auto multFillLevel = 0.6;
    const auto multPopLevel = 0.5;
    const auto initSize = 10;
    const auto iterationCount = 4;
    TBufUnboxed buffer(initSize, NUdf::TUnboxedValuePod());
    unsigned lastDirectIndex = 0;
    unsigned lastChecked = 0;
    for (unsigned iteration = 0; iteration < iterationCount; ++iteration) {
        for (auto i = 0; i < multFillLevel * initSize; ++i) {
            buffer.PushBack(NUdf::TUnboxedValuePod(++lastDirectIndex));
        }
        UNIT_ASSERT(buffer.Size() > 0);
        UNIT_ASSERT_EQUAL(buffer.Get(buffer.Size() - 1).Get<unsigned>(), lastDirectIndex);
        for (auto i = 0; i < multPopLevel * initSize; ++i) {
            auto curUnboxed = buffer.Get(0);
            auto cur = curUnboxed.Get<unsigned>();
            UNIT_ASSERT(lastChecked < cur);
            lastChecked = cur;
            buffer.PopFront();
        }
        UNIT_ASSERT_EQUAL(buffer.Size(), iteration + 1);
    }
    UNIT_ASSERT_EQUAL(buffer.Size(), lastDirectIndex - lastChecked);
    UNIT_ASSERT_EQUAL(buffer.Get(0).Get<unsigned>(), lastChecked + 1);
    UNIT_ASSERT_EQUAL(buffer.Get(buffer.Size() - 1).Get<unsigned>(), lastDirectIndex);
}

Y_UNIT_TEST(TestUnboundedDoesNotGrowSizeInfinitly) {
    // Test that buffers hold buffer capacity reasonable.
    // Create unbounded buffer, add/remove |elementsPerCycle| elements |numberOfCycles| times.
    TBufUnboxed buffer({}, TUnboxedValuePod());
    const size_t elementsPerCycle = 100;
    const size_t numberOfCycles = 100;
    const size_t physicalUpperBoundSize = 300;

    for (size_t cycle = 0; cycle < numberOfCycles; ++cycle) {
        for (size_t i = 0; i < elementsPerCycle; ++i) {
            buffer.PushBack(TUnboxedValue::Embedded(ToString(cycle * elementsPerCycle + i)));
        }
        for (size_t i = 0; i < elementsPerCycle; ++i) {
            buffer.PopFront();
        }
    }

    UNIT_ASSERT_EQUAL(buffer.Size(), 0);
    // Physical size should be reasonable (less than |physicalUpperBoundSize|)
    // This verifies that Reallocate is working efficiently.
    UNIT_ASSERT(buffer.Capacity() < physicalUpperBoundSize);
}

Y_UNIT_TEST(TestRandomOperationsVsDeque) {
    // Test circular buffer against std::deque with random operations.
    TIntrusivePtr<IRandomProvider> random = CreateDeterministicRandomProvider(42);

    const size_t numOperations = 100;
    const size_t numTests = 1000;

    for (size_t testNumber = 0; testNumber < numTests; testNumber++) {
        TBufUnboxed buffer({}, TUnboxedValuePod()); // Unbounded buffer.
        std::deque<ui32> deque;
        ui32 nextValue = 0;
        auto verifyExpectedAndActualOutput = [&]() {
            UNIT_ASSERT_EQUAL(buffer.Size(), deque.size());
            for (size_t j = 0; j < deque.size(); ++j) {
                UNIT_ASSERT_EQUAL(buffer.Get(j).Get<ui32>(), deque[j]);
            }
        };

        for (size_t i = 0; i < numOperations; ++i) {
            double rnd = random->GenRandReal2();
            bool shouldPush = rnd < 0.5;
            bool shouldPop = !shouldPush;

            if (shouldPush) {
                ui32 value = nextValue++;
                buffer.PushBack(TUnboxedValuePod(value));
                deque.push_back(value);
            }
            if (shouldPop) {
                if (deque.empty()) {
                    // Just call no op operator.
                    buffer.PopFront();
                } else {
                    buffer.PopFront();
                    deque.pop_front();
                }
            }

            verifyExpectedAndActualOutput();
        }
    }
}
Y_UNIT_TEST(TestClean) {
    const int emptyValue = -1;
    TSafeCircularBuffer<int> buffer(TMaybe<size_t>(), emptyValue);

    // Test 1: Clean after simple push operations
    for (size_t i = 0; i < 10; ++i) {
        buffer.PushBack(i);
    }
    buffer.Clean();
    UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), 10);
    for (size_t i = 0; i < buffer.Size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(buffer.Get(i), emptyValue);
    }

    // Test 2: Clean after push and pop operations
    for (size_t i = 0; i < 20; ++i) {
        buffer.PushBack(i);
    }
    for (size_t i = 0; i < 5; ++i) {
        buffer.PopFront();
    }
    buffer.Clean();
    for (size_t i = 0; i < buffer.Size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(buffer.Get(i), emptyValue);
    }
}

Y_UNIT_TEST(TestClear) {
    auto testClearBuffer = [](TSafeCircularBuffer<int>& buf, const char* testName) {
        Y_UNUSED(testName);
        buf.Clear();
        UNIT_ASSERT_VALUES_EQUAL(buf.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(buf.Capacity(), 0);
    };

    // Test 1: Clear unbounded buffer with simple push
    TSafeCircularBuffer<int> unbounded(TMaybe<size_t>(), -1);
    for (size_t i = 0; i < 100; ++i) {
        unbounded.PushBack(i);
    }
    testClearBuffer(unbounded, "unbounded simple");

    // Test 2: Clear bounded buffer
    TSafeCircularBuffer<int> bounded(10, -1);
    for (size_t i = 0; i < 5; ++i) {
        bounded.PushBack(i);
    }
    testClearBuffer(bounded, "bounded");

    // Test 3: Clear after complex operations (push, pop, push)
    TSafeCircularBuffer<int> complex(TMaybe<size_t>(), -1);
    for (size_t i = 0; i < 50; ++i) {
        complex.PushBack(i);
    }
    for (size_t i = 0; i < 30; ++i) {
        complex.PopFront();
    }
    for (size_t i = 0; i < 30; ++i) {
        complex.PushBack(50 + i);
    }
    testClearBuffer(complex, "complex operations");
}

Y_UNIT_TEST(TestGeneration) {
    const int emptyValue = -1;
    TSafeCircularBuffer<int> buffer(TMaybe<size_t>(), emptyValue);

    UNIT_ASSERT_VALUES_EQUAL(buffer.Generation(), 0);
    const size_t pushCount = 3;
    for (size_t i = 0; i < pushCount; ++i) {
        buffer.PushBack(i);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Generation(), i + 1);
    }

    const size_t popCount = 5;
    for (size_t i = 0; i < popCount; ++i) {
        buffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(buffer.Generation(), pushCount + i + 1);
    }

    ui64 genBeforeClean = buffer.Generation();
    buffer.Clean();
    UNIT_ASSERT_VALUES_EQUAL(buffer.Generation(), genBeforeClean + 1);

    ui64 genBeforeClear = buffer.Generation();
    buffer.Clear();
    UNIT_ASSERT_VALUES_EQUAL(buffer.Generation(), genBeforeClear + 1);

    TSafeCircularBuffer<int> growthBuffer(TMaybe<size_t>(), emptyValue);
    ui64 expectedGen = 0;
    const size_t operationCount = 100;

    for (size_t i = 0; i < operationCount; ++i) {
        growthBuffer.PushBack(i);
        UNIT_ASSERT_VALUES_EQUAL(growthBuffer.Generation(), ++expectedGen);
    }

    for (size_t i = 0; i < operationCount / 2; ++i) {
        growthBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(growthBuffer.Generation(), ++expectedGen);
    }
}

} // Y_UNIT_TEST_SUITE(TMiniKQLSafeCircularBuffer)

} // namespace NMiniKQL
} // namespace NKikimr
