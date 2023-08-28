#include "../mkql_safe_circular_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/public/udf/udf_value.h>

namespace NKikimr {
using namespace NUdf;
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLSafeCircularBuffer) {
    typedef TSafeCircularBuffer<TUnboxedValue> TBufUnboxed;

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
        UNIT_ASSERT_EQUAL(buffer.UsedSize(), 3);
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
        UNIT_ASSERT_EQUAL(buffer.UsedSize(), 2);
        buffer.PopFront();
        UNIT_ASSERT_EQUAL(buffer.Get(0).AsStringRef(), "bunny");
        UNIT_ASSERT_EQUAL(buffer.UsedSize(), 1);
        for (auto i = 0; i < 3; ++i) {
            buffer.PopFront();
            UNIT_ASSERT_EQUAL(buffer.UsedSize(), 0);
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
        UNIT_ASSERT_EQUAL(buffer.UsedSize(), 3);
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

        UNIT_ASSERT_EQUAL(buffer.UsedSize(), 3);
        UNIT_ASSERT_EQUAL(TStringBuf(buffer.Get(0).AsStringRef()), ToString(97));
        UNIT_ASSERT_EQUAL(TStringBuf(buffer.Get(1).AsStringRef()), ToString(98));
        UNIT_ASSERT_EQUAL(TStringBuf(buffer.Get(2).AsStringRef()), ToString(99));

        buffer.PopFront();
        buffer.PopFront();
        buffer.PopFront();
        UNIT_ASSERT_EQUAL(buffer.UsedSize(), 0);
        UNIT_ASSERT_EQUAL(buffer.Size(), 103);
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
            UNIT_ASSERT(buffer.UsedSize() > 0);
            UNIT_ASSERT_EQUAL(buffer.Get(buffer.UsedSize() - 1).Get<unsigned>(), lastDirectIndex);
            for (auto i = 0; i < multPopLevel * initSize; ++i) {
                auto curUnboxed = buffer.Get(0);
                auto cur = curUnboxed.Get<unsigned>();
                UNIT_ASSERT(lastChecked < cur);
                lastChecked = cur;
                buffer.PopFront();
            }
            UNIT_ASSERT_EQUAL(buffer.UsedSize(), iteration + 1);
        }
        UNIT_ASSERT_EQUAL(buffer.UsedSize(), lastDirectIndex - lastChecked);
        UNIT_ASSERT_EQUAL(buffer.Get(0).Get<unsigned>(), lastChecked + 1);
        UNIT_ASSERT_EQUAL(buffer.Get(buffer.UsedSize() - 1).Get<unsigned>(), lastDirectIndex);
    }
}

}
}
