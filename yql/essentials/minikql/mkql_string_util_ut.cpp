#include "mkql_alloc.h"
#include "mkql_string_util.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
using namespace NKikimr::NMiniKQL;

Y_UNIT_TEST_SUITE(TMiniKQLStringUtils) {
    Y_UNIT_TEST(SubstringWithLargeOffset) {
        TScopedAlloc alloc(__LOCATION__);
        const auto big = MakeStringNotFilled(NUdf::TUnboxedValuePod::OffsetLimit << 1U);
        const auto sub0 = SubString(big, 1U, 42U);
        const auto sub1 = SubString(big, NUdf::TUnboxedValuePod::OffsetLimit - 1U, 42U);
        const auto sub2 = SubString(big, NUdf::TUnboxedValuePod::OffsetLimit, 42U);

        UNIT_ASSERT(sub0.AsStringValue().Data() == sub1.AsStringValue().Data());
        UNIT_ASSERT(sub1.AsStringValue().Data() != sub2.AsStringValue().Data());
    }

    Y_UNIT_TEST(MakeStringWithPad) {
        TScopedAlloc alloc(__LOCATION__);

        {
            const auto buf= MakeStringNotFilled(NUdf::TUnboxedValuePod::InternalBufferSize - 1U, 1U);
            UNIT_ASSERT(buf.IsEmbedded());
            UNIT_ASSERT_VALUES_EQUAL(buf.AsStringRef().Size(), NUdf::TUnboxedValuePod::InternalBufferSize - 1U);
        }

        {
            const auto buf= MakeStringNotFilled(NUdf::TUnboxedValuePod::InternalBufferSize, 1U);
            UNIT_ASSERT(buf.IsString());
            UNIT_ASSERT_VALUES_EQUAL(buf.AsStringRef().Size(), NUdf::TUnboxedValuePod::InternalBufferSize);
            UNIT_ASSERT_VALUES_EQUAL(buf.AsStringValue().Size(), NUdf::TUnboxedValuePod::InternalBufferSize + 1U);
        }
    }

    Y_UNIT_TEST(MakeLargeString) {
        TScopedAlloc alloc(__LOCATION__);

        {
            const auto buf= MakeStringNotFilled(0xFFFFFFFFU);
            UNIT_ASSERT(buf.IsString());
            UNIT_ASSERT_VALUES_EQUAL(buf.AsStringRef().Size(), 0xFFFFFFFFU);
            const auto& value = buf.AsStringValue();
            UNIT_ASSERT_VALUES_EQUAL(value.Size(), 0xFFFFFFFFU);
            UNIT_ASSERT_VALUES_EQUAL(value.Capacity(), 0x100000000ULL);
        }

        {
            const auto buf= MakeStringNotFilled(0xFFFFFFF1U);
            UNIT_ASSERT(buf.IsString());
            UNIT_ASSERT_VALUES_EQUAL(buf.AsStringRef().Size(), 0xFFFFFFF1U);
            const auto& value = buf.AsStringValue();
            UNIT_ASSERT_VALUES_EQUAL(value.Size(), 0xFFFFFFF1U);
            UNIT_ASSERT_VALUES_EQUAL(value.Capacity(), 0x100000000ULL);
        }

        {
            const auto buf= MakeStringNotFilled(0xFFFFFFF0U);
            UNIT_ASSERT(buf.IsString());
            UNIT_ASSERT_VALUES_EQUAL(buf.AsStringRef().Size(), 0xFFFFFFF0U);
            const auto& value = buf.AsStringValue();
            UNIT_ASSERT_VALUES_EQUAL(value.Size(), 0xFFFFFFF0U);
            UNIT_ASSERT_VALUES_EQUAL(value.Capacity(), 0xFFFFFFF0ULL);
        }
    }

    Y_UNIT_TEST(ConcatLargeString) {
        TScopedAlloc alloc(__LOCATION__);

        const NUdf::TUnboxedValue buf = MakeStringNotFilled(0x80000000U);
        try {
            ConcatStrings(buf, buf);
            UNIT_FAIL("No exception!");
        } catch (const yexception&) {}

        try {
            PrependString(buf.AsStringRef(), buf);
            UNIT_FAIL("No exception!");
        } catch (const yexception&) {}

        try {
            AppendString(buf, buf.AsStringRef());
            UNIT_FAIL("No exception!");
        } catch (const yexception&) {}
    }
}

