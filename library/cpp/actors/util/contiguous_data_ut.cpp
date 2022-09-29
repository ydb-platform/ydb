#include "contiguous_data.h"
#include "ut_helpers.h"
#include "rope.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

Y_UNIT_TEST_SUITE(TContiguousData) {
    Y_UNIT_TEST(TypeSize) {
        UNIT_ASSERT_EQUAL(sizeof(TContiguousData), 4 * sizeof(uintptr_t));
    }

    Y_UNIT_TEST(CrossCompare) {
        TString str = "some very long string";
        const TString constStr(str);
        TStringBuf strbuf = str;
        const TStringBuf constStrbuf = str;
        TContiguousSpan span(str);
        const TContiguousSpan constSpan(str);
        TMutableContiguousSpan mutableSpan(const_cast<char*>(str.data()), str.size());
        const TMutableContiguousSpan constMutableSpan(const_cast<char*>(str.data()), str.size());
        TContiguousData data(str);
        const TContiguousData constData(str);
        TArrayRef<char> arrRef(const_cast<char*>(str.data()), str.size());
        const TArrayRef<char> constArrRef(const_cast<char*>(str.data()), str.size());
        TArrayRef<const char> arrConstRef(const_cast<char*>(str.data()), str.size());
        const TArrayRef<const char> constArrConstRef(const_cast<char*>(str.data()), str.size());
        NActors::TSharedData sharedData = NActors::TSharedData::Copy(str.data(), str.size());
        const NActors::TSharedData constSharedData(sharedData);

        Permutate(
            [](auto& arg1, auto& arg2) {
                UNIT_ASSERT(arg1 == arg2);
            },
            str,
            constStr,
            strbuf,
            constStrbuf,
            span,
            constSpan,
            mutableSpan,
            constMutableSpan,
            data,
            constData,
            arrRef,
            constArrRef,
            arrConstRef,
            constArrConstRef,
            sharedData,
            constSharedData);
    }

    Y_UNIT_TEST(Resize) {
        TContiguousData data = TContiguousData::Uninitialized(10, 20, 30);
        UNIT_ASSERT_EQUAL(data.size(), 10);
        UNIT_ASSERT_EQUAL(data.Headroom(), 20);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 76);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom() + 2 * sizeof(char*));
        data.GrowFront(5);
        UNIT_ASSERT_EQUAL(data.size(), 15);
        UNIT_ASSERT_EQUAL(data.Headroom(), 15);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 76);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom() + 2 * sizeof(char*));
        data.GrowBack(5);
        UNIT_ASSERT_EQUAL(data.size(), 20);
        UNIT_ASSERT_EQUAL(data.Headroom(), 15);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 25);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 76);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom() + 2 * sizeof(char*));
        data.GrowFront(21);
        UNIT_ASSERT_EQUAL(data.size(), 41);
        UNIT_ASSERT_EQUAL(data.Headroom(), 0);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 25);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 82);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom() + 2 * sizeof(char*));
        data.GrowBack(26);
        UNIT_ASSERT_EQUAL(data.size(), 67);
        UNIT_ASSERT_EQUAL(data.Headroom(), 0);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 0);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 67);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom());
    }

    Y_UNIT_TEST(ResizeUnshare) {
        TContiguousData data = TContiguousData::Uninitialized(10, 20, 30);
        TContiguousData otherData(data);
        UNIT_ASSERT_EQUAL(data.data(), otherData.data());
        UNIT_ASSERT_EQUAL(data.size(), 10);
        UNIT_ASSERT_EQUAL(data.Headroom(), 20);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 76);
        UNIT_ASSERT_EQUAL(otherData.size(), 10);
        UNIT_ASSERT_EQUAL(otherData.Headroom(), 20);
        UNIT_ASSERT_EQUAL(otherData.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(otherData.GetOccupiedMemorySize(), 76);
        data.GrowFront(5);
        data.GrowBack(5);
        UNIT_ASSERT_EQUAL(data.data() + 5, otherData.data());
        UNIT_ASSERT_EQUAL(data.size(), 20);
        UNIT_ASSERT_EQUAL(data.Headroom(), 15);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 25);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 76);
        otherData.GrowFront(5);
        UNIT_ASSERT_UNEQUAL(data.data(), otherData.data());
        UNIT_ASSERT_EQUAL(otherData.size(), 15);
        UNIT_ASSERT_EQUAL(otherData.Headroom(), 15);
        UNIT_ASSERT_EQUAL(otherData.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(otherData.GetOccupiedMemorySize(), 76);
    }

    Y_UNIT_TEST(Trim) {
        TContiguousData data = TContiguousData::Uninitialized(10, 20, 30);
        TContiguousData otherData(data);
        otherData.Trim(5);
        UNIT_ASSERT_EQUAL(data.data(), otherData.data());
        UNIT_ASSERT_EQUAL(otherData.Headroom(), 0);
        UNIT_ASSERT_EQUAL(otherData.Tailroom(), 0);
        TContiguousData otherData2(data);
        otherData2.Trim(1, 1);
        UNIT_ASSERT_EQUAL(data.data() + 1, otherData2.data());
        UNIT_ASSERT_EQUAL(otherData2.Headroom(), 0);
        UNIT_ASSERT_EQUAL(otherData2.Tailroom(), 0);
        otherData.Trim(1, 1);
        UNIT_ASSERT_EQUAL(otherData.data(), otherData2.data());
        data.GrowFront(5);
        data.GrowBack(5);
        UNIT_ASSERT_EQUAL(data.data() + 6, otherData2.data());
        UNIT_ASSERT_EQUAL(data.data() + 6, otherData.data());
        otherData.GrowFront(1);
        UNIT_ASSERT_UNEQUAL(data.data() + 7, otherData.data());
        otherData2.GrowBack(1);
        UNIT_ASSERT_UNEQUAL(data.data() + 6, otherData2.data());
        data = TContiguousData::Uninitialized(10);
        otherData = data;
        data.Trim(5);
        UNIT_ASSERT_EQUAL(data.data(), otherData.data());
        UNIT_ASSERT_EQUAL(data.size(), 5);
    }

    Y_UNIT_TEST(SliceUnshare) {
        TContiguousData data = TContiguousData::Uninitialized(10, 20, 30);
        TContiguousData otherData(TContiguousData::Slice, data.data(), data.size(), data);
        UNIT_ASSERT_EQUAL(otherData.Headroom(), 0);
        UNIT_ASSERT_EQUAL(otherData.Tailroom(), 0);
    }

    Y_UNIT_TEST(Extract) {
        TContiguousData data = TContiguousData::Uninitialized(10, 20, 30);
        NActors::TSharedData extracted = data.ExtractUnderlyingContainerOrCopy<NActors::TSharedData>();
        UNIT_ASSERT_UNEQUAL(data.data(), extracted.data());
        UNIT_ASSERT_UNEQUAL(data.data(), extracted.data() + 20);
        data = TContiguousData::Uninitialized(10);
        extracted = data.ExtractUnderlyingContainerOrCopy<NActors::TSharedData>();
        UNIT_ASSERT_EQUAL(data.data(), extracted.data());

        // TODO(innokentii) add microoptimization for this case
        // TContiguousData data = TContiguousData::Uninitialized(10, 20, 30);
        // data.GrowFront(20);
        // extracted = data.ExtractUnderlyingContainerOrCopy<NActors::TSharedData>();
        // UNIT_ASSERT_EQUAL(data.data(), extracted.data());
        // UNIT_ASSERT_EQUAL(data.size(), extracted.size());
        // data.GrowBack(30);
        // extracted = data.ExtractUnderlyingContainerOrCopy<NActors::TSharedData>();
        // UNIT_ASSERT_EQUAL(data.data(), extracted.data());
        // UNIT_ASSERT_EQUAL(data.size(), extracted.size());
    }
}
