#include "rc_buf.h"
#include "ut_helpers.h"
#include "rope.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

Y_UNIT_TEST_SUITE(TRcBuf) {
    Y_UNIT_TEST(TypeSize) {
        UNIT_ASSERT_EQUAL(sizeof(TRcBuf), 4 * sizeof(uintptr_t));
    }

    Y_UNIT_TEST(Slice) {
        auto data = TRcBuf::Copy("Hello", 5);
        UNIT_ASSERT_VALUES_EQUAL(TString(TStringBuf(data)), TString("Hello"));
        UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice()), TString("Hello"));
        UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1)), TString("ello"));
        UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1, 3)), TString("ell"));
        UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1, 100)), TString("ello"));
        UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(0, 4)), TString("Hell"));
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
        TRcBuf data(str);
        const TRcBuf constData(str);
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

    Y_UNIT_TEST(Detach) {
        TRcBuf data = TRcBuf::Copy(TString("test"));
        TRcBuf data2 = data;
        char* res = data2.Detach();
        UNIT_ASSERT_UNEQUAL(data.GetData(), data2.GetData());
        UNIT_ASSERT_EQUAL(res, data2.GetData());
        UNIT_ASSERT_EQUAL(::memcmp(res, "test", 4), 0);
        UNIT_ASSERT_EQUAL(::memcmp(data.GetData(), "test", 4), 0);
    }

    Y_UNIT_TEST(Resize) {
        TRcBuf data = TRcBuf::Uninitialized(10, 20, 30);
        UNIT_ASSERT_EQUAL(data.size(), 10);
        UNIT_ASSERT_EQUAL(data.Headroom(), 20);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 60);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom());
        data.GrowFront(5);
        UNIT_ASSERT_EQUAL(data.size(), 15);
        UNIT_ASSERT_EQUAL(data.Headroom(), 15);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 60);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom());
        data.GrowBack(5);
        UNIT_ASSERT_EQUAL(data.size(), 20);
        UNIT_ASSERT_EQUAL(data.Headroom(), 15);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 25);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 60);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom());
        data.GrowFront(21);
        UNIT_ASSERT_EQUAL(data.size(), 41);
        UNIT_ASSERT_EQUAL(data.Headroom(), 0);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 25);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 66);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom());
        data.GrowBack(32);
        UNIT_ASSERT_EQUAL(data.size(), 73);
        UNIT_ASSERT_EQUAL(data.Headroom(), 0);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 0);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 73);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), data.size() + data.Headroom() + data.Tailroom());
    }

    Y_UNIT_TEST(ResizeUnshare) {
        TRcBuf data = TRcBuf::Uninitialized(10, 20, 30);
        TRcBuf otherData(data);
        UNIT_ASSERT_EQUAL(data.data(), otherData.data());
        UNIT_ASSERT_EQUAL(data.size(), 10);
        UNIT_ASSERT_EQUAL(data.Headroom(), 20);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 60);
        UNIT_ASSERT_EQUAL(otherData.size(), 10);
        UNIT_ASSERT_EQUAL(otherData.Headroom(), 20);
        UNIT_ASSERT_EQUAL(otherData.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(otherData.GetOccupiedMemorySize(), 60);
        data.GrowFront(5);
        data.GrowBack(5);
        UNIT_ASSERT_EQUAL(data.data() + 5, otherData.data());
        UNIT_ASSERT_EQUAL(data.size(), 20);
        UNIT_ASSERT_EQUAL(data.Headroom(), 15);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 25);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 60);
        otherData.GrowFront(5);
        UNIT_ASSERT_UNEQUAL(data.data(), otherData.data());
        UNIT_ASSERT_EQUAL(otherData.size(), 15);
        UNIT_ASSERT_EQUAL(otherData.Headroom(), 15);
        UNIT_ASSERT_EQUAL(otherData.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(otherData.GetOccupiedMemorySize(), 60);
        data.TrimBack(15);
        data.TrimFront(10);
        UNIT_ASSERT_EQUAL(data.size(), 10);
        UNIT_ASSERT_EQUAL(data.Headroom(), 20);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 30);
        UNIT_ASSERT_EQUAL(data.GetOccupiedMemorySize(), 60);
    }

    Y_UNIT_TEST(Trim) {
        TRcBuf data = TRcBuf::Uninitialized(10, 20, 30);
        TRcBuf otherData(data);
        otherData.TrimBack(5);
        UNIT_ASSERT_EQUAL(data.data(), otherData.data());
        UNIT_ASSERT_EQUAL(otherData.Headroom(), 20);
        UNIT_ASSERT_EQUAL(otherData.Tailroom(), 0);
        TRcBuf otherData2(data);
        otherData2.TrimBack(2);
        otherData2.TrimFront(1);
        UNIT_ASSERT_EQUAL(data.data() + 1, otherData2.data());
        UNIT_ASSERT_EQUAL(otherData2.Headroom(), 0);
        UNIT_ASSERT_EQUAL(otherData2.Tailroom(), 0);
        otherData.TrimBack(2);
        otherData.TrimFront(1);
        UNIT_ASSERT_EQUAL(otherData.data(), otherData2.data());
        data.GrowFront(5);
        data.GrowBack(5);
        UNIT_ASSERT_EQUAL(data.data() + 6, otherData2.data());
        UNIT_ASSERT_EQUAL(data.data() + 6, otherData.data());
        otherData.GrowFront(1);
        UNIT_ASSERT_UNEQUAL(data.data() + 7, otherData.data());
        otherData2.GrowBack(1);
        UNIT_ASSERT_UNEQUAL(data.data() + 6, otherData2.data());
        data = TRcBuf::Uninitialized(10);
        otherData = data;
        data.TrimBack(5);
        UNIT_ASSERT_EQUAL(data.data(), otherData.data());
        UNIT_ASSERT_EQUAL(data.size(), 5);
    }

    Y_UNIT_TEST(SliceUnshare) {
        TRcBuf data = TRcBuf::Uninitialized(10, 20, 30);
        TRcBuf otherData(TRcBuf::Piece, data.data() + 1, data.size() - 2, data);
        UNIT_ASSERT_EQUAL(otherData.Headroom(), 0);
        UNIT_ASSERT_EQUAL(otherData.Tailroom(), 0);
    }

    Y_UNIT_TEST(Reserve) {
        TRcBuf data = TRcBuf::Copy("test", 4, 5, 6);
        TRcBuf data2 = data;
        data.reserve(1);
        data.ReserveTailroom(6);
        UNIT_ASSERT_EQUAL(data.GetData(), data2.GetData());
        UNIT_ASSERT_EQUAL(data.GetSize(), data2.GetSize());
        UNIT_ASSERT_EQUAL(data.Tailroom(), 6);
        data.ReserveHeadroom(5);
        UNIT_ASSERT_EQUAL(data.GetData(), data2.GetData());
        UNIT_ASSERT_EQUAL(data.GetSize(), data2.GetSize());
        UNIT_ASSERT_EQUAL(data.Headroom(), 5);
        data.ReserveBidi(5, 6);
        UNIT_ASSERT_EQUAL(data.GetData(), data2.GetData());
        UNIT_ASSERT_EQUAL(data.GetSize(), data2.GetSize());
        UNIT_ASSERT_EQUAL(data.Headroom(), 5);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 6);
        data.ReserveHeadroom(6);
        UNIT_ASSERT_EQUAL(data.Headroom(), 6);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 6);
        UNIT_ASSERT_EQUAL(::memcmp(data.GetData(), "test", 4), 0);
        data.ReserveTailroom(7);
        UNIT_ASSERT_EQUAL(data.Headroom(), 6);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 7);
        UNIT_ASSERT_EQUAL(::memcmp(data.GetData(), "test", 4), 0);
        data.ReserveBidi(7, 8);
        UNIT_ASSERT_EQUAL(data.Headroom(), 7);
        UNIT_ASSERT_EQUAL(data.Tailroom(), 8);
        UNIT_ASSERT_EQUAL(::memcmp(data.GetData(), "test", 4), 0);
    }
}
