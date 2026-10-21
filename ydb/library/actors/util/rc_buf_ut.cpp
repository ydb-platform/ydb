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
        UNIT_ASSERT_EQUAL(data.GetData(), data2.GetData());
        char* res = data2.Detach();
        UNIT_ASSERT_UNEQUAL(data.GetData(), data2.GetData());
        UNIT_ASSERT_EQUAL(res, data2.GetData());
        UNIT_ASSERT_EQUAL(::memcmp(res, "test", 4), 0);
        UNIT_ASSERT_EQUAL(::memcmp(data.GetData(), "test", 4), 0);
    }

    Y_UNIT_TEST(DetachAndDestroySrc) {
        TRcBuf data = TRcBuf::Copy(TString("test"));
        TRcBuf data2 = data;
        data = {};
        UNIT_ASSERT_EQUAL(data2.GetSize(), 4);
        char* res = data2.Detach();
        UNIT_ASSERT_EQUAL(data2.GetSize(), 4);
        UNIT_ASSERT_EQUAL(res, data2.GetData());
        UNIT_ASSERT_EQUAL(::memcmp(res, "test", 4), 0);
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

    Y_UNIT_TEST(CopyWithStringBackend) {
        TString str = "abcdefghijklmno";
        TRcBuf original(str);
        original.TrimFront(10);

        TRcBuf copy = original;
        UNIT_ASSERT_EQUAL(::memcmp(copy.data(), str.data() + 5, 10), 0);

        copy.GrowFront(100);
        UNIT_ASSERT_VALUES_EQUAL(copy.size(), 110);
        UNIT_ASSERT_EQUAL(::memcmp(copy.data() + 100, str.data() + 5, 10), 0);
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

    Y_UNIT_TEST(PieceWithStringBackend) {
        TString str = "abcdefghijklmno";
        TRcBuf original(str);
        original.TrimFront(12);
        original.TrimBack(8);

        TRcBuf piece(TRcBuf::Piece, original.data(), original.size(), original);
        UNIT_ASSERT_EQUAL(::memcmp(piece.data(), str.data() + 3, 8), 0);

        piece.GrowBack(100);
        UNIT_ASSERT_VALUES_EQUAL(piece.size(), 108);
        UNIT_ASSERT_EQUAL(::memcmp(piece.data(), str.data() + 3, 8), 0);
    }

    Y_UNIT_TEST(ExpandFrontReservesHeadroomInCookies) {
        constexpr size_t headroom = 16;
        const TString payloadStr(64, 'x');

        // Payload-only buffer with reserved headroom (same layout as interconnect receive path).
        TRcBuf payloadOnly = TRcBuf::Copy(payloadStr.data(), payloadStr.size(), headroom, 0);
        const char* payloadPtr = payloadOnly.GetData();

        // Sibling view sharing the backend (mirrors event Payload rope after ExpandFront).
        TRcBuf payloadSibling(payloadOnly);

        TRcBuf withHeader = payloadOnly.ExpandFront(headroom);
        UNIT_ASSERT_VALUES_EQUAL(withHeader.GetSize(), headroom + payloadStr.size());
        UNIT_ASSERT(withHeader.GetData() + headroom == payloadPtr);

        // Regression guard: once ExpandFront claims the headroom (moves the cookie front edge), the
        // sibling must no longer see it as growable, so it cannot claim the same header bytes.
        UNIT_ASSERT_VALUES_EQUAL(payloadSibling.Headroom(), 0u);
    }

    Y_UNIT_TEST(ExpandFrontZeroPrivate) {
        const TString payloadStr(64, 'x');
        constexpr size_t headroom = 16;

        TRcBuf buf = TRcBuf::Copy(payloadStr.data(), payloadStr.size(), headroom, 0);
        const char* payloadPtr = buf.GetData();

        // frontBytes == 0 must return a plain view over the same bytes without claiming headroom.
        TRcBuf same = buf.ExpandFront(0);
        UNIT_ASSERT_VALUES_EQUAL(same.GetSize(), payloadStr.size());
        UNIT_ASSERT(same.GetData() == payloadPtr);

        // The original keeps its headroom available (cookies untouched).
        UNIT_ASSERT_VALUES_EQUAL(buf.Headroom(), headroom);
    }

    Y_UNIT_TEST(ExpandFrontZeroSharedNonFront) {
        const TString payloadStr(64, 'x');
        constexpr size_t headroom = 16;

        TRcBuf payloadOnly = TRcBuf::Copy(payloadStr.data(), payloadStr.size(), headroom, 0);
        const char* payloadPtr = payloadOnly.GetData();

        // Sibling sharing the backend, so neither buffer is private.
        TRcBuf payloadSibling(payloadOnly);

        // One sibling claims the headroom, moving the cookie front edge away from payloadPtr.
        TRcBuf withHeader = payloadOnly.ExpandFront(headroom);
        UNIT_ASSERT(withHeader.GetData() + headroom == payloadPtr);
        UNIT_ASSERT_VALUES_EQUAL(payloadSibling.Headroom(), 0u);

        // ExpandFront(0) on the now non-front shared sibling must not abort; it returns a plain view.
        TRcBuf same = payloadSibling.ExpandFront(0);
        UNIT_ASSERT_VALUES_EQUAL(same.GetSize(), payloadStr.size());
        UNIT_ASSERT(same.GetData() == payloadPtr);
    }

    Y_UNIT_TEST(UninitializedAlignedZeroSize) {
        for (size_t alignment : {size_t(0), size_t(1), size_t(8), size_t(4096)}) {
            TRcBuf buf = TRcBuf::UninitializedAligned(0, alignment);
            UNIT_ASSERT_VALUES_EQUAL(buf.GetSize(), 0u);
        }
    }

    Y_UNIT_TEST(UninitializedAlignedNonZero) {
        for (size_t alignment : {size_t(8), size_t(64), size_t(4096)}) {
            TRcBuf buf = TRcBuf::UninitializedAligned(100, alignment);
            UNIT_ASSERT_VALUES_EQUAL(buf.GetSize(), 100u);
            UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(buf.GetData()) % alignment, 0u);
        }
    }
}
