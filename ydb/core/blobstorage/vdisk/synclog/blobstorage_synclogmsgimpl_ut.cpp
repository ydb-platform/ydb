#include "blobstorage_synclogmsgimpl.h"
#include "codecs.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/random/shuffle.h>
#include <util/random/random.h>
#include <util/stream/null.h>

using namespace NKikimr;
using namespace NKikimr::NSyncLog;

#define STR Cnull

std::tuple<TLogoBlobID, ui64, ui64> AsTuple(const TLogoBlobRecWithSerial& x) {
    return {x.LogoBlobID(), x.Ingress.Raw(), x.Counter};
}

std::tuple<ui64, ui32, ui64> AsTuple(const TBlockRecWithSerial& x) {
    return {x.TabletId, x.Generation, x.Counter};
}

std::tuple<ui64, ui32, ui64, ui64> AsTuple(const TBlockRecWithSerialV2& x) {
    return {x.TabletId, x.Generation, x.IssuerGuid, x.Counter};
}

std::tuple<ui64, ui32, ui32, ui32, ui32, ui32, ui32, ui64, ui64> AsTuple(const TBarrierRecWithSerial& x) {
    return {x.TabletId, x.Channel, x.Hard, x.Gen, x.GenCounter, x.CollectGeneration, x.CollectStep, x.Ingress.Raw(), x.Counter};
}

template<typename T, typename... Tx, typename = std::enable_if<std::is_same_v<decltype(AsTuple(std::declval<const T&>())), std::tuple<Tx...>>>>
bool Equal(const T& x, const T& y) {
    return AsTuple(x) == AsTuple(y);
}

template<typename T>
bool Equal(const std::vector<T>& x, const std::vector<T>& y) {
    if (x.size() != y.size()) {
        return false;
    }
    for (size_t i = 0; i < x.size(); ++i) {
        if (!Equal(x[i], y[i])) {
            return false;
        }
    }
    return true;
}

bool Equal(const TRecordsWithSerial& x, const TRecordsWithSerial& y) {
    return Equal(x.LogoBlobs, y.LogoBlobs) && Equal(x.Blocks, y.Blocks) && Equal(x.Barriers, y.Barriers) &&
        Equal(x.BlocksV2, y.BlocksV2);
}

Y_UNIT_TEST_SUITE(ReorderCodecTest) {

    Y_UNIT_TEST(Basic) {
        TRecordsWithSerial recsSrc;

        // logoblobs
        TVector<TLogoBlobRecWithSerial>& logoBlobsSrc = recsSrc.LogoBlobs;
        logoBlobsSrc.emplace_back(TLogoBlobID(66, 1, 0, 0, 110, 20), 0, 0);
        logoBlobsSrc.emplace_back(TLogoBlobID(66, 1, 0, 0, 109, 21), 0, 1);
        logoBlobsSrc.emplace_back(TLogoBlobID(66, 1, 0, 0, 108, 22), 0, 2);
        logoBlobsSrc.emplace_back(TLogoBlobID(66, 1, 0, 0, 107, 23), 0, 3);
        logoBlobsSrc.emplace_back(TLogoBlobID(66, 1, 0, 0, 106, 24), 0, 4);
        logoBlobsSrc.emplace_back(TLogoBlobID(42, 1, 1, 0, 100, 15), 0, 5);
        logoBlobsSrc.emplace_back(TLogoBlobID(42, 1, 2, 0, 100, 19), 0, 6);
        logoBlobsSrc.emplace_back(TLogoBlobID(42, 1, 3, 0, 100, 16), 0, 7);
        logoBlobsSrc.emplace_back(TLogoBlobID(42, 1, 1, 3, 100, 17), 0, 8);
        logoBlobsSrc.emplace_back(TLogoBlobID(42, 1, 2, 3, 100, 18), 0, 9);
        logoBlobsSrc.emplace_back(TLogoBlobID(42, 2, 0, 0, 100, 20), 0, 10);

        // blocks
        TVector<TBlockRecWithSerial>& blocksSrc = recsSrc.Blocks;
        blocksSrc.emplace_back(42, 1, 11);
        blocksSrc.emplace_back(73, 0, 12);

        // barriers
        TVector<TBarrierRecWithSerial>& barriersSrc = recsSrc.Barriers;
        barriersSrc.emplace_back(42, 0, 0, 345, 0, 3, false, 0, 13);

        // save original vectors
        TRecordsWithSerial origRecsSrc(recsSrc);

        // encode
        TReorderCodec codec(TReorderCodec::EEncoding::Trivial);
        TString encoded = codec.Encode(recsSrc);

        // decode
        TRecordsWithSerial recsRes;
        bool res = codec.DecodeString(encoded, recsRes);

        // check
        UNIT_ASSERT_VALUES_EQUAL(res, true);
        UNIT_ASSERT(Equal(recsRes, origRecsSrc));
    }
}


#include <library/cpp/codecs/pfor_codec.h>
#include <library/cpp/codecs/huffman_codec.h>
#include <library/cpp/codecs/solar_codec.h>

Y_UNIT_TEST_SUITE(CodecsTest) {

    using namespace NCodecs;

    template <bool delta>
    void PFor(const TVector<ui32> &v1) {
        const char *begin = (const char *)(&v1[0]);
        const char *end = begin + sizeof(v1[0]) * v1.size();
        TStringBuf in(begin, end);
        TBuffer buf;
        TPForCodec<ui32, delta> codec;
        codec.Encode(in, buf);
        STR << "v size: " << sizeof(v1[0]) * v1.size() << "; encoded size: " << buf.Size() << "\n";
    }

    void Delta(const TVector<ui32> &v1) {
        const char *begin = (const char *)(&v1[0]);
        const char *end = begin + sizeof(v1[0]) * v1.size();
        TStringBuf in(begin, end);
        TBuffer buf;
        TDeltaCodec<ui32> codec;
        codec.Encode(in, buf);
        STR << "v size: " << sizeof(v1[0]) * v1.size() << "; encoded size: " << buf.Size() << "\n";
    }

    void RunLength(const TVector<ui32> &v1) {
        const char *begin = (const char *)(&v1[0]);
        const char *end = begin + sizeof(v1[0]) * v1.size();
        TStringBuf in(begin, end);
        TBuffer buf;
        TRunLengthCodec<ui32> codec;
        codec.Encode(in, buf);
        STR << "v size: " << sizeof(v1[0]) * v1.size() << "; encoded size: " << buf.Size() << "\n";
    }

    Y_UNIT_TEST(Basic) {
        // logoblobs
        TVector<ui32> v1 {20, 21, 22, 23, 24, 15, 19, 16, 17, 18, 20, 78, 89, 90, 91, 93, 74, 68, 69};
        TVector<ui32> v2 {15, 16, 17, 19, 19, 20, 21, 22, 25, 26, 29, 25, 26, 45, 46, 47, 47, 49, 50};
        TVector<ui32> v3 {42, 42, 42, 42, 42, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66};
        TVector<ui32> v4 {66, 66, 66, 66, 66, 66, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42};

        STR << "======= PFOR DELTA ===========\n";
        PFor<true>(v1);
        PFor<true>(v2);
        PFor<true>(v3);
        PFor<true>(v4);
        STR << "======= PFOR =================\n";
        PFor<false>(v1);
        PFor<false>(v2);
        PFor<false>(v3);
        PFor<false>(v4);
        STR << "======= Delta ================\n";
        Delta(v1);
        Delta(v2);
        Delta(v3);
        Delta(v4);
    }

    Y_UNIT_TEST(NaturalNumbersAndZero) {
        TVector<ui32> v;
        const ui32 n = 100000;
        v.reserve(n);
        for (ui32 i = 0; i < n; i++)
            v.push_back(i);

        Shuffle(v.begin(), v.end());

        STR << "======= PFOR DELTA ===========\n";
        PFor<true>(v);
        STR << "======= PFOR =================\n";
        PFor<false>(v);
        STR << "======= Delta ================\n";
        Delta(v);
    }

    Y_UNIT_TEST(LargeAndRepeated) {
        TVector<ui32> v;

        for (ui32 t = 0; t < 100; t++) {
            ui32 n = RandomNumber<ui32>();
            ui32 r = RandomNumber<ui32>(1000);
            for (ui32 i = 0; i < r; i++)
                v.push_back(n);
        }

        Sort(v.begin(), v.end());

        STR << "======= PFOR DELTA ===========\n";
        PFor<true>(v);
        STR << "======= PFOR =================\n";
        PFor<false>(v);
        STR << "======= Delta ================\n";
        Delta(v);
        STR << "======= Run Length ===========\n";
        RunLength(v);
    }

}

