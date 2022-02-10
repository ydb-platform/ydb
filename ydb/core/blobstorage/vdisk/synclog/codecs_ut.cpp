#include "codecs.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <util/stream/null.h>

using namespace NCodecs;

#define STR Cnull

template <class TNumber, class TCodec>
void CheckCodec(const TVector<TNumber> &v, TCodecPtr codec = new TCodec) {
    const char *begin = (const char *)(&v[0]);
    const char *end = begin + sizeof(v[0]) * v.size();
    TStringBuf src(begin, end);

    TBuffer encoded;
    codec->Encode(src, encoded);
    STR << "v size: " << sizeof(v[0]) * v.size() << "; encoded size: " << encoded.Size() << "\n";

    TBuffer decoded;
    TStringBuf dst(encoded.Begin(), encoded.End());
    codec->Decode(dst, decoded);

    TStringBuf res(decoded.Begin(), decoded.End());

    UNIT_ASSERT(src.size() == res.size());
    UNIT_ASSERT(memcmp(src.data(), res.data(), src.size()) == 0);
}

Y_UNIT_TEST_SUITE(VarLengthIntCodec) {

    template <class TNumber>
    void VarLength(const TVector<TNumber> &v) {
        CheckCodec<TNumber, TVarLengthIntCodec<TNumber>>(v);
    }

    template <class TNumber>
    void BasicTest() {
        TVector<TNumber> v {67, 1974, 7, 0, 7283647, 2973};
        VarLength<TNumber>(v);
    }

    template <class TNumber>
    void Random() {
        TVector<TNumber> v;
        for (size_t i = 0; i < 1000000; i++)
            v.push_back(RandomNumber<ui32>()); // ui32 -- small numbers are intentionally

        VarLength<TNumber>(v);
    }

    Y_UNIT_TEST(BasicTest32) {
        BasicTest<ui32>();
    }

    Y_UNIT_TEST(BasicTest64) {
        BasicTest<ui64>();
    }

    Y_UNIT_TEST(Random32) {
        Random<ui32>();
    }

    Y_UNIT_TEST(Random64) {
        Random<ui32>();
    }
}

Y_UNIT_TEST_SUITE(RunLengthCodec) {

    template <class TNumber>
    void RunLength(const TVector<TNumber> &v) {
        CheckCodec<TNumber, TRunLengthCodec<TNumber>>(v);
    }

    template <class TNumber>
    void BasicTest() {
        TVector<TNumber> v {5, 5, 5, 5, 32, 1974, 7, 7, 7, 0, 7283647, 7283647, 7283647, 7283647, 7283647};
        RunLength<TNumber>(v);
    }

    template <class TNumber>
    void Random() {
        TVector<TNumber> v;
        for (size_t i = 0; i < 1000000; i++)
            v.push_back(RandomNumber<TNumber>());

        RunLength<TNumber>(v);
    }

    Y_UNIT_TEST(BasicTest32) {
        BasicTest<ui32>();
    }

    Y_UNIT_TEST(BasicTest64) {
        BasicTest<ui64>();
    }

    Y_UNIT_TEST(Random32) {
        Random<ui32>();
    }

    Y_UNIT_TEST(Random64) {
        Random<ui32>();
    }
}

Y_UNIT_TEST_SUITE(SemiSortedDeltaCodec) {

    template <class TNumber>
    void SemiSortedDelta(const TVector<TNumber> &v) {
        CheckCodec<TNumber, TSemiSortedDeltaCodec<TNumber>>(v);
    }

    template <class TNumber>
    void BasicTest() {
        TVector<TNumber> v {5, 6, 6, 8, 9, 11, 18, 7, 7, 8, 10, 34, 35, 36, 20, 21, 23, 26};
        SemiSortedDelta<TNumber>(v);
    }

    template <class TNumber>
    void Random() {
        TVector<TNumber> v;
        for (size_t i = 0; i < 1000000; i++)
            v.push_back(RandomNumber<TNumber>());

        SemiSortedDelta<TNumber>(v);
    }

    Y_UNIT_TEST(BasicTest32) {
        BasicTest<ui32>();
    }

    Y_UNIT_TEST(BasicTest64) {
        BasicTest<ui64>();
    }

    Y_UNIT_TEST(Random32) {
        Random<ui32>();
    }

    Y_UNIT_TEST(Random64) {
        Random<ui32>();
    }
}

Y_UNIT_TEST_SUITE(SemiSortedDeltaAndVarLengthCodec) {

    template <class TNumber>
    void SemiSortedDeltaAndVarLength(const TVector<TNumber> &v) {
        TCodecPtr p1(new TSemiSortedDeltaCodec<TNumber>);
        TCodecPtr p2(new TVarLengthIntCodec<TNumber>);
        TCodecPtr codec(new TPipelineCodec(p1, p2));
        CheckCodec<TNumber, TPipelineCodec>(v, codec);
    }

    template <class TNumber>
    void BasicTest() {
        TVector<TNumber> v {5, 6, 6, 8, 9, 11, 18, 7, 7, 8, 10, 34, 35, 36, 20, 21, 23, 26};
        SemiSortedDeltaAndVarLength<TNumber>(v);
    }

    template <class TNumber>
    void Random() {
        TVector<TNumber> v;
        for (size_t i = 0; i < 1000000; i++)
            v.push_back(RandomNumber<TNumber>());

        SemiSortedDeltaAndVarLength<TNumber>(v);
    }

    Y_UNIT_TEST(BasicTest32) {
        BasicTest<ui32>();
    }

    Y_UNIT_TEST(BasicTest64) {
        BasicTest<ui64>();
    }

    Y_UNIT_TEST(Random32) {
        Random<ui32>();
    }

    Y_UNIT_TEST(Random64) {
        Random<ui32>();
    }
}

