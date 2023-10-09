#include <ydb/core/blobstorage/crypto/crypto.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/cputimer.h>
#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/random/fast.h>
#include <util/stream/format.h>
#include <util/string/printf.h>

#include <numeric>

#include <ydb/core/blobstorage/crypto/ut/ut_helpers.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBlobStorageCryptoRope) {

constexpr size_t ENCIPHER_ALIGN = 16;

TRope RopeUnitialized(size_t size, size_t chunkSize, size_t chunkAlign = ENCIPHER_ALIGN) {
    Y_ABORT_UNLESS(chunkSize > 0);
    TRope rope;
    for (size_t i = 0; i < size / chunkSize; i++) {
        auto item = MakeIntrusive<TRopeAlignedBufferBackend>(chunkSize, chunkAlign);
        rope.Insert(rope.End(), TRope(item));
    }
    size_t tail = size % chunkSize;
    if (tail) {
        auto item = MakeIntrusive<TRopeAlignedBufferBackend>(tail, chunkAlign);
        rope.Insert(rope.End(), TRope(item));
    }
    return rope;
}

TRope TestingRope(char *inBuf, size_t size, size_t chunkSize, size_t chunkAlign = ENCIPHER_ALIGN) {
    TRope rope = RopeUnitialized(size, chunkSize, chunkAlign);
    TRopeUtils::Memcpy(rope.Begin(), inBuf, size);
    return rope;
}

template<size_t SIZE>
TRope TestingRope(size_t chunkSize, size_t chunkAlign = ENCIPHER_ALIGN) {
    alignas(16) ui8 in[SIZE];
    for (ui32 i = 0; i < SIZE; ++i) {
        in[i] = (ui8)i;
    }
    return TestingRope(reinterpret_cast<char *>(in), SIZE, chunkSize, chunkAlign);
}

// FIXME: ? check if distance between iterator and rope.End() >= size
void CompareRopes(TRope::TIterator lhsBegin, TRope::TIterator rhsBegin, size_t size) {
    TString lhs = TString::Uninitialized(size);
    lhsBegin.ExtractPlainDataAndAdvance(lhs.Detach(), size);
    TString rhs = TString::Uninitialized(size);
    rhsBegin.ExtractPlainDataAndAdvance(rhs.Detach(), size);
    UNIT_ASSERT_ARRAYS_EQUAL(lhs.data(), rhs.data(), size);
}

void CompareRopeAndBuf(TRope::TIterator ropeBegin, char* buf, size_t size) {
    TString ropeBuf = TString::Uninitialized(size);
    TRopeUtils::Memcpy(ropeBuf.Detach(), ropeBegin, size);
    UNIT_ASSERT_ARRAYS_EQUAL(ropeBuf.data(), buf, size);
}

/////////////////////////////////////////////////////////////////////////
// Equality with old code tests
/////////////////////////////////////////////////////////////////////////

    /*Y_UNIT_TEST(TestEqualStreamCypherEncryptZeroes) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;

        ui64 key = 1;
        ui64 nonce = 1;
        cypher1.SetKey(key);
        cypher2.SetKey(key);

        constexpr size_t SIZE = 5000;
        alignas(16) ui8 in[SIZE];
        alignas(16) ui8 out[SIZE];
        for (ui32 i = 0; i < SIZE; ++i) {
            in[i] = (ui8)i;
        }

        for (size_t chunkSize : {13ul, 113ul, 1001ul, SIZE, 3 * SIZE}) {
            TRope outRope = RopeUnitialized(SIZE, chunkSize);

            for (ui32 size = 0; size < SIZE; ++size) {
                cypher1.StartMessage(nonce, 0);
                cypher2.StartMessage(nonce, 0);

                cypher1.EncryptZeroes(out, size);
                cypher2.EncryptZeroes(outRope.Begin(), size);

                CompareRopeAndBuf(outRope.Begin(), reinterpret_cast<char *>(out), size);
            }
        }
    }*/

    Y_UNIT_TEST(TestEqualInplaceStreamCypher) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;

        ui64 key = 1;
        ui64 nonce = 1;
        cypher1.SetKey(key);
        cypher2.SetKey(key);

        constexpr size_t SIZE = 5000;

        for (size_t chunkSize : {13ul, 113ul, 1001ul, SIZE, 3 * SIZE}) {
            alignas(16) ui8 out[SIZE];
            for (ui32 i = 0; i < SIZE; ++i) {
                out[i] = (ui8)i;
            }

            TRope outRope = TestingRope(reinterpret_cast<char *>(out), SIZE, chunkSize);

            for (ui32 size = 0; size < SIZE; ++size) {
                cypher1.StartMessage(nonce, 0);
                cypher2.StartMessage(nonce, 0);

                cypher1.InplaceEncrypt(out, size);
                cypher2.InplaceEncrypt(outRope.Begin(), size);

                CompareRopeAndBuf(outRope.Begin(), reinterpret_cast<char *>(out), size);
            }
        }
    }

    Y_UNIT_TEST(TestEqualMixedStreamCypher) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;

        constexpr size_t SIZE = 5000;

        ui64 key = 1;
        ui64 nonce = 1;
        cypher1.SetKey(key);
        cypher2.SetKey(key);

        alignas(16) ui8 in[SIZE];
        for (ui32 i = 0; i < SIZE; ++i) {
            in[i] = (ui8)i;
        }

        for (size_t inChunkSize : {13ul, 113ul, SIZE, 3 * SIZE}) {
            for (size_t outChunkSize : {13ul, 113ul, SIZE, 3 * SIZE}) {
                TRope inRope = TestingRope<SIZE>(inChunkSize);
                TRope outRope = RopeUnitialized(SIZE, outChunkSize);
                alignas(16) ui8 out[SIZE];

                for (ui32 size = 1; size < SIZE; ++size) {
                    ui32 in_offset = size / 7;
                    cypher1.StartMessage(nonce, 0);
                    cypher2.StartMessage(nonce, 0);
                    ui32 size1 = (size - in_offset) % 257;
                    ui32 size2 = (size - in_offset - size1) % 263;
                    ui32 size3 = size - size1 - size2 - in_offset;

                    cypher1.Encrypt(outRope.Begin(), inRope.Begin() + in_offset, size1);
                    cypher1.Encrypt(outRope.Begin() + size1, inRope.Begin() + in_offset + size1, size2);
                    cypher1.Encrypt(outRope.Begin() + size1 + size2, inRope.Begin() + in_offset + size1 + size2, size3);

                    cypher2.Encrypt(out, in + in_offset, size1);
                    cypher2.Encrypt(out + size1, in + in_offset + size1, size2);
                    cypher2.Encrypt(out + size1 + size2, in + in_offset + size1 + size2, size3);

                    CompareRopeAndBuf(outRope.Begin(), reinterpret_cast<char *>(out), size1 + size2 + size3);
                }
            }
        }
    }

/////////////////////////////////////////////////////////////////////////
// Adapted old tests
/////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST(TestMixedStreamCypher) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;

        ui64 key = 1;
        ui64 nonce = 1;
        cypher1.SetKey(key);

        constexpr size_t SIZE = 5000;
        size_t chunkSize = 1000;
        TRope in = TestingRope<SIZE>(chunkSize);
        TRope out = RopeUnitialized(SIZE, chunkSize);

        for (ui32 size = 1; size < SIZE; ++size) {
            ui32 in_offset = size / 7;
            cypher1.StartMessage(nonce, 0);
            ui32 size1 = (size - in_offset) % 257;
            ui32 size2 = (size - in_offset - size1) % 263;
            ui32 size3 = size - size1 - size2 - in_offset;

            cypher1.Encrypt(out.Begin(), in.Begin() + in_offset, size1);
            cypher1.Encrypt(out.Begin() + size1, in.Begin() + in_offset + size1, size2);
            cypher1.Encrypt(out.Begin() + size1 + size2, in.Begin() + in_offset + size1 + size2, size3);

            cypher2.SetKey(key);
            cypher2.StartMessage(nonce, 0);
            cypher2.InplaceEncrypt(out.Begin(), size - in_offset);

            CompareRopes(in.Begin() + in_offset, out.Begin(), size - in_offset);
        }
    }

    Y_UNIT_TEST(TestOffsetStreamCypher) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;

        ui64 key = 1;
        ui64 nonce = 1;
        cypher1.SetKey(key);

        constexpr size_t SIZE = 5000;
        size_t chunkSize = 1000;
        TRope in = TestingRope<SIZE>(chunkSize);
        TRope out = RopeUnitialized(SIZE, chunkSize);

        for (ui32 size = 1; size < SIZE; ++size) {
            ui32 in_offset = size / 7;
            ui32 size1 = (size - in_offset) % 257;
            ui32 size2 = (size - in_offset - size1) % 263;
            ui32 size3 = size - size1 - size2 - in_offset;
            cypher1.StartMessage(nonce, 0);
            cypher1.Encrypt(out.Begin(), in.Begin() + in_offset, size1);
            cypher1.StartMessage(nonce, size1);
            cypher1.Encrypt(out.Begin() + size1, in.Begin() + in_offset + size1, size2);
            cypher1.StartMessage(nonce, size1 + size2);
            cypher1.Encrypt(out.Begin() + size1 + size2, in.Begin() + in_offset + size1 + size2, size3);

            cypher2.SetKey(key);
            cypher2.StartMessage(nonce, 0);
            cypher2.InplaceEncrypt(out.Begin(), size - in_offset);

            CompareRopes(in.Begin() + in_offset, out.Begin(), size - in_offset);
        }
    }

    Y_UNIT_TEST(TestInplaceStreamCypher) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;

        ui64 key = 1;
        ui64 nonce = 1;

        constexpr size_t SIZE = 5000;
        size_t chunkSize = 1000;
        TRope in = TestingRope<SIZE>(chunkSize);
        TRope out = RopeUnitialized(SIZE, chunkSize);

        for (ui32 size = 1; size < SIZE; ++size) {
            cypher1.SetKey(key);
            cypher1.StartMessage(nonce, 0);
            cypher1.InplaceEncrypt(in.Begin(), size);

            TRopeUtils::Memcpy(out.Begin(), in.Begin(), size);

            cypher2.SetKey(key);
            cypher2.StartMessage(nonce, 0);
            cypher2.InplaceEncrypt(out.Begin(), size);

            in = TestingRope<SIZE>(chunkSize);

            CompareRopes(in.Begin(), out.Begin(), size);
        }
    }

    Y_UNIT_TEST(PerfTestStreamCypher) {
        TStreamCypher cypher1;
        constexpr size_t BUF_SIZE = 256 << 10;
        constexpr size_t BUF_ALIGN = 32;
        constexpr size_t REPETITIONS = 16;
        size_t chunkSize = 2 << 20;

        Cout << Endl;

        auto testCase = {std::make_pair(0,0), {4, 0}, {8, 0}, {0, 4}, {0, 8}, {4, 8}, {8, 8}};
        for (auto s : testCase) {
            size_t inShift = s.first;
            size_t outShift = s.second;
            const size_t size = BUF_SIZE;

            Cout << "size# " << HumanReadableSize(size, SF_BYTES);
            Cout << " inShift# " << LeftPad(inShift, 2);
            Cout << " outShift# " << LeftPad(outShift, 2);

            TVector<TDuration> times;
            times.reserve(REPETITIONS);

            for (ui32 i = 0; i < REPETITIONS; ++i) {
                TAlignedBuf inBuf(BUF_SIZE, BUF_ALIGN);
                for (ui32 i = 0; i < size; ++i) {
                    inBuf.Data()[i] = (ui8)i;
                }

                TRope inRope = TestingRope(reinterpret_cast<char*>(inBuf.Data()), BUF_SIZE, chunkSize, BUF_ALIGN);
                TRope outRope = RopeUnitialized(BUF_SIZE, chunkSize, BUF_ALIGN);

                ui64 key = 123;
                ui64 nonce = 1;
                cypher1.SetKey(key);
                cypher1.StartMessage(nonce, 0);

                TSimpleTimer timer;
                cypher1.Encrypt(outRope.Begin() + outShift, inRope.Begin() + inShift, size - BUF_ALIGN);
                times.push_back(timer.Get());
            }
            TDuration min_time = *std::min_element(times.begin(), times.end());
            Cout << " max_speed# " << HumanReadableSize(size / min_time.SecondsFloat(), SF_QUANTITY) << "/s";
            TDuration avg_time = std::accumulate(times.begin(), times.end(), TDuration()) / times.size();
            Cout << " avg_speed# " << HumanReadableSize(size / avg_time.SecondsFloat(), SF_QUANTITY) << "/s";
            Cout << Endl;
        }
    }

    Y_UNIT_TEST(UnalignedTestStreamCypher) {
        constexpr size_t BUF_ALIGN = 8;
        size_t chunkSize = 1000;

        TStreamCypher cypher;

        for (size_t size = 151; size < 6923; size = 2*size + 1) {
            auto testCase = {std::make_pair(0,0), {8, 0}};
            for (auto s : testCase) {
                size_t inShift = s.first;

                Cout << " inShift# " << LeftPad(inShift, 2) << " ";

                TAlignedBuf inBuf(size, BUF_ALIGN);
                ui8 *in = inBuf.Data() + inShift;
                TReallyFastRng32 rng(692);
                for (ui32 i = 0; i < size; ++i) {
                    in[i] = rng.GenRand() % 256;
                }

                TRope inRope = TestingRope(reinterpret_cast<char*>(in), size, chunkSize, BUF_ALIGN);
                TRope outRope = RopeUnitialized(size, chunkSize, BUF_ALIGN);

                ui64 key = 123;
                ui64 nonce = 1;
                cypher.SetKey(key);
                cypher.StartMessage(nonce, 0);

                cypher.Encrypt(outRope.Begin(), inRope.Begin() + inShift, size - inShift);
            }
        }
        Cout << Endl;
    }

}
} // namespace NKikimr
