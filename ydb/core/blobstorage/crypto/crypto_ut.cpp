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

Y_UNIT_TEST_SUITE(TBlobStorageCrypto) {

    Y_UNIT_TEST(TestMixedStreamCypher) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;
        constexpr int SIZE = 5000;
        alignas(16) ui8 in[SIZE];
        alignas(16) ui8 out[SIZE];
        for (ui32 i = 0; i < SIZE; ++i) {
            in[i] = (ui8)i;
        }

        ui64 key = 1;
        ui64 nonce = 1;
        cypher1.SetKey(key);

        for (ui32 size = 1; size < SIZE; ++size) {
            ui32 in_offset = size / 7;
            cypher1.StartMessage(nonce, 0);
            ui32 size1 = (size - in_offset) % 257;
            ui32 size2 = (size - in_offset - size1) % 263;
            ui32 size3 = size - size1 - size2 - in_offset;
            cypher1.Encrypt(out, in + in_offset, size1);
            cypher1.Encrypt(out + size1, in + in_offset + size1, size2);
            cypher1.Encrypt(out + size1 + size2, in + in_offset + size1 + size2, size3);

            cypher2.SetKey(key);
            cypher2.StartMessage(nonce, 0);
            cypher2.InplaceEncrypt(out, size - in_offset);

            UNIT_ASSERT_ARRAYS_EQUAL(in + in_offset, out, size - in_offset);
        }
    }

    Y_UNIT_TEST(TestOffsetStreamCypher) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;
        constexpr int SIZE = 5000;
        alignas(16) ui8 in[SIZE];
        alignas(16) ui8 out[SIZE];
        for (ui32 i = 0; i < SIZE; ++i) {
            in[i] = (ui8)i;
        }

        ui64 key = 1;
        ui64 nonce = 1;
        cypher1.SetKey(key);

        for (ui32 size = 1; size < SIZE; ++size) {
            ui32 in_offset = size / 7;
            ui32 size1 = (size - in_offset) % 257;
            ui32 size2 = (size - in_offset - size1) % 263;
            ui32 size3 = size - size1 - size2 - in_offset;
            cypher1.StartMessage(nonce, 0);
            cypher1.Encrypt(out, in + in_offset, size1);
            cypher1.StartMessage(nonce, size1);
            cypher1.Encrypt(out + size1, in + in_offset + size1, size2);
            cypher1.StartMessage(nonce, size1 + size2);
            cypher1.Encrypt(out + size1 + size2, in + in_offset + size1 + size2, size3);

            cypher2.SetKey(key);
            cypher2.StartMessage(nonce, 0);
            cypher2.InplaceEncrypt(out, size - in_offset);

            for (ui32 i = 0; i < size - in_offset; ++i) {
                UNIT_ASSERT(in[i + in_offset] == out[i]);
            }
        }
    }

    Y_UNIT_TEST(TestInplaceStreamCypher) {
        TStreamCypher cypher1;
        TStreamCypher cypher2;
        constexpr int SIZE = 5000;
        ui8 in[SIZE];
        ui8 out[SIZE];
        for (ui32 i = 0; i < SIZE; ++i) {
            in[i] = (ui8)i;
        }

        ui64 key = 1;
        ui64 nonce = 1;

        for (ui32 size = 1; size < SIZE; ++size) {
            cypher1.SetKey(key);
            cypher1.StartMessage(nonce, 0);
            cypher1.InplaceEncrypt(in, size);

            memcpy(out, in, size);

            cypher2.SetKey(key);
            cypher2.StartMessage(nonce, 0);
            cypher2.InplaceEncrypt(out, size);

            for (ui32 i = 0; i < SIZE; ++i) {
                in[i] = (ui8)i;
            }

            for (ui32 i = 0; i < size; ++i) {
                UNIT_ASSERT_C(in[i] == out[i], "Mismatch at " << i << " of " << size << Endl);
            }
        }
    }

    Y_UNIT_TEST(PerfTestStreamCypher) {
        TStreamCypher cypher1;
        constexpr size_t BUF_SIZE = 256 << 10;
        constexpr size_t BUF_ALIGN = 32;
        constexpr size_t REPETITIONS = 16;

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
                TAlignedBuf outBuf(BUF_SIZE, BUF_ALIGN);

                ui8 *in = inBuf.Data() + inShift;
                ui8 *out = outBuf.Data() + outShift;
                for (ui32 i = 0; i < size; ++i) {
                    inBuf.Data()[i] = (ui8)i;
                }
                ui64 key = 123;
                ui64 nonce = 1;
                cypher1.SetKey(key);

                cypher1.StartMessage(nonce, 0);

                TSimpleTimer timer;
                cypher1.Encrypt(out, in, size);
                times.push_back(timer.Get());
            }
            TDuration min_time = *std::min_element(times.begin(), times.end());
            Cout << " max_speed# " << HumanReadableSize(size / min_time.SecondsFloat(), SF_QUANTITY) << "/s";
            TDuration avg_time = std::accumulate(times.begin(), times.end(), TDuration()) / times.size();
            Cout << " avg_speed# " << HumanReadableSize(size / avg_time.SecondsFloat(), SF_QUANTITY) << "/s";
            Cout << Endl;
        }
    }


void Test(const ui8* a, const ui8* b, size_t size) {
    for (ui32 i = 0; i < size; ++i) {
        UNIT_ASSERT_EQUAL_C(a[i], b[i],
            " a[" << i << "]# " << Hex(a[i], HF_FULL) << " != "
            " b[" << i << "]# " << Hex(b[i], HF_FULL));
    }
}

    Y_UNIT_TEST(UnalignedTestStreamCypher) {
        constexpr size_t BUF_ALIGN = 8;

        TStreamCypher cypher;

        for (size_t size = 151; size < 6923; size = 2*size + 1) {
            auto testCase = {std::make_pair(0,0), {8, 0}};
            for (auto s : testCase) {
                size_t inShift = s.first;

                Cout << " inShift# " << LeftPad(inShift, 2) << " ";

                TAlignedBuf inBuf(size, BUF_ALIGN);
                TAlignedBuf outBuf(size, BUF_ALIGN);

                ui8 *in = inBuf.Data() + inShift;
                ui8 *out = outBuf.Data();
                TReallyFastRng32 rng(692);
                for (ui32 i = 0; i < size; ++i) {
                    in[i] = rng.GenRand() % 256;
                }
                ui64 key = 123;
                ui64 nonce = 1;
                cypher.SetKey(key);
                cypher.StartMessage(nonce, 0);

                cypher.Encrypt(out, in, size);
            }
        }
    }
}

Y_UNIT_TEST_SUITE(TTest_t1ha) {
    template<class THasher>
    void TestZeroInputHashIsNotZeroImpl() {
        THasher hasher;
        ui8 zeros[128] = {0};
        for (ui32 i = 0; i < 12345; i += 97) {
            hasher.SetKey(i);
            UNIT_ASSERT_UNEQUAL(hasher.Hash(zeros, sizeof zeros), 0);
        }
    }

    Y_UNIT_TEST(TestZeroInputHashIsNotZero) {
        TestZeroInputHashIsNotZeroImpl<TT1ha0NoAvxHasher>();
        TestZeroInputHashIsNotZeroImpl<TT1ha0AvxHasher>();
        TestZeroInputHashIsNotZeroImpl<TT1ha0Avx2Hasher>();
    }

    template<class THasher>
    void PerfTestImpl() {
        THasher hasher;
        constexpr size_t BUF_SIZE = 256 << 10;
        constexpr size_t BUF_ALIGN = 32;
        constexpr size_t REPETITIONS = 16;

        auto testCase = {0, 1, 2, 4, 8};
        for (size_t inShift : testCase) {
            const size_t size = BUF_SIZE;

            Cout << "size# " << HumanReadableSize(size, SF_BYTES);
            Cout << " inShift# " << LeftPad(inShift, 2);

            TVector<TDuration> times;
            times.reserve(REPETITIONS);

            for (ui32 i = 0; i < REPETITIONS; ++i) {
                TAlignedBuf inBuf(BUF_SIZE, BUF_ALIGN);

                ui8 *in = inBuf.Data() + inShift;
                for (ui32 i = 0; i < size; ++i) {
                    inBuf.Data()[i] = (ui8)i;
                }
                ui64 key = 123;
                hasher.SetKey(key);

                TSimpleTimer timer;
                hasher.Hash(in, size);
                times.push_back(timer.Get());
            }
            TDuration min_time = *std::min_element(times.begin(), times.end());
            Cout << " max_speed# " << HumanReadableSize(size / min_time.SecondsFloat(), SF_QUANTITY) << "/s";
            TDuration avg_time = std::accumulate(times.begin(), times.end(), TDuration()) / times.size();
            Cout << " avg_speed# " << HumanReadableSize(size / avg_time.SecondsFloat(), SF_QUANTITY) << "/s";
            Cout << Endl;
        }
    }

    Y_UNIT_TEST(PerfTest) {
        PerfTestImpl<TT1ha0NoAvxHasher>();
        PerfTestImpl<TT1ha0AvxHasher>();
        PerfTestImpl<TT1ha0Avx2Hasher>();
    }

static const size_t REFERENCE_BUF_SIZE = 256 << 10;
static const ui64 REFERENCE_KEY = 123;
static const ui64 NoAvxReferenceHash[] = {
    3857587077012991658ull, 14549119052884897871ull, 254398647225044890ull, 16058200316769016579ull,
    2181725451308207419ull, 3279780031906669142ull, 2103619312639464077ull, 822922730093285578ull,
    8753250818825642536ull, 11319388241306168379ull, 220099229643599001ull, 8504415541883480728ull,
    8223470624549234967ull, 16994463204673144995ull, 17432852776700040881ull, 7799421780457361217ull,
    17218742319902176397ull, 16967740127583990941ull
};
static const ui64 Avx2ReferenceHash[] = {
    17755040588294046276ull, 446858626829897371ull, 16828903878074513235ull, 12657333507435006451ull,
    17541517186803958748ull, 6742999295364335038ull, 12165123664998125067ull, 12836101758180356638ull,
    15902773892737007852ull, 12440249596693842423ull, 1730928272460384897ull, 5176224758215524594ull,
    8223470624549234967ull, 16994463204673144995ull, 17432852776700040881ull, 7799421780457361217ull,
    17218742319902176397ull, 16967740127583990941ull
};

    void PrepareBuf(TAlignedBuf *buf) {
        ui64 *data64 = reinterpret_cast<ui64*>(buf->Data());
        UNIT_ASSERT(buf->Size() % sizeof(ui64) == 0);
        const ui64 bufSize64 = buf->Size() / sizeof(ui64);
        for (ui64 i = 0; i < bufSize64; ++i) {
            data64[i] = (i + REFERENCE_KEY) * 6364136223846793005ull + 1442695040888963407ull;
        }
    }

    template<class THasher>
    void T1haHashResultsStablilityTestImpl(const TAlignedBuf& buf) {
        ui32 offset = 0;
        ui32 resIdx = 0;
        THasher hasher;
        hasher.SetKey(REFERENCE_KEY);
        for (ui32 size = buf.Size() / 2; size >= 8; size /= 2) {
            UNIT_ASSERT(offset + size <= buf.Size());
            const ui64 hashRes = hasher.Hash(buf.Data() + offset, size);
            if constexpr (std::is_same_v<THasher, TT1ha0AvxHasher> || std::is_same_v<THasher, TT1ha0NoAvxHasher>) {
                UNIT_ASSERT_EQUAL(hashRes, NoAvxReferenceHash[resIdx]);
            } else if constexpr (std::is_same_v<THasher, TT1ha0Avx2Hasher>) {
                UNIT_ASSERT_EQUAL(hashRes, Avx2ReferenceHash[resIdx]);
            } else {
                UNIT_ASSERT(false);
            }
            ++resIdx;
            offset += size;
        }
    }

    Y_UNIT_TEST(T1haHashResultsStablilityTest) {
        TAlignedBuf buf(REFERENCE_BUF_SIZE, 32);
        PrepareBuf(&buf);
        T1haHashResultsStablilityTestImpl<TT1ha0NoAvxHasher>(buf);
        T1haHashResultsStablilityTestImpl<TT1ha0AvxHasher>(buf);
        T1haHashResultsStablilityTestImpl<TT1ha0Avx2Hasher>(buf);
    }
}
} // namespace NKikimr
