#include <util/random/fast.h>

#include "chacha_512.h"
#include "chacha_vec.h"
#include "secured_block.h"
#include <ydb/core/blobstorage/crypto/ut/ut_helpers.h>
#include <ydb/core/blobstorage/crypto/ut/chacha_test_vectors.h>


Y_UNIT_TEST_SUITE(TChaCha512)
{
#ifdef __AVX512F__
    void RunTest(int rounds, const ui8 key[KEY_SIZE], const ui8 iv[IV_SIZE],
            const ui8 expected[][DATA_SIZE])
    {
        ui8 data[DATA_SIZE] = { 0x00 };
        ui8 buf[DATA_SIZE];

        ChaCha512 cipher(rounds);
        cipher.SetIV(iv);
        cipher.SetKey(key, KEY_SIZE);

        cipher.Encipher(data, buf, sizeof(data));
        UNIT_ASSERT_ARRAYS_EQUAL(buf, expected[0], sizeof(buf));

        cipher.Encipher(data, buf, sizeof(data));
        UNIT_ASSERT_ARRAYS_EQUAL(buf, expected[1], sizeof(buf));
    }

    // TC1: All zero key and IV.
    Y_UNIT_TEST(KeystreamTest1) {
        RunTest(8, tc1_key, tc1_iv, tc1_expected_8);
        RunTest(12, tc1_key, tc1_iv, tc1_expected_12);
        RunTest(20, tc1_key, tc1_iv, tc1_expected_20);
    }

    // TC2: Single bit in key set. All zero IV
    Y_UNIT_TEST(KeystreamTest2) {
        RunTest(8, tc2_key, tc2_iv, tc2_expected_8);
        RunTest(12, tc2_key, tc2_iv, tc2_expected_12);
        RunTest(20, tc2_key, tc2_iv, tc2_expected_20);
    }

    // TC3: Single bit in IV set. All zero key
    Y_UNIT_TEST(KeystreamTest3) {
        RunTest(8, tc3_key, tc3_iv, tc3_expected_8);
        RunTest(12, tc3_key, tc3_iv, tc3_expected_12);
        RunTest(20, tc3_key, tc3_iv, tc3_expected_20);
    }

    // TC4: All bits in key and IV are set
    Y_UNIT_TEST(KeystreamTest4) {
        RunTest(8, tc4_key, tc4_iv, tc4_expected_8);
        RunTest(12, tc4_key, tc4_iv, tc4_expected_12);
        RunTest(20, tc4_key, tc4_iv, tc4_expected_20);
    }

    // TC5: Every even bit set in key and IV
    Y_UNIT_TEST(KeystreamTest5) {
        RunTest(8, tc5_key, tc5_iv, tc5_expected_8);
        RunTest(12, tc5_key, tc5_iv, tc5_expected_12);
        RunTest(20, tc5_key, tc5_iv, tc5_expected_20);
    }

    // TC6: Every odd bit set in key and IV
    Y_UNIT_TEST(KeystreamTest6) {
        RunTest(8, tc6_key, tc6_iv, tc6_expected_8);
        RunTest(12, tc6_key, tc6_iv, tc6_expected_12);
        RunTest(20, tc6_key, tc6_iv, tc6_expected_20);
    }

    // TC7: Sequence patterns in key and IV
    Y_UNIT_TEST(KeystreamTest7) {
        RunTest(8, tc7_key, tc7_iv, tc7_expected_8);
        RunTest(12, tc7_key, tc7_iv, tc7_expected_12);
        RunTest(20, tc7_key, tc7_iv, tc7_expected_20);
    }

    // TC8: key: 'All your base are belong to us!, IV: 'IETF2013'
    Y_UNIT_TEST(KeystreamTest8) {
        RunTest(8, tc8_key, tc8_iv, tc8_expected_8);
        RunTest(12, tc8_key, tc8_iv, tc8_expected_12);
        RunTest(20, tc8_key, tc8_iv, tc8_expected_20);
    }

    Y_UNIT_TEST(MultiEncipherOneDecipher) {
        TStringBuf lorem =
                "Lorem ipsum dolor sit amet, consectetur adipisicing elit, "
                "sed do eiusmod tempor incididunt ut labore et dolore magna "
                "aliqua. Ut enim ad minim veniam, quis nostrud exercitation "
                "ullamco laboris nisi ut aliquip ex ea commodo consequat. "
                "Duis aute irure dolor in reprehenderit in voluptate velit "
                "esse cillum dolore eu fugiat nulla pariatur. Excepteur sint "
                "occaecat cupidatat non proident, sunt in culpa qui officia "
                "deserunt mollit anim id est laborum.";

        TSecuredBlock<> buf(lorem.data(), lorem.size());

        ChaCha512 cipher;
        cipher.SetIV(tc8_iv);
        cipher.SetKey(tc8_key, KEY_SIZE);

        for (size_t i = 0; i < buf.Size(); ) {
            size_t len = Min(ChaCha512::BLOCK_SIZE, buf.Size() - i);
            cipher.Encipher(buf.Data() + i, buf.Data() + i, len);
            i += len;
        }

        UNIT_ASSERT_UNEQUAL(buf.AsStringBuf(), lorem);

        cipher.SetIV(tc8_iv);
        cipher.Encipher(buf, buf, buf.Size());

        UNIT_ASSERT_EQUAL(buf.AsStringBuf(), lorem);
    }

    Y_UNIT_TEST(SecondBlock) {
        TStringBuf plaintext =
                "1111111122222222333333334444444455555555666666667777777788888888"
                "qqqqqqqqwwwwwwwweeeeeeeerrrrrrrrttttttttyyyyyyyyuuuuuuuuiiiiiiii";

        TSecuredBlock<> buf(plaintext.data(), plaintext.size());

        ChaCha512 cipher;
        ui64 offset = 0;
        cipher.SetIV(tc8_iv, (ui8*)&offset);
        cipher.SetKey(tc8_key, KEY_SIZE);

        UNIT_ASSERT_EQUAL(ChaCha512::BLOCK_SIZE, 64);
        cipher.Encipher(buf.Data(), buf.Data(), ChaCha512::BLOCK_SIZE * 2);

        UNIT_ASSERT_UNEQUAL(buf.AsStringBuf(), plaintext);

        offset = 1;
        cipher.SetIV(tc8_iv, (ui8*)&offset);
        cipher.Encipher((ui8*)buf + 64, (ui8*)buf + 64, 64);

        for (size_t i = 64; i < 64 * 2; ++i) {
            UNIT_ASSERT_EQUAL(((ui8*)buf)[i], plaintext.data()[i]);
        }
    }

    Y_UNIT_TEST(CompatibilityTest) {
        ui64 offset = 0;

        ChaChaVec cipher1;
        cipher1.SetIV(tc8_iv, (ui8*)&offset);
        cipher1.SetKey(tc8_key, KEY_SIZE);

        ChaCha512 cipher2;
        cipher2.SetIV(tc8_iv, (ui8*)&offset);
        cipher2.SetKey(tc8_key, KEY_SIZE);

        TReallyFastRng32 rng(5124);
        UNIT_ASSERT_EQUAL(ChaCha512::BLOCK_SIZE, 64);
        for (size_t size = 51; size < 67953; size += 113) {
            TAlignedBuf bufOrig(size, 16);
            TAlignedBuf bufNew(size, 16);

            for (ui32 i = 0; i < size; ++i) {
                bufNew.Data()[i] = bufOrig.Data()[i] = (rng.GenRand() % 256);
            }
            cipher1.EncipherOld(bufOrig.Data(), bufOrig.Data(), size);
            cipher2.Encipher(bufNew.Data(), bufNew.Data(), size);
            UNIT_ASSERT_ARRAYS_EQUAL(bufOrig.Data(), bufNew.Data(), size);
        }

        for (size_t size = 51; size < 67953; size += 113) {
            TAlignedBuf bufOrig(size, 16);
            TAlignedBuf bufInNew(size, 8);
            TAlignedBuf bufOutNew(size, 16);

            for (ui32 i = 0; i < size; ++i) {
                (bufInNew.Data() + 8)[i] = bufOrig.Data()[i] = (rng.GenRand() % 256);
            }
            cipher1.EncipherOld(bufOrig.Data(), bufOrig.Data(), size);
            cipher2.Encipher(bufInNew.Data() + 8, bufOutNew.Data(), size);
            UNIT_ASSERT_ARRAYS_EQUAL(bufOrig.Data(), bufOutNew.Data(), size);
        }
    }
#endif
}
