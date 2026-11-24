#include "defs.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_crypto.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBlobStoragePDiskCrypto) {

    Y_UNIT_TEST(TestMixedSectorHashFormats) {
        ui8 sector[4096];
        for (size_t i = 0; i < sizeof(sector); ++i) {
            sector[i] = i;
        }
        const ui64 offset = 16384;
        const ui64 magic = 0x123456789abcdef0ULL;
        const TLogoBlobID blobId(1, 2, 3, 0, 4096, 4);
        THashCalculator legacy;
        legacy.Hash(&offset, sizeof(offset));
        legacy.Hash(&magic, sizeof(magic));
        legacy.Hash(sector, sizeof(sector) - sizeof(ui64));
        const ui64 oldHash = legacy.GetHashResult();
        NPDisk::TPDiskHashCalculator writer;
        const ui64 ordinaryHash = writer.HashSector(offset, magic, sector, sizeof(sector), {});
        const ui64 saltedHash = writer.HashSector(offset, magic, sector, sizeof(sector), blobId);
        NPDisk::TPDiskHashCalculator oldWriter(false);
        UNIT_ASSERT_VALUES_EQUAL(oldWriter.HashSector(offset, magic, sector, sizeof(sector), {}), oldHash);
        NPDisk::TPDiskHashCalculator reader;
        // Exercise every cached mode and switching back to Poly1305 after t1ha.
        for (ui64 hash : {ordinaryHash, ordinaryHash, saltedHash, saltedHash, oldHash, oldHash,
                ordinaryHash, oldHash, saltedHash}) {
            UNIT_ASSERT(reader.CheckSectorHash(offset, magic, sector, sizeof(sector), hash, blobId));
        }
        sector[42] ^= 1;
        for (ui64 hash : {ordinaryHash, saltedHash, oldHash}) {
            UNIT_ASSERT(!reader.CheckSectorHash(offset, magic, sector, sizeof(sector), hash, blobId));
        }
    }
    Y_UNIT_TEST(TestMixedStreamCypher) {
        for (ui32 enableEnctyption = 0; enableEnctyption < 2; ++enableEnctyption) {
            NPDisk::TPDiskStreamCypher cypher1(enableEnctyption);
            NPDisk::TPDiskStreamCypher cypher2(enableEnctyption);
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
                cypher1.StartMessage(nonce);
                ui32 size1 = (size - in_offset) % 257;
                ui32 size2 = (size - in_offset - size1) % 263;
                ui32 size3 = size - size1 - size2 - in_offset;
                cypher1.Encrypt(out, in + in_offset, size1);
                cypher1.Encrypt(out + size1, in + in_offset + size1, size2);
                cypher1.Encrypt(out + size1 + size2, in + in_offset + size1 + size2, size3);

                cypher2.SetKey(key);
                cypher2.StartMessage(nonce);
                cypher2.InplaceEncrypt(out, size - in_offset);

                for (ui32 i = 0; i < size - in_offset; ++i) {
                    UNIT_ASSERT(in[i + in_offset] == out[i]);
                }
            }
        }
    }

    Y_UNIT_TEST(TestInplaceStreamCypher) {
        for (ui32 enableEnctyption = 0; enableEnctyption < 2; ++enableEnctyption) {
            NPDisk::TPDiskStreamCypher cypher1(enableEnctyption);
            NPDisk::TPDiskStreamCypher cypher2(enableEnctyption);
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
                cypher1.StartMessage(nonce);
                cypher1.InplaceEncrypt(in, size);

                memcpy(out, in, size);

                cypher2.SetKey(key);
                cypher2.StartMessage(nonce);
                cypher2.InplaceEncrypt(out, size);

                for (ui32 i = 0; i < SIZE; ++i) {
                    in[i] = (ui8)i;
                }

                for (ui32 i = 0; i < size; ++i) {
                    UNIT_ASSERT_C(in[i] == out[i], "Mismatch at " << i << " of " << size << Endl);
                }
            }
        }
    }
}

} // namespace NKikimr

