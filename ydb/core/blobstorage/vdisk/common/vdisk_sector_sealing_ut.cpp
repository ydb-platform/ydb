#include "vdisk_sector_sealing.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_format_context.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TVDiskSectorSealing) {

    struct TFixture {
        NPDisk::TDiskFormat Format;
        std::shared_ptr<TPDiskCtx> PDiskCtx;
        static constexpr ui32 ChunkIdx = 7;
        static constexpr ui64 ChunkNonce = 5000;

        explicit TFixture(bool encrypt) {
            Format.Clear(encrypt);
            Format.ChunkKey = 0xFEDCBA9876543210ull;
            PDiskCtx = std::make_shared<TPDiskCtx>(nullptr, TActorId(), TString());
            PDiskCtx->FormatContext = std::make_shared<const NPDisk::TFormatContext>(Format, encrypt);
            PDiskCtx->ChunkNonces->Record({ChunkIdx}, {ChunkNonce});
            PDiskCtx->EnableSectorPreparation = TControlWrapper(1, 0, 1);
        }
    };

    // Several parts, one of them padding, spanning sector boundaries: every sector opens back to the payload.
    Y_UNIT_TEST(SealsMultiPartWrite) {
        for (const bool encrypt : {false, true}) {
            TFixture f(encrypt);
            const ui32 payloadSize = f.Format.SectorPayloadSize();
            const ui32 offset = 3 * payloadSize;

            TString a(payloadSize + 100, '\0'), b(2 * payloadSize - 300, '\0');
            for (size_t i = 0; i < a.size(); ++i) a[i] = char(i * 3 + 1);
            for (size_t i = 0; i < b.size(); ++i) b[i] = char(i * 5 + 2);
            const ui32 padding = 77;
            const NPDisk::TEvChunkWrite::TPart parts[] = {{a.data(), ui32(a.size())}, {nullptr, padding},
                {b.data(), ui32(b.size())}};
            TString expected = a + TString(padding, '\0') + b;

            NPDisk::TEvChunkWrite ev(1, 1, TFixture::ChunkIdx, offset,
                MakeIntrusive<NPDisk::TEvChunkWrite::TNonOwningParts>(parts, 3u), nullptr, true, 0);
            UNIT_ASSERT(PrepareChunkWriteSectors(ev, *f.PDiskCtx));
            UNIT_ASSERT(ev.PreformattedSectors);
            UNIT_ASSERT_VALUES_EQUAL(ev.FirstSectorNonce, TFixture::ChunkNonce + 3);
            UNIT_ASSERT_VALUES_EQUAL(ev.PartsPtr->Size(), 1u);
            const auto [data, size] = (*ev.PartsPtr)[0];
            UNIT_ASSERT_VALUES_EQUAL(size % f.Format.SectorSize, 0u);
            UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(data) % 4096, 0u);

            TString raw(static_cast<const char*>(data), size);
            NPDisk::TFormatContext::TOpener opener(f.PDiskCtx->FormatContext, TFixture::ChunkIdx, TFixture::ChunkNonce, {});
            TString opened(expected.size(), '\0');
            for (ui32 done = 0, sector = 0; done < expected.size(); ++sector) {
                const ui32 len = Min<ui32>(payloadSize, expected.size() - done);
                UNIT_ASSERT_C(opener.Open(reinterpret_cast<ui8*>(opened.begin()) + done,
                    reinterpret_cast<ui8*>(raw.begin()) + sector * f.Format.SectorSize, 3 + sector, 0, len),
                    "encrypt# " << encrypt << " sector# " << sector);
                done += len;
            }
            UNIT_ASSERT(opened == expected);
        }
    }

    // Without a known nonce, or with the control off, the write is left to the PDisk.
    Y_UNIT_TEST(LeavesWriteAlone) {
        TFixture f(true);
        TString a(100, 'x');
        const NPDisk::TEvChunkWrite::TPart parts[] = {{a.data(), ui32(a.size())}};
        auto make = [&](ui32 chunkIdx) {
            return NPDisk::TEvChunkWrite(1, 1, chunkIdx, 0,
                MakeIntrusive<NPDisk::TEvChunkWrite::TNonOwningParts>(parts, 1u), nullptr, true, 0);
        };
        auto unknown = make(TFixture::ChunkIdx + 1);
        UNIT_ASSERT(!PrepareChunkWriteSectors(unknown, *f.PDiskCtx));
        UNIT_ASSERT(!unknown.PreformattedSectors);

        f.PDiskCtx->EnableSectorPreparation = TControlWrapper(0, 0, 1);
        auto off = make(TFixture::ChunkIdx);
        UNIT_ASSERT(!PrepareChunkWriteSectors(off, *f.PDiskCtx));
        UNIT_ASSERT(!off.PreformattedSectors);
    }
}

} // NKikimr
