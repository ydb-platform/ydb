#include "blobstorage_hulldatamerger.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
Y_UNIT_TEST_SUITE(HullDataMergerBlock82) {
    Y_UNIT_TEST(InputHeaderIsDetectedFromRecordSize) {
        for (const auto species : {TErasureType::Erasure4Plus2Block, TErasureType::Erasure8Plus2Block}) {
            const TBlobStorageGroupType type(species);
            const TLogoBlobID id(1, 1, 1, 0, 8193, 0);
            NMatrix::TVectorType parts(0, type.TotalPartCount());
            parts.Set(0);
            parts.Set(type.TotalPartCount() - 2);
            parts.Set(type.TotalPartCount() - 1);
            const ui32 payload = TDiskBlob::CalculateBlobSize(type, id, parts, false);
            for (bool inputHeader : {false, true}) {
                for (bool outputHeader : {false, true}) {
                    if (!type.CanUseLegacyHeader() && (inputHeader || outputHeader)) {
                        continue;
                    }
                    const ui32 header = inputHeader ? TDiskBlob::HeaderSize : 0;
                    TMemRecLogoBlob rec(TIngress().ReplaceLocal(type, parts));
                    rec.SetDiskBlob(TDiskPart(2, 4096, payload + header));
                    TDataMerger merger(type, outputHeader);
                    merger.Add(rec, static_cast<const TDiskPart*>(nullptr), 1, id);
                    merger.Finish(false, id, true);
                    const auto& reads = merger.GetCollectTask().Reads;
                    UNIT_ASSERT_VALUES_EQUAL(reads.size(), parts.CountBits());
                    ui32 offset = 4096 + header;
                    size_t index = 0;
                    for (ui8 part : parts) {
                        const auto& [location, partIdx] = reads[index++];
                        const ui32 size = type.PartSize(TLogoBlobID(id, part + 1));
                        UNIT_ASSERT_VALUES_EQUAL(partIdx, part);
                        UNIT_ASSERT_VALUES_EQUAL(location.ChunkIdx, 2);
                        UNIT_ASSERT_VALUES_EQUAL(location.Offset, offset);
                        UNIT_ASSERT_VALUES_EQUAL(location.Size, size);
                        offset += size;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(merger.GetInplacedBlobSize(id),
                        payload + (outputHeader ? TDiskBlob::HeaderSize : 0));
                }
            }
        }
    }

    Y_UNIT_TEST(InlineToHugeAndHugeToInlineHighParts) {
        const TBlobStorageGroupType type(TErasureType::Erasure8Plus2Block);
        const TLogoBlobID id(1, 1, 1, 0, 8193, 0);
        for (ui8 partIdx : {0, 7, 8, 9}) {
            const auto parts = NMatrix::TVectorType::MakeOneHot(partIdx, 10);
            const ui32 size = type.PartSize(TLogoBlobID(id, partIdx + 1));
            TMemRecLogoBlob rec(TIngress().ReplaceLocal(type, parts));
            rec.SetDiskBlob(TDiskPart(2, 4096, size));
            TDataMerger toHuge(type, false);
            toHuge.Add(rec, static_cast<const TDiskPart*>(nullptr), 1, id);
            toHuge.Finish(true, id, true);
            UNIT_ASSERT_VALUES_EQUAL(toHuge.GetSlotsToAllocate().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(toHuge.GetSlotsToAllocate().front(), size);
            std::vector<TDiskPart> allocated{{3, 8192, size + 4096}};
            toHuge.ApplyAllocatedSlots(allocated);
            const auto& moves = toHuge.GetHugeBlobMoves();
            UNIT_ASSERT_VALUES_EQUAL(moves.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(moves.front().PartIdx, partIdx);
            UNIT_ASSERT_EQUAL(moves.front().From, TDiskPart(2, 4096, size));
            UNIT_ASSERT_EQUAL(moves.front().To, TDiskPart(3, 8192, size));
            UNIT_ASSERT_VALUES_EQUAL(toHuge.GetSavedHugeBlobs().front().Size, size);

            rec.SetHugeBlob(moves.front().To);
            TDataMerger toInline(type, false);
            toInline.Add(rec, static_cast<const TDiskPart*>(nullptr), 2, id);
            // A mixed input makes compaction collect the huge record into inline output;
            // an input containing only huge records deliberately keeps their locations.
            const ui8 inlinePart = (partIdx + 1) % 10;
            const ui32 inlineSize = type.PartSize(TLogoBlobID(id, inlinePart + 1));
            TMemRecLogoBlob small(TIngress().ReplaceLocal(type,
                NMatrix::TVectorType::MakeOneHot(inlinePart, 10)));
            small.SetDiskBlob(TDiskPart(4, 16384, inlineSize));
            toInline.Add(small, static_cast<const TDiskPart*>(nullptr), 2, id);
            toInline.Finish(false, id, true);
            UNIT_ASSERT_VALUES_EQUAL(toInline.GetCollectTask().Reads.size(), 2);
            bool found = false;
            for (const auto& [location, part] : toInline.GetCollectTask().Reads) {
                if (part == partIdx) {
                    UNIT_ASSERT_EQUAL(location, moves.front().To);
                    found = true;
                }
            }
            UNIT_ASSERT(found);
            UNIT_ASSERT_VALUES_EQUAL(toInline.GetInplacedBlobSize(id), size + inlineSize);
            UNIT_ASSERT_VALUES_EQUAL(toInline.GetDeletedHugeBlobs().size(), 1);
        }
    }
}
}
