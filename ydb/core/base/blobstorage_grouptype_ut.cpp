#include <ydb/core/blobstorage/defs.h>

#include "blobstorage_grouptype.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>
#include <util/stream/null.h>

namespace NKikimr {

#define STR Cnull

Y_UNIT_TEST_SUITE(TBlobStorageGroupTypeTest) {

    Y_UNIT_TEST(TestCorrectLayout) {
        TBlobStorageGroupType groupType(TBlobStorageGroupType::Erasure3Plus1Stripe);

        ui32 blobSubgroupSize = groupType.BlobSubgroupSize();
        ui32 totalPartCount = groupType.TotalPartCount();
        TBlobStorageGroupType::TPartLayout layout;
        layout.VDiskMask = ~((~(ui32)0) << blobSubgroupSize);
        layout.VDiskPartMask.resize(blobSubgroupSize);
        for (ui32 i = 0; i < totalPartCount; ++i) {
            layout.VDiskPartMask[i] = (1 << i);
        }
        for (ui32 i = totalPartCount; i < blobSubgroupSize; ++i) {
            layout.VDiskPartMask[i] = 0;
        }
        TBlobStorageGroupType::TPartPlacement correction;
        bool isCorrectable = groupType.CorrectLayout(layout, correction);

#define CORRECTION_DESCRIPTION \
            "isCorrectable=" << (isCorrectable ? "true" : "false") << \
            " correction.Records.size()=" << correction.Records.size() << \
            " emptyDiskIdx=" << emptyDiskIdx << \
            (correction.Records.size() ? " Record[0].VDiskIdx=" : "   ") << \
            (correction.Records.size() ? correction.Records[0].VDiskIdx : 0) << \
            (correction.Records.size() ? " Record[0].PartIdx=" : "") << \
            (correction.Records.size() ? correction.Records[0].PartIdx : 0)

        ui32 emptyDiskIdx = (ui32)-1;
        UNIT_ASSERT_C(isCorrectable && correction.Records.size() == 0, CORRECTION_DESCRIPTION);

        for (emptyDiskIdx = 0; emptyDiskIdx < totalPartCount; ++emptyDiskIdx) {
            layout.VDiskMask = ~((~(ui32)0) << blobSubgroupSize);
            layout.VDiskPartMask.resize(blobSubgroupSize);
            for (ui32 i = 0; i < totalPartCount; ++i) {
                layout.VDiskPartMask[i] = (1 << i);
            }
            for (ui32 i = totalPartCount; i < blobSubgroupSize; ++i) {
                layout.VDiskPartMask[i] = 0;
            }
            layout.VDiskPartMask[emptyDiskIdx] = 0;
            correction.Records.clear();
            isCorrectable = groupType.CorrectLayout(layout, correction);
            UNIT_ASSERT_C(isCorrectable && correction.Records.size() == 1 &&
                correction.Records[0].VDiskIdx == emptyDiskIdx && correction.Records[0].PartIdx == emptyDiskIdx,
                CORRECTION_DESCRIPTION);

            for (ui32 i = 0; i < correction.Records.size(); ++i) {
                layout.VDiskPartMask[correction.Records[i].VDiskIdx] = (1 << correction.Records[i].PartIdx);
            }

            correction.Records.clear();
            isCorrectable = groupType.CorrectLayout(layout, correction);
            UNIT_ASSERT_C(isCorrectable && correction.Records.size() == 0, CORRECTION_DESCRIPTION);
        }

        for (emptyDiskIdx = 0; emptyDiskIdx < totalPartCount; ++emptyDiskIdx) {
            layout.VDiskMask = ~((~(ui32)0) << blobSubgroupSize);
            layout.VDiskPartMask.resize(blobSubgroupSize);
            for (ui32 i = 0; i < totalPartCount; ++i) {
                layout.VDiskPartMask[i] = (1 << i);
            }
            for (ui32 i = totalPartCount; i < blobSubgroupSize; ++i) {
                layout.VDiskPartMask[i] = 0;
            }
            layout.VDiskMask &= ~((ui32)1 << emptyDiskIdx);
            correction.Records.clear();
            isCorrectable = groupType.CorrectLayout(layout, correction);
            UNIT_ASSERT_C(isCorrectable && correction.Records.size() == 1 &&
                correction.Records[0].VDiskIdx == totalPartCount && correction.Records[0].PartIdx == emptyDiskIdx,
                CORRECTION_DESCRIPTION);

            for (ui32 i = 0; i < correction.Records.size(); ++i) {
                layout.VDiskPartMask[correction.Records[i].VDiskIdx] = (1 << correction.Records[i].PartIdx);
            }

            correction.Records.clear();
            isCorrectable = groupType.CorrectLayout(layout, correction);
            UNIT_ASSERT_C(isCorrectable && correction.Records.size() == 0, CORRECTION_DESCRIPTION);
        }

        for (emptyDiskIdx = 0; emptyDiskIdx < totalPartCount-1; ++emptyDiskIdx) {
            for (ui32 emptyDisk2Idx = emptyDiskIdx + 1; emptyDisk2Idx < totalPartCount; ++emptyDisk2Idx) {
                layout.VDiskMask = ~((~(ui32)0) << blobSubgroupSize);
                layout.VDiskPartMask.resize(blobSubgroupSize);
                for (ui32 i = 0; i < totalPartCount; ++i) {
                    layout.VDiskPartMask[i] = (1 << i);
                }
                for (ui32 i = totalPartCount; i < blobSubgroupSize; ++i) {
                    layout.VDiskPartMask[i] = 0;
                }
                layout.VDiskMask &= ~((ui32)1 << emptyDiskIdx);
                layout.VDiskMask &= ~((ui32)1 << emptyDisk2Idx);
                correction.Records.clear();

                isCorrectable = groupType.CorrectLayout(layout, correction);
                UNIT_ASSERT_C(!isCorrectable && correction.Records.size() == 1 &&
                    correction.Records[0].VDiskIdx == totalPartCount &&
                    correction.Records[0].PartIdx == emptyDiskIdx,
                    CORRECTION_DESCRIPTION << " emptyDisk2Idx=" << emptyDisk2Idx);

                for (ui32 i = 0; i < correction.Records.size(); ++i) {
                    layout.VDiskPartMask[correction.Records[i].VDiskIdx] = (1 << correction.Records[i].PartIdx);
                }

                correction.Records.clear();
                isCorrectable = groupType.CorrectLayout(layout, correction);
                UNIT_ASSERT_C(!isCorrectable && correction.Records.size() == 0,
                    CORRECTION_DESCRIPTION << " emptyDisk2Idx=" << emptyDisk2Idx);
            }
        }
#undef CORRECTION_DESCRIPTION
    }

    Y_UNIT_TEST(OutputInfoAboutErasureSpecies) {
        for (int i = TErasureType::ErasureNone; i < TErasureType::ErasureSpeciesCount; i++) {
            auto es = (TErasureType::EErasureSpecies)i;
            TBlobStorageGroupType groupType(es);
            STR << groupType.ToString() << ":\n";
            STR << "  ParityParts:                " << groupType.ParityParts() << "\n";
            STR << "  DataParts:                  " << groupType.DataParts() << "\n";
            STR << "  TotalPartCount:             " << groupType.TotalPartCount() << "\n";
            STR << "  MinimalRestorablePartCount: " << groupType.MinimalRestorablePartCount() << "\n";
            STR << "  MinimalBlockSize:           " << groupType.MinimalBlockSize() << "\n";
            STR << "  Prime:                      " << groupType.Prime() << "\n";
            STR << "  BlobSubgroupSize:           " << groupType.BlobSubgroupSize() << "\n";
            STR << "  Handoff:                    " << groupType.Handoff() << "\n";
        }
    }

}

} // namespace NKikimr

