#include "blobstorage_hulldatamerger.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageHullDataMerger) {

        // A blob whose two parts on this VDisk were written on both sides of a MinHugeBlobInBytes change: part 1 is
        // stored inplace in an SST, part 2 in a huge slot. The handoff map of compaction then removes one of them.
        struct TMixedBlob {
            const TBlobStorageGroupType GType{TBlobStorageGroupType::ErasureMirror3dc};
            const EBlobHeaderMode HeaderMode = EBlobHeaderMode::OLD_HEADER;
            const TLogoBlobID Id{1, 1, 1, 0, 100 << 10, 0};
            const NMatrix::TVectorType InplacePart = NMatrix::TVectorType::MakeOneHot(0, GType.TotalPartCount());
            const NMatrix::TVectorType HugePart = NMatrix::TVectorType::MakeOneHot(1, GType.TotalPartCount());
            const TDiskPart InplaceLocation{10, 0, TDiskBlob::CalculateBlobSize(GType, Id, InplacePart, HeaderMode)};
            const TDiskPart HugeLocation{20, 0, TDiskBlob::CalculateBlobSize(GType, Id, HugePart, HeaderMode)};

            void AddTo(TDataMerger& merger) const {
                TMemRecLogoBlob inplace(TIngress().ReplaceLocal(GType, InplacePart));
                inplace.SetDiskBlob(InplaceLocation);
                merger.Add(inplace, static_cast<const TDiskPart*>(nullptr), 1, Id);

                TMemRecLogoBlob huge(TIngress().ReplaceLocal(GType, HugePart));
                huge.SetHugeBlob(HugeLocation);
                merger.Add(huge, static_cast<const TDiskPart*>(nullptr), 2, Id);
            }
        };

        Y_UNIT_TEST(FilterOutHugePartWhenInlining) {
            // the blob is not huge any more, so compaction reads the huge part to inline it, and frees its slot
            TMixedBlob blob;
            TDataMerger merger(blob.GType, blob.HeaderMode);
            blob.AddTo(merger);
            merger.Finish(false /*targetingHugeBlob*/, blob.Id, true /*keepData*/);
            merger.FilterLocalParts(blob.InplacePart);

            UNIT_ASSERT(merger.GetParts() == blob.InplacePart);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(merger.GetType()), static_cast<int>(TBlobType::DiskBlob));
            UNIT_ASSERT(merger.GetSavedHugeBlobs().empty());
            UNIT_ASSERT_VALUES_EQUAL(merger.GetDeletedHugeBlobs().size(), 1); // the slot is freed exactly once
            UNIT_ASSERT(merger.GetDeletedHugeBlobs().front() == blob.HugeLocation);
            const auto& reads = merger.GetCollectTask().Reads;
            UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(std::get<1>(reads.front())), 0);
        }

        Y_UNIT_TEST(FilterOutInplacePartWhenMakingHugeBlob) {
            // the blob became huge, so compaction asks for a huge slot for the inplace part
            TMixedBlob blob;
            TDataMerger merger(blob.GType, blob.HeaderMode);
            blob.AddTo(merger);
            merger.Finish(true /*targetingHugeBlob*/, blob.Id, true /*keepData*/);
            UNIT_ASSERT_VALUES_EQUAL(merger.GetSlotsToAllocate().size(), 1);
            merger.FilterLocalParts(blob.HugePart);

            // the record keeps one huge location for its only part and asks for no slot
            UNIT_ASSERT(merger.GetParts() == blob.HugePart);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(merger.GetType()), static_cast<int>(TBlobType::HugeBlob));
            UNIT_ASSERT_VALUES_EQUAL(merger.GetSavedHugeBlobs().size(), 1);
            UNIT_ASSERT(merger.GetSavedHugeBlobs().front() == blob.HugeLocation);
            UNIT_ASSERT(merger.GetSlotsToAllocate().empty());
            UNIT_ASSERT(merger.GetDeletedHugeBlobs().empty());
        }
    }

} // NKikimr
