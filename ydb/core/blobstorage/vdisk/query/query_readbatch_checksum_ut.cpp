#include "query_readbatch.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hulldatamerger.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

TIntrusivePtr<IContiguousChunk> AllocateChecksumTestArena() {
    return TRopeAlignedBuffer::Allocate(65536);
}

TDiskPart ReadPart(TBlobStorageGroupType gtype, const TLogoBlobID& id,
        NMatrix::TVectorType parts, ui32 writtenSize, bool huge, ui8 partId,
        ui32 shift, ui32 size) {
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto info = MakeIntrusive<TBlobStorageGroupInfo>(gtype);
    auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters,
        TVDiskID(0, 1, 0, 0, 0), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
    NMonGroup::TReplGroup repl(counters, "subsystem", "repl");
    TEvBlobStorage::TEvVGet::TPtr ev;
    auto ctx = MakeIntrusive<TReadBatcherCtx>(vctx, nullptr, ev, repl);
    TReadBatcher batcher(ctx);
    batcher.StartTraverse(id, nullptr, partId, shift, size);
    batcher(TDiskPart(1, 4096, writtenSize), parts, huge);
    batcher.FinishTraverse(TIngress().ReplaceLocal(gtype, parts));
    const auto& items = batcher.GetResult().DataItems;
    UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
    return items.front().ActualRead;
}

} // namespace

Y_UNIT_TEST_SUITE(ReadBatcherChecksum) {
    Y_UNIT_TEST(HugeToInlineDoesNotTreatChecksumAsPayload) {
        const TBlobStorageGroupType gtype(TBlobStorageGroupType::Erasure4Plus2Block);
        const TLogoBlobID id(1, 1, 1, 0, 8192, 0);
        const ui32 partSize = gtype.PartSize(TLogoBlobID(id, 1));
        const auto first = NMatrix::TVectorType::MakeOneHot(0, gtype.TotalPartCount());
        const auto second = NMatrix::TVectorType::MakeOneHot(1, gtype.TotalPartCount());
        for (const auto mode : {EBlobHeaderMode::NO_HEADER, EBlobHeaderMode::OLD_HEADER,
                EBlobHeaderMode::XXH3_64BIT_HEADER}) {
            TRopeArena arena(&AllocateChecksumTestArena);
            const TString payloads[] = {TString(partSize, 'a'), TString(partSize, 'b')};
            TRope stored[] = {
                TDiskBlob::Create(id.BlobSize(), 1, gtype.TotalPartCount(), TRope(payloads[0]), arena, mode, std::nullopt),
                TDiskBlob::Create(id.BlobSize(), 2, gtype.TotalPartCount(), TRope(payloads[1]), arena, mode, std::nullopt),
            };
            TMemRecLogoBlob huge(TIngress().ReplaceLocal(gtype, first));
            huge.SetHugeBlob(TDiskPart(1, 4096, stored[0].GetSize()));
            TMemRecLogoBlob inlined(TIngress().ReplaceLocal(gtype, second));
            inlined.SetDiskBlob(TDiskPart(2, 4096, stored[1].GetSize()));
            TDataMerger merger(gtype, EBlobHeaderMode::XXH3_64BIT_HEADER);
            merger.Add(huge, static_cast<const TDiskPart*>(nullptr), 1, id);
            merger.Add(inlined, static_cast<const TDiskPart*>(nullptr), 2, id);
            merger.Finish(false, id, true);
            const auto& task = merger.GetCollectTask();
            UNIT_ASSERT_VALUES_EQUAL(task.Reads.size(), 2);
            TDiskBlobMerger output = task.BlobMerger;
            // Feed exactly the scheduled disk ranges into the same AddPart
            // interface used by the deferred compaction queue.
            for (const auto& [location, partIdx, isHuge] : task.Reads) {
                UNIT_ASSERT_VALUES_EQUAL(isHuge, partIdx == 0);
                UNIT_ASSERT_VALUES_EQUAL(location.Size, partSize);
                TRope data(stored[partIdx].Begin() + (location.Offset - 4096),
                    stored[partIdx].Begin() + (location.Offset - 4096) + location.Size);
                output.AddPart(std::move(data), gtype, TLogoBlobID(id, partIdx + 1));
            }
            TRope result = output.CreateDiskBlob(arena, EBlobHeaderMode::XXH3_64BIT_HEADER);
            UNIT_ASSERT_VALUES_EQUAL(result.GetSize(), merger.GetInplacedBlobSize(id));
            UNIT_ASSERT(TDiskBlob::ValidateChecksum(result));
            const TDiskBlob blob(&result, first | second, gtype, id);
            for (auto it = blob.begin(); it != blob.end(); ++it) {
                UNIT_ASSERT_VALUES_EQUAL(it.GetPart().ConvertToString(), payloads[it.GetPartId() - 1]);
            }
        }
    }

    Y_UNIT_TEST(SinglePartFullAndPartialReadsAcrossFormats) {
        const TBlobStorageGroupType gtype(TBlobStorageGroupType::ErasureNone);
        const TLogoBlobID id(1, 1, 1, 0, 8192, 0);
        const auto parts = NMatrix::TVectorType::MakeOneHot(0, 1);
        for (const auto mode : {EBlobHeaderMode::NO_HEADER, EBlobHeaderMode::OLD_HEADER,
                EBlobHeaderMode::XXH3_64BIT_HEADER}) {
            TRopeArena arena(&AllocateChecksumTestArena);
            const TRope stored = TDiskBlob::Create(id.BlobSize(), 1, 1, TRope(TString(id.BlobSize(), 'x')),
                arena, mode, std::nullopt);
            const ui32 offset = mode == EBlobHeaderMode::OLD_HEADER ? TDiskBlob::GetBlobHeaderSize(mode) : 0;
            for (bool huge : {false, true}) {
                for (ui32 size : {0u, id.BlobSize()}) {
                    const auto read = ReadPart(gtype, id, parts, stored.GetSize(), huge, 1, 0, size);
                    UNIT_ASSERT_VALUES_EQUAL(read.Offset, 4096 + offset);
                    UNIT_ASSERT_VALUES_EQUAL(read.Size, id.BlobSize() +
                        (mode == EBlobHeaderMode::XXH3_64BIT_HEADER ? sizeof(ui64) : 0));
                }
                for (const auto [shift, size] : {std::pair{0u, 100u}, std::pair{100u, 0u}, std::pair{100u, 200u}}) {
                    const auto read = ReadPart(gtype, id, parts, stored.GetSize(), huge, 1, shift, size);
                    UNIT_ASSERT_VALUES_EQUAL(read.Offset, 4096 + offset + shift);
                    UNIT_ASSERT_VALUES_EQUAL(read.Size, size ? size : id.BlobSize() - shift);
                }
            }
        }
    }

    Y_UNIT_TEST(MultiplePartsDoNotReturnWholeBlobChecksumAsPartChecksum) {
        const TBlobStorageGroupType gtype(TBlobStorageGroupType::Erasure4Plus2Block);
        const TLogoBlobID id(1, 1, 1, 0, 8192, 0);
        const ui32 partSize = gtype.PartSize(TLogoBlobID(id, 1));
        const auto parts = NMatrix::TVectorType::MakeOneHot(0, gtype.TotalPartCount()) |
            NMatrix::TVectorType::MakeOneHot(1, gtype.TotalPartCount());
        for (ui8 part : {1, 2}) {
            const auto read = ReadPart(gtype, id, parts, partSize * 2 + sizeof(ui64), false, part, 0, 0);
            UNIT_ASSERT_VALUES_EQUAL(read.Offset, 4096 + partSize * (part - 1));
            UNIT_ASSERT_VALUES_EQUAL(read.Size, partSize);
        }
    }

    Y_UNIT_TEST(HugeFullReadKeepsStoredChecksum) {
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
        auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters,
            TVDiskID(0, 1, 0, 0, 0), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
        NMonGroup::TReplGroup repl(counters, "subsystem", "repl");
        TEvBlobStorage::TEvVGet::TPtr ev;
        auto ctx = MakeIntrusive<TReadBatcherCtx>(vctx, nullptr, ev, repl);

        const TString payload(8192, 'x');
        const TLogoBlobID id(1, 1, 1, 0, payload.size(), 0);
        const TLogoBlobID partId(id, 1);
        const auto parts = NMatrix::TVectorType::MakeOneHot(0, 1);
        TRopeArena arena(&AllocateChecksumTestArena);
        TRope stored = TDiskBlob::Create(payload.size(), 1, 1, TRope(payload), arena,
            EBlobHeaderMode::XXH3_64BIT_HEADER, std::nullopt);
        UNIT_ASSERT(TDiskBlob::ValidateChecksum(stored));
        UNIT_ASSERT_VALUES_EQUAL(stored.GetSize(), payload.size() + sizeof(ui64));

        TReadBatcher batcher(ctx);
        batcher.StartTraverse(id, nullptr, 1, 0, 0);
        batcher(TDiskPart(1, 4096, stored.GetSize()), parts, true);
        auto ingress = TIngress::CreateIngressWithLocal(vctx->Top.get(), vctx->ShortSelfVDisk, partId);
        UNIT_ASSERT(ingress);
        batcher.FinishTraverse(*ingress);

        const auto& items = batcher.GetResult().DataItems;
        UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(items.front().ActualRead.Offset, 4096);
        // The response processor needs the persisted checksum, not a newly
        // calculated checksum of potentially corrupted payload bytes.
        UNIT_ASSERT_VALUES_EQUAL(items.front().ActualRead.Size, stored.GetSize());
    }
}
} // namespace NKikimr
