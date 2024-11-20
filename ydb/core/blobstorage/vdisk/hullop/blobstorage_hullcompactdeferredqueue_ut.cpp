#include "blobstorage_hullcompactdeferredqueue.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <util/random/shuffle.h>
#include <util/random/fast.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

#include <bit>

using namespace NKikimr;

const TBlobStorageGroupType GType(TBlobStorageGroupType::Erasure4Plus2Block);
std::unordered_map<TString, size_t> StringToId;
std::deque<TRope> IdToRope;

size_t GetResMapId(TRope r) {
    TString s = r.ConvertToString();
    if (const auto it = StringToId.find(s); it != StringToId.end()) {
        return it->second;
    }
    const size_t id = StringToId.size();
    StringToId.emplace(std::move(s), id);
    IdToRope.push_back(std::move(r));
    return id;
}

class TTestDeferredQueue : public TDeferredItemQueueBase<TTestDeferredQueue> {
    friend class TDeferredItemQueueBase<TTestDeferredQueue>;

    void StartImpl() {
    }

    void ProcessItemImpl(const TDiskPart& /*preallocatedLocation*/, const TRope& buffer, bool /*isInline*/) {
        Results.push_back(GetResMapId(buffer));
    }

    void FinishImpl() {
    }

public:
    TVector<size_t> Results;

    TTestDeferredQueue(TRopeArena& arena)
        : TDeferredItemQueueBase<TTestDeferredQueue>(arena, ::GType, true)
    {}
};

Y_UNIT_TEST_SUITE(TBlobStorageHullCompactDeferredQueueTest) {

    Y_UNIT_TEST(Basic) {
        TString data = "AAABBBCCCDDDEEEFFF";
        TLogoBlobID blobId(0, 0, 0, 0, data.size(), 0);
        std::array<TRope, 6> parts;
        ErasureSplit(static_cast<TErasureType::ECrcMode>(blobId.CrcMode()), GType, TRope(data), parts);

        std::unordered_map<TString, size_t> resm;

        struct TItem {
            ssize_t BlobId;
            NMatrix::TVectorType BlobParts;
            TVector<std::pair<size_t, NMatrix::TVectorType>> DiskData;
            size_t Expected;
        };
        TVector<TItem> items;

        TRopeArena arena(&TRopeArenaBackend::Allocate);

        auto process = [&](ui32 mem, ui32 masks0, ui32 masks1, ui32 numDiskParts) {
            ui32 masks[2] = {masks0, masks1};

            TItem item;
            TDiskBlobMerger expm;

            // create initial memory merger
            for (ui8 i = 0; i < GType.TotalPartCount(); ++i) {
                if (mem >> i & 1) {
                    expm.AddPart(TRope(parts[i]), GType, TLogoBlobID(blobId, i + 1));
                }
            }

            // extract blob merger data into item
            if (expm.Empty()) {
                item.BlobId = -1;
            } else {
                item.BlobId = GetResMapId(expm.CreateDiskBlob(arena, true));
            }
            item.BlobParts = expm.GetDiskBlob().GetParts();

            // generate disk parts
            for (ui32 i = 0; i < numDiskParts; ++i) {
                const ui32 mask = masks[i];

                Y_ABORT_UNLESS(mask);

                TDiskBlobMerger m;
                for (ui8 i = 0; i < GType.TotalPartCount(); ++i) {
                    if (mask >> i & 1) {
                        m.AddPart(TRope(parts[i]), GType, TLogoBlobID(blobId, i + 1));
                        expm.AddPart(TRope(parts[i]), GType, TLogoBlobID(blobId, i + 1));
                    }
                }

                TRope buf = m.CreateDiskBlob(arena, true);
                Y_ABORT_UNLESS(buf);
                item.DiskData.emplace_back(GetResMapId(buf), m.GetDiskBlob().GetParts());
            }

            // generate parts to store vector and store item
            if (!expm.Empty()) {
                item.Expected = GetResMapId(expm.CreateDiskBlob(arena, true));
                items.push_back(item);
            }
        };

        Cerr << "STEP 1" << Endl;

        TVector<ui32> maskopts;
        for (ui32 mask = 0; mask < 64; ++mask) {
            ui32 num = std::popcount(mask);
            if (1 <= num && num <= 3) {
                maskopts.push_back(mask);
            }
        }

        for (ui32 mem = 0; mem < 64; ++mem) {
            process(mem, 0, 0, 0);
            for (ui32 x1 : maskopts) {
                process(mem, x1, 0, 1);
                for (ui32 x2 : maskopts) {
                    process(mem, x1, x2, 2);
                }
            }
        }

        Cerr << "STEP 2 StringToId# " << StringToId.size() << " numItems# " << items.size() << Endl;

        // shuffle items
        Shuffle(items.begin(), items.end());

        // generate read queue
        TTestDeferredQueue q(arena);
        TQueue<std::tuple<ui64, TRope, NMatrix::TVectorType>> itemQueue;
        TVector<size_t> referenceResults;
        ui64 id = 0;
        for (TItem& item : items) {
            // prepare item merger for this sample item
            TDiskBlobMerger merger;
            if (item.BlobId != -1) {
                merger.Add(TDiskBlob(&IdToRope[item.BlobId], item.BlobParts, GType, blobId));
            }

            // put the item to queue
            ui32 numReads = 0;
            for (auto& [ropeId, parts] : item.DiskData) {
                itemQueue.emplace(id, IdToRope[ropeId], parts);
                numReads += parts.CountBits();
            }
            q.Put(id, numReads, TDiskPart(0, 0, 0), std::move(merger), blobId, true);
            referenceResults.push_back(item.Expected);
            ++id;
        }

        q.Start();
        while (itemQueue) {
            auto& [id, buffer, parts] = itemQueue.front();
            TDiskBlob blob(&buffer, parts, GType, blobId);
            for (ui8 partIdx : parts) {
                TRope holder;
                q.AddReadDiskBlob(id, TRope(blob.GetPart(partIdx, &holder)), partIdx);
            }
            itemQueue.pop();
            UNIT_ASSERT_VALUES_EQUAL(q.AllProcessed(), itemQueue.empty());
        }
        q.Finish();

        UNIT_ASSERT_VALUES_EQUAL(referenceResults.size(), q.Results.size());
        for (ui32 i = 0; i < referenceResults.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(IdToRope[referenceResults[i]].ConvertToString(), IdToRope[q.Results[i]].ConvertToString());
        }
    }

}
