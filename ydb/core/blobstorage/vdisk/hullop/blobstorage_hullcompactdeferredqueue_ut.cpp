#include "blobstorage_hullcompactdeferredqueue.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <util/random/shuffle.h>
#include <util/random/fast.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

using namespace NKikimr;

const TBlobStorageGroupType GType(TBlobStorageGroupType::ErasureNone);
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

    void ProcessItemImpl(const TDiskPart& /*preallocatedLocation*/, const TRope& buffer) {
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
        TRope parts[6] = {
            TRope(TString("AAAAAA")),
            TRope(TString("BBBBBB")),
            TRope(TString("CCCCCC")),
            TRope(TString("DDDDDD")),
            TRope(TString("EEEEEE")),
            TRope(TString("FFFFFF")),
        };

        ui32 fullDataSize = 32;

        std::unordered_map<TString, size_t> resm;

        struct TItem {
            ssize_t BlobId;
            NMatrix::TVectorType BlobParts;
            NMatrix::TVectorType PartsToStore;
            TVector<std::pair<size_t, NMatrix::TVectorType>> DiskData;
            size_t Expected;
        };
        TVector<TItem> items;

        TRopeArena arena(&TRopeArenaBackend::Allocate);

        auto process = [&](ui32 mem, ui32 masks0, ui32 masks1, ui32 numDiskParts) {
            ui32 masks[3] = {masks0, masks1, 0};

            TItem item;
            TDiskBlobMerger expm;

            // create initial memory merger
            for (ui8 i = 0; i < 6; ++i) {
                if (mem >> i & 1) {
                    TRope buf = TDiskBlob::Create(fullDataSize, i + 1, 6, TRope(parts[i]), arena, true);
                    TDiskBlob blob(&buf, NMatrix::TVectorType::MakeOneHot(i, 6), GType, TLogoBlobID(0, 0, 0, 0, parts[i].GetSize(), 0));
                    expm.Add(blob);
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
            ui64 wholeMask = mem;
            for (ui32 i = 0; i < numDiskParts; ++i) {
                const ui32 mask = masks[i];

                Y_ABORT_UNLESS(mask);

                TDiskBlobMerger m;
                for (ui8 i = 0; i < 6; ++i) {
                    if (mask >> i & 1) {
                        TRope buf = TDiskBlob::Create(fullDataSize, i + 1, 6, TRope(parts[i]), arena, true);
                        TDiskBlob blob(&buf, NMatrix::TVectorType::MakeOneHot(i, 6), GType, TLogoBlobID(0, 0, 0, 0, parts[i].GetSize(), 0));
                        m.Add(blob);
                        expm.Add(blob);
                    }
                }

                TRope buf = m.CreateDiskBlob(arena, true);
                Y_ABORT_UNLESS(buf);
                item.DiskData.emplace_back(GetResMapId(buf), m.GetDiskBlob().GetParts());

                wholeMask |= mask;
            }

            // generate parts to store vector and store item
            for (ui32 p = 1; p < 64; ++p) {
                if ((p & wholeMask) == p) {
                    NMatrix::TVectorType v(0, 6);
                    for (ui8 i = 0; i < 6; ++i) {
                        if (p >> i & 1) {
                            v.Set(i);
                        }
                    }
                    item.PartsToStore = v;

                    TDiskBlobMergerWithMask mx;
                    mx.SetFilterMask(v);
                    mx.Add(expm.GetDiskBlob());
                    item.Expected = GetResMapId(mx.CreateDiskBlob(arena, true));

                    items.push_back(item);
                }
            }
        };

        Cerr << "STEP 1" << Endl;

        TVector<ui32> maskopts;
        for (ui32 mask = 0; mask < 64; ++mask) {
            ui32 num = PopCount(mask);
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
                merger.Add(TDiskBlob(&IdToRope[item.BlobId], item.BlobParts, GType, TLogoBlobID(0, 0, 0, 0, 6, 0)));
            }

            // put the item to queue
            q.Put(id, item.DiskData.size(), TDiskPart(0, 0, 0), std::move(merger), item.PartsToStore, TLogoBlobID(0, 0, 0, 0, 6, 0));
            for (auto& p : item.DiskData) {
                itemQueue.emplace(id, IdToRope[p.first], p.second);
            }
            referenceResults.push_back(item.Expected);
            ++id;
        }

        q.Start();
        while (itemQueue) {
            auto& front = itemQueue.front();
            q.AddReadDiskBlob(std::get<0>(front), std::move(std::get<1>(front)), std::get<2>(front));
            itemQueue.pop();
            UNIT_ASSERT_VALUES_EQUAL(q.AllProcessed(), itemQueue.empty());
        }
        q.Finish();

        UNIT_ASSERT_VALUES_EQUAL(referenceResults.size(), q.Results.size());
        for (ui32 i = 0; i < referenceResults.size(); ++i) {
            UNIT_ASSERT_EQUAL(referenceResults[i], q.Results[i]);
        }
    }

}
