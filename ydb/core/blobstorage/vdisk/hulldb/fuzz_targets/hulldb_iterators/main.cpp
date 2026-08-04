#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_segment.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap_it.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <algorithm>
#include <limits>
#include <optional>

namespace {

using TKey = NKikimr::TKeyLogoBlob;
using TMemRec = NKikimr::TMemRecLogoBlob;
using TFreshSegment = NKikimr::TFreshSegment<TKey, TMemRec>;
using TFreshSegmentSnapshot = NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;

class TInput {
public:
    TInput(const ui8* data, size_t size)
        : Data(data)
        , Size(size)
    {}

    bool Empty() const {
        return Pos == Size;
    }

    ui8 Byte(ui8 fallback = 0) {
        return Pos < Size ? Data[Pos++] : fallback;
    }

    ui32 Int(ui32 mod, ui32 fallback = 0) {
        ui32 value = fallback;
        for (size_t i = 0; i != 4 && Pos < Size; ++i) {
            value = (value << 8) | Data[Pos++];
        }
        return mod ? value % mod : value;
    }

private:
    const ui8* Data = nullptr;
    size_t Size = 0;
    size_t Pos = 0;
};

TKey MakeKey(ui32 value, ui32 salt = 0) {
    return TKey(NKikimr::TLogoBlobID(
        1 + value % 7,
        1 + (value / 7) % 11,
        value % 257,
        (value / 257 + salt) % 8,
        0,
        (value / 17 + salt) % 1024));
}

template <class TIterator>
TVector<TKey> ReadForward(TIterator& it, std::optional<TKey> seek) {
    if (seek) {
        it.Seek(*seek);
    } else {
        it.SeekToFirst();
    }

    TVector<TKey> keys;
    for (size_t limit = 0; limit != 512 && it.Valid(); ++limit) {
        const TKey key = it.GetCurKey();
        if (!keys.empty()) {
            Y_ABORT_UNLESS(!(key < keys.back()));
            Y_ABORT_UNLESS(!(key == keys.back()));
        }
        keys.push_back(key);
        it.Next();
    }
    return keys;
}

template <class TIterator>
TVector<TKey> ReadBackward(TIterator& it, const TKey& seek) {
    it.Seek(seek);

    TVector<TKey> keys;
    for (size_t limit = 0; limit != 512 && it.Valid(); ++limit) {
        const TKey key = it.GetCurKey();
        if (!keys.empty()) {
            Y_ABORT_UNLESS(!(keys.back() < key));
            Y_ABORT_UNLESS(!(key == keys.back()));
        }
        keys.push_back(key);
        it.Prev();
    }
    return keys;
}

void CheckSortedUnique(TVector<TKey> keys) {
    TVector<TKey> sorted = keys;
    std::sort(sorted.begin(), sorted.end());
    sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
    Y_ABORT_UNLESS(keys == sorted);
}

std::shared_ptr<TFreshSegment::TFreshAppendix> MakeAppendix(
        const NKikimr::THullCtxPtr& hullCtx,
        TVector<TKey> keys) {
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

    auto appendix = std::make_shared<TFreshSegment::TFreshAppendix>(NKikimr::TMemoryConsumer(hullCtx->VCtx->FreshIndex));
    appendix->Reserve(keys.size());
    for (const auto& key : keys) {
        appendix->Add(key, TMemRec());
    }
    return appendix;
}

void FuzzFreshSegment(TInput& in) {
    NKikimr::TTestContexts contexts;
    auto hullCtx = contexts.GetHullCtx();
    auto arena = std::make_shared<TRopeArena>(&NKikimr::TRopeArenaBackend::Allocate);
    TIntrusivePtr<TFreshSegment> segment = MakeIntrusive<TFreshSegment>(
        hullCtx,
        8u << 20u,
        TInstant::Zero(),
        arena);

    ui64 lsn = 1;
    TVector<TKey> inserted;
    TVector<TFreshSegmentSnapshot> snapshots;

    for (size_t step = 0; step != 96 && !in.Empty(); ++step) {
        const ui32 op = in.Byte() % 8;
        switch (op) {
            case 0:
            case 1: {
                const TKey key = MakeKey(in.Int(4096), in.Byte());
                segment->Put(lsn++, key, TMemRec());
                inserted.push_back(key);
                break;
            }
            case 2: {
                TVector<TKey> appendixKeys;
                const ui32 count = 1 + in.Int(12);
                appendixKeys.reserve(count);
                for (ui32 i = 0; i != count; ++i) {
                    appendixKeys.push_back(MakeKey(in.Int(4096), in.Byte()));
                }
                auto appendix = MakeAppendix(hullCtx, std::move(appendixKeys));
                if (!appendix->Empty()) {
                    const ui64 firstLsn = lsn;
                    const ui64 lastLsn = lsn + appendix->GetSize() - 1;
                    segment->PutAppendix(std::move(appendix), firstLsn, lastLsn);
                    lsn = lastLsn + 1;
                }
                break;
            }
            case 3: {
                auto snapshot = segment->GetSnapshot();
                typename TFreshSegmentSnapshot::TForwardIterator it(hullCtx, &snapshot);
                auto keys = ReadForward(it, std::nullopt);
                CheckSortedUnique(keys);
                snapshots.push_back(std::move(snapshot));
                if (snapshots.size() > 8) {
                    snapshots.erase(snapshots.begin());
                }
                break;
            }
            case 4: {
                auto snapshot = segment->GetSnapshot();
                typename TFreshSegmentSnapshot::TForwardIterator it(hullCtx, &snapshot);
                auto keys = ReadForward(it, MakeKey(in.Int(4096), in.Byte()));
                CheckSortedUnique(keys);
                break;
            }
            case 5: {
                auto snapshot = segment->GetSnapshot();
                typename TFreshSegmentSnapshot::TBackwardIterator it(hullCtx, &snapshot);
                (void)ReadBackward(it, MakeKey(in.Int(4096), in.Byte()));
                break;
            }
            case 6: {
                auto job = segment->Compact();
                if (!job.Empty()) {
                    job.Work();
                    auto nextJob = job.ApplyCompactionResult();
                    if (!nextJob.Empty()) {
                        nextJob.Work();
                        (void)nextJob.ApplyCompactionResult();
                    }
                }
                break;
            }
            default:
                Y_ABORT_UNLESS(segment->GetFirstLsn() <= segment->GetLastLsn() || segment->ElementsInserted() == 0);
                break;
        }
    }

    auto latest = segment->GetSnapshot();
    typename TFreshSegmentSnapshot::TIteratorWOMerge rawIt(hullCtx, &latest);
    rawIt.SeekToFirst();
    for (size_t limit = 0; limit != 512 && rawIt.Valid(); ++limit) {
        (void)rawIt.GetUnmergedKey();
        (void)rawIt.GetUnmergedMemRec();
        rawIt.Next();
    }

    if (!snapshots.empty()) {
        typename TFreshSegmentSnapshot::TForwardIterator left(hullCtx, &snapshots.front());
        typename TFreshSegmentSnapshot::TForwardIterator right(hullCtx, &latest);
        NKikimr::THeapIterator<TKey, TMemRec, true> heap;
        left.PutToHeap(heap);
        right.PutToHeap(heap);
        heap.SeekToFirst();
        std::optional<TKey> previous;
        for (size_t limit = 0; limit != 512 && heap.Valid(); ++limit) {
            const TKey key = heap.GetCurKey();
            if (previous) {
                Y_ABORT_UNLESS(*previous < key);
            }
            previous = key;
            heap.Next();
        }

        typename TFreshSegmentSnapshot::TBackwardIterator backLeft(hullCtx, &snapshots.front());
        typename TFreshSegmentSnapshot::TBackwardIterator backRight(hullCtx, &latest);
        NKikimr::THeapIterator<TKey, TMemRec, false> backHeap;
        backLeft.PutToHeap(backHeap);
        backRight.PutToHeap(backHeap);
        backHeap.Seek(MakeKey(std::numeric_limits<ui32>::max() % 4096));
        previous.reset();
        for (size_t limit = 0; limit != 512 && backHeap.Valid(); ++limit) {
            const TKey key = backHeap.GetCurKey();
            if (previous) {
                Y_ABORT_UNLESS(key < previous.value());
            }
            previous = key;
            backHeap.Prev();
        }
    }

    Y_UNUSED(inserted);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput in(data, size);
    FuzzFreshSegment(in);
    return 0;
}
