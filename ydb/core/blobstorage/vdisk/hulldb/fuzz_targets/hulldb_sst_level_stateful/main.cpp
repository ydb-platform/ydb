#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_params.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullwriteindexsst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it.h>

#include <util/generic/algorithm.h>
#include <util/generic/deque.h>
#include <util/generic/queue.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <algorithm>
#include <optional>

namespace {

using TKey = NKikimr::TKeyLogoBlob;
using TMemRec = NKikimr::TMemRecLogoBlob;
using TLevelIndex = NKikimr::TLevelIndex<TKey, TMemRec>;
using TLevelIndexSnapshot = NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;
using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
using TIndexSstWriter = NKikimr::TIndexSstWriter<TKey, TMemRec>;
using TSortedLevel = NKikimr::TSortedLevel<TKey, TMemRec>;

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
        1 + value % 11,
        1 + (value / 11) % 17,
        value % 4093,
        (value / 4093 + salt) % 8,
        value % 1024,
        (value / 29 + salt) % 4096));
}

TKey MakeSortedLevelKey(ui32 level, ui32 step, ui32 salt) {
    return TKey(NKikimr::TLogoBlobID(
        1000 + level,
        1,
        step,
        level % 8,
        (salt + step) % 1024,
        salt % 4096));
}

TKey MakeHighKey() {
    return TKey(NKikimr::TLogoBlobID(1'000'000, 100, 1'000'000, 7, 1024, 4096));
}

TMemRec MakeMemRec(const TKey& key, ui32 salt, bool allowExternalDiskBlob = true) {
    TMemRec memRec(NKikimr::TIngress(ui64(1) << (salt % 6)));
    const ui32 size = key.LogoBlobID().BlobSize();
    if (size == 0 && salt % 4 == 3) {
        memRec.SetNoBlob();
        return memRec;
    }
    switch (salt % 4) {
        case 0:
            memRec.SetNoBlob();
            break;
        case 1:
            if (allowExternalDiskBlob) {
                memRec.SetDiskBlob(NKikimr::TDiskPart(200 + salt % 64, (salt * 17) % 4096, size));
            } else {
                memRec.SetNoBlob();
            }
            break;
        case 2:
            memRec.SetHugeBlob(NKikimr::TDiskPart(400 + salt % 64, (salt * 31) % 4096, size));
            break;
        default:
            memRec.SetMemBlob(salt, size);
            break;
    }
    return memRec;
}

void SortUnique(TVector<TKey>& keys) {
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
}

void AddToModel(TVector<TKey>& model, const TVector<TKey>& keys) {
    model.insert(model.end(), keys.begin(), keys.end());
    SortUnique(model);
}

TVector<TKey> ExpectedForward(const TVector<TKey>& model, std::optional<TKey> seek) {
    if (!seek) {
        return model;
    }
    auto it = std::lower_bound(model.begin(), model.end(), *seek);
    return TVector<TKey>(it, model.end());
}

TVector<TKey> ExpectedBackward(const TVector<TKey>& model, const TKey& seek) {
    TVector<TKey> result;
    auto it = std::upper_bound(model.begin(), model.end(), seek);
    while (it != model.begin()) {
        --it;
        result.push_back(*it);
    }
    return result;
}

template <class TIterator>
TVector<TKey> ReadForward(TIterator& it, std::optional<TKey> seek) {
    if (seek) {
        it.Seek(*seek);
    } else {
        it.SeekToFirst();
    }

    TVector<TKey> keys;
    for (size_t limit = 0; limit != 1024 && it.Valid(); ++limit) {
        const TKey key = it.GetCurKey();
        if (!keys.empty()) {
            if (keys.back() == key) {
                it.Next();
                continue;
            }
            Y_ABORT_UNLESS(keys.back() < key);
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
    for (size_t limit = 0; limit != 1024 && it.Valid(); ++limit) {
        const TKey key = it.GetCurKey();
        if (!keys.empty()) {
            if (key == keys.back()) {
                it.Prev();
                continue;
            }
            Y_ABORT_UNLESS(key < keys.back());
        }
        keys.push_back(key);
        it.Prev();
    }
    return keys;
}

void CheckSegmentIterator(const TLevelSegmentPtr& segment, const TVector<TKey>& expected) {
    typename TLevelSegment::TMemIterator it(segment.Get());
    it.SeekToFirst();

    TVector<TKey> actual;
    for (size_t limit = 0; limit != 1024 && it.Valid(); ++limit) {
        actual.push_back(it.GetCurKey());
        (void)it.GetMemRec();
        it.Next();
    }

    Y_ABORT_UNLESS(actual == expected);
    Y_ABORT_UNLESS(segment->GetFirstLsn() <= segment->GetLastLsn());
    Y_ABORT_UNLESS(segment->Elements() == expected.size());
}

NKikimr::TPDiskCtxPtr MakePDiskCtx() {
    auto params = MakeIntrusive<NKikimr::TPDiskParams>(
        static_cast<NKikimr::NPDisk::TOwner>(1),
        ui64(1),
        ui32(0),
        64u << 10u,
        4u << 10u,
        ui64(1000),
        ui64(100u << 20u),
        ui64(100u << 20u),
        ui64(4u << 10u),
        ui64(4u << 10u),
        ui64(4u << 10u),
        NKikimr::NPDisk::DEVICE_TYPE_UNKNOWN,
        true,
        4u << 10u);
    return std::make_shared<NKikimr::TPDiskCtx>(std::move(params), NKikimr::TActorId(), "fuzz-pdisk");
}

void DrainChunkWrites(TQueue<std::unique_ptr<NKikimr::NPDisk::TEvChunkWrite>>& queue) {
    ui32 previousEnd = 0;
    ui32 previousChunk = 0;
    while (!queue.empty()) {
        auto msg = std::move(queue.front());
        queue.pop();
        Y_ABORT_UNLESS(msg);
        Y_ABORT_UNLESS(msg->ChunkIdx);
        Y_ABORT_UNLESS(msg->PartsPtr);
        Y_ABORT_UNLESS(msg->PartsPtr->ByteSize());
        if (previousChunk == msg->ChunkIdx) {
            Y_ABORT_UNLESS(previousEnd == msg->Offset);
        } else {
            previousChunk = msg->ChunkIdx;
            previousEnd = 0;
            Y_ABORT_UNLESS(msg->Offset == 0);
        }
        previousEnd = msg->Offset + msg->PartsPtr->ByteSize();
    }
}

TLevelSegmentPtr BuildWriterSst(
        const NKikimr::TVDiskContextPtr& vctx,
        const NKikimr::TPDiskCtxPtr& pdiskCtx,
        const TIntrusivePtr<TLevelIndex>& levelIndex,
        const TVector<TKey>& keys,
        ui64 firstLsn,
        ui64 lastLsn,
        ui32 chunkIdx,
        ui32 salt) {
    Y_ABORT_UNLESS(!keys.empty());

    TQueue<std::unique_ptr<NKikimr::NPDisk::TEvChunkWrite>> queue;
    TIndexSstWriter writer(vctx, pdiskCtx, levelIndex, queue);

    for (size_t i = 0; i != keys.size(); ++i) {
        (void)writer.PushRecord(keys[i], MakeMemRec(keys[i], salt + i));
    }

    writer.OnChunkReserved(chunkIdx);
    DrainChunkWrites(queue);
    writer.Finish();
    DrainChunkWrites(queue);

    auto commit = writer.GenerateCommitMessage(NKikimr::TActorId());
    Y_ABORT_UNLESS(commit);
    Y_ABORT_UNLESS(commit->LevelSegments.size() == 1);
    Y_ABORT_UNLESS(commit->CommitChunks.size() == 1);
    Y_ABORT_UNLESS(commit->CommitChunks.front() == chunkIdx);

    TLevelSegmentPtr segment = commit->LevelSegments.front();
    segment->Info.FirstLsn = firstLsn;
    segment->Info.LastLsn = lastLsn;
    CheckSegmentIterator(segment, keys);
    return segment;
}

void CheckIndexSnapshot(
        const NKikimr::THullCtxPtr& hullCtx,
        const TLevelIndexSnapshot& snapshot,
        const TVector<TKey>& model,
        std::optional<TKey> seek) {
    typename TLevelIndexSnapshot::TIndexForwardIterator forward(hullCtx, &snapshot);
    Y_ABORT_UNLESS(ReadForward(forward, seek) == ExpectedForward(model, seek));

    const TKey backSeek = seek.value_or(MakeHighKey());
    typename TLevelIndexSnapshot::TIndexBackwardIterator backward(hullCtx, &snapshot);
    Y_ABORT_UNLESS(ReadBackward(backward, backSeek) == ExpectedBackward(model, backSeek));
}

struct TSavedSnapshot {
    TLevelIndexSnapshot Snapshot;
    TVector<TKey> Model;
};

void CheckHeapMerge(
        const NKikimr::THullCtxPtr& hullCtx,
        const TSavedSnapshot& saved,
        const TLevelIndexSnapshot& current,
        const TVector<TKey>& currentModel,
        const TKey& seek) {
    typename TLevelIndexSnapshot::TForwardIterator left(hullCtx, &saved.Snapshot);
    typename TLevelIndexSnapshot::TForwardIterator right(hullCtx, &current);
    NKikimr::THeapIterator<TKey, TMemRec, true> heap;
    left.PutToHeap(heap);
    right.PutToHeap(heap);
    Y_ABORT_UNLESS(ReadForward(heap, seek) == ExpectedForward(currentModel, seek));

    typename TLevelIndexSnapshot::TBackwardIterator backLeft(hullCtx, &saved.Snapshot);
    typename TLevelIndexSnapshot::TBackwardIterator backRight(hullCtx, &current);
    NKikimr::THeapIterator<TKey, TMemRec, false> backHeap;
    backLeft.PutToHeap(backHeap);
    backRight.PutToHeap(backHeap);
    Y_ABORT_UNLESS(ReadBackward(backHeap, seek) == ExpectedBackward(currentModel, seek));

    Y_ABORT_UNLESS(std::includes(
        currentModel.begin(), currentModel.end(),
        saved.Model.begin(), saved.Model.end()));
}

void FuzzSstLevelIndex(TInput& in) {
    NKikimr::TTestContexts contexts(64u << 10u, 16u << 10u);
    const auto hullCtx = contexts.GetHullCtx();
    const auto vctx = contexts.GetVCtx();
    const auto pdiskCtx = MakePDiskCtx();
    auto arena = std::make_shared<TRopeArena>(&NKikimr::TRopeArenaBackend::Allocate);

    auto levelIndex = MakeIntrusive<TLevelIndex>(contexts.GetLevelIndexSettings(), arena);
    for (ui32 i = 0; i != 3; ++i) {
        levelIndex->CurSlice->SortedLevels.push_back(TSortedLevel(TKey()));
    }
    levelIndex->LoadCompleted();

    ui64 lsn = 1;
    ui32 nextChunk = 10;
    TVector<ui32> sortedNextStep = {1, 1, 1};
    TVector<TKey> model;
    TDeque<TSavedSnapshot> snapshots;

    for (size_t step = 0; step != 128 && !in.Empty(); ++step) {
        switch (in.Byte() % 10) {
            case 0:
            case 1: {
                const TKey key = MakeKey(in.Int(8192), in.Byte());
                levelIndex->PutToFresh(lsn++, key, MakeMemRec(key, in.Byte(), false));
                AddToModel(model, TVector<TKey>{key});
                break;
            }
            case 2:
            case 3: {
                TVector<TKey> keys;
                const ui32 count = 1 + in.Int(16);
                keys.reserve(count);
                for (ui32 i = 0; i != count; ++i) {
                    keys.push_back(MakeKey(in.Int(8192), in.Byte()));
                }
                SortUnique(keys);
                const ui64 firstLsn = lsn;
                lsn += keys.size();
                auto segment = BuildWriterSst(vctx, pdiskCtx, levelIndex, keys, firstLsn, lsn - 1, nextChunk++, in.Byte());
                levelIndex->InsertSstAtLevel0(segment, hullCtx);
                AddToModel(model, keys);
                break;
            }
            case 4: {
                const ui32 level = in.Int(sortedNextStep.size());
                const ui32 count = 1 + in.Int(12);
                const ui32 stride = 1 + in.Int(4);
                const ui32 salt = in.Byte();
                TVector<TKey> keys;
                keys.reserve(count);
                ui32 stepBase = sortedNextStep[level];
                for (ui32 i = 0; i != count; ++i) {
                    keys.push_back(MakeSortedLevelKey(level, stepBase, salt + i));
                    stepBase += stride;
                }
                sortedNextStep[level] = stepBase + 8;
                SortUnique(keys);
                const ui64 firstLsn = lsn;
                lsn += keys.size();
                auto segment = BuildWriterSst(vctx, pdiskCtx, levelIndex, keys, firstLsn, lsn - 1, nextChunk++, salt);
                TLevelSegment::TLevelSstPtr pair(level + 1, segment);
                levelIndex->CurSlice->Put(pair);
                AddToModel(model, keys);
                break;
            }
            case 5: {
                const auto snapshot = levelIndex->GetIndexSnapshot();
                CheckIndexSnapshot(hullCtx, snapshot, model, std::nullopt);
                snapshots.push_back(TSavedSnapshot{snapshot, model});
                if (snapshots.size() > 6) {
                    snapshots.pop_front();
                }
                break;
            }
            case 6: {
                const auto snapshot = levelIndex->GetIndexSnapshot();
                CheckIndexSnapshot(hullCtx, snapshot, model, MakeKey(in.Int(8192), in.Byte()));
                break;
            }
            case 7: {
                if (!snapshots.empty()) {
                    const auto current = levelIndex->GetIndexSnapshot();
                    const auto& saved = snapshots[in.Int(snapshots.size())];
                    CheckHeapMerge(hullCtx, saved, current, model, MakeKey(in.Int(8192), in.Byte()));
                }
                break;
            }
            case 8: {
                NKikimrVDiskData::TLevelIndex proto;
                levelIndex->SerializeToProto(proto);
                Y_ABORT_UNLESS(proto.GetLevel0().SstsSize() == levelIndex->CurSlice->Level0CurSstsNum());
                Y_ABORT_UNLESS(proto.OtherLevelsSize() == levelIndex->CurSlice->GetLevelXNumber());
                break;
            }
            default: {
                const auto snapshot = levelIndex->GetIndexSnapshot();
                CheckIndexSnapshot(hullCtx, snapshot, model, MakeKey(in.Int(8192), in.Byte()));
                break;
            }
        }
    }

    const auto snapshot = levelIndex->GetIndexSnapshot();
    CheckIndexSnapshot(hullCtx, snapshot, model, std::nullopt);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput in(data, size);
    FuzzSstLevelIndex(in);
    return 0;
}
