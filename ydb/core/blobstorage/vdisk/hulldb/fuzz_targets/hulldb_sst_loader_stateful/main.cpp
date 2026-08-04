#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_params.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullwriteindexsst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullload.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/queue.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <algorithm>
#include <cstring>

namespace {

using TKey = NKikimr::TKeyLogoBlob;
using TMemRec = NKikimr::TMemRecLogoBlob;
using TLevelIndex = NKikimr::TLevelIndex<TKey, TMemRec>;
using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;
using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
using TIndexSstWriter = NKikimr::TIndexSstWriter<TKey, TMemRec>;
using THullSegLoaded = NKikimr::THullSegLoaded<TLevelSegment>;
using TLevelSegmentLoader = NKikimr::TLevelSegmentLoader<TKey, TMemRec>;

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
        1 + value % 17,
        1 + (value / 17) % 23,
        value % 8191,
        (value / 8191 + salt) % 8,
        value % 1024,
        (value / 31 + salt) % 4096));
}

TMemRec MakeMemRec(const TKey& key, ui32 salt) {
    TMemRec memRec(NKikimr::TIngress(ui64(1) << (salt % 6)));
    const ui32 size = key.LogoBlobID().BlobSize();
    switch (salt % 4) {
        case 0:
            memRec.SetNoBlob();
            break;
        case 1:
            memRec.SetDiskBlob(NKikimr::TDiskPart(700 + salt % 32, (salt * 19) % 4096, size));
            break;
        case 2:
            memRec.SetHugeBlob(NKikimr::TDiskPart(900 + salt % 32, (salt * 37) % 4096, size));
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

NKikimr::TPDiskCtxPtr MakePDiskCtx(const NActors::TActorId& pdiskId = {}) {
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
    return std::make_shared<NKikimr::TPDiskCtx>(std::move(params), pdiskId, "fuzz-pdisk");
}

using TChunkBytes = TMap<ui32, TString>;

void CaptureChunkWrites(
        TQueue<std::unique_ptr<NKikimr::NPDisk::TEvChunkWrite>>& queue,
        TChunkBytes& chunks) {
    while (!queue.empty()) {
        auto msg = std::move(queue.front());
        queue.pop();
        Y_ABORT_UNLESS(msg);
        Y_ABORT_UNLESS(msg->ChunkIdx);
        Y_ABORT_UNLESS(msg->PartsPtr);

        TString& chunk = chunks[msg->ChunkIdx];
        const ui32 end = msg->Offset + msg->PartsPtr->ByteSize();
        if (chunk.size() < end) {
            chunk.resize(end, '\0');
        }

        ui32 pos = msg->Offset;
        for (ui32 i = 0; i != msg->PartsPtr->Size(); ++i) {
            const auto part = (*msg->PartsPtr)[i];
            if (part.second) {
                if (part.first) {
                    memcpy(chunk.Detach() + pos, part.first, part.second);
                } else {
                    memset(chunk.Detach() + pos, 0, part.second);
                }
                pos += part.second;
            }
        }
        Y_ABORT_UNLESS(pos == end);
    }
}

TLevelSegmentPtr BuildPersistedSst(
        const NKikimr::TVDiskContextPtr& vctx,
        const NKikimr::TPDiskCtxPtr& pdiskCtx,
        const TIntrusivePtr<TLevelIndex>& levelIndex,
        const TVector<TKey>& keys,
        ui32 chunkIdx,
        ui32 salt,
        TChunkBytes& chunks) {
    Y_ABORT_UNLESS(!keys.empty());

    TQueue<std::unique_ptr<NKikimr::NPDisk::TEvChunkWrite>> queue;
    TIndexSstWriter writer(vctx, pdiskCtx, levelIndex, queue);

    for (size_t i = 0; i != keys.size(); ++i) {
        (void)writer.PushRecord(keys[i], MakeMemRec(keys[i], salt + i));
    }

    writer.OnChunkReserved(chunkIdx);
    CaptureChunkWrites(queue, chunks);
    writer.Finish();
    CaptureChunkWrites(queue, chunks);

    auto commit = writer.GenerateCommitMessage(NActors::TActorId());
    Y_ABORT_UNLESS(commit);
    Y_ABORT_UNLESS(commit->LevelSegments.size() == 1);
    Y_ABORT_UNLESS(commit->CommitChunks.size() == 1);
    Y_ABORT_UNLESS(commit->CommitChunks.front() == chunkIdx);
    Y_ABORT_UNLESS(chunks.contains(chunkIdx));
    return commit->LevelSegments.front();
}

NActors::TTestActorRuntime::TEgg MakeRuntimeEgg() {
    return {new NKikimr::TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {}, {}};
}

TVector<TKey> ReadSegmentKeys(const TLevelSegmentPtr& segment) {
    typename TLevelSegment::TMemIterator it(segment.Get());
    it.SeekToFirst();

    TVector<TKey> keys;
    for (size_t limit = 0; limit != 1024 && it.Valid(); ++limit) {
        const TKey key = it.GetCurKey();
        if (!keys.empty()) {
            Y_ABORT_UNLESS(keys.back() < key);
        }
        keys.push_back(key);
        (void)it.GetMemRec();
        it.Next();
    }
    return keys;
}

class TFakePDiskActor : public NActors::TActorBootstrapped<TFakePDiskActor> {
    const TChunkBytes Chunks;

    friend class NActors::TActorBootstrapped<TFakePDiskActor>;

    void Bootstrap(const NActors::TActorContext&) {
        Become(&TFakePDiskActor::StateFunc);
    }

    void Handle(NKikimr::NPDisk::TEvChunkRead::TPtr ev, const NActors::TActorContext&) {
        auto* msg = ev->Get();
        auto res = std::make_unique<NKikimr::NPDisk::TEvChunkReadResult>(
            NKikimrProto::OK, msg->ChunkIdx, msg->Offset, msg->Cookie, 0, TString());

        const auto it = Chunks.find(msg->ChunkIdx);
        Y_ABORT_UNLESS(it != Chunks.end());
        Y_ABORT_UNLESS(msg->Size);
        Y_ABORT_UNLESS(msg->Offset <= it->second.size());
        Y_ABORT_UNLESS(msg->Offset + msg->Size <= it->second.size());

        res->Data.SetData(TRcBuf::Copy(it->second.data() + msg->Offset, msg->Size));
        res->Data.Commit();
        Send(ev->Sender, res.release());
    }

    STRICT_STFUNC(StateFunc,
        HFunc(NKikimr::NPDisk::TEvChunkRead, Handle)
    )

public:
    explicit TFakePDiskActor(TChunkBytes chunks)
        : Chunks(std::move(chunks))
    {}
};

void FuzzSstLoader(TInput& in) {
    NKikimr::TTestContexts contexts(64u << 10u, 16u << 10u);
    const auto vctx = contexts.GetVCtx();
    auto arena = std::make_shared<TRopeArena>(&NKikimr::TRopeArenaBackend::Allocate);

    auto levelIndex = MakeIntrusive<TLevelIndex>(contexts.GetLevelIndexSettings(), arena);
    levelIndex->LoadCompleted();

    TChunkBytes chunks;
    ui32 nextChunk = 10;

    const ui32 segments = 1 + in.Int(6);
    for (ui32 segmentIdx = 0; segmentIdx != segments && !in.Empty(); ++segmentIdx) {
        TVector<TKey> keys;
        const ui32 count = 1 + in.Int(24);
        keys.reserve(count);
        for (ui32 i = 0; i != count; ++i) {
            keys.push_back(MakeKey(in.Int(16384), in.Byte()));
        }
        SortUnique(keys);
        if (keys.empty()) {
            continue;
        }

        const auto writerPDiskCtx = MakePDiskCtx();
        const TLevelSegmentPtr written = BuildPersistedSst(vctx, writerPDiskCtx, levelIndex, keys, nextChunk++, in.Byte(), chunks);

        NActors::TTestActorRuntime runtime(1, false);
        runtime.Initialize(MakeRuntimeEgg());
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        const auto pdiskActor = runtime.Register(new TFakePDiskActor(chunks));
        const auto recipient = runtime.AllocateEdgeActor();
        const auto loaderPDiskCtx = MakePDiskCtx(pdiskActor);

        TLevelSegmentPtr diskOnly = MakeIntrusive<TLevelSegment>(vctx, written->GetEntryPoint());
        runtime.Register(new TLevelSegmentLoader(vctx, loaderPDiskCtx, diskOnly, recipient, "fuzz-loader"));

        auto loaded = runtime.GrabEdgeEventRethrow<THullSegLoaded>(recipient, TDuration::Seconds(1));
        Y_ABORT_UNLESS(loaded);
        Y_ABORT_UNLESS(loaded->Get()->LevelSegment == diskOnly);
        Y_ABORT_UNLESS(ReadSegmentKeys(diskOnly) == keys);
        Y_ABORT_UNLESS(diskOnly->Elements() == keys.size());
        Y_ABORT_UNLESS(diskOnly->AssignedSstId == written->AssignedSstId);
        Y_ABORT_UNLESS(diskOnly->GetEntryPoint() == written->GetEntryPoint());
        Y_ABORT_UNLESS(diskOnly->AllChunks);
        Y_ABORT_UNLESS(diskOnly->IndexParts.size() == 1);
        Y_ABORT_UNLESS(diskOnly->IndexParts.front() == written->GetEntryPoint());
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput in(data, size);
    FuzzSstLoader(in);
    return 0;
}
