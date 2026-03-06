#pragma once

#include "ddisk_actor.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <util/generic/overloaded.h>
#include <ydb/core/util/stlog.h>

#include <cerrno>
#include <optional>

namespace NKikimr::NDDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TDirectIoOpBase
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Direct I/O operation context passed through io_uring.
// Allocated on the actor thread (new), freed in OnComplete callback (delete).
class TDDiskActor::TDirectIoOpBase : public NPDisk::TUringOperationBase {
public:
    TDirectIoOpBase(const TActorId& ddiskId,
                    std::atomic<ui32>& inFlightCount,
                    TCounters& counters,
                    const IEventHandle* ev = nullptr);

    virtual ~TDirectIoOpBase();

    // IO uring callbacks
    virtual void OnComplete(NActors::TActorSystem* actorSystem) noexcept override final;
    virtual void OnDrop() noexcept override final;

    // reply should not access raw uring result field – use just status and data if status OK
    virtual void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status) noexcept;

    void PrepareWrite(TRope&& data, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset);
    void PrepareRead(size_t size, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset);

    void SetSpan(NWilson::TSpan&& span) { Span = std::move(span); }
    NWilson::TSpan& GetSpan() { return Span; }

    void SetCookie(ui64 cookie) { Cookie = cookie; }
    ui64 GetCookie() const { return Cookie; }

    const TActorId& GetDDiskId() const { return DDiskId; }

    TRope ExtractData();

public:
    // methods to use when we fallback to PDisk instead of direct I/O

    TChunkIdx GetChunkIdx() const { return ChunkIdx; }
    ui32 GetChunkOffset() const { return ChunkOffsetInBytes; }

    using NPDisk::TUringOperationBase::SetResult;

    void SetResult(i32 result, TRope&& data);

private:
    TActorId DDiskId;

    TActorId OriginalRequester;
    TActorId InterconnectSession;

    ui64 Cookie = 0;

    // PDisk fallback data
    TChunkIdx ChunkIdx = 0;
    ui32 ChunkOffsetInBytes = 0;

    NWilson::TSpan Span;

    TRcBuf AlignedDataHolder;
    std::optional<TRope> Data;

    // shared with DDisk actor
    std::atomic<ui32>& InFlightCount;
    TCounters& Counters;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TPersistentBufferPartIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDDiskActor::TPersistentBufferPartIoOp final : public TDDiskActor::TDirectIoOpBase {
public:
    TPersistentBufferPartIoOp(const TActorId& ddiskId, std::atomic<ui32>& inFlightCount, TCounters& counters)
        : TDirectIoOpBase(ddiskId, inFlightCount, counters)
    {}

    virtual void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status) noexcept override;

    void SetPartCookie(ui64 partCookie) {
        PartCookie = partCookie;
    }

    void SetIsErase(bool isErase) {
        IsErase = isErase;
    }

private:
    ui64 PartCookie = 0;
    bool IsErase = false;
};

class TDDiskActor::TInternalSyncWriteOp final : public TDDiskActor::TDirectIoOpBase {
public:
    TInternalSyncWriteOp(const TActorId& ddiskId, std::atomic<ui32>& inFlightCount, TCounters& counters)
        : TDirectIoOpBase(ddiskId, inFlightCount, counters)
    {}

    virtual void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status) noexcept override;

    void SetRequestId(ui64 requestId) {
        RequestId = requestId;
    }

    void SetSegment(ui64 begin, ui64 end) {
        SegmentBegin = begin;
        SegmentEnd = end;
    }

    void SetSyncId(ui64 syncId) {
        SyncId = syncId;
    }

private:
    ui64 SyncId = 0;
    ui64 RequestId = 0;
    ui64 SegmentBegin = 0;
    ui64 SegmentEnd = 0;
};

} // namespace NKikimr::NDDisk
