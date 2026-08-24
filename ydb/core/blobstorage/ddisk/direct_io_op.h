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
// Allocated via TDDiskActor::AllocateOp (pool-backed) or new,
// recycled back to the pool via SelfRecycle / TDDiskActor::ReturnOp.
class TDDiskActor::TDirectIoOpBase : public NPDisk::TUringOperationBase {
public:
    explicit TDirectIoOpBase(TDDiskActor& actor);

    virtual ~TDirectIoOpBase();

    // IO uring callbacks
    virtual void OnComplete(NActors::TActorSystem* actorSystem) noexcept override final;
    virtual void OnDrop() noexcept override final;

    // reply should not access raw uring result field – use just status and data if status OK
    virtual void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
        TString reason = {}) noexcept = 0;
    virtual bool IsIntegrityIo() const noexcept { return false; }

    virtual void ClearForRecycle() noexcept;

    void PrepareWrite(TRope&& data, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset);
    void PrepareRead(size_t size, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset);

    void Reinit(const IEventHandle* ev = nullptr);

    void SetSpan(NWilson::TSpan&& span) { Span = std::move(span); }
    NWilson::TSpan& GetSpan() { return Span; }
    NWilson::TSpan ExtractSpan() { return std::move(Span); }

    void SetCookie(ui64 cookie) { Cookie = cookie; }
    ui64 GetCookie() const { return Cookie; }

    // Read-path integrity zero mask (TIntegrityManager::TReadPlan::Mixed): bit i covers the i-th
    // IntegrityUnitSize block of the read range; unset bits are zero-filled before replying. Must
    // live in the op because the reply happens on the uring completion thread.
    void SetReadUsedBlocksMask(TDynBitMap&& usedBlocks) { ReadUsedBlocksMask.emplace(std::move(usedBlocks)); }

    const TActorId& GetDDiskId() const { return DDiskId; }
    const TActorId& GetOriginalRequester() const { return OriginalRequester; }
    const TActorId& GetInterconnectSession() const { return InterconnectSession; }

    TRope ExtractData();

    double TimePassed() const;

public:
    // methods to use when we fallback to PDisk instead of direct I/O

    TChunkIdx GetChunkIdx() const { return ChunkIdx; }
    ui32 GetChunkOffset() const { return ChunkOffsetInBytes; }

    using NPDisk::TUringOperationBase::SetResult;

    void SetResult(i32 result, TRope&& data);

protected:
    TDDiskActor& Actor;
    const TActorId DDiskId;

    virtual void SelfRecycle() noexcept { delete this; }

    // Zero-fills the blocks of freshly read data whose ReadUsedBlocksMask bits are unset.
    void ApplyReadUsedBlocksMask(TRope& data) noexcept;

private:
    NHPTimer::STime StartTs;

    TActorId OriginalRequester;
    TActorId InterconnectSession;

    ui64 Cookie = 0;

    // PDisk fallback data
    TChunkIdx ChunkIdx = 0;
    ui32 ChunkOffsetInBytes = 0;

    NWilson::TSpan Span;

    TRcBuf AlignedDataHolder;
    std::optional<TRope> Data;

    std::optional<TDynBitMap> ReadUsedBlocksMask;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TDDiskIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDDiskActor::TDDiskIoOp final : public TDDiskActor::TDirectIoOpBase {
public:
    explicit TDDiskIoOp(TDDiskActor& actor)
        : TDirectIoOpBase(actor)
    {}

    void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
        TString reason = {}) noexcept override;

    void ClearForRecycle() noexcept override;
    void SelfRecycle() noexcept override;

    void SetChunkKey(ui64 tabletId, ui64 vChunkIndex) {
        TabletId = tabletId;
        VChunkIndex = vChunkIndex;
        HasChunkKey = true;
    }

    void SetIntegrityOperationId(ui64 operationId) {
        IntegrityOperationId = operationId;
    }

    void SetReadChecksums(std::vector<ui64> checksums) {
        Checksums = std::move(checksums);
    }

private:
    ui64 TabletId = 0;
    ui64 VChunkIndex = 0;
    bool HasChunkKey = false;
    ui64 IntegrityOperationId = 0;
    std::vector<ui64> Checksums;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TPersistentBufferPartIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDDiskActor::TPersistentBufferPartIoOp final : public TDDiskActor::TDirectIoOpBase {
public:
    explicit TPersistentBufferPartIoOp(TDDiskActor& actor)
        : TDirectIoOpBase(actor)
    {}

    void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
        TString reason = {}) noexcept override;

    void ClearForRecycle() noexcept override;
    void SelfRecycle() noexcept override;

    void SetPartCookie(ui64 partCookie) {
        PartCookie = partCookie;
    }

    void SetIsErase(bool isErase) {
        IsErase = isErase;
    }

    void SetIsRestore(bool isRestore) {
        IsRestore = isRestore;
    }

private:
    ui64 PartCookie = 0;
    bool IsErase = false;
    bool IsRestore = false;
};

class TDDiskActor::TInternalSyncWriteOp final : public TDDiskActor::TDirectIoOpBase {
public:
    explicit TInternalSyncWriteOp(TDDiskActor& actor)
        : TDirectIoOpBase(actor)
    {}

    void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
        TString reason = {}) noexcept override;

    void ClearForRecycle() noexcept override;
    void SelfRecycle() noexcept override;

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

    void SetIntegrityOperationId(ui64 operationId) {
        IntegrityOperationId = operationId;
    }

private:
    ui64 SyncId = 0;
    ui64 RequestId = 0;
    ui64 SegmentBegin = 0;
    ui64 SegmentEnd = 0;
    ui64 IntegrityOperationId = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TIntegrityIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Executes one TIntegrityManager TWriteIo / TReadIo and posts TEvPrivate::TEvIntegrityIoResult
// back to the actor.
class TDDiskActor::TIntegrityIoOp final : public TDDiskActor::TDirectIoOpBase {
public:
    explicit TIntegrityIoOp(TDDiskActor& actor)
        : TDirectIoOpBase(actor)
    {}

    void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
        TString reason = {}) noexcept override;
    bool IsIntegrityIo() const noexcept override { return true; }

    void ClearForRecycle() noexcept override;
    void SelfRecycle() noexcept override;

    void SetIoId(ui64 ioId) {
        IoId = ioId;
    }

private:
    ui64 IoId = 0;
};

} // namespace NKikimr::NDDisk
