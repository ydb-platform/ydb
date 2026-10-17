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
    virtual void OnDrop(NActors::TActorSystem* actorSystem) noexcept override final;

    // reply should not access raw uring result field – use just status and data if status OK
    virtual void Reply(
        NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
        TString reason = {}) noexcept = 0;
    ui32 RetryCount = 0;
    static constexpr ui32 MaxResubmissions = 20;
    virtual bool IsRestoreIo() const noexcept { return false; }
    virtual bool IsCriticalDDiskIo() const noexcept {
        return false;
    }

    virtual void ClearForRecycle() noexcept;

    void PrepareWrite(TRope&& data, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset);
    void PrepareRead(size_t size, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset);

    void Reinit();

    void SetCookie(ui64 cookie) { Cookie = cookie; }
    ui64 GetCookie() const { return Cookie; }
    const TActorId& GetDDiskId() const { return DDiskId; }

    TRope ExtractData();
    TReadPayload ExtractReadPayload();

    double TimePassed() const;

    TOpCountersBase& GetCounters() const;

public:
    // methods to use when we fallback to PDisk instead of direct I/O

    TChunkIdx GetChunkIdx() const { return ChunkIdx; }
    ui32 GetChunkOffset() const { return ChunkOffsetInBytes; }

    using NPDisk::TUringOperationBase::SetResult;

    void SetResult(i64 result, TRope&& data);

protected:
    TDDiskActor& Actor;
    const TActorId DDiskId;

    virtual void SelfRecycle() noexcept { delete this; }
    bool PrepareRetry() noexcept;


private:
    class TCompletionGuard;
    void AccountShortIo() noexcept;

    NHPTimer::STime StartTs;

    ui64 Cookie = 0;

    // PDisk fallback data
    TChunkIdx ChunkIdx = 0;
    ui32 ChunkOffsetInBytes = 0;

    TRcBuf AlignedDataHolder;
    std::optional<TRope> Data;

};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TDDiskIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Data-path operation of one read, write, or metadata read. The shared callback
// owns its result slots even when forced teardown destroys the requesting frame.
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

    void Reinit() {
        TDirectIoOpBase::Reinit();
        Callback.reset();
        MetadataIndex.reset();
        Critical = false;
    }

    // Bound by the batch when it submits the operation, not when it is prepared.
    void SetCallback(std::shared_ptr<TBatchedIOAwaiter> callback) {
        Callback = std::move(callback);
    }

    // Routes the completion to that MetadataResults slot; client data has no index.
    void SetMetadataIndex(size_t metadataIndex) {
        MetadataIndex = metadataIndex;
    }

    // Metadata reads/writes and zero formatting share the critical retry policy.
    void SetCritical() {
        Critical = true;
    }

    bool IsCriticalDDiskIo() const noexcept override {
        return Critical;
    }

private:
    std::shared_ptr<TBatchedIOAwaiter> Callback;
    std::optional<size_t> MetadataIndex;
    bool Critical = false;
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

    bool IsRestoreIo() const noexcept override { return IsRestore; }

    void SetIsRestore(bool isRestore) {
        IsRestore = isRestore;
    }

private:
    ui64 PartCookie = 0;
    bool IsErase = false;
    bool IsRestore = false;
};

} // namespace NKikimr::NDDisk
