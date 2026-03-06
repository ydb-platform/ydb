#pragma once

#include "ddisk_actor.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <util/generic/overloaded.h>
#include <ydb/core/util/stlog.h>

#include <cerrno>

namespace NKikimr::NDDisk {

#if defined(__linux__)

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

    virtual void OnComplete(NActors::TActorSystem* actorSystem) noexcept;
    virtual void OnDrop() noexcept;

    virtual void Reply(NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status);

    void PrepareWrite(TRope&& data, ui64 offset);
    void PrepareRead(size_t size, ui64 offset);

    void SetSpan(NWilson::TSpan&& span) { Span = std::move(span); }
    NWilson::TSpan& GetSpan() { return Span; }

    void SetCookie(ui64 cookie) { Cookie = cookie; }
    ui64 GetCookie() const { return Cookie; }

    const TActorId& GetDDiskId() const { return DDiskId; }

protected:
    TRcBuf&& ExtractAlignedDataHolder() {
        return std::move(AlignedDataHolder);
    }

private:
    TActorId DDiskId;

    TActorId OriginalRequester;
    TActorId InterconnectSession;

    ui64 Cookie = 0;

    NWilson::TSpan Span;

    TRcBuf AlignedDataHolder;

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

    virtual void Reply(NActors::TActorSystem* actorSystem, NKikimrBlobStorage::NDDisk::TReplyStatus::E status) override;

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

#endif
}
