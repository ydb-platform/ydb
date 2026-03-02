#include "ddisk_actor.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <util/generic/overloaded.h>
#include <ydb/core/util/stlog.h>

#include <cerrno>

namespace NKikimr::NDDisk {

#if defined(__linux__)

    // Direct I/O operation context passed through io_uring.
    // Allocated on the actor thread (new), freed in OnComplete callback (delete).
    struct TDDiskActor::TDirectIoOpBase : NPDisk::TUringOperation {

        TActorId DDiskId;
        ui64 Cookie = 0;                    // original event cookie
        bool IsRead = false;
        ui32 Size = 0;
        ui64 DiskOffset = 0;
        ui32 BufferOffset = 0;
        TRcBuf AlignedDataHolder;

        // shared with DDisk actor
        std::atomic<ui32>& InFlightCount;
        TCounters& Counters;

        TDirectIoOpBase(std::atomic<ui32>& inFlightCount, TCounters& counters);

        virtual ~TDirectIoOpBase();

        // a poor error mapping
        static NKikimrBlobStorage::NDDisk::TReplyStatus::E UringErrorToStatus(i32 result, bool isRead) {
            const int err = -result;
            switch (err) {
                case EAGAIN:
#if EAGAIN != EWOULDBLOCK
                case EWOULDBLOCK:
#endif
                case ENOSPC:
                case ENOMEM:
                    return NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED;
                case EINVAL:
                    return NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST;
                case EIO:
                    return isRead
                        ? NKikimrBlobStorage::NDDisk::TReplyStatus::LOST_DATA
                        : NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
                default:
                    return NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
            }
        }

        static void OnDirectIoComplete(NPDisk::TUringOperation* baseOp, NActors::TActorSystem* actorSystem) noexcept {
            auto* op = static_cast<TDirectIoOpBase*>(baseOp);
            std::unique_ptr<TDirectIoOpBase> guard(op);

            const ui32 remaining = op->Size - op->BufferOffset;
            if (Y_UNLIKELY(op->Result > 0 && static_cast<ui32>(op->Result) < remaining)) {
                // this should be a very rare case
                op->BufferOffset += op->Result;
                op->DiskOffset += op->Result;
                if (op->IsRead) {
                    op->Counters.DirectIO.ShortReads->Inc();
                } else {
                    op->Counters.DirectIO.ShortWrites->Inc();
                }
                auto ddiskId = op->DDiskId;
                auto ev = std::make_unique<TDDiskActor::TEvPrivate::TEvShortIO>(std::move(guard));
                actorSystem->Send(new IEventHandle(ddiskId, {}, ev.release()));
                return;
            }

            if (Y_UNLIKELY(op->Result == 0 && remaining > 0)) {
                op->Result = -EIO;
            }

            op->Reply(actorSystem, false);
        }

        static void OnDirectIoDrop(NPDisk::TUringOperation* baseOp) noexcept {
            auto* op = static_cast<TDirectIoOpBase*>(baseOp);
            std::unique_ptr<TDirectIoOpBase> guard(op);
        }

        virtual void Reply(NActors::TActorSystem* actorSystem, bool shortIoError) = 0;

        void SetData(TRope&& data);
    };

    struct TDDiskActor::TSingleDirectIoOp : TDDiskActor::TDirectIoOpBase {
        TActorId Sender;                    // original requester
        TActorId InterconnectSession;

        TSingleDirectIoOp(std::atomic<ui32>& inFlightCount, TCounters& counters)
            : TDirectIoOpBase(inFlightCount, counters)
        {}
    };

    struct TDDiskActor::TDDiskIoOp : TDDiskActor::TSingleDirectIoOp {
        NWilson::TSpan Span;

        TDDiskIoOp(std::atomic<ui32>& inFlightCount, TCounters& counters)
            : TSingleDirectIoOp(inFlightCount, counters)
        {}

        virtual void Reply(NActors::TActorSystem* actorSystem, bool shortIoError) override;
    };

    struct TDDiskActor::TPersistentBufferPartIoOp : TDDiskActor::TDirectIoOpBase {
        ui64 PartCookie;
        bool IsErase = false;

        TPersistentBufferPartIoOp(std::atomic<ui32>& inFlightCount, TCounters& counters)
            : TDirectIoOpBase(inFlightCount, counters)
        {}

        virtual void Reply(NActors::TActorSystem* actorSystem, bool shortIoError) override;
    };

#endif
}
