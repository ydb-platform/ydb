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
        bool IsRead = false;
        ui32 Size = 0;
        ui64 DiskOffset = 0;
        ui32 BufferOffset = 0;
        TRcBuf AlignedDataHolder;

        // shared with DDisk actor
        std::atomic<ui32>& InFlightCount;

        TDirectIoOpBase(std::atomic<ui32>& inFlightCount)
            : InFlightCount(inFlightCount)
        {
            OnComplete = &TDirectIoOpBase::OnDirectIoComplete;
            OnDrop = &TDirectIoOpBase::OnDirectIoDrop;
        }

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
                auto ev = std::make_unique<TEvPrivate::TEvShortIO>(std::move(guard));
                actorSystem->Send(new IEventHandle(ddiskId, {}, ev.release()));
                return;
            }

            if (Y_UNLIKELY(op->Result == 0 && remaining > 0)) {
                op->Result = -EIO;
            }

            op->Reply();
            op->InFlightCount.fetch_sub(1, std::memory_order_relaxed);

        }

        static void OnDirectIoDrop(NPDisk::TUringOperation* baseOp) noexcept {
            auto* op = static_cast<TDirectIoOpBase*>(baseOp);
            std::unique_ptr<TDirectIoOpBase> guard(op);
            op->Drop();
            op->InFlightCount.fetch_sub(1, std::memory_order_relaxed);
        }

        virtual void Drop() = 0;
        virtual void Reply() = 0;
    };

#endif
}
