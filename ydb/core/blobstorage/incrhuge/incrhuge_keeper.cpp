#include "incrhuge_keeper.h"
#include "incrhuge.h"
#include "incrhuge_keeper_recovery_scan.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/queue.h>
#include <util/generic/bitmap.h>

using namespace NActors;

namespace NKikimr {
    namespace NIncrHuge {

        TKeeper::TKeeper(const TKeeperSettings& settings)
            : State(settings)
            , Allocator(*this)
            , Defragmenter(*this)
            , Deleter(*this)
            , Logger(*this)
            , Reader(*this)
            , Recovery(*this)
            , Writer(*this)
        {
        }

        void TKeeper::Bootstrap(const TActorContext& ctx) {
            // send yard init message
            TVDiskID myVDiskId(TGroupId::FromValue(~0), ~0, 'H', 'I', 'K');
            ctx.Send(State.Settings.PDiskActorId, new NPDisk::TEvYardInit(State.Settings.InitOwnerRound, myVDiskId,
                    State.Settings.PDiskGuid, ctx.SelfID));
            Become(&TKeeper::StateFunc);
        }

        void *TKeeper::RegisterYardCallback(std::unique_ptr<IEventCallback>&& callback) {
            void *id = reinterpret_cast<void *>(NextCallbackId++);
            CallbackMap.emplace(id, std::move(callback));
            return id;
        }

        void TKeeper::Handle(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx) {
            NPDisk::TEvYardInitResult *msg = ev->Get();
            State.PDiskParams = std::move(msg->PDiskParams);

            // calculate blocks size
            State.BlockSize = (State.Settings.UnalignedBlockSize + State.PDiskParams->AppendBlockSize - 1);
            State.BlockSize -= State.BlockSize % State.PDiskParams->AppendBlockSize;

            // calculate number of blocks in chunk
            State.BlocksInChunk = State.PDiskParams->ChunkSize / State.BlockSize;

            State.BlocksInMinBlob = (State.Settings.MinHugeBlobInBytes + State.BlockSize - 1) / State.BlockSize;

            State.MaxBlobsPerChunk = State.BlocksInChunk / State.BlocksInMinBlob;

            for (;;) {
                const ui32 indexSize = sizeof(TBlobIndexHeader) + State.MaxBlobsPerChunk * sizeof(TBlobIndexRecord);
                State.BlocksInIndexSection = (indexSize + State.BlockSize - 1) / State.BlockSize;
                State.BlocksInDataSection = State.BlocksInChunk - State.BlocksInIndexSection;
                if (State.BlocksInDataSection >= State.MaxBlobsPerChunk * State.BlocksInMinBlob) {
                    break;
                } else {
                    --State.MaxBlobsPerChunk;
                }
            }

            LOG_DEBUG(ctx, NKikimrServices::BS_INCRHUGE, "BlockSize# %" PRIu32 " BlocksInChunk# %" PRIu32
                    " BlocksInMinBlob# %" PRIu32 " MaxBlobsPerChunk# %" PRIu32 " BlocksInDataSection# %" PRIu32
                    " BlocksInIndexSection# %" PRIu32, State.BlockSize, State.BlocksInChunk, State.BlocksInMinBlob,
                    State.MaxBlobsPerChunk, State.BlocksInDataSection, State.BlocksInIndexSection);

            auto chunksIt = msg->StartingPoints.find(TLogSignature::SignatureIncrHugeChunks);
            auto deletesIt = msg->StartingPoints.find(TLogSignature::SignatureIncrHugeDeletes);
            auto end = msg->StartingPoints.end();
            Recovery.ApplyYardInit(msg->Status, chunksIt != end ? &chunksIt->second : nullptr,
                    deletesIt != end ? &deletesIt->second : nullptr, ctx);
            UpdateStatusFlags(msg->StatusFlags, ctx);
        }

        void TKeeper::Handle(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx) {
            NPDisk::TEvChunkReserveResult *msg = ev->Get();
            Allocator.ApplyAllocate(msg->Status, std::move(msg->ChunkIds), ctx);
            UpdateStatusFlags(msg->StatusFlags, ctx);
        }

        void TKeeper::Handle(NPDisk::TEvChunkWriteResult::TPtr& ev, const TActorContext& ctx) {
            NPDisk::TEvChunkWriteResult *msg = ev->Get();
            InvokeCallback(msg->Cookie, msg->Status, msg, ctx);
            UpdateStatusFlags(msg->StatusFlags, ctx);
        }

        void TKeeper::Handle(NPDisk::TEvChunkReadResult::TPtr& ev, const TActorContext& ctx) {
            NPDisk::TEvChunkReadResult *msg = ev->Get();
            InvokeCallback(msg->Cookie, msg->Status, msg, ctx);
            UpdateStatusFlags(msg->StatusFlags, ctx);
        }

        void TKeeper::Handle(NPDisk::TEvLogResult::TPtr& ev, const TActorContext& ctx) {
            NPDisk::TEvLogResult *msg = ev->Get();
            for (const auto& item : msg->Results) {
                InvokeCallback(item.Cookie, msg->Status, msg, ctx);
            }
            UpdateStatusFlags(msg->StatusFlags, ctx);
        }

        void TKeeper::InvokeCallback(void *cookie, NKikimrProto::EReplyStatus status, IEventBase *msg,
                const TActorContext& ctx) {
            auto it = CallbackMap.find(cookie);
            Y_ABORT_UNLESS(it != CallbackMap.end());
            it->second->Apply(status, msg, ctx);
            CallbackMap.erase(it);
        }

        void TKeeper::UpdateStatusFlags(NPDisk::TStatusFlags flags, const TActorContext& ctx) {
            const EDiskState newState =
                flags & NKikimrBlobStorage::StatusDiskSpaceRed    ? EDiskState::SpaceRed    :
                flags & NKikimrBlobStorage::StatusDiskSpaceOrange ? EDiskState::SpaceOrange :
                flags & NKikimrBlobStorage::StatusDiskSpaceLightYellowMove ? EDiskState::SpaceYellow : EDiskState::SpaceGreen;

            if (newState != State.DiskState) {
                // kick defragmenter when it's going to be worse
                bool kickDefrag = newState < State.DiskState;
                State.DiskState = newState;
                if (kickDefrag) {
                    for (const auto& pair : State.Chunks) {
                        Defragmenter.UpdateChunkState(pair.first, ctx);
                    }
                }
            }
        }

        void TKeeper::Handle(TEvIncrHugeCallback::TPtr& ev, const TActorContext& ctx) {
            TEvIncrHugeCallback *msg = ev->Get();
            Y_ABORT_UNLESS(msg->Callback);
            msg->Callback->Apply(NKikimrProto::OK, msg, ctx);
        }

        void TKeeper::Handle(TEvIncrHugeReadLogResult::TPtr& ev, const TActorContext& ctx) {
            Recovery.ApplyReadLog(ev->Sender, *ev->Get(), ctx);
        }

        void TKeeper::Handle(TEvIncrHugeScanResult::TPtr& ev, const TActorContext& ctx) {
            switch (EScanCookie(ev->Cookie)) {
                case EScanCookie::Recovery:
                    Recovery.ApplyScan(ev->Sender, *ev->Get(), ctx);
                    break;

                case EScanCookie::Defrag:
                    Defragmenter.ApplyScan(ev->Sender, *ev->Get(), ctx);
                    break;

                default:
                    Y_ABORT("unexpected case");
            }
        }

        void TKeeper::HandlePoison(const TActorContext& ctx) {
            for (const TActorId& actorId : State.ChildActors) {
                ctx.Send(actorId, new TEvents::TEvPoisonPill);
            }
            Die(ctx);
        }

        STRICT_STFUNC(TKeeper::StateFunc,
            // client <-> keeper interface
            HFunc(TEvIncrHugeInit, Recovery.HandleInit)
            HFunc(TEvIncrHugeWrite, Writer.HandleWrite)
            HFunc(TEvIncrHugeRead, Reader.HandleRead)
            HFunc(TEvIncrHugeDelete, Deleter.HandleDelete)
//            HFunc(TEvIncrHugeHarakiri, OwnerManager.HandleHarakiri)
            HFunc(TEvIncrHugeControlDefrag, Defragmenter.HandleControlDefrag)


            // keeper <-> yard interface
            HFunc(NPDisk::TEvYardInitResult, Handle)
            HFunc(NPDisk::TEvChunkReserveResult, Handle)
            HFunc(NPDisk::TEvChunkWriteResult, Handle)
            HFunc(NPDisk::TEvChunkReadResult, Handle)
            HFunc(NPDisk::TEvLogResult, Handle)

            // internal events
            HFunc(TEvIncrHugeCallback, Handle)
            HFunc(TEvIncrHugeReadLogResult, Handle)
            HFunc(TEvIncrHugeScanResult, Handle)

            // shutdown handling
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
        )

        IActor *CreateIncrHugeKeeper(const TKeeperSettings& settings) {
            return new TKeeper(settings);
        }


    } // NIncrHuge
} // NKikimr
