#pragma once

#include "defs.h"
#include "blobstorage_repl.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullwritesst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.h>

namespace NKikimr {

    class TReplSstStreamWriter
    {
        using TWriter = TLogoBlobsSst::TWriter;

    public:
        struct TRecoveredBlobInfo {
            TLogoBlobID Id;
            TRope Data;
            bool IsHugeBlob;
            NMatrix::TVectorType LocalParts;

            TRecoveredBlobInfo(const TLogoBlobID& id, TRope&& data, bool isHugeBlob, NMatrix::TVectorType localParts)
                : Id(id)
                , Data(std::move(data))
                , IsHugeBlob(isHugeBlob)
                , LocalParts(localParts)
            {}
        };

        enum class EState {
            INVALID,               // impossible state
            STOPPED,               // doing nothing
            PDISK_MESSAGE_PENDING, // we have a message for PDisk
            NOT_READY,             // we are waiting for some kind of external action (expecting response from PDisk)
            COLLECT,               // ready for new blobs
            COMMIT_PENDING,        // we have a commit message for Skeleton
            WAITING_FOR_COMMIT,    // waiting for commit operation to finish
            ERROR,                 // something gone wrong, further operation impossible
        };

        static TString StateToString(EState state) {
            switch (state) {
            case EState::INVALID:
                return "INVALID";
            case EState::STOPPED:
                return "STOPPED";
            case EState::PDISK_MESSAGE_PENDING:
                return "PDISK_MESSAGE_PENDING";
            case EState::NOT_READY:
                return "NOT_READY";
            case EState::COLLECT:
                return "COLLECT";
            case EState::COMMIT_PENDING:
                return "COMMIT_PENDING";
            case EState::WAITING_FOR_COMMIT:
                return "WAITING_FOR_COMMIT";
            case EState::ERROR:
                return "ERROR";
            }
        }

        enum class EOutputState {
            INVALID,
            INTERMEDIATE_CHUNK,
            FLUSH_CHUNK,
            FLUSH_LAST_CHUNK,
        };

    public:
        TReplSstStreamWriter(
                std::shared_ptr<TReplCtx> replCtx,
                TIntrusivePtr<THullDs> hullDs)
            : ReplCtx(std::move(replCtx))
            , HullDs(std::move(hullDs))
            , State(EState::STOPPED)
            , MinReservedChunksCount(ReplCtx->VDiskCfg->HullSstSizeInChunksFresh)
            , Merger(ReplCtx->VCtx->Top->GType, ReplCtx->GetAddHeader())
            , Arena(&TRopeArenaBackend::Allocate)
        {}

        void Begin() {
            Y_VERIFY_S(State == EState::STOPPED, ReplCtx->VCtx->VDiskLogPrefix);

            // ensure that we have no pending message and create new one to allocate some chunks for replicated SST
            Y_VERIFY_S(!PendingPDiskMsg, ReplCtx->VCtx->VDiskLogPrefix);
            PendingPDiskMsg = std::make_unique<NPDisk::TEvChunkReserve>(ReplCtx->PDiskCtx->Dsk->Owner,
                ReplCtx->PDiskCtx->Dsk->OwnerRound, MinReservedChunksCount - ReservedChunks.size());
            State = EState::PDISK_MESSAGE_PENDING;

            // initialize state
            PrevID = TLogoBlobID();
            NumRecoveredBlobs = 0;
            OutputState = EOutputState::INVALID;
            FlushFinished = false;
        }

        std::unique_ptr<IEventBase> GetPendingPDiskMsg() {
            Y_VERIFY_S(State == EState::PDISK_MESSAGE_PENDING, ReplCtx->VCtx->VDiskLogPrefix);
            std::unique_ptr<IEventBase> msg = std::move(PendingPDiskMsg);

            if (auto write = dynamic_cast<NPDisk::TEvChunkWrite*>(msg.get())) {
                if (write->PartsPtr) {
                    *ReplCtx->PDiskWriteBytes += write->PartsPtr->ByteSize();
                }

                // we are issuing write message; we should count it as in flight one and take some actions according
                // to the current state
                ++WritesInFlight;
                if (WritesInFlight == MaxWritesInFlight) {
                    // in any output state we shall wait for at least one message to finish
                    State = EState::NOT_READY;
                } else {
                    switch (OutputState) {
                        case EOutputState::INTERMEDIATE_CHUNK:
                            // we are ready to process further blobs, so return to COLLECT state
                            State = EState::COLLECT;
                            break;

                        case EOutputState::FLUSH_CHUNK:
                            // we are flushing index right now, so continue with this job; FlushNextPart() will internally
                            // call IssueWriteCmd if needed, and it will fill in next PendingPDiskMsg and set the State
                            // accordingly
                            FlushNextPart();
                            break;

                        case EOutputState::FLUSH_LAST_CHUNK:
                            // we were writing last chunk part now and shall wait for all pending operations to complete
                            State = EState::NOT_READY;
                            break;

                        default:
                            Y_ABORT("unexpected OutputState");
                    }
                }
            } else if (dynamic_cast<NPDisk::TEvChunkReserve*>(msg.get())) {
                // we are issuing chunk allocation message and shall wait for it to finish
                State = EState::NOT_READY;
            } else {
                Y_ABORT("unexpected message type");
            }

            return msg;
        }

        void Apply(NPDisk::TEvChunkReserveResult* ev) {
            Y_VERIFY_S(State == EState::NOT_READY, ReplCtx->VCtx->VDiskLogPrefix);

            if (ev->Status != NKikimrProto::OK) {
                State = EState::ERROR;
                return;
            }

            for (ui32 chunkId : ev->ChunkIds) {
                ReservedChunks.push_back(chunkId);
            }

            // create new writer
            Y_VERIFY_S(!Writer, ReplCtx->VCtx->VDiskLogPrefix);
            Writer = std::make_unique<TWriter>(ReplCtx->VCtx, EWriterDataType::Replication, 1, ReplCtx->PDiskCtx->Dsk->Owner,
                ReplCtx->PDiskCtx->Dsk->OwnerRound, ReplCtx->PDiskCtx->Dsk->ChunkSize,
                ReplCtx->PDiskCtx->Dsk->AppendBlockSize, ReplCtx->PDiskCtx->Dsk->BulkWriteBlockSize,
                HullDs->LogoBlobs->AllocSstId(), true, ReservedChunks, Arena, ReplCtx->GetAddHeader());

            // start collecting blobs
            State = EState::COLLECT;
        }

        bool AddRecoveredBlob(TRecoveredBlobInfo& record) {
            Y_VERIFY_S(State == EState::COLLECT, ReplCtx->VCtx->VDiskLogPrefix);
            Y_VERIFY_S(record.Id > PrevID, ReplCtx->VCtx->VDiskLogPrefix);
            Y_VERIFY_S(!record.Id.PartId(), ReplCtx->VCtx->VDiskLogPrefix);

            TMemRecLogoBlob memRec(TIngress::CreateFromRepl(ReplCtx->VCtx->Top.get(), ReplCtx->VCtx->ShortSelfVDisk,
                record.Id, record.LocalParts));

            // set merger state to match generated blob
            memRec.SetType(TBlobType::DiskBlob);
            memRec.SetDiskBlob(TDiskPart(0, 0, record.Data.GetSize()));
            Merger.FinishFromBlob();

            TDiskPart preallocatedLocation;
            const bool success = Writer->PushIndexOnly(record.Id, memRec, &Merger, &preallocatedLocation);
            if (success) {
                Y_VERIFY_DEBUG_S(Merger.GetCollectTask().Reads.empty(), ReplCtx->VCtx->VDiskLogPrefix);
                const TDiskPart writtenLocation = Writer->PushDataOnly(std::move(record.Data));
                Y_VERIFY_S(writtenLocation == preallocatedLocation, ReplCtx->VCtx->VDiskLogPrefix);

                if (auto msg = Writer->GetPendingMessage()) {
                    IssueWriteCmd(std::move(msg), EOutputState::INTERMEDIATE_CHUNK);
                }
                PrevID = record.Id;
                ++NumRecoveredBlobs;
            } else {
                // out of space in current chunks, have to start flushing; logoblob is not written
                FlushNextPart();
            }

            Merger.Clear();

            return success;
        }

        void Apply(NPDisk::TEvChunkWriteResult* ev) {
            if (ev->Status != NKikimrProto::OK) {
                State = EState::ERROR;
                return;
            }

            Y_VERIFY_S(WritesInFlight > 0, ReplCtx->VCtx->VDiskLogPrefix);
            --WritesInFlight;

            // if we were blocked for some reason when issuing last message, try to determine what to do next
            if (State == EState::NOT_READY) {
                switch (OutputState) {
                    case EOutputState::INTERMEDIATE_CHUNK:
                        State = EState::COLLECT; // continue collecting blobs
                        break;

                    case EOutputState::FLUSH_CHUNK:
                        FlushNextPart(); // continue flushing data
                        break;

                    case EOutputState::FLUSH_LAST_CHUNK:
                        if (!WritesInFlight) {
                            OnFlushComplete();
                        }
                        break;

                    default:
                        Y_ABORT("incorrect output state");
                }
            }
        }

        void Finish() {
            Y_VERIFY_S(State == EState::COLLECT, ReplCtx->VCtx->VDiskLogPrefix
                << "unexpected State# " << static_cast<ui32>(State));
            FlushNextPart();
        }

        std::unique_ptr<TEvAddBulkSst> GetPendingCommitMsg() {
            Y_VERIFY_S(State == EState::COMMIT_PENDING, ReplCtx->VCtx->VDiskLogPrefix);
            std::unique_ptr<TEvAddBulkSst> msg;
            msg.swap(PendingCommitMsg);
            Y_VERIFY_S(msg, ReplCtx->VCtx->VDiskLogPrefix);
            State = EState::WAITING_FOR_COMMIT;
            return msg;
        }

        void ApplyCommit() {
            Y_VERIFY_S(State == EState::WAITING_FOR_COMMIT, ReplCtx->VCtx->VDiskLogPrefix);
            State = EState::STOPPED;
        }

        EState GetState() const {
            return State;
        }

    private:
        void IssueWriteCmd(std::unique_ptr<NPDisk::TEvChunkWrite>&& ev, EOutputState outputState) {
            State = EState::PDISK_MESSAGE_PENDING;
            PendingPDiskMsg = std::move(ev);
            Y_VERIFY_S(outputState >= OutputState, ReplCtx->VCtx->VDiskLogPrefix);
            OutputState = outputState;
        }

        void FlushNextPart() {
            Y_VERIFY_S(!FlushFinished, ReplCtx->VCtx->VDiskLogPrefix);
            FlushFinished = Writer->FlushNext(0, 0, 1);
            if (auto msg = Writer->GetPendingMessage()) {
                IssueWriteCmd(std::move(msg), FlushFinished ? EOutputState::FLUSH_LAST_CHUNK : EOutputState::FLUSH_CHUNK);
            } else if (WritesInFlight) {
                Y_VERIFY_S(FlushFinished, ReplCtx->VCtx->VDiskLogPrefix);
                Y_VERIFY_S(OutputState == EOutputState::FLUSH_CHUNK, ReplCtx->VCtx->VDiskLogPrefix);
                OutputState = EOutputState::FLUSH_LAST_CHUNK; // promote to final state
                State = EState::NOT_READY;
            } else {
                Y_VERIFY_S(FlushFinished, ReplCtx->VCtx->VDiskLogPrefix);
                OnFlushComplete();
            }
        }

        void OnFlushComplete() {
            Y_VERIFY_S(!WritesInFlight, ReplCtx->VCtx->VDiskLogPrefix);
            Y_VERIFY_S(FlushFinished, ReplCtx->VCtx->VDiskLogPrefix);
            const auto& conclusion = Writer->GetConclusion();
            TVector<ui32> usedChunks = conclusion.UsedChunks;
            PendingCommitMsg = std::make_unique<TEvAddBulkSst>(TVector<ui32>(ReservedChunks.begin(), ReservedChunks.end()),
                    std::move(usedChunks), TLogoBlobsSstPtr(conclusion.LevelSegment), NumRecoveredBlobs);
            Writer.reset();
            State = EState::COMMIT_PENDING;
        }

    private:
        std::shared_ptr<TReplCtx> ReplCtx;
        TIntrusivePtr<THullDs> HullDs;
        TLogoBlobID PrevID;
        EState State;
        EOutputState OutputState;
        std::unique_ptr<IEventBase> PendingPDiskMsg;
        std::unique_ptr<TEvAddBulkSst> PendingCommitMsg;
        TDeque<ui32> ReservedChunks;
        ui32 MinReservedChunksCount;
        std::unique_ptr<TWriter> Writer;
        TDataMerger Merger;
        ui32 NumRecoveredBlobs = 0;
        ui32 WritesInFlight = 0;
        const ui32 MaxWritesInFlight = 5;
        TRopeArena Arena;
        bool FlushFinished = true;
    };

} // NKikimr
