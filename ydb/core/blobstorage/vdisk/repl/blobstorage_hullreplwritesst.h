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
            , Arena(&TRopeArenaBackend::Allocate)
        {}

        void Begin() {
            Y_ABORT_UNLESS(State == EState::STOPPED);

            // ensure that we have no pending message and create new one to allocate some chunks for replicated SST
            Y_ABORT_UNLESS(!PendingPDiskMsg);
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
            Y_ABORT_UNLESS(State == EState::PDISK_MESSAGE_PENDING);
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
            Y_ABORT_UNLESS(State == EState::NOT_READY);

            if (ev->Status != NKikimrProto::OK) {
                State = EState::ERROR;
                return;
            }

            for (ui32 chunkId : ev->ChunkIds) {
                ReservedChunks.push_back(chunkId);
            }

            // create new writer
            Y_ABORT_UNLESS(!Writer);
            Writer = std::make_unique<TWriter>(ReplCtx->VCtx, EWriterDataType::Replication, 1, ReplCtx->PDiskCtx->Dsk->Owner,
                ReplCtx->PDiskCtx->Dsk->OwnerRound, ReplCtx->PDiskCtx->Dsk->ChunkSize,
                ReplCtx->PDiskCtx->Dsk->AppendBlockSize, ReplCtx->PDiskCtx->Dsk->BulkWriteBlockSize,
                HullDs->LogoBlobs->AllocSstId(), true, ReservedChunks, Arena, ReplCtx->GetAddHeader());

            // start collecting blobs
            State = EState::COLLECT;
        }

        bool AddRecoveredBlob(TRecoveredBlobInfo& record) {
            Y_ABORT_UNLESS(State == EState::COLLECT);
            Y_ABORT_UNLESS(record.Id > PrevID);
            Y_ABORT_UNLESS(!record.Id.PartId());

            // generate merged ingress for all locally recovered parts
            TIngress ingress = TIngress::CreateFromRepl(ReplCtx->VCtx->Top.get(), ReplCtx->VCtx->ShortSelfVDisk,
                                                        record.Id, record.LocalParts);

            // add disk blob to merger (in order to write it to SSTable)
            Merger.AddBlob(TDiskBlob(&record.Data, record.LocalParts, ReplCtx->VCtx->Top->GType, record.Id));

            // create memory record for disk blob with correct length
            TMemRecLogoBlob memRec(ingress);
            memRec.SetDiskBlob(TDiskPart(0, 0, Merger.GetDiskBlobRawSize(ReplCtx->GetAddHeader())));

            const bool success = Writer->Push(record.Id, memRec, &Merger);
            if (success) {
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

            Y_ABORT_UNLESS(WritesInFlight > 0);
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
            Y_ABORT_UNLESS(State == EState::COLLECT, "unexpected State# %" PRIu32, static_cast<ui32>(State));
            FlushNextPart();
        }

        std::unique_ptr<TEvAddBulkSst> GetPendingCommitMsg() {
            Y_ABORT_UNLESS(State == EState::COMMIT_PENDING);
            std::unique_ptr<TEvAddBulkSst> msg;
            msg.swap(PendingCommitMsg);
            Y_ABORT_UNLESS(msg);
            State = EState::WAITING_FOR_COMMIT;
            return msg;
        }

        void ApplyCommit() {
            Y_ABORT_UNLESS(State == EState::WAITING_FOR_COMMIT);
            State = EState::STOPPED;
        }

        EState GetState() const {
            return State;
        }

    private:
        void IssueWriteCmd(std::unique_ptr<NPDisk::TEvChunkWrite>&& ev, EOutputState outputState) {
            State = EState::PDISK_MESSAGE_PENDING;
            PendingPDiskMsg = std::move(ev);
            Y_ABORT_UNLESS(outputState >= OutputState);
            OutputState = outputState;
        }

        void FlushNextPart() {
            Y_ABORT_UNLESS(!FlushFinished);
            FlushFinished = Writer->FlushNext(0, 0, 1);
            if (auto msg = Writer->GetPendingMessage()) {
                IssueWriteCmd(std::move(msg), FlushFinished ? EOutputState::FLUSH_LAST_CHUNK : EOutputState::FLUSH_CHUNK);
            } else if (WritesInFlight) {
                Y_ABORT_UNLESS(FlushFinished);
                Y_ABORT_UNLESS(OutputState == EOutputState::FLUSH_CHUNK);
                OutputState = EOutputState::FLUSH_LAST_CHUNK; // promote to final state
                State = EState::NOT_READY;
            } else {
                Y_ABORT_UNLESS(FlushFinished);
                OnFlushComplete();
            }
        }

        void OnFlushComplete() {
            Y_ABORT_UNLESS(!WritesInFlight);
            Y_ABORT_UNLESS(FlushFinished);
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
