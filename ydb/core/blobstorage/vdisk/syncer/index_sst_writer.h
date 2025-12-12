#pragma once

#include "defs.h"
#include "blobstorage_syncer_localwriter.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullwriteindexsst.h>

namespace NKikimr {

class TIndexSstWriterActor :
    public TActor<TIndexSstWriterActor>
{
    TVDiskContextPtr VCtx;
    TPDiskCtxPtr PDiskCtx;

    TActorId SyncerJobActorId;

    TIndexSstWriter<TKeyLogoBlob, TMemRecLogoBlob> LogoBlobWriter;
    TIndexSstWriter<TKeyBlock, TMemRecBlock> BlockWriter;
    TIndexSstWriter<TKeyBarrier, TMemRecBarrier> BarrierWriter;

    bool Finished = false;

    enum class EWriterType : ui64 {
        LOGOBLOBS,
        BLOCKS,
        BARRIERS,
    };

    TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>> MsgQueue;

    ui32 CommitsInFlight = 0;
    ui32 WritesInFlight = 0;
    static const ui32 MaxWritesInFlight = 5; // TODO: config

    void ProcessWrites() {
        while (!MsgQueue.empty() && WritesInFlight < MaxWritesInFlight) {
            std::unique_ptr<NPDisk::TEvChunkWrite> msg = std::move(MsgQueue.front());
            MsgQueue.pop();
            Send(PDiskCtx->PDiskId, msg.release()); // vdisk quoter?
            ++WritesInFlight;
        }
    }

    void SendLocalSyncDataResponse() {
        auto msg = std::make_unique<TEvLocalSyncDataResult>(
            NKikimrProto::OK,
            TAppData::TimeProvider->Now(),
            nullptr,
            nullptr);
        Send(SyncerJobActorId, msg.release());
    }

    void ReserveChunk(EWriterType type) {
        auto msg = std::make_unique<NPDisk::TEvChunkReserve>(
            PDiskCtx->Dsk->Owner,
            PDiskCtx->Dsk->OwnerRound,
            1);
        Send(PDiskCtx->PDiskId, msg.release(), 0, (ui64)type);
    }

    void Commit() {
        auto commit = [this]<class TWriter>(TWriter& writer) {
            auto msg = writer.GenerateCommitMessage(SelfId());
            if (msg) {
                Send(writer.GetLevelIndexActorId(), msg.release());
                ++CommitsInFlight;
            }
        };

        commit(LogoBlobWriter);
        commit(BlockWriter);
        commit(BarrierWriter);

        if (CommitsInFlight == 0) {
            Finish();
        }
    }

    void Finish() {
        Send(SyncerJobActorId, new TEvFullSyncFinished);
        PassAway();
    }

    void Handle(TEvLocalSyncData::TPtr& ev) {
        TEvLocalSyncData* msg = ev->Get();
        if (msg->Extracted.LogoBlobs && !msg->Extracted.LogoBlobs->Empty()) {
            if (!LogoBlobWriter.Push(msg->Extracted.LogoBlobs->Extract())) {
                ReserveChunk(EWriterType::LOGOBLOBS);
            }
        }
        if (msg->Extracted.Blocks && !msg->Extracted.Blocks->Empty()) {
            if (!BlockWriter.Push(msg->Extracted.Blocks->Extract())) {
                ReserveChunk(EWriterType::BLOCKS);
            }
        }
        if (msg->Extracted.Barriers && !msg->Extracted.Barriers->Empty()) {
            if (!BarrierWriter.Push(msg->Extracted.Barriers->Extract())) {
                ReserveChunk(EWriterType::BARRIERS);
            }
        }

        ProcessWrites();
        if (WritesInFlight == 0) {
            SendLocalSyncDataResponse();
        }
    }

    void Handle(TEvLocalSyncFinished::TPtr& /*ev*/) {
        LogoBlobWriter.Finish();
        BlockWriter.Finish();
        BarrierWriter.Finish();

        Finished = true;
        ProcessWrites();
    }

    void Handle(NPDisk::TEvChunkWriteResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(VCtx, ev, TActivationContext::AsActorContext());

        --WritesInFlight;
        if (!MsgQueue.empty()) {
            ProcessWrites();
            return;
        }
        if (WritesInFlight == 0) {
            if (Finished) {
                Commit();
                return;
            }
            SendLocalSyncDataResponse();
        }
    }

    void Handle(NPDisk::TEvChunkReserveResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(VCtx, ev, TActivationContext::AsActorContext());

        auto msg = ev->Get();
        Y_VERIFY_S(msg->ChunkIds.size() == 1, VCtx->VDiskLogPrefix);
        auto chunkId = msg->ChunkIds.front();

        auto type = (EWriterType)ev->Cookie;
        switch (type) {
            case EWriterType::LOGOBLOBS:
                LogoBlobWriter.OnChunkReserved(chunkId);
                break;
            case EWriterType::BLOCKS:
                BlockWriter.OnChunkReserved(chunkId);
                break;
            case EWriterType::BARRIERS:
                BarrierWriter.OnChunkReserved(chunkId);
                break;
        }
        ProcessWrites();
    }

    void Handle(TEvAddFullSyncSstsResult::TPtr& /*ev*/) {
        Y_VERIFY_S(CommitsInFlight, VCtx->VDiskLogPrefix);
        if (--CommitsInFlight == 0) {
            Finish();
        }
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr& /*ev*/, const TActorContext &ctx) {
        Die(ctx);
    }

    STRICT_STFUNC(MainFunc,
        hFunc(TEvLocalSyncData, Handle)
        hFunc(TEvLocalSyncFinished, Handle)
        hFunc(NPDisk::TEvChunkReserveResult, Handle)
        hFunc(NPDisk::TEvChunkWriteResult, Handle)
        hFunc(TEvAddFullSyncSstsResult, Handle)
        HFunc(TEvents::TEvPoisonPill, HandlePoison)
    )

    PDISK_TERMINATE_STATE_FUNC_DEF;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_SYNC_SST_WRITER;
    }

    TIndexSstWriterActor(
            TVDiskContextPtr vCtx,
            TPDiskCtxPtr pdiskCtx,
            TIntrusivePtr<TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>> levelIndexLogoBlob,
            TIntrusivePtr<TLevelIndex<TKeyBlock, TMemRecBlock>> levelIndexBlock,
            TIntrusivePtr<TLevelIndex<TKeyBarrier, TMemRecBarrier>> levelIndexBarrier)
        : TActor(&TThis::MainFunc)
        , VCtx(std::move(vCtx))
        , PDiskCtx(std::move(pdiskCtx))
        , LogoBlobWriter(VCtx, PDiskCtx, levelIndexLogoBlob, MsgQueue)
        , BlockWriter(VCtx, PDiskCtx, levelIndexBlock, MsgQueue)
        , BarrierWriter(VCtx, PDiskCtx, levelIndexBarrier, MsgQueue)
    {}

    void SetSyncerJobActorId(TActorId id) {
        SyncerJobActorId = id;
    }
};

} // NKikimr
