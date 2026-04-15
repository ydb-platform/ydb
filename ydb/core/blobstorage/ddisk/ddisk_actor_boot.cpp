#include "ddisk_actor.h"
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::InitPDiskInterface() {
        Y_ABORT_UNLESS(!IsPersistentBufferActor);
        STLOG(PRI_DEBUG, BS_DDISK, BSDD01, "TDDiskActor::InitPDiskInterface", (DDiskId, DDiskId), (PDiskActorId, BaseInfo.PDiskActorID));
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvYardInit(BaseInfo.InitOwnerRound, TVDiskID(Info->GroupID,
            Info->GroupGeneration, BaseInfo.VDiskIdShort), BaseInfo.PDiskGuid, SelfId(), SelfId(), BaseInfo.VDiskSlotId,
            0 /*groupSizeInUnits*/, true /*getDiskFd*/));
    }

    void TDDiskActor::Handle(NPDisk::TEvYardInitResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_INFO, BS_DDISK, BSDD02, "TDDiskActor::Handle(TEvYardInitResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        Y_ABORT_UNLESS(msg.Status == NKikimrProto::OK);
        Y_ABORT_UNLESS(msg.DiskFormat);

        PDiskParams = std::move(msg.PDiskParams);
        DiskFormat = std::move(msg.DiskFormat);
        OwnedChunksOnBoot = std::move(msg.OwnedChunks);
        DiskFd = std::move(msg.DiskFd);
        if (!DiskFd.IsOpen()) {
            STLOG(PRI_INFO, BS_DDISK, BSDD17,
                "TDDiskActor::Handle(TEvYardInitResult) DiskFd is invalid, all further I/O will be routed through PDisk",
                (DDiskId, DDiskId), (PDiskActorId, BaseInfo.PDiskActorID));
        }

        if (const auto it = msg.StartingPoints.find(TLogSignature::SignatureDDiskChunkMap); it != msg.StartingPoints.end()) {
            NPDisk::TLogRecord& record = it->second;
            ChunkMapSnapshotLsn = record.Lsn;
            NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord chunkMap;
            const bool success = chunkMap.ParseFromArray(record.Data.data(), record.Data.size());
            Y_ABORT_UNLESS(success);
            Y_ABORT_UNLESS(chunkMap.HasSnapshot());
            const auto& snapshot = chunkMap.GetSnapshot();
            for (const auto& tabletRecord : snapshot.GetTabletRecords()) {
                auto& tabletChunkMap = ChunkRefs[tabletRecord.GetTabletId()];
                for (const auto& chunkRef : tabletRecord.GetChunkRefs()) {
                    tabletChunkMap[chunkRef.GetVChunkIndex()].ChunkIdx = chunkRef.GetChunkIdx();
                    ++*Counters.Chunks.ChunksOwned;
                }
            }
        }
        if (const auto it = msg.StartingPoints.find(TLogSignature::SignaturePersistentBufferChunkMap); it != msg.StartingPoints.end()) {
            NPDisk::TLogRecord& record = it->second;
            PersistentBufferChunkMapSnapshotLsn = record.Lsn;

            NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord chunkMap;
            const bool success = chunkMap.ParseFromArray(record.Data.data(), record.Data.size());
            Y_ABORT_UNLESS(success);
            for (auto idx : chunkMap.GetChunkIdxs()) {
                PersistentBufferChunks.emplace_back(idx);
            }
        }
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvReadLog(PDiskParams->Owner, PDiskParams->OwnerRound));
    }

    void TDDiskActor::Handle(NPDisk::TEvReadLogResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD03, "TDDiskActor::Handle(TEvReadLogResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        ++*Counters.RecoveryLog.ReadLogChunks;

        for (const NPDisk::TLogRecord& record : msg.Results) {
            switch (record.Signature.GetUnmasked()) {
                case TLogSignature::SignatureDDiskChunkMap:
                    if (ChunkMapSnapshotLsn + 1 <= record.Lsn) {
                        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord chunkMap;
                        const bool success = chunkMap.ParseFromArray(record.Data.data(), record.Data.size());
                        Y_ABORT_UNLESS(success);
                        Y_ABORT_UNLESS(chunkMap.HasIncrement());
                        const auto& increment = chunkMap.GetIncrement();
                        ChunkRefs[increment.GetTabletId()][increment.GetVChunkIndex()].ChunkIdx = increment.GetChunkIdx();
                        ++*Counters.Chunks.ChunksOwned;
                        ++*Counters.RecoveryLog.LogRecordsApplied;
                    }
                    break;
                case TLogSignature::SignaturePersistentBufferChunkMap:
                    if (record.Lsn > PersistentBufferChunkMapSnapshotLsn) {
                        Y_ABORT("unexpected log signature SignaturePersistentBufferChunkMap");
                    }
                    break;
                default:
                    Y_ABORT("unexpected log signature");
            }
            NextLsn = record.Lsn + 1;
            ++*Counters.RecoveryLog.LogRecordsProcessed;
        }

        if (msg.IsEndOfLog) {
            StartHandlingQueries();
            CreatePersistentBuffer();
        } else {
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvReadLog(PDiskParams->Owner, PDiskParams->OwnerRound,
                msg.NextPosition));
        }
    }

    void TDDiskActor::CreatePersistentBuffer() {
        auto format = NPDisk::TDiskFormatPtr(new NPDisk::TDiskFormat(*DiskFormat), +[](NPDisk::TDiskFormat* ptr) {
            delete ptr;
        });
        auto pbActor = std::make_unique<TDDiskActor>(TVDiskConfig::TBaseInfo(BaseInfo),
            Info, TPersistentBufferFormat(PersistentBufferFormat), TDDiskConfig(Config), AppData()->Counters,
            PersistentBufferChunks, PDiskParams, std::move(format), std::move(DiskFd.Duplicate()));
        auto *as = TActivationContext::ActorSystem();
        PersistentBufferActorId = as->Register(pbActor.release(), TMailboxType::Revolving, AppData()->SystemPoolId);
        auto pbServiceId = MakeBlobStoragePersistentBufferId(BaseInfo.PDiskActorID.NodeId(), BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
        as->RegisterLocalService(pbServiceId, PersistentBufferActorId);
        STLOG(PRI_DEBUG, BS_DDISK, BSDD03, "TDDiskActor::CreatePersistentBuffer()", (DDiskId, DDiskId), (pbServiceId, pbServiceId), (PersistentBufferActorId, PersistentBufferActorId));
    }

    void TDDiskActor::InitUring() {
#if defined(__linux__)
        NPDisk::TUringRouterConfig config;
        config.QueueDepth = MaxInFlight;
        config.UseSQPoll = Config.UseSQPoll;
        config.UseIOPoll = Config.UseIOPoll;
        if (!UringRouter) {
            if (!Config.ForcePDiskFallback && DiskFd != INVALID_FHANDLE && DiskFormat && NPDisk::TUringRouter::Probe(config)) {
                UringRouter = std::make_unique<NPDisk::TUringRouter>(DiskFd, TActivationContext::ActorSystem(), config);
                if (const auto result = UringRouter->RegisterFile(); !result) {
                    STLOG(PRI_WARN, BS_DDISK, BSDD18,
                        "TDDiskActor::InitUring failed to register fixed file for io_uring",
                        (DDiskId, DDiskId), (Errno, result.error()));
                }

                UringRouter->Start();
            }
        }

        if (UringRouter) {
            const NPDisk::EUringFavor requestedFavor = config.GetUringFavor();
            const NPDisk::EUringFavor actualFavor = UringRouter->GetUringFavor();
            *Counters.DirectIO.RegularUringCount = (actualFavor == requestedFavor) ? 1 : 0;
            *Counters.DirectIO.FallbackUringCount = (actualFavor == requestedFavor) ? 0 : 1;
            *Counters.DirectIO.FallbackPDiskCount = 0;
            if (actualFavor != requestedFavor) {
                STLOG(PRI_WARN, BS_DDISK, BSDD19,
                    "TDDiskActor::InitUring io_uring mode fallback",
                    (DDiskId, DDiskId),
                    (RequestedFavor, requestedFavor),
                    (ActualFavor, actualFavor));
            }
            STLOG(PRI_INFO, BS_DDISK, BSDD20,
                "TDDiskActor::InitUring started io_uring with config",
                (DDiskId, DDiskId),
                (Config, UringRouter->GetConfig()));
        } else {
            *Counters.DirectIO.RegularUringCount = 0;
            *Counters.DirectIO.FallbackUringCount = 0;
            *Counters.DirectIO.FallbackPDiskCount = 1;
        }
#endif
    }

    void TDDiskActor::StartHandlingQueries() {
        InitUring();
        TActivationContext::Send(new IEventHandle(TEvPrivate::EvHandleSingleQuery, 0, SelfId(), SelfId(), nullptr, 0));
    }

    void TDDiskActor::HandleSingleQuery() {
        HandlingQueries = true;
        if (!PendingQueries.empty()) {
            auto temp = PendingQueries.front().Release();
            PendingQueries.pop();
            Receive(temp);
            HandlingQueries = false; // to prevent reordering of incoming queries
            StartHandlingQueries();
        }
    }

    ui64 TDDiskActor::GetFirstLsnToKeep() const {
        return std::min(ChunkMapSnapshotLsn, PersistentBufferChunkMapSnapshotLsn);
    }

    void TDDiskActor::IssuePDiskLogRecord(TLogSignature signature, TChunkIdx chunkIdxToCommit,
            const NProtoBuf::Message& data, ui64 *startingPointLsn, std::function<void()> callback) {
        TString buffer;
        const bool success = data.SerializeToString(&buffer);
        Y_ABORT_UNLESS(success);

        const ui64 lsn = NextLsn++;
        if (startingPointLsn) {
            *startingPointLsn = lsn;
        }

        NPDisk::TCommitRecord cr;
        cr.FirstLsnToKeep = startingPointLsn ? GetFirstLsnToKeep() : 0;
        cr.IsStartingPoint = startingPointLsn != nullptr;
        if (chunkIdxToCommit) {
            cr.CommitChunks.push_back(chunkIdxToCommit);
        }

        Send(BaseInfo.PDiskActorID, new NPDisk::TEvLog(PDiskParams->Owner, PDiskParams->OwnerRound, signature, cr,
            TRcBuf(std::move(buffer)), {lsn, lsn}, nullptr));

        LogCallbacks.emplace(lsn, std::move(callback));
    }

    void TDDiskActor::Handle(NPDisk::TEvLogResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD05, "TDDiskActor::Handle(TEvLogResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        for (const auto& result : msg.Results) {
            const auto it = LogCallbacks.find(result.Lsn);
            Y_ABORT_UNLESS(it != LogCallbacks.end());
            if (it->second) {
                it->second();
            }
            LogCallbacks.erase(it);
            ++*Counters.RecoveryLog.LogRecordsWritten;
        }
    }

} // NKikimr::NDDisk
