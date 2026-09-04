#include "ddisk_actor.h"
#include <ydb/core/protos/blobstorage_ddisk_internal.pb.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_DDISK

namespace NKikimr::NDDisk {

    void TDDiskActor::ValidateChecksumsModeAfterLogReplay() {
        if (!Config.EnableChecksums) {
            if (!RestoredIntegrityMapping.IntegrityChunks.empty()) {
                EnterBroken(TStringBuilder()
                    << "restored " << RestoredIntegrityMapping.IntegrityChunks.size()
                    << " integrity chunks while EnableChecksums=false");
            }
            return;
        }

        absl::flat_hash_set<TIntegrityManager::TDataChunkKey> coveredDataChunks;
        coveredDataChunks.reserve(RestoredIntegrityMapping.Extents.size());
        for (const auto& extent : RestoredIntegrityMapping.Extents) {
            coveredDataChunks.insert(extent.Key);
        }

        size_t dataChunkCount = 0;
        size_t uncoveredDataChunkCount = 0;
        for (const auto& [tabletId, chunks] : ChunkRefs) {
            for (const auto& [vChunkIndex, chunkRef] : chunks) {
                if (!chunkRef.ChunkIdx) {
                    continue;
                }
                ++dataChunkCount;
                if (!coveredDataChunks.contains({tabletId, vChunkIndex})) {
                    ++uncoveredDataChunkCount;
                }
            }
        }

        if (dataChunkCount && RestoredIntegrityMapping.IntegrityChunks.empty()) {
            EnterBroken(TStringBuilder()
                << "restored " << dataChunkCount
                << " data chunks without integrity chunks while EnableChecksums=true");
            return;
        }

        if (uncoveredDataChunkCount) {
            EnterBroken(TStringBuilder()
                << "restored " << uncoveredDataChunkCount << " of " << dataChunkCount
                << " data chunks without integrity extents while EnableChecksums=true");
        }
    }

    void TDDiskActor::InitPDiskInterface() {
        Y_ABORT_UNLESS(!IsPersistentBufferActor);
        YDB_LOG_DEBUG("TDDiskActor::InitPDiskInterface",
            {"marker", "BSDD01"},
            {"DDiskId", DDiskId},
            {"PDiskActorId", BaseInfo.PDiskActorID});
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvYardInit(BaseInfo.InitOwnerRound, TVDiskID(Info->GroupID,
            Info->GroupGeneration, BaseInfo.VDiskIdShort), BaseInfo.PDiskGuid, SelfId(), SelfId(), BaseInfo.VDiskSlotId,
            0 /*groupSizeInUnits*/, true /*getDiskFd*/));
    }

    void TDDiskActor::Handle(NPDisk::TEvYardInitResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_INFO("TDDiskActor::Handle(TEvYardInitResult)",
            {"marker", "BSDD02"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvYardInitResult)")) {
            return;
        }
        Y_ABORT_UNLESS(msg.DiskFormat);

        PDiskParams = std::move(msg.PDiskParams);
        DiskFormat = std::move(msg.DiskFormat);
        OwnedChunksOnBoot = std::move(msg.OwnedChunks);
        DiskFd = std::move(msg.DiskFd);

        if (Config.EnableChecksums) {
            // The integrity manager needs the chunk size, so it is created here rather than in the ctor.
            // VDiskSlotId + PDiskGuid identify this DDisk in TIntegrityChunkHeader.
            IntegrityManager.emplace(DiskFormat->ChunkSize, BaseInfo.VDiskSlotId, BaseInfo.PDiskGuid,
                Config.IntegrityChecksumCacheBytes);
        }
        if (!DiskFd.IsOpen()) {
            YDB_LOG_INFO("TDDiskActor::Handle(TEvYardInitResult) "
                "DiskFd is invalid, all further I/O will be routed "
                "through PDisk",
                {"marker", "BSDD17"},
                {"DDiskId", DDiskId},
                {"PDiskActorId", BaseInfo.PDiskActorID});
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
                    if (chunkRef.HasExtentRef()) {
                        const auto& ref = chunkRef.GetExtentRef();
                        RestoredIntegrityMapping.Extents.push_back({
                            .Key = {tabletRecord.GetTabletId(), chunkRef.GetVChunkIndex()},
                            .DataChunkIdx = chunkRef.GetChunkIdx(),
                            .Ref = {ref.GetIntegrityChunkIdx(), ref.GetExtentSlot(), ref.GetVChunkGeneration()},
                        });
                    }
                }
            }
            for (const auto& chunk : snapshot.GetIntegrityChunks()) {
                RestoredIntegrityMapping.IntegrityChunks.push_back(
                    {chunk.GetChunkIdx(), chunk.GetGeneration()});
                CommittedIntegrityChunks.push_back(
                    {chunk.GetChunkIdx(), chunk.GetGeneration()});
                ++*Counters.Chunks.ChunksOwned;
            }
            RestoredIntegrityMapping.GenerationCounter = snapshot.GetGenerationCounter();
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
            PersistentBufferUniqueId = chunkMap.GetUniqueId();
        }
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvReadLog(PDiskParams->Owner, PDiskParams->OwnerRound));
    }

    void TDDiskActor::Handle(NPDisk::TEvReadLogResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvReadLogResult)",
            {"marker", "BSDD03"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvReadLogResult)")) {
            return;
        }

        ++*Counters.RecoveryLog.ReadLogChunks;

        for (const NPDisk::TLogRecord& record : msg.Results) {
            switch (record.Signature.GetUnmasked()) {
                case TLogSignature::SignatureDDiskChunkMap:
                    if (ChunkMapSnapshotLsn + 1 <= record.Lsn) {
                        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord chunkMap;
                        const bool success = chunkMap.ParseFromArray(record.Data.data(), record.Data.size());
                        Y_ABORT_UNLESS(success);
                        using TChunkMapLogRecord = NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord;
                        switch (chunkMap.GetRecordCase()) {
                            case TChunkMapLogRecord::kIncrement: {
                                const auto& increment = chunkMap.GetIncrement();
                                if (increment.HasIntegrityChunk()) {
                                    const auto& chunk = increment.GetIntegrityChunk();
                                    RestoredIntegrityMapping.IntegrityChunks.push_back(
                                        {chunk.GetChunkIdx(), chunk.GetGeneration()});
                                    CommittedIntegrityChunks.push_back(
                                        {chunk.GetChunkIdx(), chunk.GetGeneration()});
                                    ++*Counters.Chunks.ChunksOwned;
                                }
                                const auto& data = increment.GetDataChunk();
                                ChunkRefs[data.GetTabletId()][data.GetVChunkIndex()].ChunkIdx =
                                    data.GetChunkIdx();
                                ++*Counters.Chunks.ChunksOwned;
                                if (data.HasExtentRef()) {
                                    const auto& ref = data.GetExtentRef();
                                    RestoredIntegrityMapping.Extents.push_back({
                                        .Key = {data.GetTabletId(), data.GetVChunkIndex()},
                                        .DataChunkIdx = data.GetChunkIdx(),
                                        .Ref = {ref.GetIntegrityChunkIdx(), ref.GetExtentSlot(), ref.GetVChunkGeneration()},
                                    });
                                }
                                break;
                            }
                            default:
                                Y_ABORT("unexpected chunk map record case");
                        }
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
            ValidateChecksumsModeAfterLogReplay();
            if (Config.EnableChecksums && !IsBroken()) {
                // Restore the DataChunk -> IntegrityExtent mapping accumulated from the snapshot and
                // the replayed increments. Used-block bitmaps are not persisted, so the restored
                // extents come up BitmapUnknown: reads of them pass through unchanged and new writes
                // are tracked again (bitmap restore from the extents on disk is a later phase).
                IntegrityManager->ApplyMappingSnapshot(RestoredIntegrityMapping);
                RestoredIntegrityMapping = {};
                // A durable increment is only logged after formatting, so restored chunks are Ready.
                // Empty integrity chunks (no restored extents) are released here.
                ReclaimUnusedIntegrityChunks();
            }
            RestoredIntegrityMapping = {};
            CreatePersistentBuffer();

            LogReplayComplete = true;
            if (DeferredCutLogFreeUpToLsn) {
                const ui64 freeUpToLsn = *DeferredCutLogFreeUpToLsn;
                DeferredCutLogFreeUpToLsn.reset();
                ProcessCutLog(freeUpToLsn);
            }
            StartHandlingQueries();
        } else {
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvReadLog(PDiskParams->Owner, PDiskParams->OwnerRound,
                msg.NextPosition));
        }
    }

    void TDDiskActor::CreatePersistentBuffer() {
        auto format = NPDisk::TDiskFormatPtr(new NPDisk::TDiskFormat(*DiskFormat), +[](NPDisk::TDiskFormat* ptr) {
            delete ptr;
        });
        if (PersistentBufferUniqueId == 0) {
            PersistentBufferUniqueId = RandomNumber<ui64>();
        }
        auto pbActor = std::make_unique<TDDiskActor>(TVDiskConfig::TBaseInfo(BaseInfo),
            Info, TPersistentBufferFormat(PersistentBufferFormat), TDDiskConfig(Config), CountersParent,
            PersistentBufferChunks, PersistentBufferUniqueId, PDiskParams, std::move(format), std::move(DiskFd.Duplicate()));
        auto *as = TActivationContext::ActorSystem();
        PersistentBufferActorId = as->Register(pbActor.release(), TMailboxType::Revolving, AppData()->SystemPoolId);
        auto pbServiceId = MakeBlobStoragePersistentBufferId(BaseInfo.PDiskActorID.NodeId(), BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
        as->RegisterLocalService(pbServiceId, PersistentBufferActorId);
        YDB_LOG_DEBUG("TDDiskActor::CreatePersistentBuffer()",
            {"marker", "BSDD03"},
            {"DDiskId", DDiskId},
            {"pbServiceId", pbServiceId},
            {"persistentBufferActorId", PersistentBufferActorId});
    }

    void TDDiskActor::InitUring() {
#if defined(__linux__)
        NPDisk::TUringRouterConfig config;
        config.QueueDepth = MaxInFlight;
        if (!UringRouter) {
            if (!Config.ForcePDiskFallback && DiskFd != INVALID_FHANDLE && DiskFormat && NPDisk::TUringRouter::Probe(config)) {
                UringRouter = std::make_unique<NPDisk::TUringRouter>(
                    DiskFd,
                    TActivationContext::ActorSystem(),
                    config,
                    &Counters.UringCounters);
                UringRouter->RegisterFile();

                // Device overestimation tracking: reuse PDisk's measured seek/speed
                // constants for the first iteration.
                // SetSampleSink must be called before Start().
                if (PDiskParams) {
                    DeviceOverestimationReadSpeedBps = PDiskParams->ReadSpeedBps;
                    DeviceOverestimationWriteSpeedBps = PDiskParams->WriteSpeedBps;
                }
                UringRouter->SetSampleSink([this](const NPDisk::TDeviceIoSample& sample) {
                    OnDeviceIoSample(sample);
                });

                UringRouter->Start();

                if (!UringRouter->IsFileRegistered()) {
                    YDB_LOG_WARN("TDDiskActor::InitUring failed to register fixed file for io_uring",
                        {"marker", "BSDD18"},
                        {"DDiskId", DDiskId},
                        {"errno", UringRouter->GetRegisterFileErrno()});
                }

                // Periodically flush buffered samples to the owning PDisk actor so it
                // can merge them with samples from other sources sharing this device.
                Schedule(DeviceOverestimationFlushPeriod,
                    new TEvents::TEvWakeup(EWakeupTag::WakeupFlushDeviceOverestimationSamples));
            }
        }

        if (UringRouter) {
            const NPDisk::EUringFavor actualFavor = UringRouter->GetUringFavor();
            const bool usedModernFlags = actualFavor == NPDisk::EUringFavor::SingleIssuer;
            *Counters.DirectIO.RegularUringCount = usedModernFlags ? 1 : 0;
            *Counters.DirectIO.FallbackUringCount = usedModernFlags ? 0 : 1;
            *Counters.DirectIO.FallbackPDiskCount = 0;
            if (!usedModernFlags) {
                YDB_LOG_WARN("TDDiskActor::InitUring io_uring mode fallback",
                    {"marker", "BSDD19"},
                    {"DDiskId", DDiskId},
                    {"actualFavor", actualFavor});
            }
            YDB_LOG_INFO("TDDiskActor::InitUring started io_uring with config",
                {"marker", "BSDD20"},
                {"DDiskId", DDiskId},
                {"config", UringRouter->GetConfig()});
        } else {
            *Counters.DirectIO.RegularUringCount = 0;
            *Counters.DirectIO.FallbackUringCount = 0;
            *Counters.DirectIO.FallbackPDiskCount = 1;
        }
#endif
    }

    void TDDiskActor::OnDeviceIoSample(const NPDisk::TDeviceIoSample& sample) {
        // Called from the io_uring I/O thread (via UringRouter's
        // sample sink). Keep this cheap: just fill BaseCostNs using the flat
        // model and append under a mutex.
        NPDisk::TDeviceIoSample sampleWithCost = sample;
        sampleWithCost.BaseCostNs = EstimateDeviceIoBaseCostNs(sample.IsWrite, sample.Size);

        TGuard<TMutex> guard(DeviceOverestimationSamplesMutex);
        if (DeviceOverestimationSamples.size() >= MaxBufferedDeviceOverestimationSamples) {
            // Drop oldest half under sustained overflow rather than the actor
            // never keeping up; this is a monitoring signal, not correctness-critical.
            const size_t toDrop = DeviceOverestimationSamples.size() / 2 + 1;
            DeviceOverestimationSamples.erase(
                DeviceOverestimationSamples.begin(), DeviceOverestimationSamples.begin() + toDrop);
        }
        DeviceOverestimationSamples.push_back(sampleWithCost);
    }

    void TDDiskActor::FlushDeviceOverestimationSamples() {
        std::vector<NPDisk::TDeviceIoSample> batch;
        {
            TGuard<TMutex> guard(DeviceOverestimationSamplesMutex);
            batch.swap(DeviceOverestimationSamples);
        }

        if (!batch.empty() && BaseInfo.PDiskActorID) {
            TVector<NPDisk::TDeviceIoSample> samples(batch.begin(), batch.end());
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvDeviceOverestimationSamples(std::move(samples)));
        }

        Schedule(DeviceOverestimationFlushPeriod,
            new TEvents::TEvWakeup(EWakeupTag::WakeupFlushDeviceOverestimationSamples));
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
            const NProtoBuf::Message& data, ui64 *startingPointLsn, std::function<void()> callback,
            TVector<TChunkIdx> chunksToDelete) {
        TVector<TChunkIdx> chunksToCommit;
        if (chunkIdxToCommit) {
            chunksToCommit.push_back(chunkIdxToCommit);
        }
        IssuePDiskLogRecord(signature, std::move(chunksToCommit), data, startingPointLsn,
            std::move(callback), std::move(chunksToDelete));
    }

    void TDDiskActor::IssuePDiskLogRecord(TLogSignature signature, TVector<TChunkIdx> chunksToCommit,
            const NProtoBuf::Message& data, ui64 *startingPointLsn, std::function<void()> callback,
            TVector<TChunkIdx> chunksToDelete) {
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
        cr.CommitChunks = std::move(chunksToCommit);
        cr.DeleteChunks = std::move(chunksToDelete);

        Send(BaseInfo.PDiskActorID, new NPDisk::TEvLog(PDiskParams->Owner, PDiskParams->OwnerRound, signature, cr,
            TRcBuf(std::move(buffer)), {lsn, lsn}, nullptr, TWriteSource::DDiskBoot));

        LogCallbacks.emplace(lsn, TLogCallback{
            .Callback = std::move(callback),
            .IsDDisk = signature == TLogSignature::SignatureDDiskChunkMap,
        });
    }

    void TDDiskActor::Handle(NPDisk::TEvLogResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvLogResult)",
            {"marker", "BSDD05"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvLogResult)")) {
            return;
        }

        for (const auto& result : msg.Results) {
            auto it = LogCallbacks.find(result.Lsn);
            Y_ABORT_UNLESS(it != LogCallbacks.end());
            // Move the callback out before erase: it may IssuePDiskLogRecord, which
            // emplaces into LogCallbacks and would invalidate `it`.
            TLogCallback cb = std::move(it->second);
            LogCallbacks.erase(it);
            if ((!IsBroken() || !cb.IsDDisk) && cb.Callback) {
                cb.Callback();
            }
            ++*Counters.RecoveryLog.LogRecordsWritten;
        }
    }

} // NKikimr::NDDisk
