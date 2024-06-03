#include "localrecovery_public.h"
#include "localrecovery_logreplay.h"
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/hulldb/recovery/hulldb_recovery.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hulldb_bulksstloaded.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullload.h>
#include <ydb/core/blobstorage/vdisk/hullop/hullop_entryserialize.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugeheap.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogrecovery.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_data.h>

using namespace NKikimrServices;
using namespace NKikimr::NSyncLog;
using namespace NKikimr::NHuge;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvLocalRecoveryDone -- local recovery done message
    ////////////////////////////////////////////////////////////////////////////
    TEvBlobStorage::TEvLocalRecoveryDone::TEvLocalRecoveryDone(
                                NKikimrProto::EReplyStatus status,
                                TIntrusivePtr<TLocalRecoveryInfo> recovInfo,
                                std::unique_ptr<NSyncLog::TSyncLogRepaired> repairedSyncLog,
                                std::shared_ptr<NHuge::THullHugeKeeperPersState> repairedHuge,
                                TIntrusivePtr<TSyncerData> syncerData,
                                std::shared_ptr<THullDbRecovery> &&reparedHullDb,
                                const TPDiskCtxPtr &pdiskCtx,
                                TIntrusivePtr<THullCtx> &hullCtx,
                                std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
                                TIntrusivePtr<TLocalRecoveryInfo> &localRecoveryInfo,
                                const TIntrusivePtr<TLsnMngr> &lsnMngr,
                                TVDiskIncarnationGuid vdiskIncarnationGuid,
                                NKikimrVDiskData::TScrubEntrypoint scrubEntrypoint,
                                ui64 scrubEntrypointLsn)
        : Status(status)
        , RecovInfo(recovInfo)
        , RepairedSyncLog(std::move(repairedSyncLog))
        , RepairedHuge(std::move(repairedHuge))
        , SyncerData(syncerData)
        , Uncond(std::move(reparedHullDb))
        , PDiskCtx(pdiskCtx)
        , HullCtx(hullCtx)
        , HugeBlobCtx(hugeBlobCtx)
        , LocalRecoveryInfo(localRecoveryInfo)
        , LsnMngr(lsnMngr)
        , VDiskIncarnationGuid(vdiskIncarnationGuid)
        , ScrubEntrypoint(std::move(scrubEntrypoint))
        , ScrubEntrypointLsn(scrubEntrypointLsn)
    {}

    TEvBlobStorage::TEvLocalRecoveryDone::~TEvLocalRecoveryDone() {
        // nothing to do
    }



    ////////////////////////////////////////////////////////////////////////////
    // TDatabaseLocalRecovery -- local recovery actor
    ////////////////////////////////////////////////////////////////////////////
    class TDatabaseLocalRecovery : public TActorBootstrapped<TDatabaseLocalRecovery> {
        friend class TActorBootstrapped<TDatabaseLocalRecovery>;
        using TStartingPoints = TMap<TLogSignature, NPDisk::TLogRecord>;
        using THullSegLoadedLogoBlob = THullSegLoaded<TLogoBlobsSst>;

        TIntrusivePtr<TVDiskConfig> Config;
        // generation independent self VDiskId (it is required for Yard init only)
        const TVDiskID SelfVDiskId;
        const TActorId SkeletonId;
        const TActorId SkeletonFrontId;
        std::shared_ptr<TLocalRecoveryContext> LocRecCtx;
        std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        TVDiskIncarnationGuid VDiskIncarnationGuid;
        std::shared_ptr<TRopeArena> Arena;
        NMonGroup::TVDiskStateGroup VDiskMonGroup;
        bool HullLogoBlobsDBInitialized = false;
        bool HullBlocksDBInitialized = false;
        bool HullBarriersDBInitialized = false;
        bool SyncLogInitialized = false;
        bool SyncerInitialized = false;
        bool HugeKeeperInitialized = false;
        ui64 RecoveredLsn = 0;
        ui64 SyncLogMaxLsnStored = 0;
        NKikimrVDiskData::TScrubEntrypoint ScrubEntrypoint;
        ui64 ScrubEntrypointLsn = 0;

        TActiveActors ActiveActors;

        bool DatabaseStateLoaded() const {
            return HullLogoBlobsDBInitialized && HullBlocksDBInitialized &&
            HullBarriersDBInitialized && SyncLogInitialized;
        }

        void SignalErrorAndDie(const TActorContext &ctx, NKikimrProto::EReplyStatus status, const TString &reason) {
            LocRecCtx->RecovInfo->SuccessfulRecovery = false;
            VDiskMonGroup.VDiskLocalRecoveryState() = TDbMon::TDbLocalRecovery::Error;
            LOG_CRIT(ctx, BS_LOCALRECOVERY,
                    VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                        "LocalRecovery FINISHED: %s reason# %s status# %s;"
                        "VDISK LOCAL RECOVERY FAILURE DUE TO LOGICAL ERROR",
                        LocRecCtx->RecovInfo->ToString().data(), reason.data(),
                        NKikimrProto::EReplyStatus_Name(status).data()));
            ctx.Send(SkeletonId, new TEvBlobStorage::TEvLocalRecoveryDone(
                                                status,
                                                LocRecCtx->RecovInfo,
                                                (LocRecCtx->SyncLogRecovery ?
                                                 LocRecCtx->SyncLogRecovery->ReleaseRepaired() :
                                                 nullptr),
                                                LocRecCtx->RepairedHuge,
                                                LocRecCtx->SyncerData,
                                                std::move(LocRecCtx->HullDbRecovery),
                                                LocRecCtx->PDiskCtx,
                                                LocRecCtx->HullCtx,
                                                HugeBlobCtx,
                                                LocRecCtx->RecovInfo,
                                                nullptr,
                                                VDiskIncarnationGuid,
                                                {},
                                                0));
            Die(ctx);
        }

        void SignalSuccessAndDie(const TActorContext &ctx) {
            // recover Lsn and ConfirmedLsn:
            // Db->Lsn now contains last seen lsn
            Y_DEBUG_ABORT_UNLESS(LocRecCtx->HullDbRecovery->GetHullDs());

            LocRecCtx->RecovInfo->SuccessfulRecovery = true;
            LocRecCtx->RecovInfo->CheckConsistency();
            ui64 lsnToSyncLogRecovered = LocRecCtx->SyncLogRecovery->GetLastLsn();
            // recovers Lsn and ConfirmedLsn
            auto lsnMngr = MakeIntrusive<TLsnMngr>(RecoveredLsn, lsnToSyncLogRecovered, true);
            LocRecCtx->RecovInfo->SetRecoveredLogStartLsn(lsnMngr->GetStartLsn());
            VDiskMonGroup.VDiskLocalRecoveryState() = TDbMon::TDbLocalRecovery::Done;
            LOG_NOTICE(ctx, BS_LOCALRECOVERY,
                       VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                            "LocalRecovery FINISHED: %s", LocRecCtx->RecovInfo->ToString().data()));
            ctx.Send(SkeletonId,
                     new TEvBlobStorage::TEvLocalRecoveryDone(NKikimrProto::OK,
                                                              LocRecCtx->RecovInfo,
                                                              LocRecCtx->SyncLogRecovery->ReleaseRepaired(),
                                                              LocRecCtx->RepairedHuge,
                                                              LocRecCtx->SyncerData,
                                                              std::move(LocRecCtx->HullDbRecovery),
                                                              LocRecCtx->PDiskCtx,
                                                              LocRecCtx->HullCtx,
                                                              HugeBlobCtx,
                                                              LocRecCtx->RecovInfo,
                                                              lsnMngr,
                                                              VDiskIncarnationGuid,
                                                              std::move(ScrubEntrypoint),
                                                              ScrubEntrypointLsn));
            Die(ctx);
        }

        void AdjustBulkFormedSegmentsLsnRange() {
            TIntrusivePtr<TLogoBlobsDs>& logoBlobs = LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs;
            TLevelSlice<TKeyLogoBlob, TMemRecLogoBlob>::TSstIterator iter(logoBlobs->CurSlice.Get(),
                logoBlobs->CurSlice->Level0CurSstsNum());

            for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
                TLogoBlobsSstPtr segment = iter.Get().SstPtr;
                if (segment->Info.IsCreatedByRepl()) {
                    Y_ABORT_UNLESS(segment->Info.FirstLsn == 0 && segment->Info.LastLsn == 0);
                    const auto& item = logoBlobs->CurSlice->BulkFormedSegments.FindIntactBulkFormedSst(segment->GetEntryPoint());
                    segment->Info.FirstLsn = item.FirstBlobLsn;
                    segment->Info.LastLsn = item.LastBlobLsn;
                }
            }
        }

        void AfterDatabaseLoaded(const TActorContext &ctx) {
            // store last indexed lsn (i.e. lsn of the last record that already in DiskRecLog)
            SyncLogMaxLsnStored = LocRecCtx->SyncLogRecovery->GetLastLsnOfIndexRecord();

            LOG_NOTICE(ctx, BS_LOCALRECOVERY,
                       VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                             "MAX LSNS: LogoBlobs# %s Blocks# %s Barriers# %s SyncLog# %" PRIu64,
                             LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs->GetCompactedLsn().ToString().data(),
                             LocRecCtx->HullDbRecovery->GetHullDs()->Blocks->GetCompactedLsn().ToString().data(),
                             LocRecCtx->HullDbRecovery->GetHullDs()->Barriers->GetCompactedLsn().ToString().data(),
                             SyncLogMaxLsnStored));

            // set up blocks cache
            LocRecCtx->HullDbRecovery->BuildBlocksCache();
            // set up barrier cache for validator
            LocRecCtx->HullDbRecovery->BuildBarrierCache();

            Become(&TThis::StateLoadBulkFormedSegments);
            VDiskMonGroup.VDiskLocalRecoveryState() = TDbMon::TDbLocalRecovery::LoadBulkFormedSegments;

            // start loading bulk-formed segments that are already not in index, but still required to recover SyncLog
            auto aid = ctx.Register(LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs->CurSlice->BulkFormedSegments.CreateLoaderActor(
                    LocRecCtx->VCtx, LocRecCtx->PDiskCtx, SyncLogMaxLsnStored, ctx.SelfID));
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvBulkSstsLoaded::TPtr& ev, const TActorContext& ctx) {
            ActiveActors.Erase(ev->Sender);
            AdjustBulkFormedSegmentsLsnRange();
            BeginApplyingLog(ctx);
        }

        void BeginApplyingLog(const TActorContext& ctx) {
            auto replayerId = ctx.RegisterWithSameMailbox(CreateRecoveryLogReplayer(ctx.SelfID, LocRecCtx));
            ActiveActors.Insert(replayerId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            Become(&TThis::StateApplyRecoveryLog);
            VDiskMonGroup.VDiskLocalRecoveryState() = TDbMon::TDbLocalRecovery::ApplyLog;
        }

        void Handle(TEvRecoveryLogReplayDone::TPtr& ev, const TActorContext& ctx) {
            ActiveActors.Erase(ev->Sender);
            auto *msg = ev->Get();
            RecoveredLsn = msg->RecoveredLsn;
            if (msg->Status == NKikimrProto::OK) {
                SignalSuccessAndDie(ctx);
            } else {
                SignalErrorAndDie(ctx, msg->Status, msg->ErrorReason);
            }
        }

        template <class TMetaBase, class TLoader, int signature>
        bool InitMetabase(const TStartingPoints &startingPoints, TIntrusivePtr<TMetaBase> &metabase,
                          bool &initFlag, NMonitoring::TDeprecatedCounter &counter, bool &emptyDb,
                          ui64 freshBufSize, ui64 compThreshold, const TActorContext &ctx) {
            TStartingPoints::const_iterator it;
            // Settings
            TLevelIndexSettings settings(LocRecCtx->HullCtx,
                                         Config->HullCompLevel0MaxSstsAtOnce,
                                         freshBufSize,
                                         compThreshold,
                                         Config->FreshHistoryWindow,
                                         Config->FreshHistoryBuckets,
                                         Config->FreshUseDreg,
                                         Config->Level0UseDreg);

            it = startingPoints.find(signature);
            if (it == startingPoints.end()) {
                // create an empty DB
                emptyDb = true;
                counter = 1;
                auto mb = MakeIntrusive<TMetaBase>(settings, Arena);
                metabase.Swap(mb);
                initFlag = true;
                metabase->LoadCompleted();
            } else {
                // read existing one
                emptyDb = false;
                counter = 0;
                const TRcBuf &data = it->second.Data;
                TString explanation;
                NKikimrVDiskData::THullDbEntryPoint pb;
                const bool good = THullDbSignatureRoutines::ParseArray(pb, data.GetData(), data.GetSize(), explanation);
                if (!good) {
                    TString dbtype = TLogSignature(signature).ToString();
                    explanation = "Entry point for Hull (" + dbtype + ") check failed: " + explanation;
                    SignalErrorAndDie(ctx, NKikimrProto::ERROR, explanation);
                    return false;
                }
                auto mb = MakeIntrusive<TMetaBase>(settings, pb.GetLevelIndex(), it->second.Lsn, Arena);
                metabase.Swap(mb);
                initFlag = false;
                // run reader actor
                auto aid = ctx.Register(new TLoader(LocRecCtx->VCtx, LocRecCtx->PDiskCtx, metabase.Get(), ctx.SelfID));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }
            return true;
        }

        bool InitLogoBlobsMetabase(const TStartingPoints &startingPoints, const TActorContext &ctx) {
            using TLoader = TLevelIndexLoader<TKeyLogoBlob, TMemRecLogoBlob, EHullDbType::LogoBlobs>;
            const int signature = TLogSignature::SignatureHullLogoBlobsDB;
            return InitMetabase<TLogoBlobsDs, TLoader, signature>(
                            startingPoints,
                            LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs,
                            HullLogoBlobsDBInitialized,
                            LocRecCtx->MonGroup.LogoBlobsDbEmpty(),
                            LocRecCtx->RecovInfo->EmptyLogoBlobsDb,
                            Config->FreshBufSizeLogoBlobs,
                            Config->FreshCompThresholdLogoBlobs,
                            ctx);
        }

        bool InitBlocksMetabase(const TStartingPoints &startingPoints, const TActorContext &ctx) {
            using TLoader = TLevelIndexLoader<TKeyBlock, TMemRecBlock, EHullDbType::Blocks>;
            const int signature = TLogSignature::SignatureHullBlocksDB;
            return InitMetabase<TBlocksDs, TLoader, signature>(
                            startingPoints,
                            LocRecCtx->HullDbRecovery->GetHullDs()->Blocks,
                            HullBlocksDBInitialized,
                            LocRecCtx->MonGroup.BlocksDbEmpty(),
                            LocRecCtx->RecovInfo->EmptyBlocksDb,
                            Config->FreshBufSizeBlocks,
                            Config->FreshCompThresholdBlocks,
                            ctx);
        }

        bool InitBarriersMetabase(const TStartingPoints &startingPoints, const TActorContext &ctx) {
            using TLoader = TLevelIndexLoader<TKeyBarrier, TMemRecBarrier, EHullDbType::Barriers>;
            const int signature = TLogSignature::SignatureHullBarriersDB;
            return InitMetabase<TBarriersDs, TLoader, signature>(
                            startingPoints,
                            LocRecCtx->HullDbRecovery->GetHullDs()->Barriers,
                            HullBarriersDBInitialized,
                            LocRecCtx->MonGroup.BarriersDbEmpty(),
                            LocRecCtx->RecovInfo->EmptyBarriersDb,
                            Config->FreshBufSizeBarriers,
                            Config->FreshCompThresholdBarriers,
                            ctx);
        }

        bool InitSyncLogData(const TStartingPoints &startingPoints, const TActorContext &ctx) {
            TStartingPoints::const_iterator it;
            it = startingPoints.find(TLogSignature::SignatureSyncLogIdx);
            TRcBuf entryPoint;
            ui64 entryPointLsn = 0;

            if (it == startingPoints.end()) {
                // create an empty DB
                LocRecCtx->RecovInfo->EmptySyncLog = true;
            } else {
                // read existing one
                LocRecCtx->RecovInfo->EmptySyncLog = false;
                entryPoint = it->second.Data;
                entryPointLsn = it->second.Lsn;
            }

            // Create SyncLogRecovery
            NSyncLog::TSyncLogParams params = {
                Config->BaseInfo.PDiskGuid,
                LocRecCtx->PDiskCtx->Dsk->ChunkSize,
                LocRecCtx->PDiskCtx->Dsk->AppendBlockSize,
                Config->SyncLogAdvisedIndexedBlockSize,
                LocRecCtx->VCtx->SyncLogCache
            };
            // parse entry point
            TString explanation;
            auto repaired = TSyncLogRepaired::Construct(std::move(params), entryPoint.GetData(), entryPoint.GetSize(), entryPointLsn, explanation);
            if (!repaired) {
                explanation = "Error parsing SyncLog entry point; explanation# " + explanation;
                SignalErrorAndDie(ctx, NKikimrProto::ERROR, explanation);
                return false;
            }
            // check pdisk guid
            if (repaired->SyncLogPtr->Header.PDiskGuid != Config->BaseInfo.PDiskGuid) {
                SignalErrorAndDie(ctx, NKikimrProto::ERROR,
                                  "Db PDiskGuid is not equal to PDiskGuid stored in SyncLog entry point");
                return false;
            }
            LocRecCtx->SyncLogRecovery = new NSyncLog::TSyncLogRecovery(std::move(repaired));
            SyncLogInitialized = true;
            VDiskIncarnationGuid = LocRecCtx->SyncLogRecovery->GetSyncLogHeader().VDiskIncarnationGuid;
            return true;
        }

        bool InitSyncer(const TStartingPoints &startingPoints, const TActorContext &ctx) {
            TStartingPoints::const_iterator it;
            it = startingPoints.find(TLogSignature::SignatureSyncerState);
            if (it == startingPoints.end()) {
                // create an empty DB
                LocRecCtx->RecovInfo->EmptySyncer = true;
                LocRecCtx->SyncerData = MakeIntrusive<TSyncerData>(
                        LocRecCtx->VCtx->VDiskLogPrefix,
                        SkeletonId,
                        LocRecCtx->VCtx->ShortSelfVDisk,
                        LocRecCtx->VCtx->Top);
            } else {
                // read existing one
                LocRecCtx->RecovInfo->EmptySyncer = false;
                const TRcBuf &entryPoint = it->second.Data;
                TString errorReason;
                if (!TSyncerData::CheckEntryPoint(LocRecCtx->VCtx->VDiskLogPrefix, SkeletonId,
                                                  LocRecCtx->VCtx->ShortSelfVDisk, LocRecCtx->VCtx->Top, entryPoint, errorReason,
                                                  AppData()->FeatureFlags.GetSuppressCompatibilityCheck())) {
                    errorReason = "Entry point for Syncer check failed, ErrorReason# " + errorReason;
                    LocRecCtx->VCtx->LocalRecoveryErrorStr = errorReason;
                    SignalErrorAndDie(ctx, NKikimrProto::ERROR, errorReason);
                    return false;
                }
                LocRecCtx->SyncerData = MakeIntrusive<TSyncerData>(
                        LocRecCtx->VCtx->VDiskLogPrefix,
                        SkeletonId,
                        LocRecCtx->VCtx->ShortSelfVDisk,
                        LocRecCtx->VCtx->Top,
                        entryPoint);
            }
            SyncerInitialized = true;
            return true;
        }

        bool InitHugeBlobKeeper(const TStartingPoints &startingPoints, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            const ui32 blocksInChunk = LocRecCtx->PDiskCtx->Dsk->ChunkSize / LocRecCtx->PDiskCtx->Dsk->AppendBlockSize;
            Y_ABORT_UNLESS(LocRecCtx->PDiskCtx->Dsk->AppendBlockSize * blocksInChunk == LocRecCtx->PDiskCtx->Dsk->ChunkSize);

            ui32 MaxLogoBlobDataSizeInBlocks = Config->MaxLogoBlobDataSize / LocRecCtx->PDiskCtx->Dsk->AppendBlockSize;
            MaxLogoBlobDataSizeInBlocks += !!(Config->MaxLogoBlobDataSize -
                    MaxLogoBlobDataSizeInBlocks * LocRecCtx->PDiskCtx->Dsk->AppendBlockSize);
            const ui32 slotsInChunk = blocksInChunk / MaxLogoBlobDataSizeInBlocks;
            Y_ABORT_UNLESS(slotsInChunk > 1);

            auto logFunc = [&] (const TString &msg) {
                LOG_DEBUG(ctx, BS_HULLHUGE, msg);
            };
            TStartingPoints::const_iterator it;
            it = startingPoints.find(TLogSignature::SignatureHugeBlobEntryPoint);
            if (it == startingPoints.end()) {
                LocRecCtx->RecovInfo->EmptyHuge = true;

                LocRecCtx->RepairedHuge = std::make_shared<THullHugeKeeperPersState>(
                            LocRecCtx->VCtx,
                            LocRecCtx->PDiskCtx->Dsk->ChunkSize,
                            LocRecCtx->PDiskCtx->Dsk->AppendBlockSize,
                            LocRecCtx->PDiskCtx->Dsk->AppendBlockSize,
                            Config->OldMinHugeBlobInBytes,
                            Config->MilestoneHugeBlobInBytes,
                            Config->MaxLogoBlobDataSize,
                            Config->HugeBlobOverhead,
                            Config->HugeBlobsFreeChunkReservation,
                            logFunc);
            } else {
                // read existing one
                LocRecCtx->RecovInfo->EmptyHuge = false;

                const ui64 lsn = it->second.Lsn;
                const TRcBuf &entryPoint = it->second.Data;
                if (!THullHugeKeeperPersState::CheckEntryPoint(entryPoint)) {
                    SignalErrorAndDie(ctx, NKikimrProto::ERROR, "Entry point for HugeKeeper check failed");
                    return false;
                }

                LocRecCtx->RepairedHuge = std::make_shared<THullHugeKeeperPersState>(
                            LocRecCtx->VCtx,
                            LocRecCtx->PDiskCtx->Dsk->ChunkSize,
                            LocRecCtx->PDiskCtx->Dsk->AppendBlockSize,
                            LocRecCtx->PDiskCtx->Dsk->AppendBlockSize,
                            Config->OldMinHugeBlobInBytes,
                            Config->MilestoneHugeBlobInBytes,
                            Config->MaxLogoBlobDataSize,
                            Config->HugeBlobOverhead,
                            Config->HugeBlobsFreeChunkReservation,
                            lsn, entryPoint, logFunc);
            }
            HugeBlobCtx = std::make_shared<THugeBlobCtx>(
                    LocRecCtx->RepairedHuge->Heap->BuildHugeSlotsMap(),
                    Config->AddHeader);
            HugeKeeperInitialized = true;
            return true;
        }

        bool InitScrub(const TStartingPoints& startingPoints, const TActorContext& ctx) {
            if (const auto it = startingPoints.find(TLogSignature::SignatureScrub); it != startingPoints.end()) {
                ScrubEntrypointLsn = it->second.Lsn;
                if (!ScrubEntrypoint.ParseFromArray(it->second.Data.GetData(), it->second.Data.GetSize())) {
                    SignalErrorAndDie(ctx, NKikimrProto::ERROR, "Entry point for Scrub actor is incorrect");
                }
            }
            return true;
        }

        void Handle(NPDisk::TEvYardInitResult::TPtr &ev, const TActorContext &ctx) {
            NKikimrProto::EReplyStatus status = ev->Get()->Status;

            if (status != NKikimrProto::OK) {
                TStringStream reason;
                reason << "Yard::Init failed, errorReason# \""
                    << ev->Get()->ErrorReason
                    << "\"";
                SignalErrorAndDie(ctx, status, reason.Str());
            } else {
                VDiskMonGroup.VDiskLocalRecoveryState() = TDbMon::TDbLocalRecovery::LoadDb;
                const auto &m = ev->Get();
                LocRecCtx->PDiskCtx = TPDiskCtx::Create(m->PDiskParams, Config);

                LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS, VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                    "INIT: TEvYardInit OK PDiskId# %s", LocRecCtx->PDiskCtx->PDiskIdString.data()));

                // create context for HullDs
                Y_ABORT_UNLESS(LocRecCtx->VCtx && LocRecCtx->VCtx->Top);
                auto hullCtx = MakeIntrusive<THullCtx>(
                        LocRecCtx->VCtx,
                        ui32(LocRecCtx->PDiskCtx->Dsk->ChunkSize),
                        ui32(LocRecCtx->PDiskCtx->Dsk->PrefetchSizeBytes),
                        Config->FreshCompaction && !Config->BaseInfo.ReadOnly,
                        Config->GCOnlySynced,
                        Config->AllowKeepFlags,
                        Config->BarrierValidation,
                        Config->HullSstSizeInChunksFresh,
                        Config->HullSstSizeInChunksLevel,
                        Config->HullCompFreeSpaceThreshold,
                        Config->FreshCompMaxInFlightWrites,
                        Config->HullCompMaxInFlightWrites,
                        Config->HullCompMaxInFlightReads,
                        Config->HullCompReadBatchEfficiencyThreshold,
                        Config->HullCompStorageRatioCalcPeriod,
                        Config->HullCompStorageRatioMaxCalcDuration,
                        Config->AddHeader);

                // create THullDbRecovery, which creates THullDs
                LocRecCtx->HullDbRecovery = std::make_shared<THullDbRecovery>(hullCtx);
                LocRecCtx->HullCtx = hullCtx;

                if (Config->UseCostTracker) {
                    NPDisk::EDeviceType trueMediaType = LocRecCtx->PDiskCtx->Dsk->TrueMediaType;
                    if (trueMediaType == NPDisk::DEVICE_TYPE_UNKNOWN) {
                        // Unable to resolve type from PDisk's properties, using type from VDisk config 
                        trueMediaType = Config->BaseInfo.DeviceType;
                    }
                    if (trueMediaType != NPDisk::DEVICE_TYPE_UNKNOWN) {
                        LocRecCtx->HullCtx->VCtx->CostTracker.reset(new TBsCostTracker(
                                LocRecCtx->HullCtx->VCtx->Top->GType, trueMediaType,
                                LocRecCtx->HullCtx->VCtx->VDiskCounters,
                                Config->CostMetricsParametersByMedia[trueMediaType]
                        ));
                    }
                }

                // store reported owned chunks
                LocRecCtx->ReportedOwnedChunks = std::move(m->OwnedChunks);

                // prepare starting points
                const TStartingPoints &startingPoints = ev->Get()->StartingPoints;
                // save starting points into info
                for (const auto &x : startingPoints) {
                    LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                              VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                    "STARTING POINT: signature# %" PRIu32 " record# %s",
                                    ui32(x.first), x.second.ToString().data()));
                    LocRecCtx->RecovInfo->SetStartingPoint(x.first, x.second.Lsn);
                    switch (x.first) {
                        case TLogSignature::SignatureSyncLogIdx:
                        case TLogSignature::SignatureHullLogoBlobsDB:
                        case TLogSignature::SignatureHullBlocksDB:
                        case TLogSignature::SignatureHullBarriersDB:
                        case TLogSignature::SignatureSyncerState:
                        case TLogSignature::SignatureHugeBlobEntryPoint:
                        case TLogSignature::SignatureScrub:
                            break;

                        default:
                            LOG_CRIT(ctx, BS_LOCALRECOVERY, VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "Unknown starting point Signature# %" PRIu32 " record# %s",
                                (ui32)x.first, x.second.ToString().data()));
                            break;
                    }
                }

                // hull DB async initialization
                if (!InitLogoBlobsMetabase(startingPoints, ctx))
                    return;
                if (!InitBlocksMetabase(startingPoints, ctx))
                    return;
                if (!InitBarriersMetabase(startingPoints, ctx))
                    return;
                // SyncLogData initialization
                if (!InitSyncLogData(startingPoints, ctx))
                    return;
                // Syncer initialization
                if (!InitSyncer(startingPoints, ctx))
                    return;
                if (!InitHugeBlobKeeper(startingPoints, ctx))
                    return;
                if (!InitScrub(startingPoints, ctx))
                    return;

                Become(&TThis::StateLoadDatabase);

                if (DatabaseStateLoaded())
                    AfterDatabaseLoaded(ctx);
            }
        }

        void SendYardInit(const TActorContext &ctx, TDuration yardInitDelay) {
            auto ev = std::make_unique<NPDisk::TEvYardInit>(Config->BaseInfo.InitOwnerRound, SelfVDiskId,
                Config->BaseInfo.PDiskGuid, SkeletonId, SkeletonFrontId, Config->BaseInfo.VDiskSlotId);
            auto handle = std::make_unique<IEventHandle>(Config->BaseInfo.PDiskActorID, SelfId(), ev.release(),
                IEventHandle::FlagTrackDelivery);
            if (yardInitDelay != TDuration::Zero()) {
                TActivationContext::Schedule(yardInitDelay, handle.release());
            } else {
                ctx.Send(handle.release());
            }

            LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                       VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                            "Sending TEvYardInit: pdiskGuid# %" PRIu64 " skeletonid# %s selfid# %s delay %lf sec",
                            ui64(Config->BaseInfo.PDiskGuid), SkeletonId.ToString().data(),
                            ctx.SelfID.ToString().data(), yardInitDelay.SecondsFloat()));
        }

        void Bootstrap(const TActorContext &ctx) {
            LOG_NOTICE(ctx, BS_LOCALRECOVERY,
                       VDISKP(LocRecCtx->VCtx->VDiskLogPrefix, "LocalRecovery START"));

            SendYardInit(ctx, TDuration::Zero());
            Become(&TThis::StateInitialize);
            VDiskMonGroup.VDiskLocalRecoveryState() = TDbMon::TDbLocalRecovery::YardInit;
        }

        void Handle(THullIndexLoaded::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            switch (ev->Get()->Type) {
                case EHullDbType::LogoBlobs:
                    HullLogoBlobsDBInitialized = true;
                    break;
                case EHullDbType::Blocks:
                    HullBlocksDBInitialized = true;
                    break;
                case EHullDbType::Barriers:
                    HullBarriersDBInitialized = true;
                    break;
                default:
                    Y_ABORT("Unexpected case");
            }

            if (DatabaseStateLoaded())
                AfterDatabaseLoaded(ctx);
        }

        void HandleUndelivered(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
            LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                       VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                            "Undelivered TEvYardInit: pdiskGuid# %" PRIu64 " skeletonid# %s selfid# %s",
                            ui64(Config->BaseInfo.PDiskGuid), SkeletonId.ToString().data(),
                            ctx.SelfID.ToString().data()));

            SendYardInit(ctx, TDuration::Seconds(1));
        }

        void HandlePoison(const TActorContext &ctx) {
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::LocalRecovInfoId);
            TStringStream str;
            LocRecCtx->RecovInfo->OutputHtml(str);
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::LocalRecovInfoId));
        }


        STRICT_STFUNC(StateInitialize,
            HFunc(NPDisk::TEvYardInitResult, Handle)
            HFunc(TEvents::TEvUndelivered, HandleUndelivered)
            CFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison)
            HFunc(NMon::TEvHttpInfo, Handle)
        )

        STRICT_STFUNC(StateLoadDatabase,
            HFunc(THullIndexLoaded, Handle)
            CFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison)
            HFunc(NMon::TEvHttpInfo, Handle)
        )

        STRICT_STFUNC(StateLoadBulkFormedSegments,
            HFunc(TEvBulkSstsLoaded, Handle)
            CFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison)
            HFunc(NMon::TEvHttpInfo, Handle)
        )

        STRICT_STFUNC(StateApplyRecoveryLog,
            HFunc(TEvRecoveryLogReplayDone, Handle)
            CFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison)
            HFunc(NMon::TEvHttpInfo, Handle)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_DB_LOCAL_RECOVERY;
        }

        TDatabaseLocalRecovery(
                const TIntrusivePtr<TVDiskContext> &vctx,
                const TIntrusivePtr<TVDiskConfig> &config,
                const TVDiskID &selfVDiskId,
                const TActorId &skeletonId,
                const TActorId skeletonFrontId,
                std::shared_ptr<TRopeArena> arena)
            : TActorBootstrapped<TDatabaseLocalRecovery>()
            , Config(config)
            , SelfVDiskId(selfVDiskId)
            , SkeletonId(skeletonId)
            , SkeletonFrontId(skeletonFrontId)
            , LocRecCtx(std::make_shared<TLocalRecoveryContext>(vctx))
            , Arena(std::move(arena))
            , VDiskMonGroup(vctx->VDiskCounters, "subsystem", "state")
        {}
    };


    IActor* CreateDatabaseLocalRecoveryActor(
            const TIntrusivePtr<TVDiskContext> &vctx,
            const TIntrusivePtr<TVDiskConfig> &config,
            const TVDiskID &selfVDiskId,
            const TActorId &skeletonId,
            const TActorId skeletonFrontId,
            std::shared_ptr<TRopeArena> arena) {
        return new TDatabaseLocalRecovery(vctx, config, selfVDiskId, skeletonId, skeletonFrontId, std::move(arena));
    }

} // NKikimr

