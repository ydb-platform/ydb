#include "blobstorage_skeleton.h"
#include "blobstorage_skeletonfront.h"
#include "blobstorage_skeletonerr.h"
#include "blobstorage_db.h"
#include "blobstorage_syncfullhandler.h"
#include "blobstorage_monactors.h"
#include "blobstorage_takedbsnap.h"
#include "skeleton_loggedrec.h"
#include "skeleton_vmultiput_actor.h"
#include "skeleton_vmovedpatch_actor.h"
#include "skeleton_vpatch_actor.h"
#include "skeleton_oos_logic.h"
#include "skeleton_oos_tracker.h"
#include "skeleton_overload_handler.h"
#include "skeleton_events.h"
#include "skeleton_capturevdisklayout.h"
#include "skeleton_compactionstate.h"
#include "skeleton_block_and_get.h"
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>
#include <ydb/core/blobstorage/vdisk/localrecovery/localrecovery_public.h>
#include <ydb/core/blobstorage/vdisk/balance/balancing_actor.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllog.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_osiris.h>
#include <ydb/core/blobstorage/vdisk/query/query_public.h>
#include <ydb/core/blobstorage/vdisk/query/query_statalgo.h>
#include <ydb/core/blobstorage/vdisk/query/assimilation.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_status.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_recoverylogwriter.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_hullrepljob.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_localwriter.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubisrunner.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogrecovery.h>
#include <ydb/core/blobstorage/vdisk/scrub/scrub_actor.h>
#include <ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.h>
#include <ydb/core/blobstorage/vdisk/defrag/defrag_actor.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_internal_interface.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/intrlist.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TSkeleton -- rational VDisk implementation
    ////////////////////////////////////////////////////////////////////////////
    class TSkeleton : public TActorBootstrapped<TSkeleton> {

        struct TEvPrivate {
            enum {
                EvCheckSnapshotExpiration = EventSpaceBegin(TEvents::ES_PRIVATE),
            };
        };

        ////////////////////////////////////////////////////////////////////////
        // WHITEBOARD SECTOR
        // Update Whiteboard with the current status
        // Update NodeWarden with current VDisk rank
        ////////////////////////////////////////////////////////////////////////
        void UpdateWhiteboard(const TActorContext &ctx) {
            // satisfaction rank
            NKikimrWhiteboard::TVDiskSatisfactionRank satisfactionRank;
            TOverloadHandler::ToWhiteboard(OverloadHandler.get(), satisfactionRank);
            // send a message to Whiteboard
            auto ev = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate>(&satisfactionRank);
            const TInstant now = ctx.Now();
            const TInstant prev = std::exchange(WhiteboardUpdateTimestamp, now);
            const ui64 bytesRead = QueryCtx ? QueryCtx->PDiskReadBytes.exchange(0) : 0;
            const ui64 bytesWritten = PDiskWriteBytes->exchange(0);
            const TDuration delta = now - prev;
            if (delta != TDuration::Zero() && prev != TInstant::Zero()) {
                auto& record = ev->Record;
                record.SetReadThroughput(bytesRead * 1000000 / delta.MicroSeconds());
                record.SetWriteThroughput(bytesWritten * 1000000 / delta.MicroSeconds());
            }
            ctx.Send(*SkeletonFrontIDPtr, ev.release());
            // send VDisk's metric to NodeWarden
            ctx.Send(NodeWardenServiceId,
                     new TEvBlobStorage::TEvControllerUpdateDiskStatus(
                         SelfVDiskId,
                         OverloadHandler ? OverloadHandler->GetIntegralRankPercent() : 0,
                         SelfId().NodeId(),
                         Config->BaseInfo.PDiskId,
                         Config->BaseInfo.VDiskSlotId));
            // repeat later
            ctx.Schedule(Config->WhiteboardUpdateInterval, new TEvTimeToUpdateWhiteboard());
        }

        ////////////////////////////////////////////////////////////////////////
        // PUT EMERGENCY SECTOR
        // Some stuff to handle a case when we can't accept TEvVPut requests
        // because of (fresh) compaction overload
        ////////////////////////////////////////////////////////////////////////
        static bool BlockWrites(NKikimrBlobStorage::TGroupDecommitStatus::E status) {
            switch (status) {
                case NKikimrBlobStorage::TGroupDecommitStatus::NONE:
                case NKikimrBlobStorage::TGroupDecommitStatus::PENDING:
                    return false;

                case NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS:
                case NKikimrBlobStorage::TGroupDecommitStatus::DONE:
                    return true;

                case NKikimrBlobStorage::TGroupDecommitStatus_E_TGroupDecommitStatus_E_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBlobStorage::TGroupDecommitStatus_E_TGroupDecommitStatus_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_DEBUG_ABORT_UNLESS(false);
                    return true;
            }
        }

        template<typename TEvent>
        bool CheckIfWriteAllowed(TAutoPtr<TEventHandle<TEvent>>& ev, const TActorContext& ctx) {
            if (!SelfVDiskId.SameDisk(ev->Get()->Record.GetVDiskID())) {
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, TAppData::TimeProvider->Now());
            } else if (Config->BaseInfo.DonorMode) {
                ReplyError(NKikimrProto::ERROR, "disk is in donor mode", ev, ctx, TAppData::TimeProvider->Now());
            } else if (BlockWrites(GInfo->DecommitStatus)) {
                ReplyError(NKikimrProto::ERROR, "group is being decommitted", ev, ctx, TAppData::TimeProvider->Now());
            } else if (Config->BaseInfo.ReadOnly) {
                ReplyError(NKikimrProto::ERROR, "disk is in readonly mode", ev, ctx, TAppData::TimeProvider->Now());
            } else {
                return true;
            }
            return false;
        }

        void ProcessPostponedEvents(const TActorContext &ctx, bool actualizeLevels) {
            if (OverloadHandler) {
                // we perform postponed events processing in batch to prioritize emergency
                // queue over new incoming messages; we still make pauses to allow handling
                // of other messages, 'Gets' for instance
                bool proceedFurther = OverloadHandler->ProcessPostponedEvents(ctx, 16, actualizeLevels);
                if (proceedFurther) {
                    ctx.Send(ctx.SelfID, new TEvKickEmergencyPutQueue());
                }
            }
        }

        void LevelIndexCompactionFinished(const TActorContext &ctx) {
            // after commit to LevelIndex recalculate Level Satisfaction Ranks
            ProcessPostponedEvents(ctx, true);
        }

        void KickEmergencyPutQueue(const TActorContext &ctx) {
            ProcessPostponedEvents(ctx, false);
        }

        void WakeupEmergencyPutQueue(const TActorContext &ctx) {
            ScheduleWakeupEmergencyPutQueue(ctx);
            ProcessPostponedEvents(ctx, false);
        }

        void ScheduleWakeupEmergencyPutQueue(const TActorContext &ctx) {
            ctx.Schedule(TDuration::MilliSeconds(50), new TEvWakeupEmergencyPutQueue());
        }

        void Handle(NPDisk::TEvConfigureSchedulerResult::TPtr &ev, const TActorContext &ctx) {
            if (OverloadHandler) {
                OverloadHandler->Feedback(*ev->Get(), ctx);
            }
        }

        void Handle(const NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx) {
            auto& record = ev->Get()->Record;
            ctx.Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
            if (!record.HasConfig() ) {
                return;
            }
            if (const auto& config = record.GetConfig(); config.HasBlobStorageConfig() && config.GetBlobStorageConfig().HasVDiskPerformanceSettings()) {
                for (auto &type : config.GetBlobStorageConfig().GetVDiskPerformanceSettings().GetVDiskTypes()) {
                    if (!type.HasPDiskType() || Config->BaseInfo.DeviceType != PDiskTypeToPDiskType(type.GetPDiskType())) {
                        continue;
                    }
                    if (!type.HasMinHugeBlobSizeInBytes()) {
                        continue;
                    }
                    if (!ApplyHugeBlobSize(type.GetMinHugeBlobSizeInBytes())) {
                        continue;
                    }
                    Y_ABORT_UNLESS(MinREALHugeBlobInBytes);
                    if (Config->RunRepl) {
                        ctx.Send(Db->ReplID, new TEvMinHugeBlobSizeUpdate(MinREALHugeBlobInBytes));
                    }
                    ctx.Send(*SkeletonFrontIDPtr, new TEvMinHugeBlobSizeUpdate(MinREALHugeBlobInBytes));
                }
            }
        }

        bool ApplyHugeBlobSize(ui32 minHugeBlobInBytes) {
            ui32 alignedSize = HugeBlobCtx->HugeSlotsMap->AlignByBlockSize(minHugeBlobInBytes) + 1;
            if (MinREALHugeBlobInBytes == alignedSize) {
                return false;
            }
            MinREALHugeBlobInBytes = alignedSize;
            IFaceMonGroup->MinHugeBlobInBytes(MinREALHugeBlobInBytes);
            return true;
        }

        ////////////////////////////////////////////////////////////////////////
        // SEND REPLY
        ////////////////////////////////////////////////////////////////////////
        template <class TOrigEv>
        void SendReply(const TActorContext &ctx, std::unique_ptr<IEventBase> result, TOrigEv &orig, EServiceKikimr logService) {
            Y_UNUSED(logService);
            SendVDiskResponse(ctx, orig->Sender, result.release(), orig->Cookie, VCtx);
        }

        ////////////////////////////////////////////////////////////////////////
        // PATCH SECTOR
        ////////////////////////////////////////////////////////////////////////

        void Handle(TEvBlobStorage::TEvVMovedPatch::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVMovedPatch: receive request;"
                    << " Event# " << ev->Get()->ToString());
            if (!CheckIfWriteAllowed(ev, ctx)) {
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVMovedPatch: is not allowed;"
                        << " Event# " << ev->Get()->ToString());
                return;
            }
            const bool postpone = OverloadHandler->PostponeEvent(ev);
            if (!postpone) {
                PrivateHandle(ev, ctx);
            } else {
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVMovedPatch: is postponned;"
                        << " Event# " << ev->Get()->ToString());
            }
        }

         void PrivateHandle(TEvBlobStorage::TEvVMovedPatch::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVMovedPatch: register actor;"
                    << " Event# " << ev->Get()->ToString());
            IFaceMonGroup->MovedPatchMsgs()++;
            TOutOfSpaceStatus oosStatus = VCtx->GetOutOfSpaceState().GetGlobalStatusFlags();
            Register(CreateSkeletonVMovedPatchActor(SelfId(), oosStatus, ev, SkeletonFrontIDPtr,
                    IFaceMonGroup->MovedPatchResMsgsPtr(), Db->GetVDiskIncarnationGuid(), VCtx));
        }

        void UpdateVPatchCtx() {
            if (!VPatchCtx) {
                TIntrusivePtr<::NMonitoring::TDynamicCounters> patchGroup = VCtx->VDiskCounters->GetSubgroup("subsystem", "patch");
                VPatchCtx = MakeIntrusive<TVPatchCtx>();
                NBackpressure::TQueueClientId patchQueueClientId(NBackpressure::EQueueClientType::VPatch,
                            VCtx->Top->GetOrderNumber(VCtx->ShortSelfVDisk));
                CreateQueuesForVDisks(VPatchCtx->AsyncBlobQueues, SelfId(), GInfo, VCtx, GInfo->GetVDisks(), patchGroup,
                        patchQueueClientId, NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob,
                        "PeerVPatch",  TInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA);
            }
        }

        void Handle(TEvProxyQueueState::TPtr &/*ev*/, const TActorContext &/*ctx*/) {
            // TODO(kruall): Make it better
        }

        template <typename TEvPtr>
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvPtr &ev, const TActorContext& ctx,
                TInstant now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res = ErroneousResult(VCtx, status, errorReason, ev, now,
                    SkeletonFrontIDPtr, SelfVDiskId, Db->GetVDiskIncarnationGuid(), GInfo);
            SendReply(ctx, std::move(res), ev, BS_VDISK_PATCH);
        }

        void Handle(TEvBlobStorage::TEvVPatchStart::TPtr &ev, const TActorContext &ctx) {
            if (!CheckIfWriteAllowed(ev, ctx)) {
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatchStart: receive request;"
                        << " Event# " << ev->Get()->ToString());
                return;
            }
            const bool postpone = OverloadHandler->PostponeEvent(ev);
            if (!postpone) {
                PrivateHandle(ev, ctx);
            } else {
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatchStart: postponned;"
                        << " Event# " << ev->Get()->ToString());
            }
        }

        void PrivateHandle(TEvBlobStorage::TEvVPatchStart::TPtr &ev, const TActorContext &ctx) {
            TInstant now = ctx.Now();
            if (!EnableVPatch.Update(now)) {
                ReplyError(NKikimrProto::ERROR, "VPatch is disabled", ev, ctx, TAppData::TimeProvider->Now());
                return;
            }

            TLogoBlobID patchedBlobId = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetPatchedBlobId());

            if (VPatchActors.count(patchedBlobId)) {
                ReplyError(NKikimrProto::ERROR, "The patching request already is running", ev, ctx, TAppData::TimeProvider->Now());
                return;
            }

            LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatch: register actor;"
                    << " Event# " << ev->Get()->ToString());
            IFaceMonGroup->PatchStartMsgs()++;
            UpdateVPatchCtx();
            std::unique_ptr<IActor> actor{CreateSkeletonVPatchActor(SelfId(), GInfo->Type, ev, now, SkeletonFrontIDPtr,
                    IFaceMonGroup->PatchFoundPartsMsgsPtr(), IFaceMonGroup->PatchResMsgsPtr(),
                    VCtx->Histograms.GetHistogram(NKikimrBlobStorage::FastRead), VCtx->Histograms.GetHistogram(NKikimrBlobStorage::AsyncBlob),
                    VPatchCtx, VCtx->VDiskLogPrefix, Db->GetVDiskIncarnationGuid(), VCtx)};
            TActorId vPatchActor = Register(actor.release());
            VPatchActors.emplace(patchedBlobId, vPatchActor);
        }

        template <typename TEvDiffPtr>
        void HandleVPatchDiffResending(TEvDiffPtr &ev, const TActorContext &ctx) {
            if (!CheckIfWriteAllowed(ev, ctx)) {
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatch: is not allowed;"
                        << " Event# " << ev->Get()->ToString());
                return;
            }
            if constexpr (std::is_same_v<TEvDiffPtr, TEvBlobStorage::TEvVPatchDiff::TPtr>) {
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatch: recieve diff;"
                        << " Event# " << ev->Get()->ToString());
                IFaceMonGroup->PatchDiffMsgs()++;
            } else if constexpr (std::is_same_v<TEvDiffPtr, TEvBlobStorage::TEvVPatchXorDiff::TPtr>) {
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatch: recieve xor diff;"
                        << " Event# " << ev->Get()->ToString());
                IFaceMonGroup->PatchXorDiffMsgs()++;
            } else {
                LOG_ERROR_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatch: UNKNOWN diff;"
                        << " Event# " << ev->Get()->ToString());
            }
            TLogoBlobID patchedBlobId = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetPatchedPartBlobId()).FullID();
            auto it = VPatchActors.find(patchedBlobId);
            if (it != VPatchActors.end()) {
                TActivationContext::Send(ev->Forward(it->second));
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatch: diff sent to actor;"
                        << " Event# " << ev->Get()->ToString());
            } else {
                ReplyError(NKikimrProto::ERROR, "VPatchActor doesn't exist", ev, ctx, TAppData::TimeProvider->Now());
                LOG_DEBUG_S(ctx, BS_VDISK_PATCH, VCtx->VDiskLogPrefix << "TEvVPatch: diff didn't send to actor; actor didn't exist"
                        << " Event# " << ev->Get()->ToString());
            }
        }

        void Handle(TEvVPatchDyingRequest::TPtr &ev) {
            auto it = VPatchActors.find(ev->Get()->PatchedBlobId);
            if (it != VPatchActors.end()) {
                VPatchActors.erase(it);
            }
            Send(ev->Sender, new TEvVPatchDyingConfirm);
        }

        ////////////////////////////////////////////////////////////////////////
        // MULTIPUT SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVMultiPut::TPtr &ev,
                        const TActorContext &ctx, TInstant now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendReply(ctx, std::move(res), ev, BS_VDISK_PUT);
        }

        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVMultiPut::TPtr &ev,
                        const TActorContext &ctx, TInstant now, const TBatchedVec<NKikimrProto::EReplyStatus> &statuses) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId, statuses,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendReply(ctx, std::move(res), ev, BS_VDISK_PUT);
        }

        void Handle(TEvBlobStorage::TEvVMultiPut::TPtr &ev, const TActorContext &ctx) {
            if (!CheckIfWriteAllowed(ev, ctx)) {
                return;
            }
            const bool postpone = OverloadHandler->PostponeEvent(ev);
            if (!postpone) {
                PrivateHandle(ev, ctx);
            }
        }

        struct TVPutInfo {
            TRope Buffer;
            TLogoBlobID BlobId;
            TIngress Ingress;
            TLsnSeg Lsn;
            THullCheckStatus HullStatus;
            bool IsHugeBlob = false;
            NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck> ExtraBlockChecks;
            NWilson::TTraceId TraceId;
            bool WrittenBeyondBarrier = false;

            TVPutInfo(TLogoBlobID blobId, TRope &&buffer,
                    NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck> *extraBlockChecks,
                    NWilson::TTraceId traceId)
                : Buffer(std::move(buffer))
                , BlobId(blobId)
                , HullStatus({NKikimrProto::UNKNOWN, 0, false})
                , TraceId(std::move(traceId))
            {
                ExtraBlockChecks.Swap(extraBlockChecks);
            }
        };

        void UpdatePDiskWriteBytes(size_t size) {
            *PDiskWriteBytes += size; // actual size for small blobs may be up to one block, but it may be
            // batched along with other VDisk log entries on the PDisk
        }

        template<typename TEvResult> struct TLoggedRecType {};
        template<> struct TLoggedRecType<TEvBlobStorage::TEvVPutResult> { using T = TLoggedRecVPut; };
        template<> struct TLoggedRecType<TEvVMultiPutItemResult> { using T = TLoggedRecVMultiPutItem; };

        template <typename TEvResult>
        std::pair<std::unique_ptr<NPDisk::TEvLog>, NWilson::TTraceId> CreatePutLogEvent(const TActorContext &ctx, TString evPrefix,
                NActors::TActorId sender, ui64 cookie, NLWTrace::TOrbit &&orbit, TVPutInfo &info, std::unique_ptr<TEvResult> result)
        {
            Y_DEBUG_ABORT_UNLESS(info.HullStatus.Status == NKikimrProto::OK);
            const TLogoBlobID &id = info.BlobId;
            TRope &buffer = info.Buffer;
            const TLsnSeg &seg = info.Lsn;
            const TIngress &ingress = info.Ingress;

#ifdef OPTIMIZE_SYNC
            // nothing to do, don't create synclog record
            std::unique_ptr<NSyncLog::TEvSyncLogPut> syncLogMsg;
#else
            std::unique_ptr<NSyncLog::TEvSyncLogPut> syncLogMsg(
                new NSyncLog::TEvSyncLogPut(Db->GType, seg.Point(), TLogoBlobID(id, 0), info.Ingress));
#endif
            // FIXME(innokentii) unclear semantics: we make only copy of wrapper here
            // so, in right scenario we only take underlying container and call .GrowFront() on him
            // which (if there is headroom) doesn't invalidate anything and change behavior only for wrapper
            // and even if there is no headroom - copy will occur, making wrappers point to two unrelated memory
            // regions

            // prepare message to recovery log
            TRcBuf dataToWrite = TPutRecoveryLogRecOpt::SerializeZeroCopy(Db->GType, id, TRope(buffer));
            LOG_DEBUG_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix
                    << evPrefix << ": userDataSize# " << buffer.GetSize()
                    << " writtenSize# " << dataToWrite.size()
                    << " channel# " << id.Channel()
                    << " Marker# BSVS04");
            UpdatePDiskWriteBytes(dataToWrite.size());

            bool confirmSyncLogAlso = static_cast<bool>(syncLogMsg);
            auto loggedRec = new typename TLoggedRecType<TEvResult>::T(seg, confirmSyncLogAlso,
                id, ingress, std::move(buffer), std::move(result), sender, cookie, std::move(info.TraceId));
            intptr_t loggedRecId = LoggedRecsVault.Put(loggedRec);
            void *loggedRecCookie = reinterpret_cast<void *>(loggedRecId);
            // create log msg
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignatureLogoBlobOpt, dataToWrite,
                    seg, loggedRecCookie, std::move(syncLogMsg), nullptr);
            // send prepared message to recovery log
            logMsg->Orbit = std::move(orbit);
            return {std::move(logMsg), loggedRec->GetTraceId()};
        }

        std::unique_ptr<TEvHullWriteHugeBlob> CreateHullWriteHugeBlob(NActors::TActorId sender,
                ui64 cookie, bool ignoreBlock, NKikimrBlobStorage::EPutHandleClass handleClass, TVPutInfo &info,
                std::unique_ptr<TEvBlobStorage::TEvVPutResult> res)
        {
            Y_DEBUG_ABORT_UNLESS(info.HullStatus.Status == NKikimrProto::OK);
            info.Buffer = TDiskBlob::Create(info.BlobId.BlobSize(), info.BlobId.PartId(), Db->GType.TotalPartCount(),
                std::move(info.Buffer), *Arena, HullCtx->AddHeader);
            UpdatePDiskWriteBytes(info.Buffer.GetSize());
            return std::make_unique<TEvHullWriteHugeBlob>(sender, cookie, info.BlobId, info.Ingress,
                    std::move(info.Buffer), ignoreBlock, handleClass, std::move(res), &info.ExtraBlockChecks);
        }

        THullCheckStatus ValidateVPut(const TActorContext &ctx, TString evPrefix,
                TLogoBlobID id, ui64 bufSize, bool ignoreBlock,
                const NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck>& extraBlockChecks,
                bool *writtenBeyondBarrier)
        {
            ui64 blobPartSize = 0;
            try {
                blobPartSize = GInfo->Type.PartSize(id);
            } catch (yexception ex) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << ex.what() << " Marker# BSVS40");
                return {NKikimrProto::ERROR, ex.what()};
            }

            if (bufSize != blobPartSize) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix
                        << evPrefix << ": buffer size does not match with part size;"
                        << " buffer size# " << bufSize
                        << " PartSize# " << blobPartSize
                        << " id# " << id
                        << " Marker# BSVS01");
                return {NKikimrProto::ERROR, "buffer size mismatch"};
            }

            if (bufSize > Config->MaxLogoBlobDataSize) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << evPrefix << ": data is too large;"
                        << " id# " << id
                        << " size# " << bufSize
                        << " chunkSize# " << PDiskCtx->Dsk->ChunkSize
                        << " Marker# BSVS02");
                return {NKikimrProto::ERROR, "buffer is too large"};
            }

            if (id.TabletID() == 0) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << evPrefix << ": TabletID cannot be empty;"
                        << " id# " << id
                        << " Marker# BSVS43");
                return {NKikimrProto::ERROR, "empty TabletID"};
            }

            auto status = Hull->CheckLogoBlob(ctx, id, ignoreBlock, extraBlockChecks, writtenBeyondBarrier);
            if (status.Status != NKikimrProto::OK) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << evPrefix << ": failed to pass the Hull check;"
                        << " id# " << id
                        << " status# " << status
                        << " Marker# BSVS03");
            }
            return status;
        }

        void PrivateHandle(TEvBlobStorage::TEvVMultiPut::TPtr &ev, const TActorContext &ctx) {
            IFaceMonGroup->MultiPutMsgs()++;
            IFaceMonGroup->PutTotalBytes() += ev->GetSize();

            NKikimrBlobStorage::TEvVMultiPut &record = ev->Get()->Record;
            TInstant now = TAppData::TimeProvider->Now();

            if (!record.ItemsSize()) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << "TEvVMultiPut: empty multiput;"
                    << " event# " << ev->Get()->ToString()
                    << " sender actorId# " << ev->Sender
                    << " Marker# BSVS05");
                ReplyError(NKikimrProto::ERROR, "empty multiput", ev, ctx, now);
                return;
            }

            TLogoBlobID firstBlobId = LogoBlobIDFromLogoBlobID(record.GetItems(0).GetBlobID());
            LWTRACK(VDiskSkeletonVMultiPutRecieved, ev->Get()->Orbit, VCtx->NodeId, VCtx->GroupId.GetRawId(),
                    VCtx->Top->GetFailDomainOrderNumber(VCtx->ShortSelfVDisk),
                    firstBlobId.TabletID(), ev->Get()->GetSumBlobSize());

            if (!OutOfSpaceLogic->Allow(ctx, ev)) {
                ReplyError(NKikimrProto::OUT_OF_SPACE, "out of space", ev, ctx, now);
                return;
            }

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << "TEvVMultiPut: race;"
                        << " Marker# BSVS06");
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
                return;
            }

            bool hasPostponed = false;
            bool ignoreBlock = record.GetIgnoreBlock();

            TBatchedVec<TVPutInfo> putsInfo;
            ui64 lsnCount = 0;
            for (ui64 itemIdx = 0; itemIdx < record.ItemsSize(); ++itemIdx) {
                auto &item = *record.MutableItems(itemIdx);
                TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                putsInfo.emplace_back(blobId, ev->Get()->GetItemBuffer(itemIdx), item.MutableExtraBlockChecks(),
                    item.HasTraceId() ? item.GetTraceId() : NWilson::TTraceId());
                TVPutInfo &info = putsInfo.back();

                try {
                    info.IsHugeBlob = HugeBlobCtx->IsHugeBlob(VCtx->Top->GType, blobId.FullID(), MinREALHugeBlobInBytes);
                    if (info.IsHugeBlob) {
                        LOG_CRIT_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << "TEvVMultiPut: TEvVMultiPut has huge blob# "
                            << blobId << " Marker# BSVS08");
                        info.HullStatus = THullCheckStatus(NKikimrProto::ERROR, "TEvVMultiPut with huge blob");
                    }
                } catch (const std::exception& ex) {
                    LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << ex.what() << " Marker# BSVS39");
                    info.HullStatus = THullCheckStatus(NKikimrProto::ERROR, TStringBuilder() << "exception# " << ex.what());
                }

                if (info.HullStatus.Status == NKikimrProto::UNKNOWN) {
                    info.HullStatus = ValidateVPut(ctx, "TEvVMultiPut", blobId, info.Buffer.GetSize(), ignoreBlock,
                        info.ExtraBlockChecks, &info.WrittenBeyondBarrier);
                }

                if (info.HullStatus.Status == NKikimrProto::OK) {
                    auto ingressOpt = TIngress::CreateIngressWithLocal(VCtx->Top.get(), VCtx->ShortSelfVDisk, blobId);
                    if (!ingressOpt) {
                        LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << "TEvVMultiPut: ingress mismatch;"
                                << " id# " << blobId
                                << " Marker# BSVS07");
                        info.HullStatus = {NKikimrProto::ERROR, 0, false};
                    } else {
                        info.Ingress = *ingressOpt;
                    }
                }
                hasPostponed |= info.HullStatus.Postponed;

                lsnCount += info.HullStatus.Status == NKikimrProto::OK;
            }

            TBatchedVec<NKikimrProto::EReplyStatus> statuses;
            for (auto &info : putsInfo) {
                if (info.HullStatus.Postponed) {
                    statuses.push_back(NKikimrProto::OK);
                } else {
                    statuses.push_back(info.HullStatus.Status);
                }
            }
            if (!lsnCount && !hasPostponed) {
                LOG_INFO_S(ctx, BS_VDISK_PUT, Db->VCtx->VDiskLogPrefix << "TEvVMultiPut: all items have errors"
                        << " Marker# BSVS09");
                ReplyError(NKikimrProto::OK, TString(), ev, ctx, now, statuses);
                return;
            }

            TOutOfSpaceStatus oosStatus = VCtx->GetOutOfSpaceState().GetGlobalStatusFlags();
            NLWTrace::TOrbit orbit = std::move(ev->Get()->Orbit);

            std::unique_ptr<NPDisk::TEvMultiLog> evLogs = std::make_unique<NPDisk::TEvMultiLog>();
            ui64 cookie = ev->Cookie;

            IActor* vMultiPutActor = CreateSkeletonVMultiPutActor(SelfId(), statuses, oosStatus, ev,
                    SkeletonFrontIDPtr, IFaceMonGroup->MultiPutResMsgsPtr(), Db->GetVDiskIncarnationGuid(), VCtx);
            NActors::TActorId vMultiPutActorId = ctx.Register(vMultiPutActor);
            ActiveActors.Insert(vMultiPutActorId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            TLsnSeg lsnBatch;
            if (lsnCount) {
#ifdef OPTIMIZE_SYNC
                lsnBatch = Db->LsnMngr->AllocDiscreteLsnBatchForHull(lsnCount);
#else
                lsnBatch = Db->LsnMngr->AllocDiscreteLsnBatchForHullAndSyncLog(lsnCount);
#endif
            }

            for (ui64 itemIdx = 0; itemIdx < record.ItemsSize(); ++itemIdx) {
                TVPutInfo &info = putsInfo[itemIdx];
                NKikimrProto::EReplyStatus status = info.HullStatus.Status;

                auto result = std::make_unique<TEvVMultiPutItemResult>(info.BlobId, itemIdx, status,
                    info.HullStatus.ErrorReason, info.WrittenBeyondBarrier);

                if (info.HullStatus.Postponed) {
                    Hull->PostponeReplyUntilCommitted(result.release(), vMultiPutActorId, itemIdx, std::move(info.TraceId),
                        info.HullStatus.Lsn);
                    continue;
                }

                if (status != NKikimrProto::OK) {
                    continue;
                }

                Y_ABORT_UNLESS(lsnBatch.First <= lsnBatch.Last);

                info.Lsn = TLsnSeg(lsnBatch.First, lsnBatch.First);
                lsnBatch.First++;
                auto [logMsg, traceId] = CreatePutLogEvent(ctx, "TEvVMultiPut", vMultiPutActorId, cookie,
                    (itemIdx ? NLWTrace::TOrbit{} : std::move(orbit)), info, std::move(result));
                evLogs->AddLog(THolder<NPDisk::TEvLog>(logMsg.release()), std::move(traceId));
            }

            // Manage PDisk scheduler weights
            OverloadHandler->ActualizeWeights(ctx, Mask(EHullDbType::LogoBlobs));

            if (lsnCount) {
                ctx.Send(Db->LoggerID, evLogs.release());
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // PUT SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(THullCheckStatus status, TEvBlobStorage::TEvVPut::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status.Status, status.ErrorReason, ev, now, SkeletonFrontIDPtr,
                SelfVDiskId, Db->GetVDiskIncarnationGuid(), GInfo));
            if (status.Postponed) {
                Hull->PostponeReplyUntilCommitted(res.release(), ev->Sender, ev->Cookie, std::move(ev->TraceId), status.Lsn);
            } else {
                SendReply(ctx, std::move(res), ev, BS_VDISK_PUT);
            }
        }

        void HandlePutSyncGuidRecovery(TEvBlobStorage::TEvVPut::TPtr& ev, const TActorContext& ctx) {
            Y_ABORT_UNLESS(ev->Get()->RewriteBlob);
            Handle(ev, ctx);
        }

        void Handle(TEvBlobStorage::TEvVPut::TPtr &ev, const TActorContext &ctx) {
            if (!CheckIfWriteAllowed(ev, ctx)) {
                return;
            }
            const bool postpone = OverloadHandler->PostponeEvent(ev);
            if (!postpone) {
                PrivateHandle(ev, ctx);
            }
        }

        void PrivateHandle(TEvBlobStorage::TEvVPut::TPtr &ev, const TActorContext &ctx) {
            IFaceMonGroup->PutMsgs()++;
            IFaceMonGroup->PutTotalBytes() += ev->GetSize();
            TInstant now = TAppData::TimeProvider->Now();
            NKikimrBlobStorage::TEvVPut &record = ev->Get()->Record;
            const TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetBlobID());
            LWTRACK(VDiskSkeletonVPutRecieved, ev->Get()->Orbit, VCtx->NodeId, VCtx->GroupId.GetRawId(),
                   VCtx->Top->GetFailDomainOrderNumber(VCtx->ShortSelfVDisk), id.TabletID(), id.BlobSize());
            TVPutInfo info(id, ev->Get()->GetBuffer(), record.MutableExtraBlockChecks(), std::move(ev->TraceId));
            const ui64 bufSize = info.Buffer.GetSize();

            try {
                info.IsHugeBlob = HugeBlobCtx->IsHugeBlob(VCtx->Top->GType, id.FullID(), MinREALHugeBlobInBytes);
            } catch (yexception ex) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << ex.what()  << " Marker# BSVS41");
                info.HullStatus = {NKikimrProto::ERROR, 0, false};
                ReplyError({NKikimrProto::ERROR, ex.what(), 0, false}, ev, ctx, now);
                return;
            }

            const bool ignoreBlock = record.GetIgnoreBlock();

            if (!OutOfSpaceLogic->Allow(ctx, ev)) {
                ReplyError({NKikimrProto::OUT_OF_SPACE, "out of space", 0, false}, ev, ctx, now);
                return;
            }

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << "TEvVPut: race; id# " << id
                        << " Marker# BSVS10");
                ReplyError({NKikimrProto::RACE, "group generation mismatch", 0, false}, ev, ctx, now);
                return;
            }

            info.HullStatus = ValidateVPut(ctx, "TEvVPut", id, bufSize, ignoreBlock, info.ExtraBlockChecks, &info.WrittenBeyondBarrier);
            if (info.HullStatus.Status != NKikimrProto::OK) {
                ReplyError(info.HullStatus, ev, ctx, now);
                return;
            }

            auto ingressOpt = TIngress::CreateIngressWithLocal(VCtx->Top.get(), VCtx->ShortSelfVDisk, id);
            if (!ingressOpt) {
                LOG_ERROR_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << "TEvVPut: ingress mismatch; id# " << id
                        << " Marker# BSVS11");
                ReplyError({NKikimrProto::ERROR, "ingress mismatch", 0, false}, ev, ctx, now);
                return;
            }
            info.Ingress = *ingressOpt;

            LOG_DEBUG_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix <<"TEvVPut: " << " result# " << ev->Get()->ToString()
                    << " Marker# BSVS12");

            if (!info.IsHugeBlob) {

#ifdef OPTIMIZE_SYNC
                info.Lsn = Db->LsnMngr->AllocLsnForHull();
#else
                info.Lsn = Db->LsnMngr->AllocLsnForHullAndSyncLog();
#endif
            }

            // no more errors (at least for for log writes)
            std::unique_ptr<TEvBlobStorage::TEvVPutResult> result;
            if (ev->Get()->IsInternal) {
                result = CreateInternalResult(VCtx, NKikimrProto::OK, TString(), ev, now,
                        SelfVDiskId, Db->GetVDiskIncarnationGuid());
            } else {
                result = CreateResult(VCtx, NKikimrProto::OK, TString(), ev, now,
                        SkeletonFrontIDPtr, SelfVDiskId, Db->GetVDiskIncarnationGuid());
            }
            if (info.WrittenBeyondBarrier) {
                result->Record.SetWrittenBeyondBarrier(true);
            }

            // Manage PDisk scheduler weights
            OverloadHandler->ActualizeWeights(ctx, Mask(EHullDbType::LogoBlobs));

            if (!info.IsHugeBlob) {
                auto [logMsg, traceId] = CreatePutLogEvent(ctx, "TEvVPut", ev->Sender, ev->Cookie,
                        std::move(ev->Get()->Orbit), info, std::move(result));
                ctx.Send(Db->LoggerID, logMsg.release(), 0, 0, std::move(traceId));
            } else if (info.Buffer) {
                auto traceId = std::move(info.TraceId);
                // pass the work to huge blob writer
                NKikimrBlobStorage::EPutHandleClass handleClass = record.GetHandleClass();
                auto hugeWrite = CreateHullWriteHugeBlob(ev->Sender, ev->Cookie, ignoreBlock, handleClass, info,
                    std::move(result));
                hugeWrite->Orbit = std::move(ev->Get()->Orbit);
                ctx.Send(Db->HugeKeeperID, hugeWrite.release(), 0, 0, std::move(traceId));
            } else {
                ctx.Send(SelfId(), new TEvHullLogHugeBlob(0, info.BlobId, info.Ingress, TDiskPart(), ignoreBlock,
                    ev->Sender, ev->Cookie, std::move(result), &info.ExtraBlockChecks), 0, 0, std::move(info.TraceId));
            }
        }

        void Handle(TEvHullLogHugeBlob::TPtr &ev, const TActorContext &ctx) {
            TEvHullLogHugeBlob *msg = ev->Get();

            // update hull write duration
            msg->Result->MarkHugeWriteTime();
            bool writtenBeyondBarrier = false;
            auto status = Hull->CheckLogoBlob(ctx, msg->LogoBlobID, msg->IgnoreBlock, msg->ExtraBlockChecks, &writtenBeyondBarrier);
            if (status.Status != NKikimrProto::OK) {
                msg->Result->UpdateStatus(status.Status); // modify status in result
                LOG_DEBUG_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix
                        << "TEvVPut: realtime# false result# " << msg->Result->ToString()
                        << " Marker# BSVS13");
                if (msg->HugeBlob != TDiskPart()) {
                    ctx.Send(Db->HugeKeeperID, new TEvHullHugeBlobLogged(msg->WriteId, msg->HugeBlob, 0, false));
                }
                if (status.Postponed) {
                    Hull->PostponeReplyUntilCommitted(msg->Result.release(), msg->OrigClient, msg->OrigCookie,
                        std::move(ev->TraceId), status.Lsn);
                } else {
                    SendVDiskResponse(ctx, msg->OrigClient, msg->Result.release(), msg->OrigCookie, VCtx);
                }

                return;
            } else if (writtenBeyondBarrier) {
                msg->Result->Record.SetWrittenBeyondBarrier(true);
            }

#ifdef OPTIMIZE_SYNC
            TLsnSeg seg = Db->LsnMngr->AllocLsnForHull();
#else
            TLsnSeg seg = Db->LsnMngr->AllocLsnForHullAndSyncLog();
#endif

            LOG_DEBUG_S(ctx, BS_VDISK_PUT, VCtx->VDiskLogPrefix << "TEvHullHugeBlobLogged Id# " << msg->LogoBlobID
                << " HugeBlob# " << msg->HugeBlob.ToString() << " Lsn# " << seg);

            // prepare synclog msg in advance
#ifdef OPTIMIZE_SYNC
            // nothing to do, don't create synclog record
            std::unique_ptr<NSyncLog::TEvSyncLogPut> syncLogMsg;
#else
            auto syncLogMsg = std::make_unique<NSyncLog::TEvSyncLogPut>(Db->GType, seg.Point(), msg->LogoBlobID.FullID(),
                msg->Ingress);
#endif
            // prepare message to recovery
            NHuge::TPutRecoveryLogRec logRec(msg->LogoBlobID, msg->Ingress, msg->HugeBlob);
            auto dataToWrite = logRec.Serialize();
            UpdatePDiskWriteBytes(dataToWrite.size());
            // prepare TLoggedRecVPutHuge
            auto traceId = ev->TraceId.Clone();
            bool confirmSyncLogAlso = static_cast<bool>(syncLogMsg);
            intptr_t loggedRecId = LoggedRecsVault.Put(new TLoggedRecVPutHuge(seg, confirmSyncLogAlso, Db->HugeKeeperID, ev));
            void *loggedRecCookie = reinterpret_cast<void *>(loggedRecId);
            // create log msg
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignatureHugeLogoBlob, dataToWrite, seg,
                    loggedRecCookie, std::move(syncLogMsg), nullptr);
            // send prepared message to recovery log
            ctx.Send(Db->LoggerID, logMsg.release(), 0, 0, std::move(traceId));
        }

        ////////////////////////////////////////////////////////////////////////
        // SYNCLOG UPDATE SECTOR
        // Currently is used for Handoff deletes (needs to be rewritten)
        // TODO: remove it
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvDelLogoBlobDataSyncLog::TPtr &ev, const TActorContext &ctx) {
            TInstant now = TAppData::TimeProvider->Now();
            auto msg = ev->Get();

            TLsnSeg seg = Db->LsnMngr->AllocLsnForHullAndSyncLog();
            TString serializedLogRecord;
            NKikimrBlobStorage::THandoffDelLogoBlob dump;
            dump.SetIngress(msg->Ingress.Raw());
            LogoBlobIDFromLogoBlobID(msg->Id, dump.MutableBlobID());
            Y_PROTOBUF_SUPPRESS_NODISCARD dump.SerializeToString(&serializedLogRecord);

            std::unique_ptr<NSyncLog::TEvSyncLogPut> syncLogMsg(
                    new NSyncLog::TEvSyncLogPut(Db->GType, seg.Point(), msg->Id, msg->Ingress));
            std::unique_ptr<TEvDelLogoBlobDataSyncLogResult> result(new TEvDelLogoBlobDataSyncLogResult(msg->Id, msg->OrderId, now,
                    nullptr, nullptr));

            bool confirmSyncLogAlso = static_cast<bool>(syncLogMsg);
            THullDbInsert insert{.Id=msg->Id, .Ingress=msg->Ingress};
            intptr_t loggedRecId = LoggedRecsVault.Put(
                    new TLoggedRecDelLogoBlobDataSyncLog(seg, confirmSyncLogAlso, insert, std::move(result), ev->Sender, ev->Cookie));
            void *loggedRecCookie = reinterpret_cast<void *>(loggedRecId);
            // create log msg
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignatureHandoffDelLogoBlob,
                    serializedLogRecord, seg, loggedRecCookie, std::move(syncLogMsg), nullptr);
            // send prepared message to recovery log
            ctx.Send(Db->LoggerID, logMsg.release());
        }


        ////////////////////////////////////////////////////////////////////////
        // ADD BULK SSTABLE SECTOR
        // Add already constructed ssts
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvAddBulkSst::TPtr &ev, const TActorContext &ctx) {
            Y_ABORT("not implemented yet");

            const TLsnSeg seg = Db->LsnMngr->AllocLsnForHull(ev->Get()->Essence.GetLsnRange());
            NPDisk::TCommitRecord commitRecord;
            TString data = ev->Get()->Serialize(commitRecord);
            auto traceId = ev->TraceId.Clone();
            intptr_t loggedRecId = LoggedRecsVault.Put(new TLoggedRecAddBulkSst(seg, false, ev));
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignatureAddBulkSst, commitRecord, data, seg,
                reinterpret_cast<void*>(loggedRecId), nullptr);
            ctx.Send(Db->LoggerID, logMsg.release(), 0, 0, std::move(traceId));
        }


        ////////////////////////////////////////////////////////////////////////
        // GET SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVGet::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendVDiskResponse(ctx, ev->Sender, res.release(), ev->Cookie, VCtx);
        }

        void Handle(TEvBlobStorage::TEvVGet::TPtr &ev, const TActorContext &ctx) {
            IFaceMonGroup->GetMsgs()++;
            TInstant now = TAppData::TimeProvider->Now();
            NKikimrBlobStorage::TEvVGet &record = ev->Get()->Record;

            // FIXME: check PartId() is not null and is not too large

            LOG_DEBUG_S(ctx, BS_VDISK_GET, VCtx->VDiskLogPrefix
                    << "TEvVGet: " << TEvBlobStorage::TEvVGet::ToString(record)
                    << " Marker# BSVS14");

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
            } else if (!CheckVGetQuery(record)) {
                ReplyError(NKikimrProto::ERROR, "get query is invalid", ev, ctx, now);
            } else if (record.HasReaderTabletData()
                    && Hull->IsBlocked(record.GetReaderTabletData().GetId(), {record.GetReaderTabletData().GetGeneration(), 0}).Status != TBlocksCache::EStatus::OK) {
                ReplyError(NKikimrProto::BLOCKED, "tablet's generation is blocked", ev, ctx, now);
            } else {
                if (ev->Get()->Record.HasForceBlockTabletData() && ev->Get()->Record.GetForceBlockTabletData().GetGeneration() > 0) {
                    IFaceMonGroup->BlockAndGetMsgs()++;

                    auto tabletId = ev->Get()->Record.GetForceBlockTabletData().GetId();
                    auto requiredTabletGeneration = ev->Get()->Record.GetForceBlockTabletData().GetGeneration();
                    ui32 blockedTabletGeneration = 0;
                    if (!Hull->GetBlocked(tabletId, &blockedTabletGeneration) || blockedTabletGeneration < requiredTabletGeneration) {
                        // block vdisk and do a get from it
                        auto actor = CreateBlockAndGetActor(
                            std::move(ev),
                            SelfId(),
                            VCtx,
                            SkeletonFrontIDPtr,
                            SelfVDiskId,
                            Db->GetVDiskIncarnationGuid(),
                            GInfo
                        );
                        ctx.Register(actor.release());
                        return;
                    }
                }

                std::optional<THullDsSnap> fullSnap;
                if (record.HasSnapshotId()) {
                    const auto it = Snapshots.find(record.GetSnapshotId());
                    if (it == Snapshots.end()) {
                        return ReplyError(NKikimrProto::ERROR, "snapshot not found", ev, ctx, now);
                    } else {
                        fullSnap.emplace(*it->second.Snap);
                    }
                } else {
                    fullSnap.emplace(Hull->GetSnapshot());
                }

                TMaybe<ui64> cookie;
                if (record.HasCookie())
                    cookie = record.GetCookie();
                auto handleClass = ev->Get()->Record.GetHandleClass();

                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result;
                if (ev->Get()->IsInternal) {
                    result.reset(new TEvBlobStorage::TEvVGetResult(NKikimrProto::OK, SelfVDiskId, now,
                            ev->Get()->GetCachedByteSize(), &record, nullptr, nullptr, nullptr, cookie, ev->GetChannel(),
                            Db->GetVDiskIncarnationGuid()));
                } else {
                    result.reset(new TEvBlobStorage::TEvVGetResult(NKikimrProto::OK, SelfVDiskId, now,
                            ev->Get()->GetCachedByteSize(), &record, ev->Get()->GetIsLocalMon() ? nullptr : SkeletonFrontIDPtr,
                            IFaceMonGroup->GetResMsgsPtr(), VCtx->Histograms.GetHistogram(handleClass), cookie, ev->GetChannel(),
                            Db->GetVDiskIncarnationGuid()));
                }

                if (record.GetAcquireBlockedGeneration()) {
                    ui64 tabletId = record.GetTabletId();
                    if (tabletId) {
                        ui32 blockedGen = 0;
                        Hull->GetBlocked(tabletId, &blockedGen);
                        result->Record.SetBlockedGeneration(blockedGen);
                    }
                }

                // fast keep checker, implemented by Hull
                auto keepChecker = [&hull=Hull] (const TLogoBlobID& id, bool keepByIngress, TString *explanation) {
                    return hull->FastKeep(id, keepByIngress, explanation);
                };
                // create a query actor and pass read-only snapshot to it
                IActor *actor = CreateLevelIndexQueryActor(QueryCtx, std::move(keepChecker), ctx,
                    std::move(*fullSnap), ctx.SelfID, ev, std::move(result), Db->ReplID);

                if (actor) {
                    auto aid = ctx.Register(actor);
                    ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                } else {
                    auto res = std::make_unique<TEvBlobStorage::TEvVGetResult>();
                    res->MakeError(NKikimrProto::ERROR, "incorrect query", record);
                    ctx.Send(ev->Sender, res.release(), 0, ev->Cookie);
                }
                // ReadQuery is responsible for sending result to the recipient
            }
        }


        ////////////////////////////////////////////////////////////////////////
        // BLOCK SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVBlock::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendReply(ctx, std::move(res), ev, BS_VDISK_BLOCK);
        }

        void Handle(TEvBlobStorage::TEvVBlock::TPtr &ev, const TActorContext &ctx) {
            if (!CheckIfWriteAllowed(ev, ctx)) {
                return;
            }
            ++IFaceMonGroup->BlockMsgs();
            TInstant now = TAppData::TimeProvider->Now();
            NKikimrBlobStorage::TEvVBlock &record = ev->Get()->Record;
            const ui64 tabletId = record.GetTabletId();
            const ui32 gen = record.GetGeneration();
            const ui64 issuerGuid = record.GetIssuerGuid();

            if (!OutOfSpaceLogic->Allow(ctx, ev)) {
                ReplyError(NKikimrProto::OUT_OF_SPACE, "out of space", ev, ctx, now);
                return;
            }

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
                return;
            }

            LOG_DEBUG_S(ctx, BS_VDISK_BLOCK, VCtx->VDiskLogPrefix
                    << "TEvVBlock: tabletId# " << tabletId << " gen# " << gen
                    << " Marker# BSVS14");

            TLsnSeg seg;
            ui32 actGen = 0;
            auto checkStatus = Hull->CheckBlockCmdAndAllocLsn(tabletId, gen, issuerGuid, &actGen, &seg);
            NKikimrProto::EReplyStatus status = checkStatus.Status;
            bool postponed = checkStatus.Postponed;
            bool postponeUntilLsn = checkStatus.Lsn;
            TEvBlobStorage::TEvVBlockResult::TTabletActGen act(tabletId, actGen);
            std::unique_ptr<TEvBlobStorage::TEvVBlockResult> result(CreateResult(VCtx, status, checkStatus.ErrorReason, &act,
                ev, now, SkeletonFrontIDPtr, SelfVDiskId, Db->GetVDiskIncarnationGuid()));

            if (status != NKikimrProto::OK) {
                if (postponed) {
                    Hull->PostponeReplyUntilCommitted(result.release(), ev->Sender, ev->Cookie, std::move(ev->TraceId),
                        postponeUntilLsn);
                } else {
                    LOG_DEBUG_S(ctx, BS_VDISK_BLOCK, VCtx->VDiskLogPrefix << "TEvVBlockResult: " << result->ToString()
                            << " Marker# BSVS15");
                    SendReply(ctx, std::move(result), ev, BS_VDISK_BLOCK);
                }

                return;
            }

            OverloadHandler->ActualizeWeights(ctx, Mask(EHullDbType::Blocks));
            // prepare synclog msg in advance
            std::unique_ptr<NSyncLog::TEvSyncLogPut> syncLogMsg(new NSyncLog::TEvSyncLogPut(seg.Point(), tabletId, gen,
                record.GetIssuerGuid()));

            bool confirmSyncLogAlso = static_cast<bool>(syncLogMsg);
            intptr_t loggedRecId = LoggedRecsVault.Put(new TLoggedRecVBlock(seg, confirmSyncLogAlso, tabletId, gen,
                issuerGuid, std::move(result), ev->Sender, ev->Cookie));
            void *loggedRecCookie = reinterpret_cast<void *>(loggedRecId);
            // create log msg
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignatureBlock,
                    ev->GetChainBuffer()->GetString(), seg, loggedRecCookie, std::move(syncLogMsg), nullptr);
            // send prepared message to recovery log
            ctx.Send(Db->LoggerID, logMsg.release(), 0, 0, std::move(ev->TraceId));
        }

        ////////////////////////////////////////////////////////////////////////
        // GET BLOCK SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvBlobStorage::TEvVGetBlock::TPtr &ev, const TActorContext &ctx) {
            using namespace NErrBuilder;
            IFaceMonGroup->GetBlockMsgs()++;
            TInstant now = TAppData::TimeProvider->Now();
            const NKikimrBlobStorage::TEvVGetBlock &record = ev->Get()->Record;
            const ui64 tabletId = record.GetTabletId();

            LOG_DEBUG_S(ctx, BS_VDISK_BLOCK, VCtx->VDiskLogPrefix
                    << "TEvVGetBlock: tabletId# " << tabletId
                    << " Marker# BSVS16");

            std::unique_ptr<TEvBlobStorage::TEvVGetBlockResult> result;
            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                result = std::make_unique<TEvBlobStorage::TEvVGetBlockResult>(NKikimrProto::RACE, tabletId,
                    SelfVDiskId, now, ev->Get()->GetCachedByteSize(), &ev->Get()->Record, SkeletonFrontIDPtr,
                    IFaceMonGroup->GetBlockResMsgsPtr(), nullptr);
            } else {
                ui32 blockedGen = 0;
                bool isBlocked = Hull->GetBlocked(tabletId, &blockedGen);
                if (isBlocked) {
                    result = std::make_unique<TEvBlobStorage::TEvVGetBlockResult>(NKikimrProto::OK, tabletId, blockedGen,
                        SelfVDiskId, now, ev->Get()->GetCachedByteSize(), &ev->Get()->Record, SkeletonFrontIDPtr,
                        IFaceMonGroup->GetBlockResMsgsPtr(), nullptr);
                } else {
                    result = std::make_unique<TEvBlobStorage::TEvVGetBlockResult>(NKikimrProto::NODATA, tabletId,
                        SelfVDiskId, now, ev->Get()->GetCachedByteSize(), &ev->Get()->Record, SkeletonFrontIDPtr,
                        IFaceMonGroup->GetBlockResMsgsPtr(), nullptr);
                }
            }

            LOG_DEBUG_S(ctx, BS_VDISK_BLOCK, VCtx->VDiskLogPrefix
                    << "TEvVGetBlockResult: " << result->ToString()
                    << " Marker# BSVS17");
            SendVDiskResponse(ctx, ev->Sender, result.release(), ev->Cookie, VCtx);
        }

        ////////////////////////////////////////////////////////////////////////
        // GARBAGE COLLECTION SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(THullCheckStatus status, TEvBlobStorage::TEvVCollectGarbage::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status.Status, status.ErrorReason, ev, now,
                SkeletonFrontIDPtr, SelfVDiskId, Db->GetVDiskIncarnationGuid(), GInfo));
            if (status.Postponed) {
                Hull->PostponeReplyUntilCommitted(res.release(), ev->Sender, ev->Cookie, std::move(ev->TraceId), status.Lsn);
            } else {
                SendReply(ctx, std::move(res), ev, BS_VDISK_GC);
            }
        }

        void Handle(TEvBlobStorage::TEvVCollectGarbage::TPtr &ev, const TActorContext &ctx) {
            if (!CheckIfWriteAllowed(ev, ctx)) {
                return;
            }
            IFaceMonGroup->GCMsgs()++;
            TInstant now = TAppData::TimeProvider->Now();
            NKikimrBlobStorage::TEvVCollectGarbage &record = ev->Get()->Record;

            if (!OutOfSpaceLogic->Allow(ctx, ev)) {
                ReplyError({NKikimrProto::OUT_OF_SPACE, "out of space"}, ev, ctx, now);
                return;
            }

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                ReplyError({NKikimrProto::RACE, "group generation mismatch"}, ev, ctx, now);
                return;
            }

            LOG_DEBUG_S(ctx, BS_VDISK_GC, VCtx->VDiskLogPrefix
                    << "TEvVCollectGarbage: " << ev->Get()->ToString()
                    << " Marker# BSVS18");

            TLsnSeg seg;
            TBarrierIngress ingress(HullCtx->IngressCache.Get());
            THullCheckStatus status = Hull->CheckGCCmdAndAllocLsn(ctx, record, ingress, &seg);
            if (status.Status != NKikimrProto::OK) {
                ReplyError(status, ev, ctx, now);
                return;
            }

            OverloadHandler->ActualizeWeights(ctx,
                Mask(EHullDbType::LogoBlobs) | Mask(EHullDbType::Barriers));

            std::unique_ptr<TEvBlobStorage::TEvVCollectGarbageResult> result(CreateResult(VCtx, NKikimrProto::OK, TString(), ev,
                now, SkeletonFrontIDPtr, SelfVDiskId, Db->GetVDiskIncarnationGuid()));

            // prepare synclog msg in advance
            std::unique_ptr<NSyncLog::TEvSyncLogPut> syncLogMsg(
                new NSyncLog::TEvSyncLogPut(Db->GType, seg.Last, record, ingress));

            auto traceId = ev->TraceId.Clone();
            TString data = ev->GetChainBuffer()->GetString();
            intptr_t loggedRecId = LoggedRecsVault.Put(new TLoggedRecVCollectGarbage(seg, true, ingress, std::move(result), ev));
            void *loggedRecCookie = reinterpret_cast<void *>(loggedRecId);
            // create log msg
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignatureGC, data, seg, loggedRecCookie,
                    std::move(syncLogMsg), nullptr);
            // send prepared message to recovery log
            ctx.Send(Db->LoggerID, logMsg.release(), 0, 0, std::move(traceId));
        }


        ////////////////////////////////////////////////////////////////////////
        // GET BARRIER SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVGetBarrier::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendVDiskResponse(ctx, ev->Sender, res.release(), ev->Cookie, VCtx);
        }

        void Handle(TEvBlobStorage::TEvVGetBarrier::TPtr &ev, const TActorContext &ctx) {
            IFaceMonGroup->GetBarrierMsgs()++;
            TInstant now = TAppData::TimeProvider->Now();
            NKikimrBlobStorage::TEvVGetBarrier &record = ev->Get()->Record;
            LOG_DEBUG_S(ctx, BS_VDISK_GC, VCtx->VDiskLogPrefix
                    << "TEvVGetBarrier: " << ev->Get()->ToString()
                    << " Marker# BSVS19");

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
            } else if (!CheckVGetBarrierQuery(record)) {
                ReplyError(NKikimrProto::ERROR, "get barrier query invalid", ev, ctx, now);
            } else {
                std::unique_ptr<TEvBlobStorage::TEvVGetBarrierResult> result;
                result = std::make_unique<TEvBlobStorage::TEvVGetBarrierResult>(NKikimrProto::OK, SelfVDiskId,
                    now, ev->Get()->GetCachedByteSize(), &record, SkeletonFrontIDPtr,
                    IFaceMonGroup->GetBarrierResMsgsPtr(), nullptr);
                THullDsSnap fullSnap = Hull->GetIndexSnapshot();
                fullSnap.LogoBlobsSnap.Destroy();
                fullSnap.BlocksSnap.Destroy();
                IActor *actor = CreateLevelIndexBarrierQueryActor(HullCtx, ctx.SelfID, std::move(fullSnap.BarriersSnap),
                    ev, std::move(result));
                auto aid = ctx.Register(actor);
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                // ReadBarrier is responsible for sending result to the recipient
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // STATUS SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvBlobStorage::TEvVStatus::TPtr &ev, const TActorContext &ctx) {
            IFaceMonGroup->StatusMsgs()++;
            TInstant now = TAppData::TimeProvider->Now();
            LOG_DEBUG_S(ctx, BS_VDISK_OTHER, VCtx->VDiskLogPrefix << "TEvVStatus Marker# BSVS20");
            auto aid = ctx.Register(CreateStatusRequestHandler(VCtx, Db->SkeletonID, Db->SyncerID, Db->SyncLogID,
                IFaceMonGroup, SelfVDiskId, Db->GetVDiskIncarnationGuid(), GInfo, ev, ctx.SelfID, now, ReplDone));
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvLocalStatus::TPtr &ev, const TActorContext &ctx) {
            std::unique_ptr<TEvLocalStatusResult> result(new TEvLocalStatusResult());
            // hull status
            Hull->StatusRequest(ctx, result.get());
            // local recovery status
            NKikimrBlobStorage::TLocalRecoveryInfo *localRecoveryStatus = result->Record.MutableLocalRecoveryInfo();
            LocalRecovInfo->FillIn(localRecoveryStatus);
            // return result
            ctx.Send(ev->Sender, result.release());
        }

        ////////////////////////////////////////////////////////////////////////
        // DBSTAT SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVDbStat::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendVDiskResponse(ctx, ev->Sender, res.release(), ev->Cookie, VCtx);
        }

        void Handle(TEvBlobStorage::TEvVDbStat::TPtr &ev, const TActorContext &ctx) {
            IFaceMonGroup->DbStatMsgs()++;
            TInstant now = TAppData::TimeProvider->Now();
            const NKikimrBlobStorage::TEvVDbStat &record = ev->Get()->Record;
            LOG_DEBUG_S(ctx, BS_VDISK_OTHER, VCtx->VDiskLogPrefix << "TEvVDbStat"
                    << " Marker# BSVS21");

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
            } else {
                auto result = std::make_unique<TEvBlobStorage::TEvVDbStatResult>(NKikimrProto::OK, SelfVDiskId, now,
                    IFaceMonGroup->DbStatResMsgsPtr(), nullptr);
                THullDsSnap fullSnap = Hull->GetIndexSnapshot();
                IActor *actor = CreateDbStatActor(HullCtx, HugeBlobCtx, ctx, std::move(fullSnap),
                        ctx.SelfID, ev, std::move(result));
                if (actor) {
                    auto aid = ctx.Register(actor);
                    ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                }
                // CreateDbStatActor is responsible for sending result to the recipient
            }
        }

        void Handle(TEvGetLogoBlobIndexStatRequest::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, BS_VDISK_OTHER, VCtx->VDiskLogPrefix << "TEvGetLogoBlobIndexStatRequest"
                    << " Marker# BSVS42");

            auto result = std::make_unique<TEvGetLogoBlobIndexStatResponse>(NKikimrProto::OK, SelfVDiskId, ctx.Now(),
                nullptr, nullptr);
            THullDsSnap fullSnap = Hull->GetIndexSnapshot();
            IActor *actor = CreateDbStatActor(HullCtx, HugeBlobCtx, ctx, std::move(fullSnap),
                    ctx.SelfID, ev, std::move(result));
            if (actor) {
                auto aid = ctx.Register(actor);
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // STREAM QUERIES
        ////////////////////////////////////////////////////////////////////////

        THashMap<TString, TActorId> MonStreamActors;

        void Handle(TEvBlobStorage::TEvMonStreamQuery::TPtr& ev, const TActorContext& ctx) {
            TActorId& actorId = MonStreamActors[ev->Get()->StreamId];
            if (actorId == TActorId()) {
                actorId = RunInBatchPool(ctx, CreateMonStreamActor(Hull->GetIndexSnapshot(), ev));
                ActiveActors.Insert(actorId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }
            ctx.ExecutorThread.ActorSystem->Send(ev->Forward(actorId));
        }

        void Handle(TEvBlobStorage::TEvMonStreamActorDeathNote::TPtr& ev, const TActorContext& /*ctx*/) {
            auto it = MonStreamActors.find(ev->Get()->StreamId);
            Y_ABORT_UNLESS(it != MonStreamActors.end());
            ActiveActors.Erase(it->second);
            MonStreamActors.erase(it);
        }

        ////////////////////////////////////////////////////////////////////////
        // ASSIMILATION
        ////////////////////////////////////////////////////////////////////////

        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVAssimilate::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr,
                SelfVDiskId, Db->GetVDiskIncarnationGuid(), GInfo));
            SendVDiskResponse(ctx, ev->Sender, res.release(), ev->Cookie, VCtx);
        }

        void Handle(TEvBlobStorage::TEvVAssimilate::TPtr& ev, const TActorContext& ctx) {
            if (!SelfVDiskId.SameDisk(ev->Get()->Record.GetVDiskID())) {
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, TAppData::TimeProvider->Now());
            } else if (!BlockWrites(GInfo->DecommitStatus) && !ev->Get()->Record.GetIgnoreDecommitState()) {
                ReplyError(NKikimrProto::ERROR, "decommission didn't start yet", ev, ctx, TAppData::TimeProvider->Now());
            } else {
                const TActorId actorId = RunInBatchPool(ctx, CreateAssimilationActor(Hull->GetIndexSnapshot(), ev, SelfVDiskId));
                ActiveActors.Insert(actorId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // COMPACT SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Reply(const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/, TEvBlobStorage::TEvVCompact::TPtr &ev,
                   const TActorContext &ctx, const TInstant &/*now*/) {
            auto result = std::make_unique<TEvBlobStorage::TEvVCompactResult>(status, SelfVDiskId);
            SendVDiskResponse(ctx, ev->Sender, result.release(), ev->Cookie, VCtx);
        }

        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVCompact::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            Reply(status, errorReason, ev, ctx, now);
        }

        void Handle(TEvBlobStorage::TEvVCompact::TPtr &ev, const TActorContext &ctx) {
            TInstant now = TAppData::TimeProvider->Now();
            const NKikimrBlobStorage::TEvVCompact &record = ev->Get()->Record;
            LOG_DEBUG_S(ctx, BS_VDISK_OTHER, VCtx->VDiskLogPrefix << "TEvVCompact"
                    << " Marker# BSVS22");

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
            } else if (!VDiskCompactionState) {
                ReplyError(NKikimrProto::NOTREADY, "vdisk is not initialized", ev, ctx, now);
            } else {
                auto opType = record.GetOpType();

                switch (opType) {
                    case NKikimrBlobStorage::TEvVCompact::ASYNC:
                    {
                        Y_ABORT_UNLESS(Db->LoggerID);
                        // forward this message to logger, because it knows correct lsn
                        ctx.Send(ev->Forward(Db->LoggerID));
                        // reply back
                        Reply(NKikimrProto::OK, TString(), ev, ctx, now);
                        break;
                    }
                    default:
                    {
                        // for SYNC option we can run an actor that sends local TEvCompactVDisk message
                        // to Skeleton, waits for result, and replies back to the client
                        // reply back: not implemented/don't understand the command
                        Reply(NKikimrProto::ERROR, TString(), ev, ctx, now);
                    }
                }
            }
        }

        // local message TEvCompactVDisk, replies back with TEvCompactVDiskResult when compaction finished
        void Handle(TEvCompactVDisk::TPtr &ev, const TActorContext &ctx) {
            std::optional<ui64> lsn = LoggedRecsVault.GetLastLsnInFlight();
            TVDiskCompactionState::TCompactionReq req;
            req.CompactLogoBlobs = bool(ev->Get()->Mask & Mask(EHullDbType::LogoBlobs));
            req.CompactBlocks = bool(ev->Get()->Mask & Mask(EHullDbType::Blocks));
            req.CompactBarriers = bool(ev->Get()->Mask & Mask(EHullDbType::Barriers));
            req.Mode = ev->Get()->Mode;
            req.ClientId = ev->Sender;
            req.ClientCookie = ev->Cookie;
            req.Reply = std::make_unique<TEvCompactVDiskResult>();
            VDiskCompactionState->Setup(ctx, lsn, std::move(req));
        }

        void Handle(TEvHullCompactResult::TPtr &ev, const TActorContext &ctx) {
            Y_ABORT_UNLESS(VDiskCompactionState);
            VDiskCompactionState->Compacted(ctx, ev->Get()->RequestId, ev->Get()->Type, VCtx);
        }

        ////////////////////////////////////////////////////////////////////////
        // BALD SYNC LOG SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Reply(const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                TEvBlobStorage::TEvVBaldSyncLog::TPtr &ev, const TActorContext &ctx, const TInstant &/*now*/) {
            auto result = std::make_unique<TEvBlobStorage::TEvVBaldSyncLogResult>(status, SelfVDiskId);
            SendVDiskResponse(ctx, ev->Sender, result.release(), ev->Cookie, VCtx);
        }

        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVBaldSyncLog::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            Reply(status, errorReason, ev, ctx, now);
        }

        void Handle(TEvBlobStorage::TEvVBaldSyncLog::TPtr &ev, const TActorContext &ctx) {
            TInstant now = TAppData::TimeProvider->Now();
            const NKikimrBlobStorage::TEvVBaldSyncLog &record = ev->Get()->Record;
            LOG_DEBUG_S(ctx, BS_VDISK_OTHER, VCtx->VDiskLogPrefix << "TEvVBaldSyncLog"
                    << " Marker# BSVS23");

            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
            } else {
                Y_ABORT_UNLESS(Db->SyncLogID);
                // forward this message to SyncLog
                ctx.Send(ev->Forward(Db->SyncLogID));
                // reply back
                Reply(NKikimrProto::OK, TString(), ev, ctx, now);
            }
        }


        ////////////////////////////////////////////////////////////////////////
        // SYNC SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVSync::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendVDiskResponse(ctx, ev->Sender, res.release(), ev->Cookie, VCtx);
        }

        void Handle(TEvBlobStorage::TEvVSync::TPtr &ev, const TActorContext &ctx) {
            ctx.Send(ev->Forward(Db->SyncLogID));
        }

        ////////////////////////////////////////////////////////////////////////
        // SYNC GUID SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVSyncGuid::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendVDiskResponse(ctx, ev->Sender, res.release(), ev->Cookie, VCtx);
        }

        // FIXME: check for RACE in other handlers!!!

        void Handle(TEvBlobStorage::TEvVSyncGuid::TPtr &ev, const TActorContext &ctx) {
            const NKikimrBlobStorage::TEvVSyncGuid &record = ev->Get()->Record;
            TInstant now = TAppData::TimeProvider->Now();
            if (!SelfVDiskId.SameGroupAndGeneration(record.GetSourceVDiskID())) {
                auto protoVDisk = VDiskIDFromVDiskID(record.GetSourceVDiskID());
                LOG_WARN_S(ctx, NKikimrServices::BS_SKELETON, VCtx->VDiskLogPrefix
                        << "TSkeleton::Handle(TEvBlobStorage::TEvVSyncGuid): Source:"
                        << " Self# " << SelfVDiskId << " Source# " << protoVDisk
                        << " Marker# BSVS24");
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
            }
            if (!SelfVDiskId.SameDisk(record.GetTargetVDiskID())) {
                auto protoVDisk = VDiskIDFromVDiskID(record.GetTargetVDiskID());
                LOG_WARN_S(ctx, NKikimrServices::BS_SKELETON, VCtx->VDiskLogPrefix
                        << "TSkeleton::Handle(TEvBlobStorage::TEvVSyncGuid): Target:"
                        << " Self# " << SelfVDiskId << " Source# " << protoVDisk
                        << " Marker# BSVS25");
                ReplyError(NKikimrProto::RACE, "group generation mismatch", ev, ctx, now);
            }

            ctx.Send(ev->Forward(Db->SyncerID));
        }

        ////////////////////////////////////////////////////////////////////////
        // LOCAL SYNC DATA SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/, TEvLocalSyncData::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            auto result = std::make_unique<TEvLocalSyncDataResult>(status, now, SyncLogIFaceGroup.LocalSyncResMsgsPtr(),
                nullptr);
            SendReply(ctx, std::move(result), ev, BS_VDISK_OTHER);
        }

        void Handle(TEvLocalSyncData::TPtr &ev, const TActorContext &ctx) {
            const bool postpone = OverloadHandler->PostponeEvent(ev);
            if (!postpone) {
                PrivateHandle(ev, ctx);
            }
        }

        void PrivateHandle(TEvLocalSyncData::TPtr &ev, const TActorContext &ctx) {
            TInstant now = TAppData::TimeProvider->Now();
            SyncLogIFaceGroup.LocalSyncMsgs()++;

            if (!OutOfSpaceLogic->Allow(ctx, ev)) {
                ReplyError(NKikimrProto::OUT_OF_SPACE, "out of space", ev, ctx, now);
                return;
            }

#ifdef UNPACK_LOCALSYNCDATA
            Y_ABORT_UNLESS(ev->Get()->Extracted.IsReady());
            TLsnSeg seg = Hull->AllocateLsnForSyncDataCmd(ev->Get()->Extracted);
#else
            TLsnSeg seg = Hull->AllocateLsnForSyncDataCmd(ev->Get()->Data);
#endif
            std::unique_ptr<TEvLocalSyncDataResult> result(
                new TEvLocalSyncDataResult(NKikimrProto::OK, now, SyncLogIFaceGroup.LocalSyncResMsgsPtr(),
                nullptr));

            OverloadHandler->ActualizeWeights(ctx, AllEHullDbTypes);

            auto traceId = ev->TraceId.Clone();
            TString data = ev->Get()->Serialize();
            intptr_t loggedRecId = LoggedRecsVault.Put(new TLoggedRecLocalSyncData(seg, false, std::move(result), ev));
            void *loggedRecCookie = reinterpret_cast<void *>(loggedRecId);
            // create log msg
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignatureLocalSyncData, data, seg,
                    loggedRecCookie, nullptr, nullptr);
            // send prepared message to recovery log
            ctx.Send(Db->LoggerID, logMsg.release(), 0, 0, std::move(traceId));
        }

        ////////////////////////////////////////////////////////////////////////
        // ANUBIS/OSIRIS SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvCallOsiris::TPtr &ev, const TActorContext &ctx) {
            // NOTE:
            // We take ordinary snapshot. Their can be a race, that LocalSyncData
            // may be in-flight at this moment, so we can't take lsn exclusively.
            // Alternatively we can make a barrier and wait until all data is written
            // and confirmed.
            const ui64 confirmedLsn = Db->LsnMngr->GetConfirmedLsnForHull();
            THullDsSnap fullSnap = Hull->GetIndexSnapshot();
            IActor *actor = CreateHullOsiris(ev->Sender, ctx.SelfID, ctx.SelfID, std::move(fullSnap), confirmedLsn,
                    Config->AnubisOsirisMaxInFly);
            auto aid = ctx.Register(actor);
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvAnubisOsirisPut::TPtr &ev, const TActorContext &ctx) {
            const bool postpone = OverloadHandler->PostponeEvent(ev);
            if (!postpone) {
                PrivateHandle(ev, ctx);
            }
        }

        void ReplyError(const NKikimrProto::EReplyStatus status,
                        const TString& /*errorReason*/,
                        TEvAnubisOsirisPut::TPtr &ev,
                        const TActorContext &ctx,
                        const TInstant &now) {
            std::unique_ptr<IEventBase> res(new TEvAnubisOsirisPutResult(status, now, IFaceMonGroup->PutResMsgsPtr(), nullptr));
            SendReply(ctx, std::move(res), ev, BS_VDISK_PUT);
        }

        void PrivateHandle(TEvAnubisOsirisPut::TPtr &ev, const TActorContext &ctx) {
            const auto *msg = ev->Get();

            // update basic counters
            TInstant now = TAppData::TimeProvider->Now();
            (msg->IsAnubis() ? IFaceMonGroup->AnubisPutMsgs() : IFaceMonGroup->OsirisPutMsgs())++;

            if (!OutOfSpaceLogic->Allow(ctx, ev)) {
                ReplyError(NKikimrProto::OUT_OF_SPACE, "out of space", ev, ctx, now);
                return;
            }

            THullDbInsert insert = msg->PrepareInsert(VCtx->Top.get(), VCtx->ShortSelfVDisk);
            TLsnSeg seg = Db->LsnMngr->AllocLsnForHullAndSyncLog();

            // Manage PDisk scheduler weights
            OverloadHandler->ActualizeWeights(ctx, Mask(EHullDbType::LogoBlobs));

            std::unique_ptr<TEvAnubisOsirisPutResult> result(new TEvAnubisOsirisPutResult(NKikimrProto::OK, now,
                (msg->IsAnubis() ? IFaceMonGroup->AnubisPutResMsgsPtr() : IFaceMonGroup->OsirisPutResMsgsPtr()), nullptr));
            // log data
            TAnubisOsirisPutRecoveryLogRec logRec(*msg);
            TString data = logRec.Serialize();

            // prepare synclog msg in advance
            auto syncLogMsg = std::make_unique<NSyncLog::TEvSyncLogPut>(Db->GType, seg.Point(), insert.Id, insert.Ingress);

            intptr_t loggedRecId = LoggedRecsVault.Put(new TLoggedRecAnubisOsirisPut(seg, true, insert, std::move(result), ev));
            void *loggedRecCookie = reinterpret_cast<void *>(loggedRecId);
            // create log msg
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignatureAnubisOsirisPut, data, seg,
                    loggedRecCookie, std::move(syncLogMsg), nullptr);
            // send prepared message to recovery log
            ctx.Send(Db->LoggerID, logMsg.release());
        }


        ////////////////////////////////////////////////////////////////////////
        // TAKE SNAPSHOT SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvTakeHullSnapshot::TPtr &ev, const TActorContext &ctx) {
            auto fullSnap = ev->Get()->Index ? Hull->GetIndexSnapshot() : Hull->GetSnapshot();
            ctx.Send(ev->Sender, new TEvTakeHullSnapshotResult(std::move(fullSnap)));
        }

        ////////////////////////////////////////////////////////////////////////
        // SYNC FULL SECTOR
        ////////////////////////////////////////////////////////////////////////
        void ReplyError(const NKikimrProto::EReplyStatus status, const TString& errorReason, TEvBlobStorage::TEvVSyncFull::TPtr &ev,
                        const TActorContext &ctx, const TInstant &now) {
            using namespace NErrBuilder;
            std::unique_ptr<IEventBase> res(ErroneousResult(VCtx, status, errorReason, ev, now, SkeletonFrontIDPtr, SelfVDiskId,
                    Db->GetVDiskIncarnationGuid(), GInfo));
            SendVDiskResponse(ctx, ev->Sender, res.release(), ev->Cookie, VCtx);
        }

        void Handle(TEvBlobStorage::TEvVSyncFull::TPtr &ev, const TActorContext &ctx) {
            // run handler in the same mailbox
            TInstant now = TAppData::TimeProvider->Now();

            ui64 dbBirthLsn = 0;
            const ui64 confirmedLsn = Db->LsnMngr->GetConfirmedLsnForHull();
            dbBirthLsn = *DbBirthLsn;
            auto aid = ctx.RegisterWithSameMailbox(CreateHullSyncFullHandler(Db, HullCtx, SelfVDiskId, ctx.SelfID, Hull,
                IFaceMonGroup, ev, now, dbBirthLsn, confirmedLsn));
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        ////////////////////////////////////////////////////////////////////////
        // LoggedRecord SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
            CHECK_PDISK_RESPONSE(VCtx, ev, ctx);
            const NPDisk::TEvLogResult::TResults &results = ev->Get()->Results;
            for (const auto &elem : results) {
                intptr_t loggedRecId = reinterpret_cast<intptr_t>(elem.Cookie);
                LWTRACK(VDiskSkeletonRecordLogged, elem.Orbit, elem.Lsn);

                std::unique_ptr<ILoggedRec> loggedRec(LoggedRecsVault.Extract(loggedRecId));
                Db->LsnMngr->ConfirmLsnForHull(loggedRec->Seg, loggedRec->ConfirmSyncLogAlso);
                loggedRec->Replay(*Hull, ctx);
            }
            if (VDiskCompactionState && !results.empty()) {
                VDiskCompactionState->Logged(ctx, results.back().Lsn);
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // REPL SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvRecoveredHugeBlob::TPtr &ev, const TActorContext &ctx) {
            TInstant now = TAppData::TimeProvider->Now();
            IFaceMonGroup->RecoveredHugeBlobMsgs()++;

            const TEvRecoveredHugeBlob *msg = ev->Get();
            const TLogoBlobID& id = msg->Id;
            LOG_DEBUG_S(ctx, BS_REPL, VCtx->VDiskLogPrefix << "TSkeleton::Handle(TEvRecoveredHugeBlob): id# " << id
                    << " Marker# BSVS26");

            TRope buf = std::move(msg->Data);
            const ui64 bufSize = buf.GetSize();
            Y_ABORT_UNLESS(bufSize <= Config->MaxLogoBlobDataSize,
                    "TEvRecoveredHugeBlob: blob is huge bufSize# %zu", bufSize);
            UpdatePDiskWriteBytes(bufSize);

            auto oosStatus = VCtx->GetOutOfSpaceState().GetGlobalStatusFlags();
            auto result = std::make_unique<TEvBlobStorage::TEvVPutResult>(NKikimrProto::OK, id, SelfVDiskId, nullptr,
                oosStatus, now, 0, nullptr, nullptr, IFaceMonGroup->RecoveredHugeBlobResMsgsPtr(), nullptr, bufSize,
                0, TString());

            // pass the work to huge blob writer
            TIngress ingress = *TIngress::CreateIngressWithLocal(VCtx->Top.get(), SelfVDiskId, id);
            if (buf) {
                ctx.Send(Db->HugeKeeperID, new TEvHullWriteHugeBlob(ev->Sender, ev->Cookie, id, ingress, std::move(buf),
                    true, NKikimrBlobStorage::EPutHandleClass::AsyncBlob, std::move(result), nullptr));
            } else {
                ctx.Send(SelfId(), new TEvHullLogHugeBlob(0, id, ingress, TDiskPart(), true, ev->Sender, ev->Cookie,
                    std::move(result), nullptr));
            }
        }

        void Handle(TEvDetectedPhantomBlob::TPtr& ev, const TActorContext& ctx) {
            TEvDetectedPhantomBlob *msg = ev->Get();

            for (const TLogoBlobID& logoBlobId : msg->Phantoms) {
                LOG_NOTICE_S(ctx, NKikimrServices::BS_SKELETON, VCtx->VDiskLogPrefix
                        << "adding DoNotKeep to phantom LogoBlobId# " << logoBlobId
                        << " Marker# BSVS27");
            }

            TLsnSeg seg = Hull->AllocateLsnForPhantoms(msg->Phantoms);

            // generate sync log message with collected blobs
            auto syncLogMsg = std::make_unique<NSyncLog::TEvSyncLogPut>(Db->GType, seg.First, msg->Phantoms);

            // serialize message to pass it to log
            NKikimrVDiskData::TPhantomLogoBlobs record;
            for (const TLogoBlobID& id : msg->Phantoms) {
                LogoBlobIDFromLogoBlobID(id, record.AddLogoBlobs());
            }
            TString data;
            bool res = record.SerializeToString(&data);
            Y_ABORT_UNLESS(res);

            intptr_t loggedRecId = LoggedRecsVault.Put(new TLoggedRecPhantoms(seg, true, ev));
            void *loggedRecCookie = reinterpret_cast<void *>(loggedRecId);
            // create log msg
            auto logMsg = CreateHullUpdate(HullLogCtx, TLogSignature::SignaturePhantomBlobs, data, seg,
                    loggedRecCookie, std::move(syncLogMsg), nullptr);
            // send prepared message to recovery log
            ctx.Send(Db->LoggerID, logMsg.release());
        }

        ////////////////////////////////////////////////////////////////////////
        // RECOVERY SECTOR
        ////////////////////////////////////////////////////////////////////////
        void DumpDatabases(IOutputStream &resultStream) {
            using TDumper = TDbDumper<TKeyLogoBlob, TMemRecLogoBlob>;
            // create dumper
            const ui64 limitInBytes = 10u * 1024u * 1024u; // limit number of bytes in output
            THullDsSnap fullSnap = Hull->GetSnapshot();
            TDumper dumper(HullCtx, std::move(fullSnap.LogoBlobsSnap), limitInBytes, {}, {});

            // final stream
            TStringStream str;

            // dump db
            TStringStream dump;
            typename TDumper::EDumpRes status = dumper.Dump(dump);
            Y_ABORT_UNLESS(status == TDumper::EDumpRes::OK);

            str << "========= " << VCtx->VDiskLogPrefix << " ==========\n";
            str << dump.Str() << "\n";
            str << "=======================================================\n";

            resultStream << str.Str();
        }

        void SkeletonIsUpAndRunning(const TActorContext &ctx, bool runRepl = false) {
            Become(&TThis::StateNormal);
            VDiskMonGroup.VDiskState(NKikimrWhiteboard::EVDiskState::OK);
            LOG_INFO_S(ctx, BS_SKELETON, VCtx->VDiskLogPrefix << "SKELETON IS UP AND RUNNING"
                    << " Marker# BSVS28");
            // notify SkeletonFront
            auto msg = std::make_unique<TEvFrontRecoveryStatus>(TEvFrontRecoveryStatus::SyncGuidRecoveryDone,
                                                          NKikimrProto::OK,
                                                          (PDiskCtx ? PDiskCtx->Dsk : nullptr),
                                                          MinREALHugeBlobInBytes,
                                                          Db->GetVDiskIncarnationGuid());
            ctx.Send(*SkeletonFrontIDPtr, msg.release());
            ctx.Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
                NKikimrConsole::TConfigItem::BlobStorageConfigItem,
                SelfId()
            ));

            Hull->PermitGarbageCollection(ctx);
            // propagate status to Node Warden unless replication is on -- in that case it sets the status itself
            if (!runRepl) {
                ReplDone = true;
            }
            UpdateReplState();
            RunBalancing(ctx);
        }

        void SkeletonErrorState(const TActorContext &ctx,
                                TEvFrontRecoveryStatus::EPhase phase,
                                NKikimrWhiteboard::EVDiskState state)
        {
            Become(&TThis::StateDatabaseError);
            VDiskMonGroup.VDiskState(state);
            // notify SkeletonFront
            auto msg = std::make_unique<TEvFrontRecoveryStatus>(phase,
                                                          NKikimrProto::ERROR,
                                                          (PDiskCtx ? PDiskCtx->Dsk : nullptr),
                                                          MinREALHugeBlobInBytes,
                                                          Db->GetVDiskIncarnationGuid());
            ctx.Send(*SkeletonFrontIDPtr, msg.release());
            // push the status
            UpdateVDiskStatus(NKikimrBlobStorage::ERROR);
        }

        void StartDefrag(const TActorContext &ctx) {
            auto defragCtx = std::make_shared<TDefragCtx>(VCtx, Config, HugeBlobCtx, PDiskCtx, ctx.SelfID,
                Db->HugeKeeperID, true);
            DefragId = ctx.Register(CreateDefragActor(defragCtx, GInfo));
            ActiveActors.Insert(DefragId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE); // keep forever
        }

        void StartScrubberActor(const TActorContext &ctx, NKikimrVDiskData::TScrubEntrypoint scrubEntrypoint, ui64 scrubEntrypointLsn) {
            auto scrubCtx = MakeIntrusive<TScrubContext>(
                VCtx,
                PDiskCtx,
                GInfo,
                SelfId(),
                Hull->GetHullDs()->LogoBlobs->LIActor,
                SelfId().NodeId(),
                Config->BaseInfo.PDiskId,
                Config->BaseInfo.VDiskSlotId,
                Config->BaseInfo.ScrubCookie,
                Db->GetVDiskIncarnationGuid(),
                Db->LsnMngr,
                Db->LoggerID,
                Db->LogCutterID);
            ScrubId = ctx.Register(CreateScrubActor(std::move(scrubCtx), std::move(scrubEntrypoint), scrubEntrypointLsn));
            ActiveActors.Insert(ScrubId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvBlobStorage::TEvLocalRecoveryDone::TPtr &ev, const TActorContext &ctx) {
            LocalRecovInfo = ev->Get()->RecovInfo;
            LocalDbRecoveryID = TActorId();
            ActiveActors.Erase(ev->Sender);

            PDiskCtx = ev->Get()->PDiskCtx;
            HullCtx = ev->Get()->HullCtx;
            HugeBlobCtx = ev->Get()->HugeBlobCtx;
            Db->LocalRecoveryInfo = ev->Get()->RecovInfo;
            Db->LsnMngr = ev->Get()->LsnMngr;
            Db->SetVDiskIncarnationGuid(ev->Get()->VDiskIncarnationGuid);

            // check status
            if (ev->Get()->Status == NKikimrProto::OK) {
                ApplyHugeBlobSize(Config->MinHugeBlobInBytes);
                Y_ABORT_UNLESS(MinREALHugeBlobInBytes); 
                // handle special case when donor disk starts and finds out that it has been wiped out
                if (ev->Get()->LsnMngr->GetOriginallyRecoveredLsn() == 0 && Config->BaseInfo.DonorMode) {
                    // send drop donor cmd to NodeWarden
                    const TVDiskID vdiskId(GInfo->GroupID, GInfo->GroupGeneration, VCtx->ShortSelfVDisk);
                    Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvBlobStorage::TEvDropDonor(SelfId().NodeId(),
                        Config->BaseInfo.PDiskId, Config->BaseInfo.VDiskSlotId, vdiskId));

                    // transit to error state and await deletion
                    return SkeletonErrorState(ctx, TEvFrontRecoveryStatus::LocalRecoveryDone,
                        NKikimrWhiteboard::EVDiskState::LocalRecoveryError);
                }

                // notify skeketon front about recovery status
                auto msg = std::make_unique<TEvFrontRecoveryStatus>(TEvFrontRecoveryStatus::LocalRecoveryDone,
                                                              NKikimrProto::OK,
                                                              PDiskCtx->Dsk,
                                                              MinREALHugeBlobInBytes,
                                                              Db->GetVDiskIncarnationGuid());
                ctx.Send(*SkeletonFrontIDPtr, msg.release());

                // place new incarnation guid on whiteboard
                using TEv = NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate;
                ctx.Send(*SkeletonFrontIDPtr, new TEv(TEv::UpdateIncarnationGuid, Db->GetVDiskIncarnationGuid()));

                // we got a recovered local DB here
                LOG_INFO_S(ctx, BS_SKELETON, VCtx->VDiskLogPrefix << "SKELETON LOCAL RECOVERY SUCCEEDED"
                        << " Marker# BSVS29");

                // run logger forwarder
                auto logWriter = CreateRecoveryLogWriter(PDiskCtx->PDiskId, Db->SkeletonID,
                        PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound, Db->LsnMngr->GetStartLsn(),
                        VCtx->VDiskCounters);
                Db->LoggerID.Set(ctx.Register(logWriter));
                ActiveActors.Insert(Db->LoggerID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE); // keep forever

                // run out of disk space tracker
                Db->DskSpaceTrackerID.Set(ctx.Register(CreateDskSpaceTracker(VCtx, PDiskCtx,
                    Config->DskTrackerInterval)));
                ActiveActors.Insert(Db->DskSpaceTrackerID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE); // keep forever

                // run LogCutter in the same mailbox
                TLogCutterCtx logCutterCtx = {VCtx, PDiskCtx, Db->LsnMngr, Config,
                        (TActorId)(Db->LoggerID)};
                Db->LogCutterID.Set(ctx.RegisterWithSameMailbox(CreateRecoveryLogCutter(std::move(logCutterCtx))));
                ActiveActors.Insert(Db->LogCutterID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE); // keep forever

                // run HugeBlobKeeper
                TString localRecovInfoStr = Db->LocalRecoveryInfo ? Db->LocalRecoveryInfo->ToString() : TString("{}");
                auto hugeKeeperCtx = std::make_shared<THugeKeeperCtx>(VCtx, PDiskCtx, Db->LsnMngr,
                        ctx.SelfID, (TActorId)(Db->LoggerID), (TActorId)(Db->LogCutterID),
                        localRecovInfoStr, Config->BaseInfo.ReadOnly);
                auto hugeKeeper = CreateHullHugeBlobKeeper(hugeKeeperCtx, ev->Get()->RepairedHuge);
                Db->HugeKeeperID.Set(ctx.Register(hugeKeeper));
                ActiveActors.Insert(Db->HugeKeeperID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE); // keep forever

                // run SyncLogActor
                std::unique_ptr<NSyncLog::TSyncLogRepaired> repairedSyncLog = std::move(ev->Get()->RepairedSyncLog);
                Y_ABORT_UNLESS(SelfVDiskId == GInfo->GetVDiskId(VCtx->ShortSelfVDisk));
                auto slCtx = MakeIntrusive<NSyncLog::TSyncLogCtx>(
                        VCtx,
                        Db->LsnMngr,
                        PDiskCtx,
                        Db->LoggerID,
                        Db->LogCutterID,
                        Config->SyncLogMaxDiskAmount,
                        Config->SyncLogMaxEntryPointSize,
                        Config->SyncLogMaxMemAmount,
                        Config->MaxResponseSize,
                        Db->SyncLogFirstLsnToKeep,
                        Config->BaseInfo.ReadOnly);
                Db->SyncLogID.Set(ctx.Register(CreateSyncLogActor(slCtx, GInfo, SelfVDiskId, std::move(repairedSyncLog))));
                ActiveActors.Insert(Db->SyncLogID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE); // keep forever

                // create HullLogCtx
                HullLogCtx = std::make_shared<THullLogCtx>(VCtx, PDiskCtx, Db->SkeletonID, Db->SyncLogID,
                    Db->HugeKeeperID);

                // create Hull
                Hull = std::make_shared<THull>(Db->LsnMngr, PDiskCtx, Db->SkeletonID,
                        Config->FeatureFlags.GetUseVDisksBalancing(), std::move(*ev->Get()->Uncond),
                        ctx.ExecutorThread.ActorSystem, Config->BarrierValidation);
                ActiveActors.Insert(Hull->RunHullServices(Config, HullLogCtx, Db->SyncLogFirstLsnToKeep,
                        Db->LoggerID, Db->LogCutterID, ctx), ctx, NKikimrServices::BLOBSTORAGE);

                // create VDiskCompactionState
                VDiskCompactionState = std::make_unique<TVDiskCompactionState>(Hull->GetHullDs()->LogoBlobs->LIActor,
                    Hull->GetHullDs()->Blocks->LIActor, Hull->GetHullDs()->Barriers->LIActor);

                // initialize Out Of Space Logic
                OutOfSpaceLogic = std::make_shared<TOutOfSpaceLogic>(VCtx, Hull);

                // initialize QueryCtx
                QueryCtx = std::make_shared<TQueryCtx>(HullCtx, PDiskCtx, SelfId());

                // create overload handler
                auto vMovedPatch = [this] (const TActorContext &ctx, TEvBlobStorage::TEvVMovedPatch::TPtr ev) {
                    this->PrivateHandle(ev, ctx);
                };
                auto vPatchStart = [this] (const TActorContext &ctx, TEvBlobStorage::TEvVPatchStart::TPtr ev) {
                    this->PrivateHandle(ev, ctx);
                };
                auto vput = [this] (const TActorContext &ctx, TEvBlobStorage::TEvVPut::TPtr ev) {
                    this->PrivateHandle(ev, ctx);
                };
                auto vMultiPutHandler = [this] (const TActorContext &ctx, TEvBlobStorage::TEvVMultiPut::TPtr ev) {
                    this->PrivateHandle(ev, ctx);
                };
                auto loc = [this] (const TActorContext &ctx, TEvLocalSyncData::TPtr ev) {
                    this->PrivateHandle(ev, ctx);
                };
                auto aoput = [this] (const TActorContext &ctx, TEvAnubisOsirisPut::TPtr ev) {
                    this->PrivateHandle(ev, ctx);
                };
                NMonGroup::TSkeletonOverloadGroup overloadMonGroup(VCtx->VDiskCounters, "subsystem", "emergency");
                OverloadHandler = std::make_unique<TOverloadHandler>(VCtx, PDiskCtx, Hull,
                    std::move(overloadMonGroup), std::move(vMovedPatch), std::move(vPatchStart), std::move(vput),
                    std::move(vMultiPutHandler), std::move(loc), std::move(aoput));
                ScheduleWakeupEmergencyPutQueue(ctx);

                // actualize weights before we start
                OverloadHandler->ActualizeWeights(ctx, AllEHullDbTypes, true);

                // run Anubis
                if (Config->RunAnubis && !Config->BaseInfo.DonorMode) {
                    auto anubisCtx = std::make_shared<TAnubisCtx>(HullCtx, ctx.SelfID,
                        Config->ReplInterconnectChannel, Config->AnubisOsirisMaxInFly, Config->AnubisTimeout);
                    Db->AnubisRunnerID.Set(ctx.Register(CreateAnubisRunner(anubisCtx, GInfo)));
                }

                if (Config->RunDefrag && AppData()->FeatureFlags.GetAllowVDiskDefrag()) {
                    StartDefrag(ctx);
                }

                // create scrubber actor
                if (Config->RunScrubber) {
                    StartScrubberActor(ctx, std::move(ev->Get()->ScrubEntrypoint), ev->Get()->ScrubEntrypointLsn);
                }

                // create syncer actor
                if (Config->RunSyncer && !Config->BaseInfo.DonorMode) {
                    // switch to syncronization step
                    Become(&TThis::StateSyncGuidRecovery);
                    VDiskMonGroup.VDiskState(NKikimrWhiteboard::EVDiskState::SyncGuidRecovery);
                    // create syncer context
                    auto sc = MakeIntrusive<TSyncerContext>(VCtx,
                        Db->LsnMngr,
                        PDiskCtx,
                        ctx.SelfID,
                        Db->AnubisRunnerID,
                        Db->LoggerID,
                        Db->LogCutterID,
                        Db->SyncLogID,
                        Config);
                    // syncer performes sync recovery
                    Db->SyncerID.Set(ctx.Register(CreateSyncerActor(sc, GInfo, ev->Get()->SyncerData)));
                    ActiveActors.Insert(Db->SyncerID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE); // keep forever
                } else {
                    // continue without sync
                    SkeletonIsUpAndRunning(ctx);
                }

                // Deliver CutLog that we may receive if not initialized
                DeliverDelayedCutLogIfAny(ctx);
            } else {
                LOG_INFO_S(ctx, BS_SKELETON, VCtx->VDiskLogPrefix << "SKELETON LOCAL RECOVERY FAILED"
                        << " Marker# BSVS30");
                auto phase = TEvFrontRecoveryStatus::LocalRecoveryDone;
                auto state = NKikimrWhiteboard::EVDiskState::LocalRecoveryError;
                SkeletonErrorState(ctx, phase, state);
            }
        }

        void Handle(TEvSyncGuidRecoveryDone::TPtr &ev, const TActorContext &ctx) {
            if (ev->Get()->Status == NKikimrProto::OK) {
                LOG_INFO_S(ctx, BS_SKELETON, VCtx->VDiskLogPrefix << "SKELETON SYNC GUID RECOVERY SUCCEEDED"
                        << " Marker# BSVS31");
                DbBirthLsn = ev->Get()->DbBirthLsn;
                SkeletonIsUpAndRunning(ctx, Config->RunRepl);
                if (Config->RunRepl) {
                    auto replCtx = std::make_shared<TReplCtx>(VCtx, HullCtx, PDiskCtx, HugeBlobCtx, MinREALHugeBlobInBytes, Hull->GetHullDs(),
                        GInfo, SelfId(), Config, PDiskWriteBytes, Config->ReplPausedAtStart);
                    Db->ReplID.Set(ctx.Register(CreateReplActor(replCtx)));
                    ActiveActors.Insert(Db->ReplID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE); // keep forever
                    if (CommenceRepl) {
                        TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvCommenceRepl, 0, Db->ReplID, SelfId(),
                            nullptr, 0));
                    }
                }
            } else {
                LOG_INFO_S(ctx, BS_SKELETON, VCtx->VDiskLogPrefix << "SKELETON SYNC GUID RECOVERY FAILED"
                        << " Marker# BSVS32");
                auto phase = TEvFrontRecoveryStatus::SyncGuidRecoveryDone;
                auto state = NKikimrWhiteboard::EVDiskState::SyncGuidRecoveryError;
                SkeletonErrorState(ctx, phase, state);
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // MONITORING SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            switch (auto subrequest = ev->Get()->SubRequestId) {
                case 0: {
                    // calculate id for the actor who'll tell us about local recovery
                    TActorId locRecovActor = LocalDbRecoveryID ? LocalDbRecoveryID : ctx.SelfID;
                    auto aid = ctx.Register(CreateSkeletonMonRequestHandler(Db, ev, ctx.SelfID, locRecovActor));
                    ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                    break;
                }
                case TDbMon::SkeletonStateId: {
                    TStringStream str;
                    RenderState(str, ctx);
                    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::SkeletonStateId));
                    break;
                }
                case TDbMon::HullInfoId: {
                    TStringStream str;
                    if (Hull) {
                        Hull->OutputHtmlForDb(str);
                    }
                    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::HullInfoId));
                    break;
                }
                case TDbMon::LocalRecovInfoId: {
                    TStringStream str;
                    if (LocalRecovInfo)
                        LocalRecovInfo->OutputHtml(str);
                    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::LocalRecovInfoId));
                    break;
                }
                case TDbMon::DelayedCompactionDeleterId: {
                    TStringStream str;
                    if (Hull) {
                        Hull->OutputHtmlForHugeBlobDeleter(str);
                    }
                    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), subrequest));
                    break;
                }
                case TDbMon::ScrubId:
                    if (ScrubId) {
                        ctx.Send(ev->Forward(ScrubId));
                    } else {
                        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("scrub actor is not started", subrequest));
                    }
                    break;
                case TDbMon::DbMainPageLogoBlobs:
                case TDbMon::DbMainPageBlocks:
                case TDbMon::DbMainPageBarriers: {
                    TStringStream str;
                    VDiskCompactionState->RenderHtml(str, TDbMon::ESubRequestID(subrequest));
                    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), subrequest));
                    break;
                }
                case TDbMon::Defrag:
                    if (DefragId) {
                        ctx.Send(ev->Forward(DefragId));
                    } else {
                        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("defrag actor is not started", subrequest));
                    }
                    break;
                default:
                    break;
            }
        }

        void Handle(TEvVDiskStatRequest::TPtr &ev, const TActorContext &ctx) {
            auto res = std::make_unique<TEvVDiskStatResponse>();
            if (Hull) {
                Hull->OutputProtoForDb(res->Record.mutable_statistic());
            }
            ctx.Send(ev->Sender, res.release());
        }

        void RenderState(IOutputStream &str, const TActorContext &ctx) {
            constexpr ui32 threshold = 10000u;
            std::pair<ui32, ui32> actorQueues = ctx.CountMailboxEvents(threshold);

            HTML(str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Skeleton";
                    }
                    DIV_CLASS("panel-body") {
                        if (OverloadHandler) {
                            OverloadHandler->RenderHtml(str);
                        }

                        TABLE_CLASS ("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {str << "Queues";}
                                    TABLEH() {str << "Size";}
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() {str << "ActorQueue";}
                                    TABLED() {
                                        if (actorQueues.first >= threshold)
                                            str << "More than " << threshold;
                                        else
                                            str << actorQueues.first;
                                    }
                                }
                                TABLER() {
                                    TABLED() {str << "MailboxQueue";}
                                    TABLED() {
                                        if (actorQueues.second >= threshold)
                                            str << "More than " << threshold;
                                        else
                                            str << actorQueues.second;
                                    }
                                }
                                TABLER() {
                                    TABLED() {str << "ElapsedTicksAsSeconds";}
                                    TABLED() {str << GetElapsedTicksAsSeconds();}
                                }
                                TABLER() {
                                    TABLED() {str << "HandledEvents";}
                                    TABLED() {str << GetHandledEvents();}
                                }
                            }
                        }

                        TABLE_CLASS("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {str << "Setting";}
                                    TABLEH() {str << "Value";}
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() {str << "SelfVDiskID";}
                                    TABLED() {str << SelfVDiskId.ToString();}
                                }
                                TABLER() {
                                    TABLED() {str << "StoragePoolName";}
                                    TABLED() {str << Config->BaseInfo.StoragePoolName;}
                                }
                                TABLER() {
                                    TABLED() {str << "Erasure";}
                                    TABLED() {str << Db->GType.GetErasure();}
                                }
                                TABLER() {
                                    TABLED() {str << "OrderNum/TotalVDisks";}
                                    TABLED() {
                                        if (HullCtx && HullCtx->IngressCache) {
                                            str << ui32(HullCtx->IngressCache->VDiskOrderNum) << "/"
                                                << ui32(HullCtx->IngressCache->TotalVDisks);
                                        } else {
                                            str << "Unknown/Unknown";
                                        }
                                    }
                                }
                                TABLER() {
                                    TABLED() {str << "VDiskKind";}
                                    TABLED() {str << Config->BaseInfo.Kind;}
                                }
                                TABLER() {
                                    TABLED() {str << "PDiskId";}
                                    TABLED() {str << Config->BaseInfo.PDiskId;}
                                }
                                TABLER() {
                                    TABLED() {str << "BlobStorage GroupId (decimal)";}
                                    TABLED() {str << GInfo->GroupID;}
                                }
                                TABLER() {
                                    TABLED() {str << "VDiskIncarnationGuid";}
                                    TABLED() {str << Db->GetVDiskIncarnationGuid(true);}
                                }

                                if (PDiskCtx && PDiskCtx->Dsk) {
                                    NPDisk::EDeviceType trueMedia = PDiskCtx->Dsk->TrueMediaType;
                                    TABLER() {
                                        TABLED() {str << "TrueMediaType";}
                                        TABLED() {str << NPDisk::DeviceTypeStr(trueMedia, true); }
                                    }
                                    TABLER() {
                                        TABLED() {str << "BurstThresholdNs";}
                                        TABLED() {str << (i64)Config->CostMetricsParametersByMedia[trueMedia].BurstThresholdNs;}
                                    }
                                    TABLER() {
                                        TABLED() {str << "DiskTimeAvailableScale";}
                                        TABLED() {str << 0.001 * Config->CostMetricsParametersByMedia[trueMedia].DiskTimeAvailableScale;}
                                    }
                                }
                            }
                        }

                        if (PDiskCtx && PDiskCtx->Dsk)
                            PDiskCtx->Dsk->OutputHtml(str);

                        if (OutOfSpaceLogic) {
                            str << "<br/>";
                            COLLAPSED_BUTTON_CONTENT("outofspacedetails", "Out of Space Logic Details") {
                                OutOfSpaceLogic->RenderHtml(str);
                            }
                        }
                    }
                }
            }

        }

        ////////////////////////////////////////////////////////////////////////
        // CUT LOG FORWARDER SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
            if (Config->BaseInfo.ReadOnly) {
                return;
            }
            std::unique_ptr<NPDisk::TEvCutLog> msg(ev->Release().Release());

            if (LocalDbInitialized) {
                Y_DEBUG_ABORT_UNLESS(msg->Owner == PDiskCtx->Dsk->Owner);
                Y_ABORT_UNLESS(!CutLogDelayedMsg);
                LOG_DEBUG_S(ctx, BS_LOGCUTTER, VCtx->VDiskLogPrefix
                        << "Handle " << msg->ToString()
                        << " actorid# " << ctx.SelfID.ToString()
                        << " Marker# BSVS33");
                SpreadCutLog(std::move(msg), ctx);
            } else {
                LOG_DEBUG_S(ctx, BS_LOGCUTTER, VCtx->VDiskLogPrefix
                        << "Handle " << msg->ToString()
                        << " DELAYED actorid# " << ctx.SelfID.ToString()
                        << " Marker# BSVS34");
                CutLogDelayedMsg = std::move(msg);
            }
        }

        void SpreadCutLog(std::unique_ptr<NPDisk::TEvCutLog> msg, const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(msg->Owner == PDiskCtx->Dsk->Owner);

            ui32 counter = 0;
            // setup FreeUpToLsn for Hull Database
            if (Hull) {
                Hull->CutRecoveryLog(ctx, std::unique_ptr<NPDisk::TEvCutLog>(msg->Clone()));
                ++counter;
            }
            // setup FreeUpToLsn for Syncer
            if (Db->SyncerID) {
                ctx.Send(Db->SyncerID, msg->Clone());
                ++counter;
            }
            // setup FreeUpToLsn for SyncLog
            if (Db->SyncLogID) {
                ctx.Send(Db->SyncLogID, msg->Clone());
                ++counter;
            }
            if (Db->HugeKeeperID) {
                ctx.Send(Db->HugeKeeperID, msg->Clone());
                ++counter;
            }
            if (Db->LogCutterID) {
                ctx.Send(Db->LogCutterID, msg->Clone());
                ++counter;
            }
            if (ScrubId) {
                ctx.Send(ScrubId, msg->Clone());
                ++counter;
            }

            LOG_DEBUG_S(ctx, BS_LOGCUTTER, VCtx->VDiskLogPrefix
                    << "SpreadCutLog: Handle " << msg->ToString()
                    << " DELAYED; counter# " << counter
                    << " actorid# " << ctx.SelfID.ToString()
                    << " Marker# BSVS35");
        }

        // NOTE: We can get NPDisk::TEvCutLog when local recovery is not finished.
        // We save this message in CutLogDelayedMsg and deliver it later after
        // completion local recovery
        void DeliverDelayedCutLogIfAny(const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, BS_LOGCUTTER, VCtx->VDiskLogPrefix
                    << "DeliverDelayedCutLogIfAny: hasMsg# " << (CutLogDelayedMsg ? "true" : "false")
                    << " actorid# " << ctx.SelfID.ToString()
                    << " Marker# BSVS36");

            LocalDbInitialized = true;
            if (CutLogDelayedMsg) {
                Y_DEBUG_ABORT_UNLESS(CutLogDelayedMsg->Owner == PDiskCtx->Dsk->Owner);
                SpreadCutLog(std::exchange(CutLogDelayedMsg, nullptr), ctx);
                Y_ABORT_UNLESS(!CutLogDelayedMsg);
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // OTHER MESSAGES SECTOR
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            auto *msg = ev->Get();

            // Save locally
            GInfo = msg->NewInfo;
            SelfVDiskId = msg->NewVDiskId;

            // clear VPatchCtx
            VPatchCtx = nullptr;

            // send command to Synclog
            ctx.Send(Db->SyncLogID, ev->Get()->Clone());
            // send command to Syncer
            ctx.Send(Db->SyncerID, ev->Get()->Clone());
            // send command to Repl
            ctx.Send(Db->ReplID, ev->Get()->Clone());
            // send command to AnubisRunner
            ctx.Send(Db->AnubisRunnerID, ev->Get()->Clone());
            // send command to scrub actor
            ctx.Send(ScrubId, ev->Get()->Clone());
            // send command to defrag actor
            if (DefragId) {
                ctx.Send(DefragId, ev->Get()->Clone());
            }
            if (BalancingId) {
                ctx.Send(BalancingId, ev->Get()->Clone());
            }

            // FIXME: reconfigure handoff
        }

        void HandleReplDone(STFUNC_SIG) {
            if (ev->Cookie) { // semi-finished replication, only phantom blobs
                ReplOnlyPhantomsRemain = true;
            } else {
                ReplDone = true;
            }
            UpdateReplState();
        }

        void Ignore(const TActorContext&)
        {}

        void UpdateVDiskStatus(NKikimrBlobStorage::EVDiskStatus status) {
            const auto& base = Db->Config->BaseInfo;
            Send(NodeWardenServiceId, new TEvStatusUpdate(SelfId().NodeId(), base.PDiskId, base.VDiskSlotId, status,
                ReplOnlyPhantomsRemain));
        }

        ////////////////////////////////////////////////////////////////////////
        // STATES SECTOR
        ////////////////////////////////////////////////////////////////////////
        friend class TActorBootstrapped<TSkeleton>;

        void Bootstrap(const TActorContext &ctx) {
            LOG_INFO_S(ctx, BS_SKELETON, VCtx->VDiskLogPrefix << "SKELETON START"
                    << " Marker# BSVS37");
            Become(&TThis::StateLocalRecovery);
            Db->SkeletonID.Set(ctx.SelfID);
            // generation independent self VDisk Id
            auto genIndSelfVDiskId = SelfVDiskId;
            genIndSelfVDiskId.GroupGeneration = -1;
            LocalDbRecoveryID = ctx.Register(CreateDatabaseLocalRecoveryActor(VCtx, Config, genIndSelfVDiskId, SelfId(),
                *SkeletonFrontIDPtr, Arena));
            ActiveActors.Insert(LocalDbRecoveryID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            UpdateWhiteboard(ctx);
        }

        void Handle(TEvents::TEvActorDied::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            ActiveActors.Erase(ev->Sender);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        void HandleCommenceRepl(const TActorContext& /*ctx*/) {
            CommenceRepl = true;
            if (Db->ReplID) {
                TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvCommenceRepl, 0, Db->ReplID, SelfId(), nullptr, 0));
            }
        }
        
        void PassAway() override {
            Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest(
                SelfId()
            ));
            TActor::PassAway();
        }

        void ForwardToScrubActor(STFUNC_SIG) {
            Forward(ev, ScrubId);
        }

        void ForwardToDefragActor(STFUNC_SIG) {
            Forward(ev, DefragId);
        }

        void Handle(TEvReportScrubStatus::TPtr ev, const TActorContext& ctx) {
            HasUnreadableBlobs = ev->Get()->HasUnreadableBlobs;
            UpdateReplState();
            ctx.Send(ev->Forward(*SkeletonFrontIDPtr));
        }

        void UpdateReplState() {
            const bool ready = ReplDone && !HasUnreadableBlobs;
            UpdateVDiskStatus(ready ? NKikimrBlobStorage::READY : NKikimrBlobStorage::REPLICATING);
        }

        void Handle(TEvRestoreCorruptedBlob::TPtr ev, const TActorContext& ctx) {
            ctx.Register(CreateRestoreCorruptedBlobActor(SelfId(), ev, GInfo, VCtx, PDiskCtx));
        }

        void Handle(TEvBlobStorage::TEvCaptureVDiskLayout::TPtr ev, const TActorContext& ctx) {
            ctx.Register(new TCaptureVDiskLayoutActor(ev, Hull->GetSnapshot()));
        }

        void ForwardToLogoBlobsLevelIndexActor(STFUNC_SIG) {
            Forward(ev, Hull->GetHullDs()->LogoBlobs->LIActor);
        }

        void Handle(NPDisk::TEvChunkForgetResult::TPtr ev) {
            CHECK_PDISK_RESPONSE(VCtx, ev, TActivationContext::AsActorContext());
        }

        void Handle(TEvBlobStorage::TEvVTakeSnapshot::TPtr ev, const TActorContext& ctx) {
            const auto& record = ev->Get()->Record;
            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                SendVDiskResponse(ctx, ev->Sender, new TEvBlobStorage::TEvVTakeSnapshotResult(NKikimrProto::RACE,
                    "group generation race", SelfVDiskId), ev->Cookie, VCtx);
            } else {
                const auto [it, inserted] = Snapshots.try_emplace(record.GetSnapshotId());
                TSnapshotInfo& snapshot = it->second;
                const TMonotonic expirationTimestamp = ctx.Monotonic() + TDuration::Seconds(record.GetTimeToLiveSec());
                NKikimrProto::EReplyStatus status = NKikimrProto::OK;
                if (inserted) {
                    snapshot.SnapshotId = it->first;
                    snapshot.Snap.emplace(Hull->GetSnapshot());
                    snapshot.ExpirationIt = SnapshotExpirationMap.emplace(expirationTimestamp, &snapshot);
                } else {
                    status = NKikimrProto::ALREADY;
                    auto nh = SnapshotExpirationMap.extract(snapshot.ExpirationIt);
                    nh.key() = expirationTimestamp;
                    snapshot.ExpirationIt = SnapshotExpirationMap.insert(std::move(nh));
                }
                RescheduleSnapshotExpirationCheck();
                SendVDiskResponse(ctx, ev->Sender, new TEvBlobStorage::TEvVTakeSnapshotResult(status,
                    {}, SelfVDiskId), ev->Cookie, VCtx);
            }
        }

        void Handle(TEvBlobStorage::TEvVReleaseSnapshot::TPtr ev, const TActorContext& ctx) {
            const auto& record = ev->Get()->Record;
            if (!SelfVDiskId.SameDisk(record.GetVDiskID())) {
                SendVDiskResponse(ctx, ev->Sender, new TEvBlobStorage::TEvVReleaseSnapshotResult(NKikimrProto::RACE,
                    "group generation race", SelfVDiskId), ev->Cookie, VCtx);
            } else if (const auto it = Snapshots.find(record.GetSnapshotId()); it != Snapshots.end()) {
                TSnapshotInfo& snapshot = it->second;
                SnapshotExpirationMap.erase(snapshot.ExpirationIt);
                Snapshots.erase(it);
                SendVDiskResponse(ctx, ev->Sender, new TEvBlobStorage::TEvVReleaseSnapshotResult(NKikimrProto::OK,
                    {}, SelfVDiskId), ev->Cookie, VCtx);
            } else {
                SendVDiskResponse(ctx, ev->Sender, new TEvBlobStorage::TEvVReleaseSnapshotResult(NKikimrProto::NODATA,
                    {}, SelfVDiskId), ev->Cookie, VCtx);
            }
        }

        void RescheduleSnapshotExpirationCheck() {
            if (!SnapshotExpirationMap.empty()) {
                const TMonotonic when = SnapshotExpirationMap.begin()->first;
                if (SnapshotExpirationCheckSchedule.empty() || when < SnapshotExpirationCheckSchedule.front()) {
                    TActivationContext::Schedule(when, new IEventHandle(TEvPrivate::EvCheckSnapshotExpiration, 0,
                        SelfId(), {}, nullptr, when.GetValue()));
                    SnapshotExpirationCheckSchedule.push_front(when);
                }
            }
        }

        void CheckSnapshotExpiration(TAutoPtr<IEventHandle> ev, const TActorContext& ctx) {
            auto schedIt = std::find(SnapshotExpirationCheckSchedule.begin(), SnapshotExpirationCheckSchedule.end(),
                TMonotonic::FromValue(ev->Cookie));
            Y_ABORT_UNLESS(schedIt != SnapshotExpirationCheckSchedule.end());
            SnapshotExpirationCheckSchedule.erase(schedIt);

            const TMonotonic now = ctx.Monotonic();
            TSnapshotExpirationMap::iterator it;
            for (it = SnapshotExpirationMap.begin(); it != SnapshotExpirationMap.end() && now <= it->first; ++it) {
                Snapshots.erase(TString(it->second->SnapshotId));
            }
            SnapshotExpirationMap.erase(SnapshotExpirationMap.begin(), it);

            RescheduleSnapshotExpirationCheck();
        }

        void Handle(TEvReplInvoke::TPtr ev) {
            if (Db->ReplID) {
                TActivationContext::Send(ev->Forward(Db->ReplID));
            } else {
                HandleReplNotInProgress(ev);
            }
        }

        void HandleReplNotInProgress(TEvReplInvoke::TPtr ev) {
            ev->Get()->Callback({}, "replication is not in progress");
        }

        void RunBalancing(const TActorContext &ctx) {
            if (!Config->FeatureFlags.GetUseVDisksBalancing() || VCtx->Top->GType.GetErasure() == TErasureType::ErasureMirror3of4) {
                return;
            }
            if (BalancingId) {
                Send(BalancingId, new NActors::TEvents::TEvPoison());
                ActiveActors.Erase(BalancingId);
            }
            auto balancingCtx = std::make_shared<TBalancingCtx>(
                VCtx, PDiskCtx, HugeBlobCtx, SelfId(), Hull->GetSnapshot(), Config, GInfo, MinREALHugeBlobInBytes);
            BalancingId = ctx.Register(CreateBalancingActor(balancingCtx));
            ActiveActors.Insert(BalancingId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        // NOTES: we have 4 state functions, one of which is an error state (StateDatabaseError) and
        // others are good: StateLocalRecovery, StateSyncGuidRecovery, StateNormal
        // We switch between states in the following manner:
        // 1. StateLocalRecovery. Initial state when we recover local DB from the recovery log.
        //    In this state we initialize Hull DB, Sync log, Syncer etc from snapshot and redo
        //    what we have in the recovery log.
        //    In this state we don't accept any request that changes local (and group) db
        // 2. StateSyncGuidRecovery. We determine can we trust our local database or not,
        //    i.e. did we loose the data. If we lost our data we peform data recovery that makes
        //    ourself trustable. We can't serve user requests in this state, because we may lie.
        // 3. StateNormal. After quorum sync we get into this state. We serve all requests in this
        //    state. We don't care about sync quorum anymore, it's responsibility of blobstorage
        //    proxy to perform some action if too many vdisks become unavailable.

        STRICT_STFUNC(StateLocalRecovery,
            // We should not get these requests while performing LocalRecovery
            // TEvBlobStorage::TEvVPut
            // TEvDelLogoBlobDataSyncLog
            // TEvAddBulkSst
            // TEvBlobStorage::TEvVGet
            // TEvBlobStorage::TEvVBlock
            // TEvBlobStorage::TEvVGetBlock
            // TEvBlobStorage::TEvVCollectGarbage
            // TEvBlobStorage::TEvVGetBarrier
            // TEvBlobStorage::TEvVSync
            // TEvBlobStorage::TEvVSyncFull
            // TEvCallOsiris
            // TEvAnubisOsirisPut
            // TEvBlobStorage::TEvVSyncGuid
            // TEvSyncGuidRecoveryDone -- can't get in this state
            // TEvBlobStorage::TEvVStatus
            // TEvBlobStorage::TEvVDbStat
            // TEvBlobStorage::TEvVCompact
            IgnoreFunc(TEvBlobStorage::TEvVDefrag);
            // TEvHullCompactResult
            // TEvCompactVDisk
            // TEvBlobStorage::TEvVBaldSyncLog
            HFunc(TEvBlobStorage::TEvLocalRecoveryDone, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvVDiskStatRequest, Handle)
            CFunc(TEvBlobStorage::EvTimeToUpdateWhiteboard, UpdateWhiteboard)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvVGenerationChange, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvents::TEvActorDied, Handle)
            CFunc(TEvBlobStorage::EvCommenceRepl, HandleCommenceRepl)
            fFunc(TEvBlobStorage::EvScrubAwait, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvRecoverBlob, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvNonrestoredCorruptedBlobNotify, ForwardToScrubActor)
            HFunc(TEvProxyQueueState, Handle)
            hFunc(NPDisk::TEvChunkForgetResult, Handle)
            FFunc(TEvPrivate::EvCheckSnapshotExpiration, CheckSnapshotExpiration)
            hFunc(TEvReplInvoke, HandleReplNotInProgress)
        )

        STRICT_STFUNC(StateSyncGuidRecovery,
            HFunc(TEvBlobStorage::TEvVPut, HandlePutSyncGuidRecovery)
            HFunc(TEvHullLogHugeBlob, Handle)
            HFunc(TEvDelLogoBlobDataSyncLog, Handle)
            HFunc(TEvAddBulkSst, Handle)
            // We should not get these requests while performing SyncGuidRecovery
            // TEvBlobStorage::TEvVGet
            // TEvBlobStorage::TEvVBlock
            // TEvBlobStorage::TEvVGetBlock
            // TEvBlobStorage::TEvVCollectGarbage
            // TEvBlobStorage::TEvVGetBarrier
            // TEvBlobStorage::TEvVSync
            // TEvBlobStorage::TEvVSyncFull
            HFunc(TEvCallOsiris, Handle)
            HFunc(TEvAnubisOsirisPut, Handle)
            HFunc(TEvBlobStorage::TEvVSyncGuid, Handle)
            HFunc(TEvSyncGuidRecoveryDone, Handle)
            HFunc(TEvLocalSyncData, Handle)
            // TEvBlobStorage::TEvVStatus
            // TEvBlobStorage::TEvVDbStat
            HFunc(TEvBlobStorage::TEvVCompact, Handle)
            fFunc(TEvBlobStorage::EvVDefrag, ForwardToDefragActor)
            HFunc(TEvCompactVDisk, Handle)
            HFunc(TEvHullCompactResult, Handle)
            HFunc(TEvBlobStorage::TEvVBaldSyncLog, Handle)
            HFunc(NPDisk::TEvLogResult, Handle)
            CFunc(TEvBlobStorage::EvCompactionFinished, LevelIndexCompactionFinished)
            CFunc(TEvBlobStorage::EvKickEmergencyPutQueue, KickEmergencyPutQueue)
            CFunc(TEvBlobStorage::EvWakeupEmergencyPutQueue, WakeupEmergencyPutQueue)
            HFunc(TEvTakeHullSnapshot, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvVDiskStatRequest, Handle)
            CFunc(TEvBlobStorage::EvTimeToUpdateWhiteboard, UpdateWhiteboard)
            HFunc(TEvLocalStatus, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(NPDisk::TEvConfigureSchedulerResult, Handle)
            HFunc(TEvVGenerationChange, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvents::TEvActorDied, Handle)
            CFunc(TEvBlobStorage::EvCommenceRepl, HandleCommenceRepl)
            fFunc(TEvBlobStorage::EvControllerScrubStartQuantum, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvScrubAwait, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvRecoverBlob, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvNonrestoredCorruptedBlobNotify, ForwardToScrubActor)
            HFunc(TEvReportScrubStatus, Handle)
            HFunc(TEvRestoreCorruptedBlob, Handle)
            HFunc(TEvBlobStorage::TEvCaptureVDiskLayout, Handle)
            HFunc(TEvProxyQueueState, Handle)
            hFunc(NPDisk::TEvChunkForgetResult, Handle)
            FFunc(TEvPrivate::EvCheckSnapshotExpiration, CheckSnapshotExpiration)
            hFunc(TEvReplInvoke, HandleReplNotInProgress)
        )

        STRICT_STFUNC(StateNormal,
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(TEvBlobStorage::TEvVMovedPatch, Handle)
            HFunc(TEvBlobStorage::TEvVPatchStart, Handle)
            HFunc(TEvBlobStorage::TEvVPatchDiff, HandleVPatchDiffResending)
            HFunc(TEvBlobStorage::TEvVPatchXorDiff, HandleVPatchDiffResending)
            hFunc(TEvVPatchDyingRequest, Handle)
            HFunc(TEvBlobStorage::TEvVPut, Handle)
            HFunc(TEvBlobStorage::TEvVMultiPut, Handle)
            HFunc(TEvHullLogHugeBlob, Handle)
            HFunc(TEvDelLogoBlobDataSyncLog, Handle)
            HFunc(TEvAddBulkSst, Handle)
            HFunc(TEvBlobStorage::TEvVGet, Handle)
            HFunc(TEvBlobStorage::TEvVBlock, Handle)
            HFunc(TEvBlobStorage::TEvVGetBlock, Handle)
            HFunc(TEvBlobStorage::TEvVCollectGarbage, Handle)
            HFunc(TEvBlobStorage::TEvVGetBarrier, Handle)
            HFunc(TEvBlobStorage::TEvVSync, Handle)
            HFunc(TEvBlobStorage::TEvVSyncFull, Handle)
            // TEvCallOsiris
            // TEvAnubisOsirisPut
            HFunc(TEvBlobStorage::TEvVSyncGuid, Handle)
            // TEvSyncGuidRecoveryDone
            HFunc(TEvLocalSyncData, Handle)
            HFunc(NPDisk::TEvLogResult, Handle)
            CFunc(TEvBlobStorage::EvCompactionFinished, LevelIndexCompactionFinished)
            CFunc(TEvBlobStorage::EvKickEmergencyPutQueue, KickEmergencyPutQueue)
            CFunc(TEvBlobStorage::EvWakeupEmergencyPutQueue, WakeupEmergencyPutQueue)
            HFunc(TEvRecoveredHugeBlob, Handle)
            HFunc(TEvDetectedPhantomBlob, Handle)
            HFunc(TEvBlobStorage::TEvVStatus, Handle)
            HFunc(TEvBlobStorage::TEvVAssimilate, Handle)
            HFunc(TEvBlobStorage::TEvVDbStat, Handle)
            HFunc(TEvGetLogoBlobIndexStatRequest, Handle)
            HFunc(TEvBlobStorage::TEvMonStreamQuery, Handle)
            HFunc(TEvBlobStorage::TEvMonStreamActorDeathNote, Handle)
            HFunc(TEvBlobStorage::TEvVCompact, Handle)
            fFunc(TEvBlobStorage::EvVDefrag, ForwardToDefragActor)
            HFunc(TEvCompactVDisk, Handle)
            HFunc(TEvHullCompactResult, Handle)
            HFunc(TEvBlobStorage::TEvVBaldSyncLog, Handle)
            HFunc(TEvBlobStorage::TEvVTakeSnapshot, Handle)
            HFunc(TEvBlobStorage::TEvVReleaseSnapshot, Handle)
            HFunc(TEvTakeHullSnapshot, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvVDiskStatRequest, Handle)
            CFunc(TEvBlobStorage::EvTimeToUpdateWhiteboard, UpdateWhiteboard)
            HFunc(TEvLocalStatus, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(NPDisk::TEvConfigureSchedulerResult, Handle)
            HFunc(TEvVGenerationChange, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvents::TEvActorDied, Handle)
            fFunc(TEvBlobStorage::EvReplDone, HandleReplDone)
            CFunc(TEvBlobStorage::EvCommenceRepl, HandleCommenceRepl)
            fFunc(TEvBlobStorage::EvControllerScrubStartQuantum, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvScrubAwait, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvRecoverBlob, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvNonrestoredCorruptedBlobNotify, ForwardToScrubActor)
            HFunc(TEvReportScrubStatus, Handle)
            HFunc(TEvRestoreCorruptedBlob, Handle)
            HFunc(TEvBlobStorage::TEvCaptureVDiskLayout, Handle)
            HFunc(TEvProxyQueueState, Handle)
            hFunc(NPDisk::TEvChunkForgetResult, Handle)
            FFunc(TEvPrivate::EvCheckSnapshotExpiration, CheckSnapshotExpiration)
            hFunc(TEvReplInvoke, Handle)
            CFunc(TEvStartBalancing::EventType, RunBalancing)
        )

        STRICT_STFUNC(StateDatabaseError,
            HFunc(TEvBlobStorage::TEvVSyncGuid, Handle)
            CFunc(TEvBlobStorage::EvCompactionFinished, LevelIndexCompactionFinished)
            CFunc(TEvBlobStorage::EvWakeupEmergencyPutQueue, WakeupEmergencyPutQueue)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvVDiskStatRequest, Handle)
            CFunc(TEvBlobStorage::EvTimeToUpdateWhiteboard, UpdateWhiteboard)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvents::TEvActorDied, Handle)
            HFunc(TEvVGenerationChange, Handle)
            CFunc(TEvBlobStorage::EvReplDone, Ignore)
            CFunc(TEvBlobStorage::EvCommenceRepl, HandleCommenceRepl)
            fFunc(TEvBlobStorage::EvControllerScrubStartQuantum, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvScrubAwait, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvRecoverBlob, ForwardToScrubActor)
            fFunc(TEvBlobStorage::EvNonrestoredCorruptedBlobNotify, ForwardToScrubActor)
            IgnoreFunc(TEvBlobStorage::TEvVDefrag);
            HFunc(TEvReportScrubStatus, Handle)
            HFunc(TEvRestoreCorruptedBlob, Handle)
            HFunc(TEvBlobStorage::TEvCaptureVDiskLayout, Handle)
            HFunc(TEvProxyQueueState, Handle)
            hFunc(TEvVPatchDyingRequest, Handle)
            hFunc(NPDisk::TEvChunkForgetResult, Handle)
            FFunc(TEvPrivate::EvCheckSnapshotExpiration, CheckSnapshotExpiration)
            hFunc(TEvReplInvoke, HandleReplNotInProgress)
        )

        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::VDISK_SKELETON;
        }

        TSkeleton(TIntrusivePtr<TVDiskConfig> cfg,
                TIntrusivePtr<TBlobStorageGroupInfo> info,
                const TActorId &skeletonFrontID,
                const TVDiskContextPtr &vctx)
            : TActorBootstrapped<TSkeleton>()
            , Config(cfg)
            , VCtx(vctx)
            , Db(new TDb(cfg, vctx))
            , GInfo(info)
            , Hull()
            , LocalRecovInfo()
            , SkeletonFrontIDPtr(new TActorId(skeletonFrontID))
            , LocalDbRecoveryID()
            , NodeWardenServiceId(MakeBlobStorageNodeWardenID(vctx->NodeId))
            , SelfVDiskId(GInfo->GetVDiskId(VCtx->ShortSelfVDisk))
            , Arena(std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate))
            , VDiskMonGroup(VCtx->VDiskCounters, "subsystem", "state")
            , SyncLogIFaceGroup(VCtx->VDiskCounters, "subsystem", "synclog")
            , IFaceMonGroup(std::make_shared<NMonGroup::TVDiskIFaceGroup>(
                VCtx->VDiskCounters, "subsystem", "interface"))
            , EnableVPatch(cfg->EnableVPatch)
        {}

        virtual ~TSkeleton() {
        }

    private:
        struct TSnapshotInfo;
        using TSnapshotExpirationMap = std::multimap<TMonotonic, TSnapshotInfo*>;

        struct TSnapshotInfo {
            std::string_view SnapshotId;
            std::optional<THullDsSnap> Snap;
            TSnapshotExpirationMap::iterator ExpirationIt;
        };

    private:
        TIntrusivePtr<TVDiskConfig> Config;
        TIntrusivePtr<TVDiskContext> VCtx;
        TIntrusivePtr<TDb> Db;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TPDiskCtxPtr PDiskCtx;
        THullCtxPtr HullCtx;
        THugeBlobCtxPtr HugeBlobCtx;
        std::shared_ptr<THullLogCtx> HullLogCtx;
        ui32 MinREALHugeBlobInBytes = 0;
        std::shared_ptr<THull> Hull; // run it after local recovery
        std::shared_ptr<TOutOfSpaceLogic> OutOfSpaceLogic;
        std::shared_ptr<TQueryCtx> QueryCtx;
        TIntrusivePtr<TVPatchCtx> VPatchCtx;
        TIntrusivePtr<TLocalRecoveryInfo> LocalRecovInfo; // just info we got after local recovery
        std::unique_ptr<TOverloadHandler> OverloadHandler;
        TActorIDPtr SkeletonFrontIDPtr;
        TActorId LocalDbRecoveryID;
        const TActorId NodeWardenServiceId;
        TVDiskID SelfVDiskId;
        TMaybe<ui64> DbBirthLsn;
        TActiveActors ActiveActors;
        // fields for handling NPDisk::TEvCutLog
        std::unique_ptr<NPDisk::TEvCutLog> CutLogDelayedMsg;
        bool LocalDbInitialized = false;
        std::shared_ptr<TRopeArena> Arena;
        NMonGroup::TVDiskStateGroup VDiskMonGroup;
        NMonGroup::TSyncLogIFaceGroup SyncLogIFaceGroup;
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup;
        bool ReplDone = false;
        bool ReplOnlyPhantomsRemain = false;
        TInstant WhiteboardUpdateTimestamp = TInstant::Zero();
        std::shared_ptr<std::atomic_uint64_t> PDiskWriteBytes = std::make_shared<std::atomic_uint64_t>();
        TLoggedRecsVault LoggedRecsVault;
        bool CommenceRepl = false;
        TActorId ScrubId;
        TActorId DefragId;
        TActorId BalancingId;
        bool HasUnreadableBlobs = false;
        std::unique_ptr<TVDiskCompactionState> VDiskCompactionState;
        TMemorizableControlWrapper EnableVPatch;
        THashMap<TLogoBlobID, TActorId> VPatchActors;

        std::unordered_map<TString, TSnapshotInfo> Snapshots;
        TSnapshotExpirationMap SnapshotExpirationMap;
        std::deque<TMonotonic> SnapshotExpirationCheckSchedule;
    };

    ////////////////////////////////////////////////////////////////////////////
    // SKELETON CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateVDiskSkeleton(const TIntrusivePtr<TVDiskConfig> &cfg,
                                const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                const TActorId &skeletonFrontID,
                                const TVDiskContextPtr &vctx) {
        return new TSkeleton(cfg, info, skeletonFrontID, vctx);
    }

} // NKikimr
