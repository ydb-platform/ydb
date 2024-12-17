#include "skeleton_overload_handler.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullsatisfactionrank.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>
#include <ydb/core/util/queue_inplace.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKikimr {

    //////////////////////////////////////////////////////////////////////////
    // TEmergencyQueue
    //////////////////////////////////////////////////////////////////////////
    class TEmergencyQueue {
        struct TItem {
            std::unique_ptr<IEventHandle> Ev;
            ui64 Size = 0;
            NWilson::TSpan Span;

            TItem() = default;

            template<typename T>
            TItem(TAutoPtr<TEventHandle<T>> ev, ui64 size)
                : Ev(ev.Release())
                , Size(size)
                , Span(TWilson::VDiskInternals, std::move(Ev->TraceId), "VDisk.Skeleton.EmergencyQueue")
            {
                Ev->TraceId = Span.GetTraceId();
            }
        };

        // emergency queue of 'put' events
        using TQueueType = TQueueInplace<TItem, 4096>;

        NMonGroup::TSkeletonOverloadGroup &Mon;
        TQueueType Queue;
        TVMovedPatchHandler VMovedPatchHandler;
        TVPatchStartHandler VPatchStartHandler;
        TVPutHandler VPutHandler;
        TVMultiPutHandler VMultiPutHandler;
        TLocalSyncDataHandler LocalSyncDataHandler;
        TAnubisOsirisPutHandler AnubisOsirisPutHandler;

    public:
        TEmergencyQueue(
                NMonGroup::TSkeletonOverloadGroup &mon,
                TVMovedPatchHandler &&vMovedPatch,
                TVPatchStartHandler &&vPatchStart,
                TVPutHandler &&vput,
                TVMultiPutHandler &&vMultiPut,
                TLocalSyncDataHandler &&loc,
                TAnubisOsirisPutHandler &&aoput)
            : Mon(mon)
            , VMovedPatchHandler(std::move(vMovedPatch))
            , VPatchStartHandler(std::move(vPatchStart))
            , VPutHandler(std::move(vput))
            , VMultiPutHandler(std::move(vMultiPut))
            , LocalSyncDataHandler(std::move(loc))
            , AnubisOsirisPutHandler(std::move(aoput))
        {}

        ~TEmergencyQueue() {
            while (Queue.Head()) {
                Queue.Pop();
            }
        }

        void Push(TEvBlobStorage::TEvVMovedPatch::TPtr ev) {
            ++Mon.EmergencyMovedPatchQueueItems();
            auto size = ev->GetSize();
            Mon.EmergencyMovedPatchQueueBytes() += size;
            Queue.Push(TItem(ev, size));
        }

        void Push(TEvBlobStorage::TEvVPatchStart::TPtr ev) {
            ++Mon.EmergencyPatchStartQueueItems();
            auto size = ev->GetSize();
            Mon.EmergencyPatchStartQueueBytes() += size;
            Queue.Push(TItem(ev, size));
        }

        void Push(TEvBlobStorage::TEvVPut::TPtr ev) {
            ++Mon.EmergencyPutQueueItems();
            auto size = ev->GetSize();
            Mon.EmergencyPutQueueBytes() += size;
            Queue.Push(TItem(ev, size));
        }

        void Push(TEvBlobStorage::TEvVMultiPut::TPtr ev) {
            ++Mon.EmergencyMultiPutQueueItems();
            auto size = ev->GetSize();
            Mon.EmergencyMultiPutQueueBytes() += size;
            Queue.Push(TItem(ev, size));
        }

        void Push(TEvLocalSyncData::TPtr ev) {
            ++Mon.EmergencyLocalSyncDataQueueItems();
            auto size = ev->Get()->ByteSize();
            Mon.EmergencyLocalSyncDataQueueBytes() += size;
            Queue.Push(TItem(ev, size));
        }

        void Push(TEvAnubisOsirisPut::TPtr ev) {
            ++Mon.EmergencyAnubisOsirisPutQueueItems();
            auto size = ev->Get()->ByteSize();
            Mon.EmergencyAnubisOsirisPutQueueBytes() += size;
            Queue.Push(TItem(ev, size));
        }

        bool Empty() {
            return !Queue.Head();
        }

        ui64 GetHeadEventSize() {
            Y_ABORT_UNLESS(Queue.Head());
            return Queue.Head()->Size;
        }

        ui64 Process(const TActorContext &ctx) {
            auto item = Queue.Head();
            Y_ABORT_UNLESS(item);
            auto size = item->Size;
            TAutoPtr<IEventHandle> ev = item->Ev.release();
            item->Span.EndOk();
            Queue.Pop();
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVMovedPatch: {
                    auto *evMovedPatch = reinterpret_cast<TEvBlobStorage::TEvVMovedPatch::TPtr*>(&ev);
                    --Mon.EmergencyMovedPatchQueueItems();
                    Mon.EmergencyMovedPatchQueueBytes() -= size;
                    VMovedPatchHandler(ctx, *evMovedPatch);
                    break;
                }
                case TEvBlobStorage::EvVPatchStart: {
                    auto *evPatchStart = reinterpret_cast<TEvBlobStorage::TEvVPatchStart::TPtr*>(&ev);
                    --Mon.EmergencyPatchStartQueueItems();
                    Mon.EmergencyPatchStartQueueBytes() -= size;
                    VPatchStartHandler(ctx, *evPatchStart);
                    break;
                }
                case TEvBlobStorage::EvVPut: {
                    auto *evPut = reinterpret_cast<TEvBlobStorage::TEvVPut::TPtr*>(&ev);
                    --Mon.EmergencyPutQueueItems();
                    Mon.EmergencyPutQueueBytes() -= size;
                    VPutHandler(ctx, *evPut);
                    break;
                }
                case TEvBlobStorage::EvVMultiPut: {
                    auto *evMultiPut = reinterpret_cast<TEvBlobStorage::TEvVMultiPut::TPtr*>(&ev);
                    --Mon.EmergencyMultiPutQueueItems();
                    Mon.EmergencyMultiPutQueueBytes() -= size;
                    VMultiPutHandler(ctx, *evMultiPut);
                    break;
                }
                case TEvBlobStorage::EvLocalSyncData: {
                    auto *evLocalSyncData = reinterpret_cast<TEvLocalSyncData::TPtr*>(&ev);
                    --Mon.EmergencyLocalSyncDataQueueItems();
                    Mon.EmergencyLocalSyncDataQueueBytes() -= size;
                    LocalSyncDataHandler(ctx,*evLocalSyncData);
                    break;
                }
                case TEvBlobStorage::EvAnubisOsirisPut: {
                    auto *evAnubisOsirisPut = reinterpret_cast<TEvAnubisOsirisPut::TPtr*>(&ev);
                    --Mon.EmergencyAnubisOsirisPutQueueItems();
                    Mon.EmergencyAnubisOsirisPutQueueBytes() -= size;
                    AnubisOsirisPutHandler(ctx, *evAnubisOsirisPut);
                    break;
                }
                default:
                    Y_ABORT("unexpected event type in emergency queue(%" PRIu64 ")", (ui64)ev->GetTypeRewrite());
            }
            return size;
        }
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    // TThrottlingController
    ///////////////////////////////////////////////////////////////////////////////////////////////////

    class TThrottlingController {
    private:
        struct TControls {
            TControlWrapper MinSstCount;
            TControlWrapper MaxSstCount;
            TControlWrapper DeviceSpeed;

            TControls()
                : MinSstCount(16, 1, 200)
                , MaxSstCount(64, 1, 200)
                , DeviceSpeed(50 << 20, 1 << 20, 1 << 30)
            {}

            void Register(TIntrusivePtr<TControlBoard> icb) {
                icb->RegisterSharedControl(MinSstCount, "VDiskControls.ThrottlingMinSstCount");
                icb->RegisterSharedControl(MaxSstCount, "VDiskControls.ThrottlingMaxSstCount");
                icb->RegisterSharedControl(DeviceSpeed, "VDiskControls.ThrottlingDeviceSpeed");
            }
        };
        TControls Controls;

        ui64 CurrentSstCount = 0;

        TInstant CurrentTime;
        ui64 AvailableBytes = 0;

        ui64 GetCurrentSpeedLimit() const {
            ui64 minSstCount = (ui64)Controls.MinSstCount;
            ui64 maxSstCount = (ui64)Controls.MaxSstCount;
            ui64 deviceSpeed = (ui64)Controls.DeviceSpeed;

            if (maxSstCount <= minSstCount) {
                return deviceSpeed;
            }
            if (CurrentSstCount <= minSstCount) {
                return deviceSpeed;
            }
            if (CurrentSstCount >= maxSstCount) {
                return 0;
            }
            return (double)(maxSstCount - CurrentSstCount) * deviceSpeed / (maxSstCount - minSstCount);
        }

    public:
        void RegisterIcbControls(TIntrusivePtr<TControlBoard> icb) {
            Controls.Register(icb);
        }

        bool IsActive() const {
            ui64 minSstCount = (ui64)Controls.MinSstCount;
            return CurrentSstCount > minSstCount;
        }

        TDuration BytesToDuration(ui64 bytes) const {
            auto limit = GetCurrentSpeedLimit();
            if (limit == 0) {
                return TDuration::Seconds(1);
            }
            return TDuration::Seconds((double)bytes / limit);
        }

        ui64 GetAvailableBytes() const {
            return AvailableBytes;
        }

        ui64 Consume(ui64 bytes) {
            AvailableBytes -= bytes;
            return AvailableBytes;
        }

        void UpdateState(TInstant now, ui64 sstCount) {
            bool prevActive = IsActive();

            CurrentSstCount = sstCount;

            if (!IsActive()) {
                CurrentTime = {};
                AvailableBytes = 0;
            } else if (!prevActive) {
                CurrentTime = now;
                AvailableBytes = 0;
            }
        }

        void UpdateTime(TInstant now) {
            if (now <= CurrentTime) {
                return;
            }
            auto us = (now - CurrentTime).MicroSeconds();
            AvailableBytes += GetCurrentSpeedLimit() * us / 1000000; // overflow ?
            ui64 deviceSpeed = (ui64)Controls.DeviceSpeed;
            AvailableBytes = Min(AvailableBytes, deviceSpeed);
            CurrentTime = now;
        }
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    // TOverloadHandler
    ///////////////////////////////////////////////////////////////////////////////////////////////////
    TOverloadHandler::TOverloadHandler(
            const TIntrusivePtr<TVDiskContext> &vctx,
            const TPDiskCtxPtr &pdiskCtx,
            std::shared_ptr<THull> hull,
            NMonGroup::TSkeletonOverloadGroup &&mon,
            TVMovedPatchHandler &&vMovedPatch,
            TVPatchStartHandler &&vPatchStart,
            TVPutHandler &&vput,
            TVMultiPutHandler &&vMultiPut,
            TLocalSyncDataHandler &&loc,
            TAnubisOsirisPutHandler &&aoput)
        : Hull(std::move(hull))
        , Mon(std::move(mon))
        , EmergencyQueue(new TEmergencyQueue(Mon, std::move(vMovedPatch), std::move(vPatchStart), std::move(vput),
                std::move(vMultiPut), std::move(loc), std::move(aoput)))
        , DynamicPDiskWeightsManager(std::make_shared<TDynamicPDiskWeightsManager>(vctx, pdiskCtx))
        , ThrottlingController(new TThrottlingController)
    {}

    TOverloadHandler::~TOverloadHandler() {}

    void TOverloadHandler::ActualizeWeights(const TActorContext &ctx, ui32 mask, bool actualizeLevels) {
        // Manage PDisk scheduler weights
        const ui32 f = ui32(EHullDbType::First);
        const ui32 m = ui32(EHullDbType::Max);
        // actualize fresh weights
        for (ui32 t = f; t < m; t++) {
            if ((1u << t) & mask) {
                auto type = EHullDbType(t);
                auto rank = Hull->GetSatisfactionRank(type, ESatisfactionRankType::Fresh);
                DynamicPDiskWeightsManager->UpdateFreshSatisfactionRank(type, rank);
            }
        }
        // actualize level weights
        if (actualizeLevels) {
            for (ui32 t = f; t < m; t++) {
                auto type = EHullDbType(t);
                auto rank = Hull->GetSatisfactionRank(type, ESatisfactionRankType::Level);
                DynamicPDiskWeightsManager->UpdateLevelSatisfactionRank(type, rank);
            }
        }
        DynamicPDiskWeightsManager->ApplyUpdates(ctx);
        Mon.FreshSatisfactionRankPercent() = (DynamicPDiskWeightsManager->GetFreshRank() * 100u).ToUi64();
        Mon.LevelSatisfactionRankPercent() = (DynamicPDiskWeightsManager->GetLevelRank() * 100u).ToUi64();
    }

    bool TOverloadHandler::ProcessPostponedEvents(const TActorContext &ctx, int batchSize, bool actualizeLevels) {
        ActualizeWeights(ctx, AllEHullDbTypes, actualizeLevels);

        if (DynamicPDiskWeightsManager->StopPuts()) {
            return false;
        }

        if (AppData()->FeatureFlags.GetEnableVDiskThrottling()) {
            auto snapshot = Hull->GetSnapshot(); // THullDsSnap
            auto& logoBlobsSnap = snapshot.LogoBlobsSnap; // TLogoBlobsSnapshot
            auto& sliceSnap = logoBlobsSnap.SliceSnap; // TLevelSliceSnapshot
            auto sstCount = sliceSnap.GetLevel0SstsNum();

            auto now = ctx.Now();
            ThrottlingController->UpdateState(now, sstCount);

            if (ThrottlingController->IsActive()) {
                ThrottlingController->UpdateTime(now);

                int count = batchSize;
                auto bytes = ThrottlingController->GetAvailableBytes();

                while (count > 0 &&
                    !EmergencyQueue->Empty() &&
                    bytes >= EmergencyQueue->GetHeadEventSize())
                {
                    auto size = EmergencyQueue->Process(ctx);
                    bytes = ThrottlingController->Consume(size);
                    --count;
                }

                if (EmergencyQueue->Empty()) {
                    return false;
                }

                if (bytes >= EmergencyQueue->GetHeadEventSize()) {
                    return true;
                }

                if (!KickInFlight) {
                    auto left = EmergencyQueue->GetHeadEventSize() - bytes;
                    auto duration = ThrottlingController->BytesToDuration(left);
                    duration += TDuration::MilliSeconds(1);
                    ctx.Schedule(duration, new TEvKickEmergencyPutQueue);
                    KickInFlight = true;
                }

                return false;
            }
        }

        // process batchSize events maximum and once
        int count = batchSize;
        while (count > 0 && !DynamicPDiskWeightsManager->StopPuts() && !EmergencyQueue->Empty()) {
            // process single event from the emergency queue
            EmergencyQueue->Process(ctx);
            --count;
        }

        bool proceedFurther = !DynamicPDiskWeightsManager->StopPuts() && !EmergencyQueue->Empty();
        return proceedFurther;
    }

    void TOverloadHandler::ToWhiteboard(const TOverloadHandler *this_, NKikimrWhiteboard::TVDiskSatisfactionRank &v) {
        if (this_) {
            this_->DynamicPDiskWeightsManager->ToWhiteboard(v);
        } else {
            TDynamicPDiskWeightsManager::DefWhiteboard(v);
        }
    }

    ui32 TOverloadHandler::GetIntegralRankPercent() const {
        Y_DEBUG_ABORT_UNLESS(DynamicPDiskWeightsManager);
        ui32 integralRankPercent = ((DynamicPDiskWeightsManager->GetFreshRank()
                    + DynamicPDiskWeightsManager->GetLevelRank()) * 100u).ToUi64();
        return integralRankPercent;
    }

    void TOverloadHandler::Feedback(const NPDisk::TEvConfigureSchedulerResult &res, const TActorContext &ctx) {
        DynamicPDiskWeightsManager->Feedback(res, ctx);
    }

    void TOverloadHandler::RenderHtml(IOutputStream &str) {
        DynamicPDiskWeightsManager->RenderHtml(str);
    }

    void TOverloadHandler::OnKickEmergencyPutQueue() {
        KickInFlight = false;
    }

    void TOverloadHandler::RegisterIcbControls(TIntrusivePtr<TControlBoard> icb) {
        ThrottlingController->RegisterIcbControls(icb);
    }

    template <class TEv>
    inline bool TOverloadHandler::PostponeEvent(TAutoPtr<TEventHandle<TEv>> &ev) {
        if (DynamicPDiskWeightsManager->StopPuts() ||
            ThrottlingController->IsActive() ||
            !EmergencyQueue->Empty())
        {
            EmergencyQueue->Push(ev);
            return true;
        } else {
            return false;
        }
    }

    template bool TOverloadHandler::PostponeEvent(TEvBlobStorage::TEvVMovedPatch::TPtr &ev);
    template bool TOverloadHandler::PostponeEvent(TEvBlobStorage::TEvVPatchStart::TPtr &ev);
    template bool TOverloadHandler::PostponeEvent(TEvBlobStorage::TEvVPut::TPtr &ev);
    template bool TOverloadHandler::PostponeEvent(TEvBlobStorage::TEvVMultiPut::TPtr &ev);
    template bool TOverloadHandler::PostponeEvent(TEvLocalSyncData::TPtr &ev);
    template bool TOverloadHandler::PostponeEvent(TEvAnubisOsirisPut::TPtr &ev);

} // NKikimr
