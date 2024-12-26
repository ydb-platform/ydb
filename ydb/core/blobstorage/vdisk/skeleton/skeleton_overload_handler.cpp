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
        NMonGroup::TSkeletonOverloadGroup& Mon;
        TIntrusivePtr<TVDiskConfig> VCfg;

        ui64 CurrentSstCount = 0;
        ui64 CurrentInplacedSize = 0;

        TInstant CurrentTime;
        ui64 CurrentSpeedLimit = 0;
        ui64 AvailableBytes = 0;

    private:
        static ui64 LinearInterpolation(ui64 curX, ui64 minX, ui64 maxX, ui64 fromY) {
            if (maxX <= minX) {
                return fromY;
            }
            if (curX <= minX) {
                return fromY;
            }
            if (curX >= maxX) {
                return 0;
            }
            return (double)(maxX - curX) * fromY / (maxX - minX);
        }

        ui64 CalcSstCountSpeedLimit() const {
            ui64 deviceSpeed = (ui64)VCfg->ThrottlingDeviceSpeed;
            ui64 minSstCount = (ui64)VCfg->ThrottlingMinSstCount;
            ui64 maxSstCount = (ui64)VCfg->ThrottlingMaxSstCount;

            return LinearInterpolation(CurrentSstCount, minSstCount, maxSstCount, deviceSpeed);
        }

        ui64 CalcInplacedSizeSpeedLimit() const {
            ui64 deviceSpeed = (ui64)VCfg->ThrottlingDeviceSpeed;
            ui64 minInplacedSize = (ui64)VCfg->ThrottlingMinInplacedSize;
            ui64 maxInplacedSize = (ui64)VCfg->ThrottlingMaxInplacedSize;

            return LinearInterpolation(CurrentInplacedSize, minInplacedSize, maxInplacedSize, deviceSpeed);
        }

        ui64 CalcCurrentSpeedLimit() const {
            ui64 sstCountSpeedLimit = CalcSstCountSpeedLimit();
            ui64 inplacedSizeSpeedLimit = CalcInplacedSizeSpeedLimit();
            return std::min(sstCountSpeedLimit, inplacedSizeSpeedLimit);
        }

    public:
        explicit TThrottlingController(
            const TIntrusivePtr<TVDiskConfig> &vcfg,
            NMonGroup::TSkeletonOverloadGroup& mon)
            : Mon(mon)
            , VCfg(vcfg)
        {}

        bool IsActive() const {
            ui64 minSstCount = (ui64)VCfg->ThrottlingMinSstCount;
            ui64 minInplacedSize = (ui64)VCfg->ThrottlingMinInplacedSize;

            return CurrentSstCount > minSstCount ||
                CurrentInplacedSize > minInplacedSize;
        }

        TDuration BytesToDuration(ui64 bytes) const {
            auto limit = CurrentSpeedLimit;
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

        void UpdateState(TInstant now, ui64 sstCount, ui64 inplacedSize) {
            bool prevActive = IsActive();

            CurrentSstCount = sstCount;
            Mon.ThrottlingLevel0SstCount() = sstCount;

            CurrentInplacedSize = inplacedSize;
            Mon.ThrottlingAllLevelsInplacedSize() = inplacedSize;

            Mon.ThrottlingIsActive() = (ui64)IsActive();

            if (!IsActive()) {
                CurrentTime = {};
                AvailableBytes = 0;
                CurrentSpeedLimit = (ui64)VCfg->ThrottlingDeviceSpeed;
            } else {
                if (!prevActive) {
                    CurrentTime = now;
                    AvailableBytes = 0;
                }
                CurrentSpeedLimit = CalcCurrentSpeedLimit();
            }

            Mon.ThrottlingCurrentSpeedLimit() = CurrentSpeedLimit;
        }

        void UpdateTime(TInstant now) {
            if (now <= CurrentTime) {
                return;
            }
            auto us = (now - CurrentTime).MicroSeconds();
            AvailableBytes += CurrentSpeedLimit * us / 1000000;
            ui64 deviceSpeed = (ui64)VCfg->ThrottlingDeviceSpeed;
            AvailableBytes = Min(AvailableBytes, deviceSpeed);
            CurrentTime = now;
        }
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    // TOverloadHandler
    ///////////////////////////////////////////////////////////////////////////////////////////////////
    TOverloadHandler::TOverloadHandler(
            const TIntrusivePtr<TVDiskConfig> &vcfg,
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
        , ThrottlingController(new TThrottlingController(vcfg, Mon))
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
            auto dataInplacedSize = logoBlobsSnap.AllLevelsDataInplaced;

            auto now = ctx.Now();
            ThrottlingController->UpdateState(now, sstCount, dataInplacedSize);

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
