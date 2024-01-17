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
            NWilson::TSpan Span;

            TItem() = default;

            template<typename T>
            TItem(TAutoPtr<TEventHandle<T>> ev)
                : Ev(ev.Release())
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
            Mon.EmergencyMovedPatchQueueBytes() += ev->Get()->Record.ByteSize();
            Queue.Push(TItem(ev));
        }

        void Push(TEvBlobStorage::TEvVPatchStart::TPtr ev) {
            ++Mon.EmergencyPatchStartQueueItems();
            Mon.EmergencyPatchStartQueueBytes() += ev->Get()->Record.ByteSize();
            Queue.Push(TItem(ev));
        }

        void Push(TEvBlobStorage::TEvVPut::TPtr ev) {
            ++Mon.EmergencyPutQueueItems();
            Mon.EmergencyPutQueueBytes() += ev->Get()->Record.ByteSize();
            Queue.Push(TItem(ev));
        }

        void Push(TEvBlobStorage::TEvVMultiPut::TPtr ev) {
            ++Mon.EmergencyMultiPutQueueItems();
            Mon.EmergencyMultiPutQueueBytes() += ev->Get()->Record.ByteSize();
            Queue.Push(TItem(ev));
        }

        void Push(TEvLocalSyncData::TPtr ev) {
            ++Mon.EmergencyLocalSyncDataQueueItems();
            Mon.EmergencyLocalSyncDataQueueBytes() += ev->Get()->ByteSize();
            Queue.Push(TItem(ev));
        }

        void Push(TEvAnubisOsirisPut::TPtr ev) {
            ++Mon.EmergencyAnubisOsirisPutQueueItems();
            Mon.EmergencyAnubisOsirisPutQueueBytes() += ev->Get()->ByteSize();
            Queue.Push(TItem(ev));
        }

        bool Empty() {
            return !Queue.Head();
        }

        void Process(const TActorContext &ctx) {
            auto item = Queue.Head();
            Y_ABORT_UNLESS(item);
            TAutoPtr<IEventHandle> ev = item->Ev.release();
            item->Span.EndOk();
            Queue.Pop();
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVMovedPatch: {
                    auto *evMovedPatch = reinterpret_cast<TEvBlobStorage::TEvVMovedPatch::TPtr*>(&ev);
                    --Mon.EmergencyMovedPatchQueueItems();
                    Mon.EmergencyMovedPatchQueueBytes() -= (*evMovedPatch)->Get()->Record.ByteSize();
                    VMovedPatchHandler(ctx, *evMovedPatch);
                    break;
                }
                case TEvBlobStorage::EvVPatchStart: {
                    auto *evPatchStart = reinterpret_cast<TEvBlobStorage::TEvVPatchStart::TPtr*>(&ev);
                    --Mon.EmergencyPatchStartQueueItems();
                    Mon.EmergencyPatchStartQueueBytes() -= (*evPatchStart)->Get()->Record.ByteSize();
                    VPatchStartHandler(ctx, *evPatchStart);
                    break;
                }
                case TEvBlobStorage::EvVPut: {
                    auto *evPut = reinterpret_cast<TEvBlobStorage::TEvVPut::TPtr*>(&ev);
                    --Mon.EmergencyPutQueueItems();
                    Mon.EmergencyPutQueueBytes() -= (*evPut)->Get()->Record.ByteSize();
                    VPutHandler(ctx, *evPut);
                    break;
                }
                case TEvBlobStorage::EvVMultiPut: {
                    auto *evMultiPut = reinterpret_cast<TEvBlobStorage::TEvVMultiPut::TPtr*>(&ev);
                    --Mon.EmergencyMultiPutQueueItems();
                    Mon.EmergencyMultiPutQueueBytes() -= (*evMultiPut)->Get()->Record.ByteSize();
                    VMultiPutHandler(ctx, *evMultiPut);
                    break;
                }
                case TEvBlobStorage::EvLocalSyncData: {
                    auto *evLocalSyncData = reinterpret_cast<TEvLocalSyncData::TPtr*>(&ev);
                    --Mon.EmergencyLocalSyncDataQueueItems();
                    Mon.EmergencyLocalSyncDataQueueBytes() -= (*evLocalSyncData)->Get()->ByteSize();
                    LocalSyncDataHandler(ctx,*evLocalSyncData);
                    break;
                }
                case TEvBlobStorage::EvAnubisOsirisPut: {
                    auto *evAnubisOsirisPut = reinterpret_cast<TEvAnubisOsirisPut::TPtr*>(&ev);
                    --Mon.EmergencyAnubisOsirisPutQueueItems();
                    Mon.EmergencyAnubisOsirisPutQueueBytes() -= (*evAnubisOsirisPut)->Get()->ByteSize();
                    AnubisOsirisPutHandler(ctx, *evAnubisOsirisPut);
                    break;
                }
                default:
                    Y_ABORT("unexpected event type in emergency queue(%" PRIu64 ")", (ui64)ev->GetTypeRewrite());
            }
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

    template <class TEv>
    inline bool TOverloadHandler::PostponeEvent(TAutoPtr<TEventHandle<TEv>> &ev) {
        if (DynamicPDiskWeightsManager->StopPuts() || !EmergencyQueue->Empty()) {
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
