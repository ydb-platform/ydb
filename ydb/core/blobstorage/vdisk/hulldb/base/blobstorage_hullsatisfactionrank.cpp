#include "blobstorage_hullsatisfactionrank.h"
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/stream/output.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TDynamicPDiskWeightsManager
    ////////////////////////////////////////////////////////////////////////////
    const TDynamicPDiskWeightsManager::TDecimal TDynamicPDiskWeightsManager::FreshRankYellow =
        TDecimal::MkDecimal(1, 300);
    const TDynamicPDiskWeightsManager::TDecimal TDynamicPDiskWeightsManager::FreshRankRed =
        TDecimal::MkDecimal(2, 500);
    const TDynamicPDiskWeightsManager::TDecimal TDynamicPDiskWeightsManager::LevelRankYellow =
        TDecimal::MkDecimal(1, 800);
    const TDynamicPDiskWeightsManager::TDecimal TDynamicPDiskWeightsManager::LevelRankRed =
        TDecimal::MkDecimal(3, 000);
    const ui64 TDynamicPDiskWeightsManager::FreshWeightDefault = 2;
    const ui64 TDynamicPDiskWeightsManager::LevelWeightDefault = 7;


    TDynamicPDiskWeightsManager::TDynamicPDiskWeightsManager(
            const TVDiskContextPtr &vctx,
            const TPDiskCtxPtr &pdiskCtx)
        : VCtx(vctx)
        , PDiskCtx(pdiskCtx)
        , FreshWeight(TLinearTrackBar(FreshWeightDefault,   // def weight
                                      8,                    // max weight
                                      1,                    // discreteness
                                      FreshRankYellow,
                                      FreshRankRed))
        , LevelWeight(TLinearTrackBar(LevelWeightDefault,   // def weight
                                      16,                   // max weight
                                      1,                    // discreteness
                                      LevelRankYellow,
                                      LevelRankRed))
    {
        AtomicSet(AcceptWritesState, 1);

        // default weight must correspond to global VDisk weights settings
        static_assert(FreshWeightDefault == ::NKikimr::FreshWeightDefault,
                      "incorrect default fresh weight");
        static_assert(LevelWeightDefault == ::NKikimr::CompWeightDefault,
                      "incorrect default level weight");
    }

    void TDynamicPDiskWeightsManager::ApplyUpdates(const TActorContext &ctx) {
        // cache old value
        const bool prevVal = StopPuts();
        FreshRank.ApplyUpdates();
        LevelRank.ApplyUpdates();
        std::unique_ptr<NPDisk::TEvConfigureScheduler> msg;
        TLinearTrackBar::TStatus status;
        // fresh
        status = FreshWeight.Update(FreshRank.GetRank());
        if (status.Changed) {
            if (!msg) {
                msg = std::make_unique<NPDisk::TEvConfigureScheduler>(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound);
            }
            msg->SchedulerCfg.FreshWeight = status.Weight;
        }
        // level
        status = LevelWeight.Update(LevelRank.GetRank());
        if (status.Changed) {
            if (!msg) {
                msg = std::make_unique<NPDisk::TEvConfigureScheduler>(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound);
            }
            msg->SchedulerCfg.CompWeight = status.Weight;
        }
        // send msg if any
        if (msg) {
            ctx.Send(PDiskCtx->PDiskId, msg.release());

            LOG_DEBUG(ctx, BS_VDISK_OTHER,
                      VDISKP(VCtx->VDiskLogPrefix, "TDynamicPDiskWeightsManager: "
                             "update pdisk scheduler weights: msg# %s freshWeightStatus# %s",
                             msg->ToString().data(), status.ToString().data()));
        }
        // calculate new value
        const bool newVal = StopPuts();
        // update AcceptWritesState if required
        if (prevVal != newVal) {
            TAtomicBase val = newVal;
            AtomicSet(AcceptWritesState, val);
        }
    }

    void TDynamicPDiskWeightsManager::Feedback(const NPDisk::TEvConfigureSchedulerResult &res,
                                               const TActorContext &ctx) {
        NActors::NLog::EPriority pri = res.Status == NKikimrProto::OK ?
                NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR;

        LOG_LOG(ctx, pri, BS_VDISK_OTHER,
                VDISKP(VCtx->VDiskLogPrefix, "TDynamicPDiskWeightsManager: "
                       "response from Yard: msg# %s", res.ToString().data()));
    }

    void TDynamicPDiskWeightsManager::RenderHtml(IOutputStream &str) const {
        HTML(str) {
            TABLE_CLASS ("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Component"; }
                        TABLEH() { str << "Status"; }
                    }
                }
                TABLEBODY() {
                    TABLER() {
                        TABLED() { str << "FreshRank"; }
                        TABLED() { FreshWeight.RenderHtml(str); }
                    }
                    TABLER() {
                        TABLED() { str << "LevelRank"; }
                        TABLED() { LevelWeight.RenderHtml(str); }
                    }
                }
            }
        }
    }

    void TDynamicPDiskWeightsManager::ToWhiteboard(
                        NKikimrWhiteboard::TVDiskSatisfactionRank &v) const {

        auto set = [] (TDecimal rank,
                       TDecimal yellow,
                       TDecimal red,
                       NKikimrWhiteboard::TVDiskSatisfactionRank::TRank &r) {
            //r.SetRankPercent((rank * 100u).ToUi64());
            if (rank < yellow) {
                r.SetFlag(NKikimrWhiteboard::Green);
            } else if (rank < red) {
                r.SetFlag(NKikimrWhiteboard::Yellow);
            } else {
                r.SetFlag(NKikimrWhiteboard::Red);
            }
        };

        set(FreshRank.GetRank(), FreshRankYellow, FreshRankRed, *v.MutableFreshRank());
        set(LevelRank.GetRank(), LevelRankYellow, LevelRankRed, *v.MutableLevelRank());
    }

    void TDynamicPDiskWeightsManager::DefWhiteboard(NKikimrWhiteboard::TVDiskSatisfactionRank &v) {
        auto set = [] (NKikimrWhiteboard::TVDiskSatisfactionRank::TRank &r) {
            //r.SetRankPercent(0);
            r.SetFlag(NKikimrWhiteboard::Green);
        };

        set(*v.MutableFreshRank());
        set(*v.MutableLevelRank());
    }

} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NSat::TDecimalViaDouble, stream, value) {
    value.Output(stream);
}

Y_DECLARE_OUT_SPEC(, NKikimr::NSat::TDecimal<>, stream, value) {
    value.Output(stream);
}

Y_DECLARE_OUT_SPEC(
        ,
        typename NKikimr::NSat::TLinearTrackBar<NKikimr::NSat::TDecimal<>>::TStatus,
        stream,
        value) {
    value.Output(stream);
}

Y_DECLARE_OUT_SPEC(
        ,
        typename NKikimr::NSat::TLinearTrackBar<NKikimr::NSat::TDecimalViaDouble>::TStatus,
        stream,
        value) {
    value.Output(stream);
}
