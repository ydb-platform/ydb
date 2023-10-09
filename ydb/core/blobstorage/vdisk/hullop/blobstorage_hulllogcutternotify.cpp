#include "blobstorage_hulllogcutternotify.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullLogCutterNotifier
    ////////////////////////////////////////////////////////////////////////////
    // must be run on the same mailbox with TSkeleton
    class THullLogCutterNotifier : public TActor<THullLogCutterNotifier> {
        const TVDiskContextPtr VCtx;
        const TActorId LogCutterId;
        TIntrusivePtr<THullDs> HullDs;
        TMaybe<ui64> PreviousCutLsn;

        TString PreviousCutLsnToString() const {
            TStringStream s;
            s << PreviousCutLsn;
            return s.Str();
        }

        void Handle(TEvents::TEvCompleted::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);

            const ui64 lsn = HullDs->GetFirstLsnToKeep();
            LOG_DEBUG(ctx, NKikimrServices::BS_LOGCUTTER,
                    VDISKP(VCtx->VDiskLogPrefix,
                        "THullLogCutterNotifier: lsn# %" PRIu64 " PreviousCutLsn# %s",
                        lsn, PreviousCutLsnToString().data()));

            if (lsn != ui64(-1)) {
                Y_ABORT_UNLESS(!PreviousCutLsn || *PreviousCutLsn <= lsn);
                if (!PreviousCutLsn || PreviousCutLsn < lsn) {
                    ctx.Send(LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Hull, lsn));
                    PreviousCutLsn = lsn;
                }
            }
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvCompleted, Handle);
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_LOG_CUTTER_NOTIFY;
        }

        THullLogCutterNotifier(
                const TVDiskContextPtr &vctx,
                const TActorId &logCutterId,
                TIntrusivePtr<THullDs> hullDs)
            : TActor<THullLogCutterNotifier>(&TThis::StateFunc)
            , VCtx(vctx)
            , LogCutterId(logCutterId)
            , HullDs(std::move(hullDs))
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateHullLogCutterNotifier
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateHullLogCutterNotifier(
            const TVDiskContextPtr &vctx,
            const TActorId &logCutterId,
            TIntrusivePtr<THullDs> hullDs) {
        Y_ABORT_UNLESS(logCutterId);
        return new THullLogCutterNotifier(vctx, logCutterId, hullDs);
    }

} // NKikimr
