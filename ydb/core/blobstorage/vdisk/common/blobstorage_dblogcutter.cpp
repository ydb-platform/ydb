#include "blobstorage_dblogcutter.h"
#include "vdisk_mon.h"
#include "vdisk_config.h"
#include "vdisk_context.h"
#include "vdisk_pdiskctx.h"
#include "vdisk_lsnmngr.h"
#include <ydb/core/blobstorage/base/utility.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TRecoveryLogCutter -- actor cuts log for the VDisk depending on lsn
    // advance of different components
    ////////////////////////////////////////////////////////////////////////////
    // must be run on the same mailbox with TSkeleton
    class TRecoveryLogCutter : public TActorBootstrapped<TRecoveryLogCutter> {
        TLogCutterCtx LogCutterCtx;
        bool WriteInProgress = false;

        ui64 HullLsnToKeep = 0;
        ui64 SyncLogLsnToKeep = 0;
        ui64 SyncerLsnToKeep = 0;
        ui64 HugeKeeperLsnToKeep = 0;
        ui64 ScrubLsnToKeep = 0;
        TInstant HullLastTime;
        TInstant SyncLogLastTime;
        TInstant SyncerLastTime;
        TInstant HugeKeeperLastTime;
        TInstant ScrubLastTime;

        TInstant LastCutTime;
        TDeque<ui64> FreeUpToLsn;
        ui64 FirstLsnToKeepLastWritten = 0;

        const TDuration FirstDuration;
        const TDuration RegularDuration;

        friend class TActorBootstrapped<TRecoveryLogCutter>;

        void Bootstrap(const TActorContext &ctx) {
            Become(&TThis::StateFunc);
            ScheduleActivity(ctx, FirstDuration);
        }

        void ScheduleActivity(const TActorContext &ctx, const TDuration &timeout) {
            ctx.Schedule(timeout, new TEvents::TEvWakeup(), nullptr);
        }

        void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
            CHECK_PDISK_RESPONSE(LogCutterCtx.VCtx, ev, ctx);

            WriteInProgress = false;
            Process(ctx);
        }

        void Handle(TEvVDiskCutLog::TPtr &ev, const TActorContext &ctx) {
            TEvVDiskCutLog *msg = ev->Get();

            auto update = [&](ui64 &target, TInstant &time, const char *name) {
                if (msg->LastKeepLsn < target) {
                    LOG_CRIT(ctx, NKikimrServices::BS_LOGCUTTER,
                             VDISKP(LogCutterCtx.VCtx->VDiskLogPrefix,
                                "Log rollback component# %s current# %" PRIu64
                                " new# %" PRIu64, name, target, msg->LastKeepLsn));
                } else {
                    target = msg->LastKeepLsn;
                    time = msg->GenerationTime;

                    LOG_DEBUG(ctx, NKikimrServices::BS_LOGCUTTER,
                            VDISKP(LogCutterCtx.VCtx->VDiskLogPrefix,
                                "UPDATED: Component# %s Hull# %" PRIu64 " SyncLog# %" PRIu64
                                " Syncer# %" PRIu64 " Huge# %" PRIu64 " Db# LogoBlobs Db# Barriers Db# Blocks",
                                name, HullLsnToKeep, SyncLogLsnToKeep, SyncerLsnToKeep, HugeKeeperLsnToKeep));
                }
            };

            switch (msg->Component) {
                case TEvVDiskCutLog::Hull:
                    update(HullLsnToKeep, HullLastTime, "Hull");
                    break;
                case TEvVDiskCutLog::SyncLog:
                    update(SyncLogLsnToKeep, SyncLogLastTime, "SyncLog");
                    break;
                case TEvVDiskCutLog::Syncer:
                    update(SyncerLsnToKeep, SyncerLastTime, "Syncer");
                    break;
                case TEvVDiskCutLog::HugeKeeper:
                    update(HugeKeeperLsnToKeep, HugeKeeperLastTime, "HugeKeeper");
                    break;
                case TEvVDiskCutLog::Scrub:
                    update(ScrubLsnToKeep, ScrubLastTime, "Scrub");
                    break;
                default:
                    Y_ABORT("Unexpected case: %d", msg->Component);
            }

            Process(ctx);
        }

        void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
            FreeUpToLsn.push_back(ev->Get()->FreeUpToLsn);
            Process(ctx);
        }

        void Timeout(const TActorContext &ctx) {
            Process(ctx);
            ScheduleActivity(ctx, RegularDuration);
        }

        void Process(const TActorContext &ctx) {
            if (WriteInProgress)
                return;

            const ui64 curLsn = Min(HullLsnToKeep, SyncLogLsnToKeep, SyncerLsnToKeep, HugeKeeperLsnToKeep, ScrubLsnToKeep);

            // only issue command if there is a progress in FreeUpToLsn queue
            bool progress = false;
            for (; FreeUpToLsn && FreeUpToLsn.front() < curLsn; FreeUpToLsn.pop_front()) {
                progress = true;
            }

            if (progress) {
                LastCutTime = TAppData::TimeProvider->Now();

                // generate clear log message
                NPDisk::TCommitRecord commitRec;
                commitRec.FirstLsnToKeep = curLsn;
                commitRec.IsStartingPoint = false;
                TLsnSeg seg = LogCutterCtx.LsnMngr->AllocLsnForLocalUse();
                ui8 signature = TLogSignature::SignatureHullCutLog;
                ctx.Send(LogCutterCtx.LoggerId,
                    new NPDisk::TEvLog(LogCutterCtx.PDiskCtx->Dsk->Owner,
                        LogCutterCtx.PDiskCtx->Dsk->OwnerRound, signature, commitRec, TRcBuf(), seg, nullptr));
                WriteInProgress = true;
                FirstLsnToKeepLastWritten = curLsn;

                LOG_DEBUG(ctx, NKikimrServices::BS_LOGCUTTER,
                        VDISKP(LogCutterCtx.VCtx->VDiskLogPrefix,
                            "CUT: Lsn# %" PRIu64 " Hull# %" PRIu64 " SyncLog# %" PRIu64
                            " Syncer# %" PRIu64 " Huge# %" PRIu64 " Db# LogoBlobs Db# Barriers Db# Blocks",
                            curLsn, HullLsnToKeep, SyncLogLsnToKeep, SyncerLsnToKeep, HugeKeeperLsnToKeep));
            }
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::LogCutterId);

            TStringStream str;
            str << "\n";
            HTML(str) {
                DIV_CLASS("panel panel-default") {
                    DIV_CLASS("panel-heading") {str << "LogCutter";}
                    DIV_CLASS("panel-body") {
                        str << "Hull: [LsnToKeep=" << HullLsnToKeep
                            << ", LastUpdate=" << ToStringLocalTimeUpToSeconds(HullLastTime) << "]<br>";
                        str << "SyncLog: [LsnToKeep=" << SyncLogLsnToKeep
                            << ", LastUpdate=" << ToStringLocalTimeUpToSeconds(SyncLogLastTime) << "]<br>";
                        str << "Syncer: [LsnToKeep=" << SyncerLsnToKeep
                            << ", LastUpdate=" << ToStringLocalTimeUpToSeconds(SyncerLastTime) << "]<br>";
                        str << "HugeKeeper: [LsnToKeep=" << HugeKeeperLsnToKeep
                            << ", LastUpdate=" << ToStringLocalTimeUpToSeconds(HugeKeeperLastTime) << "]<br>";
                        str << "Scrub: [LsnToKeep=" << ScrubLsnToKeep
                            << ", LastUpdate=" << ToStringLocalTimeUpToSeconds(ScrubLastTime) << "]<br>";

                        str << "FreeUpToLsn: " << FormatList(FreeUpToLsn) << "<br>";

                        str << "FirstLsnToKeepLastWritten: [Lsn=" << FirstLsnToKeepLastWritten
                            << ", LastUpdate=" << ToStringLocalTimeUpToSeconds(LastCutTime) << "]<br>";

                        str << "FirstDuration: " << FirstDuration << "<br>";
                        str << "RegularDuration: " << RegularDuration << "<br>";
                    }
                }
            }
            str << "\n";

            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::LogCutterId));
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvLogResult, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvVDiskCutLog, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            CFunc(TEvents::TSystem::Wakeup, Timeout)
        )

        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_RECOVERY_LOG_CUTTER;
        }

        TRecoveryLogCutter(TLogCutterCtx &&logCutterCtx)
            : TActorBootstrapped<TRecoveryLogCutter>()
            , LogCutterCtx(std::move(logCutterCtx))
            , FirstDuration(LogCutterCtx.Config->RecoveryLogCutterFirstDuration)
            , RegularDuration(LogCutterCtx.Config->RecoveryLogCutterRegularDuration)
        {
            if (!LogCutterCtx.Config->RunSyncer || LogCutterCtx.Config->BaseInfo.DonorMode) {
                SyncerLsnToKeep = Max<ui64>();
            }
        }
    };


    IActor* CreateRecoveryLogCutter(TLogCutterCtx &&logCutterCtx) {
        return new TRecoveryLogCutter(std::move(logCutterCtx));
    }

} // NKikimr
