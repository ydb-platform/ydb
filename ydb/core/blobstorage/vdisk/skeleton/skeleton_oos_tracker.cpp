#include "skeleton_oos_tracker.h"

#include <ydb/core/blobstorage/base/html.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TDskSpaceTrackerActor
    ////////////////////////////////////////////////////////////////////////////
    class TDskSpaceTrackerActor : public TActorBootstrapped<TDskSpaceTrackerActor> {
    private:
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        const TDuration DskTrackerInterval;
        NMonGroup::TDskOutOfSpaceGroup MonGroup;
        NMonGroup::TCostGroup CostGroup;
        // how many 'wait intervals' we spent in 'bad' zones
        ui64 YellowZonePeriods = 0;
        ui64 OrangeZonePeriods = 0;
        ui64 RedZonePeriods = 0;
        ui64 BlackZonePeriods = 0;
        ui64 TotalChunks = 0;
        ui64 FreeChunks = 0;

        friend class TActorBootstrapped<TDskSpaceTrackerActor>;

        void CheckState(const TActorContext &ctx) {
            auto zone = VCtx->OutOfSpaceState.GetGlobalColor();
            MonGroup.DskOutOfSpace() = zone;

            auto priority = NActors::NLog::PRI_TRACE;

            switch (zone) {
                case TSpaceColor::YELLOW:
                    priority = NActors::NLog::PRI_WARN;
                    ++YellowZonePeriods;
                    break;
                case TSpaceColor::LIGHT_ORANGE:
                case TSpaceColor::ORANGE:
                    priority = NActors::NLog::PRI_ERROR;
                    ++OrangeZonePeriods;
                    break;
                case TSpaceColor::RED:
                    priority = NActors::NLog::PRI_CRIT;
                    ++RedZonePeriods;
                    break;
                case TSpaceColor::BLACK:
                    priority = NActors::NLog::PRI_CRIT;
                    ++BlackZonePeriods;
                    break;
                default:
                    break;
            }

            LOG_LOG_S(ctx, priority, NKikimrServices::BS_SKELETON, VCtx->VDiskLogPrefix
                    << "TDskSpaceTrackerActor: " << zone << " ZONE" << " Marker# BSVSOOST01");
            // send message to PDisk
            Become(&TThis::AskFunc);
            ctx.Send(PDiskCtx->PDiskId,
                    new NPDisk::TEvCheckSpace(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound));
        }

        void Bootstrap(const TActorContext &ctx) {
            CheckState(ctx);
        }

        void HandleWakeup(const TActorContext &ctx) {
            CheckState(ctx);
        }

        void Handle(NPDisk::TEvCheckSpaceResult::TPtr &ev, const TActorContext &ctx) {
            const auto *msg = ev->Get();
            LOG_DEBUG_S(ctx, NKikimrServices::BS_SKELETON, VCtx->VDiskLogPrefix
                    << "TDskSpaceTrackerActor:handle TEvCheckSpaceResult; msg# " << msg->ToString()
                    << " Marker# BSVSOOST02");

            CHECK_PDISK_RESPONSE(VCtx, ev, ctx);

            Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK, "Expected OK from PDisk on every TEvCheckSpace request, "
                     "but got Status# %s", NKikimrProto::EReplyStatus_Name(msg->Status).data());

            TotalChunks = msg->TotalChunks;
            FreeChunks = msg->FreeChunks;
            VCtx->OutOfSpaceState.UpdateLocalChunk(msg->StatusFlags);
            VCtx->OutOfSpaceState.UpdateLocalLog(msg->LogStatusFlags);
            VCtx->OutOfSpaceState.UpdateLocalFreeSpaceShare(ui64(1 << 24) * (1.0 - msg->Occupancy));
            VCtx->OutOfSpaceState.UpdateLocalUsedChunks(msg->UsedChunks);
            MonGroup.DskTotalBytes() = msg->TotalChunks * PDiskCtx->Dsk->ChunkSize;
            MonGroup.DskFreeBytes() = msg->FreeChunks * PDiskCtx->Dsk->ChunkSize;
            MonGroup.DskUsedBytes() = msg->UsedChunks * PDiskCtx->Dsk->ChunkSize;
            if (msg->NumSlots > 0) {
                ui32 timeAvailable = 1'000'000'000 / msg->NumSlots;
                CostGroup.DiskTimeAvailableNs() = timeAvailable;
                if (VCtx->CostTracker) {
                    VCtx->CostTracker->SetTimeAvailable(timeAvailable);
                }
            }

            Become(&TThis::WaitFunc);
            ctx.Schedule(DskTrackerInterval, new TEvents::TEvWakeup());
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::DskSpaceTrackerId);
            TStringStream str;
            auto oosStatus = VCtx->OutOfSpaceState.GetGlobalStatusFlags();

            HTML(str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Disk Space Tracker";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS ("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {str << "Param";}
                                    TABLEH() {str << "Value";}
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    auto flags = VCtx->OutOfSpaceState.GetLocalStatusFlags();
                                    TABLED() {str << "Local Disk State";}
                                    TABLED() {str << StatusFlagToSpaceColor(flags);}
                                }
                                TABLER() {
                                    auto flags = VCtx->OutOfSpaceState.GetLocalChunkStatusFlags();
                                    TABLED() {str << "Local Disk State (chunks)";}
                                    TABLED() {str << StatusFlagToSpaceColor(flags);}
                                }
                                TABLER() {
                                    auto flags = VCtx->OutOfSpaceState.GetLocalLogStatusFlags();
                                    TABLED() {str << "Local Disk State (log)";}
                                    TABLED() {str << StatusFlagToSpaceColor(flags);}
                                }
                                TABLER() {
                                    TABLED() {str << "Global BlobStorage Group State";}
                                    TABLED() {str << StatusFlagToSpaceColor(oosStatus.Flags);}
                                }
                                TABLER() {
                                    TABLED() {str << "Global Whiteboard Flag";}
                                    TABLED() {
                                        auto wb_flag = VCtx->OutOfSpaceState.GlobalWhiteboardFlag();
                                        THtmlLightSignalRenderer(wb_flag, TStringBuilder() << wb_flag).Output(str);
                                    }
                                }
                                TABLER() {
                                    TABLED() {str << "Local Disk Approximate Free Space Share";}
                                    TABLED() {str << oosStatus.ApproximateFreeSpaceShare * 100 << "%";}
                                }
                                TABLER() {
                                    TABLED() {str << "BLACK Zone Duration";}
                                    TABLED() {str << (DskTrackerInterval * BlackZonePeriods).ToString();}
                                }
                                TABLER() {
                                    TABLED() {str << "RED Zone Duration";}
                                    TABLED() {str << (DskTrackerInterval * RedZonePeriods).ToString();}
                                }
                                TABLER() {
                                    TABLED() {str << "ORANGE Zone Duration";}
                                    TABLED() {str << (DskTrackerInterval * OrangeZonePeriods).ToString();}
                                }
                                TABLER() {
                                    TABLED() {str << "YELLOW Zone Duration";}
                                    TABLED() {str << (DskTrackerInterval * YellowZonePeriods).ToString();}
                                }
                                if (TotalChunks) {
                                    TABLER() {
                                        TABLED() {str << "FreeChunks/TotalChunks";}
                                        TABLED() {str << FreeChunks << "/" << TotalChunks;}
                                    }
                                }
                            }
                        }
                    }
                }
            }

            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::DskSpaceTrackerId));
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(WaitFunc,
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )

        STRICT_STFUNC(AskFunc,
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(NPDisk::TEvCheckSpaceResult, Handle)
        )

        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_DISK_SPACE_TRACKER;
        }

        TDskSpaceTrackerActor(
                TIntrusivePtr<TVDiskContext> vctx,
                TPDiskCtxPtr pdiskCtx,
                TDuration dskTrackerInterval)
            : TActorBootstrapped<TDskSpaceTrackerActor> ()
            , VCtx(std::move(vctx))
            , PDiskCtx(pdiskCtx)
            , DskTrackerInterval(dskTrackerInterval)
            , MonGroup(VCtx->VDiskCounters, "subsystem", "outofspace")
            , CostGroup(VCtx->VDiskCounters, "subsystem", "cost")
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateDskSpaceTracker -- tracks (refreshes) actual state of 'out of disk space'
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateDskSpaceTracker(
                TIntrusivePtr<TVDiskContext> vctx,
                TPDiskCtxPtr pdiskCtx,
                TDuration dskTrackerInterval)
    {
        return new TDskSpaceTrackerActor(std::move(vctx), std::move(pdiskCtx), dskTrackerInterval);
    }

} // NKikimr
