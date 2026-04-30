#include "blobstorage_syncloghttp.h"
#include "blobstorage_synclogdata.h"
#include "blobstorage_synclog_private_events.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/cast.h>


using namespace NKikimrServices;
using namespace NKikimr::NSyncLog;

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogGetHttpInfoActor
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogGetHttpInfoActor : public TActorBootstrapped<TSyncLogGetHttpInfoActor> {
            TIntrusivePtr<TVDiskContext> VCtx;
            TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
            NMon::TEvHttpInfo::TPtr Ev;
            const TActorId NotifyId;
            const TActorId KeeperId;
            TSyncLogNeighborsPtr NeighborsPtr;
            // we must obtain SnapPtr and NodesInfoMsg before calling Finish
            TSyncLogSnapshotPtr SnapPtr;
            TString SublogContent;
            TSyncLogKeeperDebugInfo DebugInfo;
            TEvInterconnect::TEvNodesInfo::TPtr NodesInfoMsg;

            friend class TActorBootstrapped<TSyncLogGetHttpInfoActor>;

            void Bootstrap(const TActorContext &ctx) {
                Become(&TThis::StateFunc);
                const TCgiParameters& cgi = Ev->Get()->Request.GetParams();
                if (cgi.Get("action") == "kickSyncLogCutLog") {
                    ui64 freeUpToLsn = 0;
                    if (TryFromString(cgi.Get("freeUpToLsn"), freeUpToLsn) && freeUpToLsn) {
                        ctx.Send(KeeperId, new NPDisk::TEvCutLog(0, 0, freeUpToLsn, 0, 0, 0, 0));
                    }
                }

                // obtain snapshot
                const bool introspection = true;
                ctx.Send(KeeperId, new TEvSyncLogSnapshot(introspection));
                // obtain nodes list
                ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
            }

            void Handle(TEvSyncLogSnapshotResult::TPtr &ev, const TActorContext &ctx) {
                Y_ABORT_UNLESS(!SnapPtr);
                SnapPtr = std::move(ev->Get()->SnapshotPtr);
                SublogContent = std::move(ev->Get()->SublogContent);
                DebugInfo = std::move(ev->Get()->DebugInfo);
                if (NodesInfoMsg)
                    Finish(ctx);
            }

            void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
                Y_ABORT_UNLESS(!NodesInfoMsg);
                NodesInfoMsg = ev;
                if (SnapPtr)
                    Finish(ctx);
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                Die(ctx);
            }

            void Finish(const TActorContext &ctx) {
                TStringStream str;
                str << "\n";
                HTML(str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "SyncLog (LogStartLsn=" << SnapPtr->LogStartLsn << "; "
                                << "EntryPointDbgInfo="
                                << SnapPtr->LastEntryPointDbgInfo.ToString() << ")";
                        }
                        DIV_CLASS("panel-body") {
                            DIV_CLASS("panel panel-warning") {
                                DIV_CLASS("panel-heading") {str << "SyncLog Cut Debug";}
                                DIV_CLASS("panel-body") {
                                    TABLE_CLASS("table table-condensed") {
                                        TABLEBODY() {
                                            TABLER() {
                                                TABLED() {str << "FirstLsnToKeep";}
                                                TABLED() {str << DebugInfo.FirstLsnToKeep;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "FreeUpToLsn";}
                                                TABLED() {str << DebugInfo.FreeUpToLsn;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "FreeUpToLsnSatisfied";}
                                                TABLED() {str << DebugInfo.FreeUpToLsnSatisfied;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "HasDelayedActions";}
                                                TABLED() {str << DebugInfo.HasDelayedActions;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "CommitInProgress";}
                                                TABLED() {str << DebugInfo.CommitInProgress;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "CutLogRetries";}
                                                TABLED() {str << DebugInfo.CutLogRetries;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "LastCutLogRetryFirstLsnToKeep";}
                                                TABLED() {str << DebugInfo.LastCutLogRetryFirstLsnToKeep;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "LastCommit";}
                                                TABLED() {
                                                    str << "EntryPointLsn# " << DebugInfo.LastCommitEntryPointLsn
                                                        << " RecoveryLogConfirmedLsn# " << DebugInfo.LastCommitRecoveryLogConfirmedLsn;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "SyncLog tail";}
                                                TABLED() {
                                                    str << "DiskLastLsn# " << DebugInfo.DiskLastLsn
                                                        << " LastLsn# " << DebugInfo.LastLsn;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Memory pages";}
                                                TABLED() {
                                                    str << DebugInfo.MemPages << " / " << DebugInfo.MaxMemPages;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Disk chunks";}
                                                TABLED() {
                                                    str << DebugInfo.DiskChunks << " / " << DebugInfo.MaxDiskChunks;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Emergency rescue writes";}
                                                TABLED() {
                                                    str << (!VCtx->IsLogRescueMode() ? "NormalMode"
                                                        : VCtx->AreLogRescueWritesAllowed() ? "AllowedContinuously"
                                                        : VCtx->GetLogRescueWriteTokens() ? "OneShotPending" : "Blocked")
                                                        << " OneShotTokens# " << VCtx->GetLogRescueWriteTokens();
                                                }
                                            }
                                            if (VCtx->IsLogRescueMode()) {
                                                TABLER() {
                                                    TABLED() {str << "Next commit plan";}
                                                    TABLED() {
                                                        str << "HasWork# " << DebugInfo.NextCommitPlan.HasWork
                                                            << " TrimTailPending# " << DebugInfo.NextCommitPlan.TrimTailPending
                                                            << " CutLogCommitRequired# "
                                                            << DebugInfo.NextCommitPlan.CutLogCommitRequired
                                                            << " MemOverflowCommitRequired# "
                                                            << DebugInfo.NextCommitPlan.MemOverflowCommitRequired
                                                            << " DeleteChunkCommitRequired# "
                                                            << DebugInfo.NextCommitPlan.DeleteChunkCommitRequired
                                                            << " InitialCommitRequired# "
                                                            << DebugInfo.NextCommitPlan.InitialCommitRequired;
                                                    }
                                                }
                                                TABLER() {
                                                    TABLED() {str << "Next commit IO estimate";}
                                                    TABLED() {
                                                        str << "CurrentDiskChunks# "
                                                            << DebugInfo.NextCommitPlan.CurrentDiskChunks
                                                            << " ChunksToAdd# " << DebugInfo.NextCommitPlan.ChunksToAdd
                                                            << " ChunksToTrimForQuota# "
                                                            << DebugInfo.NextCommitPlan.ChunksToTrimForQuota
                                                            << " ChunksToDeleteDelayed# "
                                                            << DebugInfo.NextCommitPlan.ChunksToDeleteDelayed
                                                            << " ChunksToDeleteReady# "
                                                            << DebugInfo.NextCommitPlan.ChunksToDeleteReady;
                                                    }
                                                }
                                                TABLER() {
                                                    TABLED() {str << "Next swap bounds";}
                                                    TABLED() {
                                                        str << "Attempted# " << DebugInfo.NextCommitPlan.Swap.Attempted
                                                            << " WantToCutRecoveryLog# "
                                                            << DebugInfo.NextCommitPlan.Swap.WantToCutRecoveryLog
                                                            << " StillMemOverflow# "
                                                            << DebugInfo.NextCommitPlan.Swap.StillMemOverflow
                                                            << " DiskLastLsn# "
                                                            << DebugInfo.NextCommitPlan.Swap.DiskLastLsn
                                                            << " FreeUpToLsn# "
                                                            << DebugInfo.NextCommitPlan.Swap.FreeUpToLsn
                                                            << " FreeNPages# " << DebugInfo.NextCommitPlan.Swap.FreeNPages
                                                            << " MaxSwapPages# "
                                                            << DebugInfo.NextCommitPlan.Swap.MaxSwapPages
                                                            << " SwapSnapPages# "
                                                            << DebugInfo.NextCommitPlan.Swap.SwapSnapPages;
                                                    }
                                                }
                                                TABLER() {
                                                    TABLED() {str << "Next swap boundaries";}
                                                    TABLED() {str << DebugInfo.NextCommitPlan.Swap.SwapSnapBoundaries;}
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Last swap checkpoint";}
                                                TABLED() {
                                                    str << "Attempted# " << DebugInfo.LastSwap.Attempted
                                                        << " WantToCutRecoveryLog# " << DebugInfo.LastSwap.WantToCutRecoveryLog
                                                        << " StillMemOverflow# " << DebugInfo.LastSwap.StillMemOverflow;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Last swap bounds";}
                                                TABLED() {
                                                    str << "DiskLastLsn# " << DebugInfo.LastSwap.DiskLastLsn
                                                        << " FreeUpToLsn# " << DebugInfo.LastSwap.FreeUpToLsn
                                                        << " FreeNPages# " << DebugInfo.LastSwap.FreeNPages
                                                        << " MaxSwapPages# " << DebugInfo.LastSwap.MaxSwapPages
                                                        << " SwapSnapPages# " << DebugInfo.LastSwap.SwapSnapPages;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Last swap boundaries";}
                                                TABLED() {str << DebugInfo.LastSwap.SwapSnapBoundaries;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "Last commit attempt";}
                                                TABLED() {
                                                    str << "SeqNo# " << DebugInfo.LastCommitAttempt.AttemptSeqNo
                                                        << " Time# " << ToStringLocalTimeUpToSeconds(DebugInfo.LastCommitAttempt.AttemptTime)
                                                        << " Status# " << DebugInfo.LastCommitAttempt.Status;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Last commit input";}
                                                TABLED() {
                                                    str << "SwapSnapPages# " << DebugInfo.LastCommitAttempt.AttemptSwapSnapPages
                                                        << " ChunksToDeleteDelayed# " << DebugInfo.LastCommitAttempt.AttemptChunksToDeleteDelayed
                                                        << " ChunksToDelete# " << DebugInfo.LastCommitAttempt.AttemptChunksToDelete
                                                        << " RecoveryLogConfirmedLsn# "
                                                        << DebugInfo.LastCommitAttempt.AttemptRecoveryLogConfirmedLsn;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Last commit input boundaries";}
                                                TABLED() {str << DebugInfo.LastCommitAttempt.AttemptSwapSnapBoundaries;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "Last commit result";}
                                                TABLED() {
                                                    str << "SeqNo# " << DebugInfo.LastCommitAttempt.ResultSeqNo
                                                        << " Time# " << ToStringLocalTimeUpToSeconds(DebugInfo.LastCommitAttempt.ResultTime)
                                                        << " EntryPointLsn# " << DebugInfo.LastCommitAttempt.ResultEntryPointLsn
                                                        << " RecoveryLogConfirmedLsn# "
                                                        << DebugInfo.LastCommitAttempt.ResultRecoveryLogConfirmedLsn
                                                        << " SwapAppends# " << DebugInfo.LastCommitAttempt.ResultSwapAppends
                                                        << " SwapPages# " << DebugInfo.LastCommitAttempt.ResultSwapPages;
                                                }
                                            }
                                            TABLER() {
                                                TABLED() {str << "Last commit entry point";}
                                                TABLED() {str << DebugInfo.LastCommitAttempt.ResultEntryPointDbgInfo;}
                                            }
                                            TABLER() {
                                                TABLED() {str << "Decomposed";}
                                                TABLED() {str << DebugInfo.FirstLsnToKeepDecomposed;}
                                            }
                                        }
                                    }
                                    if (DebugInfo.FreeUpToLsn) {
                                        str << "<a class=\"btn btn-warning\" href=\"?action=kickSyncLogCutLog&freeUpToLsn="
                                            << DebugInfo.FreeUpToLsn
                                            << "\">Kick SyncLog cut commit</a>";
                                    }
                                }
                            }
                            DIV_CLASS("row") {
                                DIV_CLASS("col-md-6") {SnapPtr->MemSnapPtr->OutputHtml(str);}
                                DIV_CLASS("col-md-6") {SnapPtr->DiskSnapPtr->OutputHtml(str);}
                            }
                            DIV_CLASS("row") {
                                DIV_CLASS("col-md-12") {
                                    NeighborsPtr->OutputHtml(str, *GInfo, NodesInfoMsg);
                                }
                            }
                            COLLAPSED_BUTTON_CONTENT("synclog_logcontent", "Log") {
                                PRE() {str << SublogContent;}
                            }
                        }
                    }
                }
                str << "\n";

                ctx.Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::SyncLogId));
                ctx.Send(NotifyId, new TEvents::TEvCompleted());
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvSyncLogSnapshotResult, Handle)
                HFunc(TEvInterconnect::TEvNodesInfo, Handle)
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
            )

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_SYNCLOG_HTTPINFO;
            }

            TSyncLogGetHttpInfoActor(const TIntrusivePtr<TVDiskContext> &vctx,
                                     const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                                     NMon::TEvHttpInfo::TPtr &ev,
                                     const TActorId &notifyId,
                                     const TActorId &keeperId,
                                     TSyncLogNeighborsPtr neighborsPtr)
                : TActorBootstrapped<TSyncLogGetHttpInfoActor>()
                , VCtx(vctx)
                , GInfo(ginfo)
                , Ev(ev)
                , NotifyId(notifyId)
                , KeeperId(keeperId)
                , NeighborsPtr(neighborsPtr)
            {
                Y_DEBUG_ABORT_UNLESS(Ev->Get()->SubRequestId == TDbMon::SyncLogId);
            }
        };

        IActor* CreateGetHttpInfoActor(const TIntrusivePtr<TVDiskContext> &vctx,
                                       const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                                       NMon::TEvHttpInfo::TPtr &ev,
                                       const TActorId &notifyId,
                                       const TActorId &keeperId,
                                       TSyncLogNeighborsPtr neighborsPtr) {
            return new TSyncLogGetHttpInfoActor(vctx, ginfo, ev, notifyId, keeperId, neighborsPtr);
        }

    } // NSyncLog
} // NKikimr
