#include "blobstorage_syncloghttp.h"
#include "blobstorage_synclogdata.h"
#include "blobstorage_synclog_private_events.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/monlib/service/pages/templates.h>


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
            TEvInterconnect::TEvNodesInfo::TPtr NodesInfoMsg;

            friend class TActorBootstrapped<TSyncLogGetHttpInfoActor>;

            void Bootstrap(const TActorContext &ctx) {
                Become(&TThis::StateFunc);
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

