#include "blobstorage_synclogneighbors.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_log.h>

using namespace NKikimrServices;

namespace NKikimr {

    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TPrinter
        ////////////////////////////////////////////////////////////////////////////
        // outputs state in HTML
        struct TSyncLogNeighbors::TPrinter {
            const TBlobStorageGroupInfo &GInfo;
            TEvInterconnect::TEvNodesInfo::TPtr NodesInfo;

            TPrinter(const TBlobStorageGroupInfo &ginfo,
                     TEvInterconnect::TEvNodesInfo::TPtr nodesInfo)
                : GInfo(ginfo)
                , NodesInfo(nodesInfo)
            {}

            void operator() (IOutputStream &str, TNeighbors::TConstIterator it) {
                HTML(str) {
                    SMALL() {
                        STRONG() {
                            // output VDiskID
                            auto vd = GInfo.GetVDiskId(it->VDiskIdShort);
                            str << "VDiskId: " << vd.ToStringWOGeneration() << "<br>";
                            // output node info
                            TActorId aid = GInfo.GetActorId(it->VDiskIdShort);
                            ui32 nodeId = aid.NodeId();
                            using TNodeInfo = TEvInterconnect::TNodeInfo;
                            const TNodeInfo *info = NodesInfo->Get()->GetNodeInfo(nodeId);
                            if (!info) {
                                str << "Node: NameService Error<br>";
                            } else {
                                str << "Node: " << info->Host << ":" << info->Port << "<br>";
                            }
                        }
                    }

                    if (it->Myself) {
                        PARA_CLASS("text-info") {str << "Self";}
                    } else {
                        SMALL() {
                            SMALL() {
                                str << "SyncedLsn: " << it->Get().SyncedLsn << "<br>";
                                ui64 lockedLsn = it->Get().LockedLsn;
                                if (lockedLsn == ui64(-1))
                                    str << "LockedLsn: no<br>";
                                else
                                    str << "LockedLsn: " << lockedLsn << "<br>";
                            }
                        }
                    }
                }
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // SYNC LOG NEIGHBORS AND POSITIONS
        ////////////////////////////////////////////////////////////////////////////
        void TSyncLogNeighbors::Lock(const TVDiskID &vdisk, ui64 lsn) {
            LOG_DEBUG(*ActorSystem, BS_SYNCLOG,
                      VDISKP(LogPrefix, "Lock: vdisk# %s lsn# %" PRIu64,
                             vdisk.ToString().data(), lsn));

            TNeighbors::TValue &ref = Neighbors[vdisk];
            Y_ABORT_UNLESS(ref.Get().LockedLsn == (ui64)-1);
            ref.Get().LockedLsn = lsn;
            LocksNum++;
        }

        void TSyncLogNeighbors::Unlock(const TVDiskID &vdisk) {
            LOG_DEBUG(*ActorSystem, BS_SYNCLOG,
                      VDISKP(LogPrefix, "Unlock: vdisk# %s",
                             vdisk.ToString().data()));

            TNeighbors::TValue &ref = Neighbors[vdisk];
            Y_ABORT_UNLESS(ref.Get().LockedLsn != (ui64)-1);
            ref.Get().LockedLsn = (ui64)-1;
            LocksNum--;
        }

        bool TSyncLogNeighbors::IsLocked(const TVDiskID &vdisk) {
            TNeighbors::TValue &ref = Neighbors[vdisk];
            const bool isLocked = ref.Get().LockedLsn != (ui64)-1;

            LOG_DEBUG(*ActorSystem, BS_SYNCLOG,
                      VDISKP(LogPrefix, "IsLocked: vdisk# %s res# %s",
                             vdisk.ToString().data(),
                             (isLocked ? "true" : "false")));

            return isLocked;
        }

        void TSyncLogNeighbors::OutputHtml(IOutputStream &str,
                                           const TBlobStorageGroupInfo &ginfo,
                                           TEvInterconnect::TEvNodesInfo::TPtr nodesInfo) {
            TPrinter printer(ginfo, nodesInfo);
            Neighbors.OutputHtml(str, printer, "Position of other disks in the group",
                                 "panel panel-default");
        }

    } // NSyncLog

} // NKikimr
