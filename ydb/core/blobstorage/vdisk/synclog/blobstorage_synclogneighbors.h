#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_syncneighbors.h>
#include <ydb/library/actors/core/interconnect.h>

#include <library/cpp/containers/intrusive_avl_tree/avltree.h>

namespace NKikimr {

    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // SYNC LOG NEIGHBORS AND POSITIONS
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogNeighbors : public TThrRefBase {
            struct TSyncPos;
            struct TSyncPosCompare {
                static inline bool Compare(const TSyncPos& l, const TSyncPos& r);
            };

            struct TSyncPos : public TAvlTreeItem<TSyncPos, TSyncPosCompare> {
                ui64 SyncedLsn;
                ui64 LockedLsn;

                TSyncPos()
                    : SyncedLsn(0)
                    , LockedLsn(ui64(-1))
                {}

                TSyncPos(TSyncPos &&p)
                    : SyncedLsn(p.SyncedLsn)
                    , LockedLsn(p.LockedLsn)
                {}

                TString ToString() const {
                    return Sprintf("{SyncedLsn: %" PRIu64 " LockedLsn: %" PRIu64 "}", SyncedLsn, LockedLsn);
                }

                TSyncPos(IInputStream &) {
                    Y_ABORT("Not supported");
                }
            };

            typedef NSync::TVDiskNeighbors<TSyncPos> TNeighbors;
            typedef TAvlTree<TSyncPos, TSyncPosCompare> TSyncPosQueue;

            TNeighbors Neighbors;
            TSyncPosQueue SyncPosQueue;
            const TString LogPrefix;
            TActorSystem *ActorSystem;
            ui32 LocksNum;

        public:
            bool CheckSyncPosQueue() const {
                if (Neighbors.GetTotalDisks() == 1) {
                    return true;
                }
                TSyncPosQueue &spq = (const_cast<TSyncLogNeighbors*>(this))->SyncPosQueue;
                ui64 minLsn = (ui64)(-1);
                for (TSyncPosQueue::iterator it = spq.Begin(), e = spq.End(); it != e; ++it) {
                    minLsn = Min(minLsn, it->SyncedLsn);
                }
                return (spq.Begin()->SyncedLsn == minLsn);
            }

            ui64 GlobalSyncedLsn() const {
                if (Neighbors.GetTotalDisks() == 1) {
                    return (ui64)-1;
                }
                Y_DEBUG_ABORT_UNLESS(CheckSyncPosQueue());
                // TAvlTree doesn't have const Begin, se we have to remove 'const' qualifier
                ui64 result = (const_cast<TSyncLogNeighbors*>(this))->SyncPosQueue.Begin()->SyncedLsn;
                return result;
            }

            void Lock(const TVDiskID &vdisk, ui64 lsn);
            void Unlock(const TVDiskID &vdisk);
            bool IsLocked(const TVDiskID &vdisk);
            void OutputHtml(IOutputStream &str,
                            const TBlobStorageGroupInfo &ginfo,
                            TEvInterconnect::TEvNodesInfo::TPtr nodesInfo);

            ui64 LockedDiapason() {
                ui64 lockedDiapason = (ui64)-1;
                if (LocksNum != 0) {
                    // check all vdisks
                    for (TNeighbors::TIterator it = Neighbors.Begin(), e = Neighbors.End(); it != e; ++it) {
                        lockedDiapason = Min(lockedDiapason, it->Get().LockedLsn);
                    }
                }
                return lockedDiapason;
            }

            void UpdateSyncedLsn(const TVDiskID &vdisk, ui64 syncedLsn) {
                TNeighbors::TValue &diskData = Neighbors[vdisk];
                TSyncPos &ref = diskData.Get();
                // reorder
                Y_DEBUG_ABORT_UNLESS(syncedLsn > ref.SyncedLsn);
                ref.SyncedLsn = syncedLsn;
                ref.Unlink();
                SyncPosQueue.Insert(&ref);

                Y_DEBUG_ABORT_UNLESS(CheckSyncPosQueue());
            }

            ui64 GetSyncedLsn(const TVDiskID &vdisk) {
                TNeighbors::TValue &diskData = Neighbors[vdisk];
                return diskData.Get().SyncedLsn;
            }

            TString ToString(char sep = '\0') const {
                return Sprintf("{Global: %" PRIu64 " LocksNum: %" PRIu32 " Neighbors: %s}",
                               GlobalSyncedLsn(), LocksNum, Neighbors.ToString(sep).data());
            }

            TSyncLogNeighbors(const TVDiskIdShort &selfVDisk,
                              std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                              const TString &logPrefix, TActorSystem *actorSystem)
                : Neighbors(selfVDisk, top)
                , SyncPosQueue()
                , LogPrefix(logPrefix)
                , ActorSystem(actorSystem)
                , LocksNum(0)
            {
                for (TNeighbors::TIterator it = Neighbors.Begin(), e = Neighbors.End(); it != e; ++it) {
                    if (!it->Myself) {
                        SyncPosQueue.Insert(&(it->Get()));
                    }
                }
                Y_DEBUG_ABORT_UNLESS(CheckSyncPosQueue());
            }

        private:
            // outputs state in HTML
            struct TPrinter;
        };

        typedef TIntrusivePtr<TSyncLogNeighbors> TSyncLogNeighborsPtr;

        inline bool TSyncLogNeighbors::TSyncPosCompare::Compare(
                        const TSyncLogNeighbors::TSyncPos& l,
                        const TSyncLogNeighbors::TSyncPos& r) {
            return l.SyncedLsn < r.SyncedLsn || (l.SyncedLsn == r.SyncedLsn && &l < &r);
        }

    } // NSyncLog

} // NKikimr

