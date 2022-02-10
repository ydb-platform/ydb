#pragma once

#include "defs.h"
#include "blobstorage_synclogdata.h"

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TCommitHistory
        ////////////////////////////////////////////////////////////////////////////
        struct TCommitHistory {
            // commit time
            TInstant Time;
            // lsn of entry point
            ui64 EntryPointLsn = 0;
            // this is last confirmed lsn at commit moment
            ui64 RecoveryLogConfirmedLsn = 0;

            TCommitHistory(const TInstant &time, ui64 entryPointLsn, ui64 recoveryLogConfirmedLsn)
                : Time(time)
                , EntryPointLsn(entryPointLsn)
                , RecoveryLogConfirmedLsn(recoveryLogConfirmedLsn)
            {}

            TCommitHistory(const TCommitHistory &) = default;
            TCommitHistory(TCommitHistory &&) = default;
            TCommitHistory &operator=(const TCommitHistory &) = default;
            TCommitHistory &operator=(TCommitHistory &&) = default;

            void Output(IOutputStream &str) const {
                str << "{Time# " << Time
                    << " EntryPointLsn# " << EntryPointLsn
                    << " RecoveryLogConfirmedLsn# " << RecoveryLogConfirmedLsn << "}";
            }

            TString ToString() const {
                TStringStream s;
                Output(s);
                return s.Str();
            }

            ui64 FirstLsnToKeep() const {
                return Min(EntryPointLsn, RecoveryLogConfirmedLsn);
            }
        };


        ////////////////////////////////////////////////////////////////////////////
        // TEvSyncLogCommitDone
        ////////////////////////////////////////////////////////////////////////////
        struct TEvSyncLogCommitDone
            : public TEventLocal<TEvSyncLogCommitDone, TEvBlobStorage::EvSyncLogCommitDone>
        {
            const TCommitHistory CommitInfo;
            const TEntryPointDbgInfo EntryPointDbgInfo;
            TDeltaToDiskRecLog Delta;

            TEvSyncLogCommitDone(const TCommitHistory &commitInfo,
                                 const TEntryPointDbgInfo &layout,
                                 TDeltaToDiskRecLog &&delta)
                : CommitInfo(commitInfo)
                , EntryPointDbgInfo(layout)
                , Delta(std::move(delta))
            {}

            void Output(IOutputStream &s) const {
                s << "{CommitInfo# ";
                CommitInfo.Output(s);
                s << " EntryPointDbgInfo# ";
                EntryPointDbgInfo.Output(s);
                s << " Delta# ";
                Delta.Output(s);
                s << "}";
            }

            TString ToString() const {
                TStringStream s;
                Output(s);
                return s.Str();
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogKeeperCommitData
        ////////////////////////////////////////////////////////////////////////////
        struct TSyncLogKeeperCommitData {
            TSyncLogSnapshotPtr SyncLogSnap;
            TMemRecLogSnapshotPtr SwapSnap;
            TVector<ui32> ChunksToDeleteDelayed;
            TVector<ui32> ChunksToDelete;
            const ui64 RecoveryLogConfirmedLsn;

            TSyncLogKeeperCommitData(
                    TSyncLogSnapshotPtr &&syncLogSnap,
                    TMemRecLogSnapshotPtr &&swapSnap,
                    TVector<ui32> &&chunksToDeleteDelayed,
                    TVector<ui32> &&chunksToDelete,
                    ui64 recoveryLogConfirmedLsn)
                : SyncLogSnap(std::move(syncLogSnap))
                , SwapSnap(std::move(swapSnap))
                , ChunksToDeleteDelayed(std::move(chunksToDeleteDelayed))
                , ChunksToDelete(std::move(chunksToDelete))
                , RecoveryLogConfirmedLsn(recoveryLogConfirmedLsn)
            {}
            TSyncLogKeeperCommitData(TSyncLogKeeperCommitData &&) = default;
            TSyncLogKeeperCommitData &operator=(TSyncLogKeeperCommitData &&) = default;
            TSyncLogKeeperCommitData(const TSyncLogKeeperCommitData &) = delete;
            TSyncLogKeeperCommitData &operator=(const TSyncLogKeeperCommitData &) = delete;

            void Output(IOutputStream &str) const {
                str << "{SyncLogSnap->Boundaries# " << SyncLogSnap->BoundariesToString()
                    << " SwapSnap# " << (SwapSnap ? SwapSnap->BoundariesToString() : "<empty>")
                    << " ChunksToDeleteDelayed.size# " << ChunksToDeleteDelayed.size()
                    << " ChunksToDelete# " << ChunksToDelete.size()
                    << " RecoveryLogConfirmedLsn# " << RecoveryLogConfirmedLsn
                    << "}";
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // CreateSyncLogCommitter
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogCtx;
        IActor *CreateSyncLogCommitter(
                TIntrusivePtr<TSyncLogCtx> slCtx,
                const TActorId &notifyID,
                TSyncLogKeeperCommitData &&commitData);

    } // NSyncLog
} // NKikimr
