#pragma once

#include "defs.h"
#include "blobstorage_synclogdata.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/base/blobstorage.h>
#include <util/stream/str.h>

namespace NKikimr {
    namespace NSyncLog {

        struct TEvSyncLogTrim
            : public TEventLocal<TEvSyncLogTrim, TEvBlobStorage::EvSyncLogTrim>
        {
            const ui64 Lsn; // all disks confirmed that they got a record with this Lsn
            TEvSyncLogTrim(ui64 lsn)
                : Lsn(lsn)
            {}
        };

        struct TEvSyncLogFreeChunk
            : public TEventLocal<TEvSyncLogFreeChunk, TEvBlobStorage::EvSyncLogFreeChunk>
        {
            const ui32 ChunkIdx;
            TEvSyncLogFreeChunk(ui32 chunkIdx)
                : ChunkIdx(chunkIdx)
            {}
        };

        struct TEvSyncLogSnapshot
            : public TEventLocal<TEvSyncLogSnapshot, TEvBlobStorage::EvSyncLogSnapshot>
        {
            const bool IntrospectionInfo;
            TEvSyncLogSnapshot(bool introspectionInfo = false)
                : IntrospectionInfo(introspectionInfo)
            {}
        };

        class TSyncLogSnapshot;
        struct TSyncLogKeeperSwapDebugInfo {
            bool Attempted = false;
            bool WantToCutRecoveryLog = false;
            bool StillMemOverflow = false;
            ui64 DiskLastLsn = 0;
            ui64 FreeUpToLsn = 0;
            ui32 FreeNPages = 0;
            ui32 MaxSwapPages = 0;
            ui32 SwapSnapPages = 0;
            TString SwapSnapBoundaries;

            TString ToString() const {
                TStringStream str;
                str << "{Attempted# " << Attempted
                    << " WantToCutRecoveryLog# " << WantToCutRecoveryLog
                    << " StillMemOverflow# " << StillMemOverflow
                    << " DiskLastLsn# " << DiskLastLsn
                    << " FreeUpToLsn# " << FreeUpToLsn
                    << " FreeNPages# " << FreeNPages
                    << " MaxSwapPages# " << MaxSwapPages
                    << " SwapSnapPages# " << SwapSnapPages
                    << " SwapSnapBoundaries# " << SwapSnapBoundaries
                    << "}";
                return str.Str();
            }
        };

        struct TSyncLogKeeperCommitDebugInfo {
            ui64 AttemptSeqNo = 0;
            TInstant AttemptTime;
            ui32 AttemptSwapSnapPages = 0;
            ui32 AttemptChunksToDeleteDelayed = 0;
            ui32 AttemptChunksToDelete = 0;
            ui64 AttemptRecoveryLogConfirmedLsn = 0;
            TString AttemptSwapSnapBoundaries;
            TString Status;
            ui64 ResultSeqNo = 0;
            TInstant ResultTime;
            ui64 ResultEntryPointLsn = 0;
            ui64 ResultRecoveryLogConfirmedLsn = 0;
            ui32 ResultSwapAppends = 0;
            ui32 ResultSwapPages = 0;
            TString ResultEntryPointDbgInfo;

            TString ToString() const {
                TStringStream str;
                str << "{AttemptSeqNo# " << AttemptSeqNo
                    << " AttemptTime# " << AttemptTime
                    << " AttemptSwapSnapPages# " << AttemptSwapSnapPages
                    << " AttemptChunksToDeleteDelayed# " << AttemptChunksToDeleteDelayed
                    << " AttemptChunksToDelete# " << AttemptChunksToDelete
                    << " AttemptRecoveryLogConfirmedLsn# " << AttemptRecoveryLogConfirmedLsn
                    << " AttemptSwapSnapBoundaries# " << AttemptSwapSnapBoundaries
                    << " Status# " << Status
                    << " ResultSeqNo# " << ResultSeqNo
                    << " ResultTime# " << ResultTime
                    << " ResultEntryPointLsn# " << ResultEntryPointLsn
                    << " ResultRecoveryLogConfirmedLsn# " << ResultRecoveryLogConfirmedLsn
                    << " ResultSwapAppends# " << ResultSwapAppends
                    << " ResultSwapPages# " << ResultSwapPages
                    << " ResultEntryPointDbgInfo# " << ResultEntryPointDbgInfo
                    << "}";
                return str.Str();
            }
        };

        struct TSyncLogKeeperCommitPlanDebugInfo {
            bool HasWork = false;
            bool TrimTailPending = false;
            bool CutLogCommitRequired = false;
            bool MemOverflowCommitRequired = false;
            bool DeleteChunkCommitRequired = false;
            bool InitialCommitRequired = false;
            ui32 CurrentDiskChunks = 0;
            ui32 ChunksToAdd = 0;
            ui32 ChunksToTrimForQuota = 0;
            ui32 ChunksToDeleteDelayed = 0;
            ui32 ChunksToDeleteReady = 0;
            TSyncLogKeeperSwapDebugInfo Swap;

            TString ToString() const {
                TStringStream str;
                str << "{HasWork# " << HasWork
                    << " TrimTailPending# " << TrimTailPending
                    << " CutLogCommitRequired# " << CutLogCommitRequired
                    << " MemOverflowCommitRequired# " << MemOverflowCommitRequired
                    << " DeleteChunkCommitRequired# " << DeleteChunkCommitRequired
                    << " InitialCommitRequired# " << InitialCommitRequired
                    << " CurrentDiskChunks# " << CurrentDiskChunks
                    << " ChunksToAdd# " << ChunksToAdd
                    << " ChunksToTrimForQuota# " << ChunksToTrimForQuota
                    << " ChunksToDeleteDelayed# " << ChunksToDeleteDelayed
                    << " ChunksToDeleteReady# " << ChunksToDeleteReady
                    << " Swap# " << Swap.ToString()
                    << "}";
                return str.Str();
            }
        };

        struct TSyncLogKeeperDebugInfo {
            ui64 FirstLsnToKeep = 0;
            ui64 FreeUpToLsn = 0;
            ui64 LastCutLogRetryFirstLsnToKeep = 0;
            ui64 LastCommitEntryPointLsn = 0;
            ui64 LastCommitRecoveryLogConfirmedLsn = 0;
            ui64 DiskLastLsn = 0;
            ui64 LastLsn = 0;
            ui32 MemPages = 0;
            ui32 MaxMemPages = 0;
            ui32 DiskChunks = 0;
            ui32 MaxDiskChunks = 0;
            ui32 CutLogRetries = 0;
            bool FreeUpToLsnSatisfied = true;
            bool HasDelayedActions = false;
            bool CommitInProgress = false;
            TSyncLogKeeperCommitPlanDebugInfo NextCommitPlan;
            TSyncLogKeeperSwapDebugInfo LastSwap;
            TSyncLogKeeperCommitDebugInfo LastCommitAttempt;
            TString FirstLsnToKeepDecomposed;

            TString ToString() const {
                TStringStream str;
                str << "{FirstLsnToKeep# " << FirstLsnToKeep
                    << " FreeUpToLsn# " << FreeUpToLsn
                    << " FreeUpToLsnSatisfied# " << FreeUpToLsnSatisfied
                    << " HasDelayedActions# " << HasDelayedActions
                    << " CommitInProgress# " << CommitInProgress
                    << " NextCommitPlan# " << NextCommitPlan.ToString()
                    << " CutLogRetries# " << CutLogRetries
                    << " LastCutLogRetryFirstLsnToKeep# " << LastCutLogRetryFirstLsnToKeep
                    << " LastCommitEntryPointLsn# " << LastCommitEntryPointLsn
                    << " LastCommitRecoveryLogConfirmedLsn# " << LastCommitRecoveryLogConfirmedLsn
                    << " DiskLastLsn# " << DiskLastLsn
                    << " LastLsn# " << LastLsn
                    << " MemPages# " << MemPages
                    << " MaxMemPages# " << MaxMemPages
                    << " DiskChunks# " << DiskChunks
                    << " MaxDiskChunks# " << MaxDiskChunks
                    << " LastSwap# " << LastSwap.ToString()
                    << " LastCommitAttempt# " << LastCommitAttempt.ToString()
                    << " FirstLsnToKeepDecomposed# " << FirstLsnToKeepDecomposed
                    << "}";
                return str.Str();
            }
        };

        struct TEvSyncLogSnapshotResult
            : public TEventLocal<TEvSyncLogSnapshotResult,
                                 TEvBlobStorage::EvSyncLogSnapshotResult>
        {
            TIntrusivePtr<TSyncLogSnapshot> SnapshotPtr;
            TString SublogContent = {};
            TSyncLogKeeperDebugInfo DebugInfo;

            TEvSyncLogSnapshotResult(const TIntrusivePtr<TSyncLogSnapshot> &ptr,
                    const TString &sublogContent,
                    TSyncLogKeeperDebugInfo debugInfo);
            ~TEvSyncLogSnapshotResult();
        };

        struct TEvSyncLogReadFinished
            : TEventLocal<TEvSyncLogReadFinished, TEvBlobStorage::EvSyncLogReadFinished>
        {
            const TVDiskID VDiskID;
            TEvSyncLogReadFinished(const TVDiskID &vdisk)
                : VDiskID(vdisk)
            {}
        };

        struct TEvSyncLogLocalStatus
            : TEventLocal<TEvSyncLogLocalStatus, TEvBlobStorage::EvSyncLogLocalStatus>
        {
        };

        struct TEvSyncLogLocalStatusResult
            : TEventLocal<TEvSyncLogLocalStatusResult, TEvBlobStorage::EvSyncLogLocalStatusResult>
        {
            TLogEssence Essence;

            TEvSyncLogLocalStatusResult(const TLogEssence &e)
                : Essence(e)
            {}
        };

    } // NSyncLog
} // NKikimr
