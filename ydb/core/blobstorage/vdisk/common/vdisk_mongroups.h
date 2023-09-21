#pragma once

#include "defs.h"

#include <ydb/core/protos/node_whiteboard.pb.h>

namespace NKikimr {
    namespace NMonGroup {

        class TBase {
        public:
            TBase(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
                  const TString& name,
                  const TString& value)
                : DerivedCounters(counters)
                , GroupCounters(DerivedCounters->GetSubgroup(name, value))
            {}

            TIntrusivePtr<::NMonitoring::TDynamicCounters> GetGroup() const { return GroupCounters; }

        protected:
            TIntrusivePtr<::NMonitoring::TDynamicCounters> DerivedCounters;
            TIntrusivePtr<::NMonitoring::TDynamicCounters> GroupCounters;
        };


#define COUNTER_DEF(name)                                                                   \
protected:                                                                                  \
    ::NMonitoring::TDynamicCounters::TCounterPtr name##_;                                     \
public:                                                                                     \
    NMonitoring::TDeprecatedCounter &name() { return *name##_; }                            \
    const NMonitoring::TDeprecatedCounter &name() const { return *name##_; }                \
    const ::NMonitoring::TDynamicCounters::TCounterPtr &name##Ptr() const { return name##_; }

#define COUNTER_INIT(name, derivative)                                                      \
    name##_ = GroupCounters->GetCounter(#name, derivative)

#define COUNTER_INIT_PRIVATE(name, derivative)                                              \
    name##_ = GroupCounters->GetCounter(#name, derivative,                                  \
        NMonitoring::TCountableBase::EVisibility::Private)

#define GROUP_CONSTRUCTOR(name)                                                             \
    name(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,                      \
         const TString& name,                                                               \
         const TString& value)                                                              \
    : TBase(counters, name, value)


        ///////////////////////////////////////////////////////////////////////////////////
        // TLsmHullGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TLsmHullGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TLsmHullGroup)
            {
                COUNTER_INIT(LsmCompactionBytesRead, true);
                COUNTER_INIT(LsmCompactionBytesWritten, true);
                COUNTER_INIT(LsmHugeBytesWritten, true);
                COUNTER_INIT(LsmLogBytesWritten, true);
            }

            COUNTER_DEF(LsmCompactionBytesRead)
            COUNTER_DEF(LsmCompactionBytesWritten)
            COUNTER_DEF(LsmHugeBytesWritten)
            COUNTER_DEF(LsmLogBytesWritten)
        };


        ///////////////////////////////////////////////////////////////////////////////////
        // TLsmHullSpaceGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TLsmHullSpaceGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TLsmHullSpaceGroup)
            {
                COUNTER_INIT(DskSpaceCurIndex, false);
                COUNTER_INIT(DskSpaceCurInplacedData, false);
                COUNTER_INIT(DskSpaceCurHugeData, false);
                COUNTER_INIT(DskSpaceCompIndex, false);
                COUNTER_INIT(DskSpaceCompInplacedData, false);
                COUNTER_INIT(DskSpaceCompHugeData, false);
            }

            COUNTER_DEF(DskSpaceCurIndex);
            COUNTER_DEF(DskSpaceCurInplacedData);
            COUNTER_DEF(DskSpaceCurHugeData);
            COUNTER_DEF(DskSpaceCompIndex);
            COUNTER_DEF(DskSpaceCompInplacedData);
            COUNTER_DEF(DskSpaceCompHugeData);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TSkeletonOverloadGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TSkeletonOverloadGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TSkeletonOverloadGroup)
            {
                COUNTER_INIT(EmergencyMovedPatchQueueItems, false);
                COUNTER_INIT(EmergencyPatchStartQueueItems, false);
                COUNTER_INIT(EmergencyPutQueueItems, false);
                COUNTER_INIT(EmergencyMultiPutQueueItems, false);
                COUNTER_INIT(EmergencyLocalSyncDataQueueItems, false);
                COUNTER_INIT(EmergencyAnubisOsirisPutQueueItems, false);

                COUNTER_INIT(EmergencyMovedPatchQueueBytes, false);
                COUNTER_INIT(EmergencyPatchStartQueueBytes, false);
                COUNTER_INIT(EmergencyPutQueueBytes, false);
                COUNTER_INIT(EmergencyMultiPutQueueBytes, false);
                COUNTER_INIT(EmergencyLocalSyncDataQueueBytes, false);
                COUNTER_INIT(EmergencyAnubisOsirisPutQueueBytes, false);

                COUNTER_INIT(FreshSatisfactionRankPercent, false);
                COUNTER_INIT(LevelSatisfactionRankPercent, false);
            }

            COUNTER_DEF(EmergencyMovedPatchQueueItems);
            COUNTER_DEF(EmergencyPatchStartQueueItems);
            COUNTER_DEF(EmergencyPutQueueItems);
            COUNTER_DEF(EmergencyMultiPutQueueItems);
            COUNTER_DEF(EmergencyLocalSyncDataQueueItems);
            COUNTER_DEF(EmergencyAnubisOsirisPutQueueItems);

            COUNTER_DEF(EmergencyMovedPatchQueueBytes);
            COUNTER_DEF(EmergencyPatchStartQueueBytes);
            COUNTER_DEF(EmergencyPutQueueBytes);
            COUNTER_DEF(EmergencyMultiPutQueueBytes);
            COUNTER_DEF(EmergencyLocalSyncDataQueueBytes);
            COUNTER_DEF(EmergencyAnubisOsirisPutQueueBytes);

            COUNTER_DEF(FreshSatisfactionRankPercent);
            COUNTER_DEF(LevelSatisfactionRankPercent);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TDskOutOfSpaceGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TDskOutOfSpaceGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TDskOutOfSpaceGroup)
            {
                COUNTER_INIT(DskOutOfSpace, false);
                COUNTER_INIT(DskTotalBytes, false);
                COUNTER_INIT(DskFreeBytes, false);
                COUNTER_INIT(DskUsedBytes, false);
                COUNTER_INIT(HugeUsedChunks, false);
                COUNTER_INIT(HugeCanBeFreedChunks, false);
                COUNTER_INIT(HugeLockedChunks, false);
            }

            COUNTER_DEF(DskOutOfSpace);
            COUNTER_DEF(DskTotalBytes);        // total bytes available on PDisk for this VDisk
            COUNTER_DEF(DskFreeBytes);         // free bytes available on PDisk for this VDisk
            COUNTER_DEF(DskUsedBytes);         // bytes used by this VDisk on PDisk
            // huge heap chunks
            COUNTER_DEF(HugeUsedChunks);       // chunks used by huge heap
            COUNTER_DEF(HugeCanBeFreedChunks); // number of chunks that can be freed after defragmentation
            COUNTER_DEF(HugeLockedChunks);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TCostGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TCostGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TCostGroup)
            {
                COUNTER_INIT(DiskTimeAvailableNs, false);
                COUNTER_INIT(VDiskUserCostNs, true);
                COUNTER_INIT(VDiskInternalCostNs, true);
            }
            COUNTER_DEF(DiskTimeAvailableNs);
            COUNTER_DEF(VDiskUserCostNs);
            COUNTER_DEF(VDiskInternalCostNs);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TSyncerGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TSyncerGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TSyncerGroup)
            {
                COUNTER_INIT(SyncerVSyncMessagesSent, true);
                COUNTER_INIT(SyncerVSyncBytesSent, true);
                COUNTER_INIT(SyncerVSyncBytesReceived, true);
                COUNTER_INIT(SyncerVSyncFullMessagesSent, true);
                COUNTER_INIT(SyncerVSyncFullBytesSent, true);
                COUNTER_INIT(SyncerVSyncFullBytesReceived, true);
                COUNTER_INIT(SyncerUnsyncedDisks, false);
                COUNTER_INIT(SyncerLoggerRecords, true);
                COUNTER_INIT(SyncerLoggedBytes, true);
            }

            COUNTER_DEF(SyncerVSyncMessagesSent);
            COUNTER_DEF(SyncerVSyncBytesSent);
            COUNTER_DEF(SyncerVSyncBytesReceived);
            COUNTER_DEF(SyncerVSyncFullMessagesSent);
            COUNTER_DEF(SyncerVSyncFullBytesSent);
            COUNTER_DEF(SyncerVSyncFullBytesReceived);
            COUNTER_DEF(SyncerUnsyncedDisks);
            COUNTER_DEF(SyncerLoggerRecords);
            COUNTER_DEF(SyncerLoggedBytes);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TReplGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TReplGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TReplGroup)
            {
                COUNTER_INIT(ReplRecoveryGroupTypeErrors, true);
                COUNTER_INIT(ReplBlobsRecovered, true);
                COUNTER_INIT(ReplBlobBytesRecovered, true);
                COUNTER_INIT(ReplHugeBlobsRecovered, true);
                COUNTER_INIT(ReplHugeBlobBytesRecovered, true);
                COUNTER_INIT(ReplChunksWritten, true);
                COUNTER_INIT(ReplUnreplicatedVDisks, false);
                COUNTER_INIT(ReplVGetBytesReceived, true);
                COUNTER_INIT(ReplPhantomLikeDiscovered, false);
                COUNTER_INIT(ReplPhantomLikeRecovered, false);
                COUNTER_INIT(ReplPhantomLikeUnrecovered, false);
                COUNTER_INIT(ReplPhantomLikeDropped, false);
                COUNTER_INIT(ReplWorkUnitsDone, false);
                COUNTER_INIT(ReplWorkUnitsRemaining, false);
                COUNTER_INIT(ReplItemsDone, false);
                COUNTER_INIT(ReplItemsRemaining, false);
                COUNTER_INIT(ReplUnreplicatedPhantoms, false);
                COUNTER_INIT(ReplUnreplicatedNonPhantoms, false);
                COUNTER_INIT(ReplSecondsRemaining, false);
            }

            COUNTER_DEF(SyncerVSyncMessagesSent);
            COUNTER_DEF(ReplRecoveryGroupTypeErrors);
            COUNTER_DEF(ReplBlobsRecovered);
            COUNTER_DEF(ReplBlobBytesRecovered);
            COUNTER_DEF(ReplHugeBlobsRecovered);
            COUNTER_DEF(ReplHugeBlobBytesRecovered);
            COUNTER_DEF(ReplChunksWritten);
            COUNTER_DEF(ReplUnreplicatedVDisks);
            COUNTER_DEF(ReplVGetBytesReceived);
            COUNTER_DEF(ReplPhantomLikeDiscovered);
            COUNTER_DEF(ReplPhantomLikeRecovered);
            COUNTER_DEF(ReplPhantomLikeUnrecovered);
            COUNTER_DEF(ReplPhantomLikeDropped);
            COUNTER_DEF(ReplWorkUnitsDone);
            COUNTER_DEF(ReplWorkUnitsRemaining);
            COUNTER_DEF(ReplItemsDone);
            COUNTER_DEF(ReplItemsRemaining);
            COUNTER_DEF(ReplUnreplicatedPhantoms);
            COUNTER_DEF(ReplUnreplicatedNonPhantoms);
            COUNTER_DEF(ReplSecondsRemaining);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TLocalRecoveryGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TLocalRecoveryGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TLocalRecoveryGroup)
            {
                COUNTER_INIT_PRIVATE(LogoBlobsDbEmpty, false);
                COUNTER_INIT_PRIVATE(BlocksDbEmpty, false);
                COUNTER_INIT_PRIVATE(BarriersDbEmpty, false);
                COUNTER_INIT(LocalRecovRecsDispatched, true);
                COUNTER_INIT(LocalRecovBytesDispatched, true);
                COUNTER_INIT(LocalRecovRecsApplied, true);
                COUNTER_INIT(LocalRecovBytesApplied, true);
                COUNTER_INIT(BulkLogoBlobs, true);
            }

            COUNTER_DEF(LogoBlobsDbEmpty);
            COUNTER_DEF(BlocksDbEmpty);
            COUNTER_DEF(BarriersDbEmpty);
            COUNTER_DEF(LocalRecovRecsDispatched);
            COUNTER_DEF(LocalRecovBytesDispatched);
            COUNTER_DEF(LocalRecovRecsApplied);
            COUNTER_DEF(LocalRecovBytesApplied);
            COUNTER_DEF(BulkLogoBlobs);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TInterfaceGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TInterfaceGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TInterfaceGroup)
            {
                COUNTER_INIT(PutTotalBytes, true);
                COUNTER_INIT(GetTotalBytes, true);
            }

            COUNTER_DEF(PutTotalBytes);
            COUNTER_DEF(GetTotalBytes);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TSyncLogIFaceGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TSyncLogIFaceGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TSyncLogIFaceGroup)
            {
                COUNTER_INIT(SyncPutMsgs, true);
                COUNTER_INIT(SyncPutSstMsgs, true);
                COUNTER_INIT(SyncReadMsgs, true);
                COUNTER_INIT(SyncReadResMsgs, true);
                COUNTER_INIT(LocalSyncMsgs, true);
                COUNTER_INIT(LocalSyncResMsgs, true);
                COUNTER_INIT(SyncLogGetSnapshot, true);
                COUNTER_INIT(SyncLogLocalStatus, true);
            }

            COUNTER_DEF(SyncPutMsgs);
            COUNTER_DEF(SyncPutSstMsgs);
            COUNTER_DEF(SyncReadMsgs);
            COUNTER_DEF(SyncReadResMsgs);
            COUNTER_DEF(LocalSyncMsgs);
            COUNTER_DEF(LocalSyncResMsgs);
            COUNTER_DEF(SyncLogGetSnapshot);
            COUNTER_DEF(SyncLogLocalStatus);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TSyncLogCountersGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TSyncLogCountersGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TSyncLogCountersGroup)
            {
                COUNTER_INIT(VDiskCheckFailed, true);
                COUNTER_INIT(UnequalGuid, true);
                COUNTER_INIT(DiskLocked, true);
                COUNTER_INIT(ReplyError, true);
                COUNTER_INIT(FullRecovery, true);
                COUNTER_INIT(NormalSync, true);
                COUNTER_INIT(ReadsFromDisk, true);
                COUNTER_INIT(ReadsFromDiskBytes, true);
            }

            // status of read request:
            COUNTER_DEF(VDiskCheckFailed);
            COUNTER_DEF(UnequalGuid);
            COUNTER_DEF(DiskLocked);
            COUNTER_DEF(ReplyError);
            COUNTER_DEF(FullRecovery);
            COUNTER_DEF(NormalSync);
            // additional counters:
            COUNTER_DEF(ReadsFromDisk);
            COUNTER_DEF(ReadsFromDiskBytes);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TVDiskStateGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TVDiskStateGroup: public TBase {
            std::array<::NMonitoring::TDynamicCounters::TCounterPtr, NKikimrWhiteboard::EVDiskState_MAX + 1> VDiskStates;
            ::NMonitoring::TDynamicCounters::TCounterPtr CurrentState;

        public:
            GROUP_CONSTRUCTOR(TVDiskStateGroup)
            {
                // depracated, only for compatibility
                TString name = "VDiskState";
                CurrentState = GroupCounters->GetCounter(name, false);
                *CurrentState = NKikimrWhiteboard::Initial;

                for (size_t i = NKikimrWhiteboard::EVDiskState_MIN; i <= NKikimrWhiteboard::EVDiskState_MAX; ++i) {
                    VDiskStates[i] = GroupCounters->GetCounter(name + "_" + NKikimrWhiteboard::EVDiskState_Name(i), false);
                }
                COUNTER_INIT(VDiskLocalRecoveryState, false);
            }

            void VDiskState(NKikimrWhiteboard::EVDiskState s) {
                *VDiskStates[*CurrentState] = 0;
                *CurrentState = s;
                *VDiskStates[s] = 1;
            }

            NKikimrWhiteboard::EVDiskState VDiskState() const {
                return static_cast<NKikimrWhiteboard::EVDiskState>(CurrentState->Val());
            }

            COUNTER_DEF(VDiskLocalRecoveryState);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TLsmLevelGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TLsmLevelGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TLsmLevelGroup)
            {
                COUNTER_INIT_PRIVATE(SstNum, false);
                COUNTER_INIT(NumItems, false);
                COUNTER_INIT(NumItemsInplaced, false);
                COUNTER_INIT(NumItemsHuge, false);
                COUNTER_INIT(DataInplaced, false);
                COUNTER_INIT(DataHuge, false);
            }

            COUNTER_DEF(SstNum);
            COUNTER_DEF(NumItems);
            COUNTER_DEF(NumItemsInplaced);
            COUNTER_DEF(NumItemsHuge);
            COUNTER_DEF(DataInplaced);
            COUNTER_DEF(DataHuge);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TLsmAllLevelsStat
        ///////////////////////////////////////////////////////////////////////////////////
        class TLsmAllLevelsStat {
        public:
            TIntrusivePtr<::NMonitoring::TDynamicCounters> Group;
            // per-level information
            TLsmLevelGroup Level0;
            TLsmLevelGroup Level1to8;
            TLsmLevelGroup Level9to16;
            TLsmLevelGroup Level17;
            TLsmLevelGroup Level18;

            TLsmAllLevelsStat(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
                : Group(counters->GetSubgroup("subsystem", "levels"))
                , Level0(Group, "level", "0")
                , Level1to8(Group, "level", "1..8")
                , Level9to16(Group, "level", "9..16")
                , Level17(Group, "level", "17")
                , Level18(Group, "level", "18")
            {}
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TVDiskIFaceGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TVDiskIFaceGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TVDiskIFaceGroup)
            {
                COUNTER_INIT(MovedPatchMsgs, true);
                COUNTER_INIT(PatchStartMsgs, true);
                COUNTER_INIT(PatchDiffMsgs, true);
                COUNTER_INIT(PatchXorDiffMsgs, true);
                COUNTER_INIT(PutMsgs, true);
                COUNTER_INIT(MultiPutMsgs, true);
                COUNTER_INIT(GetMsgs, true);
                COUNTER_INIT(BlockMsgs, true);
                COUNTER_INIT(GetBlockMsgs, true);
                COUNTER_INIT(BlockAndGetMsgs, true);
                COUNTER_INIT(GCMsgs, true);
                COUNTER_INIT(GetBarrierMsgs, true);
                COUNTER_INIT(SyncMsgs, true);
                COUNTER_INIT(SyncFullMsgs, true);
                COUNTER_INIT(RecoveredHugeBlobMsgs, true);
                COUNTER_INIT(StatusMsgs, true);
                COUNTER_INIT(DbStatMsgs, true);
                COUNTER_INIT(AnubisPutMsgs, true);
                COUNTER_INIT(OsirisPutMsgs, true);

                COUNTER_INIT_PRIVATE(MovedPatchResMsgs, true);
                COUNTER_INIT_PRIVATE(PatchFoundPartsMsgs, true);
                COUNTER_INIT_PRIVATE(PatchXorDiffResMsgs, true);
                COUNTER_INIT_PRIVATE(PatchResMsgs, true);
                COUNTER_INIT_PRIVATE(PutResMsgs, true);
                COUNTER_INIT_PRIVATE(MultiPutResMsgs, true);
                COUNTER_INIT_PRIVATE(GetResMsgs, true);
                COUNTER_INIT_PRIVATE(BlockResMsgs, true);
                COUNTER_INIT_PRIVATE(GetBlockResMsgs, true);
                COUNTER_INIT_PRIVATE(GCResMsgs, true);
                COUNTER_INIT_PRIVATE(GetBarrierResMsgs, true);
                COUNTER_INIT_PRIVATE(SyncResMsgs, true);
                COUNTER_INIT_PRIVATE(SyncFullResMsgs, true);
                COUNTER_INIT_PRIVATE(RecoveredHugeBlobResMsgs, true);
                COUNTER_INIT_PRIVATE(StatusResMsgs, true);
                COUNTER_INIT_PRIVATE(DbStatResMsgs, true);
                COUNTER_INIT_PRIVATE(AnubisPutResMsgs, true);
                COUNTER_INIT_PRIVATE(OsirisPutResMsgs, true);

                COUNTER_INIT(PutTotalBytes, true);
                COUNTER_INIT(GetTotalBytes, true);
            }

            COUNTER_DEF(MovedPatchMsgs);
            COUNTER_DEF(PatchStartMsgs);
            COUNTER_DEF(PatchDiffMsgs);
            COUNTER_DEF(PatchXorDiffMsgs);
            COUNTER_DEF(PutMsgs);
            COUNTER_DEF(MultiPutMsgs);
            COUNTER_DEF(GetMsgs);
            COUNTER_DEF(BlockMsgs);
            COUNTER_DEF(GetBlockMsgs);
            COUNTER_DEF(BlockAndGetMsgs);
            COUNTER_DEF(GCMsgs);
            COUNTER_DEF(GetBarrierMsgs);
            COUNTER_DEF(SyncMsgs);
            COUNTER_DEF(SyncFullMsgs);
            COUNTER_DEF(RecoveredHugeBlobMsgs);
            COUNTER_DEF(StatusMsgs);
            COUNTER_DEF(DbStatMsgs);
            COUNTER_DEF(AnubisPutMsgs);
            COUNTER_DEF(OsirisPutMsgs);

            COUNTER_DEF(MovedPatchResMsgs);
            COUNTER_DEF(PatchFoundPartsMsgs);
            COUNTER_DEF(PatchXorDiffResMsgs);
            COUNTER_DEF(PatchResMsgs);
            COUNTER_DEF(PutResMsgs);
            COUNTER_DEF(MultiPutResMsgs);
            COUNTER_DEF(GetResMsgs);
            COUNTER_DEF(BlockResMsgs);
            COUNTER_DEF(GetBlockResMsgs);
            COUNTER_DEF(GCResMsgs);
            COUNTER_DEF(GetBarrierResMsgs);
            COUNTER_DEF(SyncResMsgs);
            COUNTER_DEF(SyncFullResMsgs);
            COUNTER_DEF(RecoveredHugeBlobResMsgs);
            COUNTER_DEF(StatusResMsgs);
            COUNTER_DEF(DbStatResMsgs);
            COUNTER_DEF(AnubisPutResMsgs);
            COUNTER_DEF(OsirisPutResMsgs);

            COUNTER_DEF(PutTotalBytes);
            COUNTER_DEF(GetTotalBytes);
        };

        ///////////////////////////////////////////////////////////////////////////////////
        // TDefragGroup
        ///////////////////////////////////////////////////////////////////////////////////
        class TDefragGroup : public TBase {
        public:
            GROUP_CONSTRUCTOR(TDefragGroup)
            {
                COUNTER_INIT(DefragBytesRewritten, true);
            }

            COUNTER_DEF(DefragBytesRewritten);
        };

    } // NMonGroup
} // NKikimr

