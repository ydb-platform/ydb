#pragma once

#include "defs.h"
#include "prepare.h"


struct TFastVDiskSetup : public TDefaultVDiskSetup {
    TFastVDiskSetup() {
        auto modifier = [] (NKikimr::TVDiskConfig *cfg) {
            cfg->HullCompSchedulingInterval = TDuration::MilliSeconds(100);
            cfg->LevelCompaction = true;
            cfg->FreshCompaction = true;
            cfg->RecoveryLogCutterFirstDuration = TDuration::Seconds(1);
            cfg->RecoveryLogCutterRegularDuration = TDuration::Seconds(1);
            cfg->AdvanceEntryPointTimeout = TDuration::Seconds(1);
            cfg->HullCompStorageRatioCalcPeriod = TDuration::MilliSeconds(1);
            cfg->HullCompLevel0MaxSstsAtOnce = 4;
            cfg->SyncTimeInterval = TDuration::MilliSeconds(100);
            cfg->SyncJobTimeout = TDuration::Seconds(2);
            cfg->RunSyncer = true;
            cfg->ReplTimeInterval = TDuration::MilliSeconds(20);
        };
        AddConfigModifier(modifier);
    }
};

struct TFastVDiskSetupMinHugeBlob : public TFastVDiskSetup {
    TFastVDiskSetupMinHugeBlob(ui32 minHugeBlobInBytes) {
        auto modifier = [=] (NKikimr::TVDiskConfig *cfg) {
            cfg->MinHugeBlobInBytes = minHugeBlobInBytes;
        };
        AddConfigModifier(modifier);
    }
};

struct TFastVDiskSetupSmallVDiskQueues : public TFastVDiskSetup {
    TFastVDiskSetupSmallVDiskQueues() {
        auto modifier = [] (NKikimr::TVDiskConfig *cfg) {
            cfg->SkeletonFrontGets_MaxInFlightCount = 2;
            cfg->SkeletonFrontGets_MaxInFlightCost = 2000000;              // 2ms
            cfg->SkeletonFrontDiscover_MaxInFlightCount = 6;
            cfg->SkeletonFrontDiscover_MaxInFlightCost = 3000000;          // 3ms
            cfg->SkeletonFrontLogPuts_MaxInFlightCount = 4;
            cfg->SkeletonFrontLogPuts_MaxInFlightCost = 2000000;           // 2ms
            cfg->SkeletonFrontHugePuts_MaxInFlightCount = 1;
            cfg->SkeletonFrontHugePuts_MaxInFlightCost = 7000000;          // 7ms

            cfg->SkeletonFrontExtPutTabletLog_TotalCost = 3000000;         // 3ms
            cfg->SkeletonFrontExtPutAsyncBlob_TotalCost = 7000000;         // 7ms
            cfg->SkeletonFrontExtPutUserData_TotalCost = 3000000;          // 3ms
            cfg->SkeletonFrontExtGetAsync_TotalCost = 3000000;             // 3ms
            cfg->SkeletonFrontExtGetFast_TotalCost = 3000000;              // 3ms
            cfg->SkeletonFrontExtGetDiscover_TotalCost = 3000000;          // 3ms
            cfg->SkeletonFrontExtGetLow_TotalCost = 3000000;              // 3ms
        };
        AddConfigModifier(modifier);
    }
};

struct TFastVDiskSetupRepl : public TFastVDiskSetup {
    TFastVDiskSetupRepl() {
        auto modifier = [] (NKikimr::TVDiskConfig *cfg) {
            cfg->HullCompSchedulingInterval = TDuration::Seconds(1);
            cfg->AdvanceEntryPointTimeout = TDuration::Seconds(10);
            cfg->ReplTimeInterval = TDuration::Seconds(5);
            cfg->MaxLogoBlobDataSize = 2u << 20u;
            cfg->MinHugeBlobInBytes = 512u << 10u;
            cfg->MilestoneHugeBlobInBytes = 512u << 10u;
        };
        AddConfigModifier(modifier);
    }
};

struct TFastVDiskSetupHndOff : public TFastVDiskSetup {
    TFastVDiskSetupHndOff() {
        auto modifier = [] (NKikimr::TVDiskConfig *cfg) {
            cfg->HullCompLevelRateThreshold = 0.01; // to compact very few chunks from level 0
        };
        AddConfigModifier(modifier);
    }
};

struct TFastVDiskSetupCompacted : public TFastVDiskSetup {
    TFastVDiskSetupCompacted() {
        auto modifier = [] (NKikimr::TVDiskConfig *cfg) {
            cfg->HullCompLevelRateThreshold = 0.01; // to compact very few chunks from level 0
        };
        AddConfigModifier(modifier);
    }
};

struct TFastVDiskSetupCompactedHndOff : public TFastVDiskSetupCompacted {
    TFastVDiskSetupCompactedHndOff() = default;
};

struct TFastCompactionGCNoSyncVDiskSetup : public TFastVDiskSetup {
    TFastCompactionGCNoSyncVDiskSetup() {
        auto modifier = [] (NKikimr::TVDiskConfig *cfg) {
            cfg->HullCompLevelRateThreshold = 0.01; // to compact very few chunks from level 0
            cfg->GCOnlySynced = false;
            cfg->RunSyncer = false;
            cfg->RunRepl = false;
        };
        AddConfigModifier(modifier);
    }
};

struct TFastVDiskSetupWODisk2 : public TFastVDiskSetup {
    bool SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j, ui32 pDiskID,
            ui32 slotId, bool runRepl, ui64 initOwnerRound) {
        if (id == 2)
            return false;

        return TFastVDiskSetup::SetUp(vdisk, pdisks, id, d, j, pDiskID, slotId, runRepl, initOwnerRound);
    }
};

struct TFastVDiskSetupWODisk2Compacted : public TFastVDiskSetup {
    bool SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j, ui32 pDiskID,
               ui32 slotId, bool runRepl, ui64 initOwnerRound) {
        if (id == 2)
            return false;

        TFastVDiskSetup::SetUp(vdisk, pdisks, id, d, j, pDiskID, slotId, runRepl, initOwnerRound);
        TIntrusivePtr<NKikimr::TVDiskConfig> &vDiskConfig = vdisk.Cfg;
        vDiskConfig->HullCompLevelRateThreshold = 0.01; // to compact very few chunks from level 0
        return true;
    }
};

struct TFastVDiskSetupWODisk4 : public TFastVDiskSetup {
    bool SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j, ui32 pDiskID,
            ui32 slotId, bool runRepl, ui64 initOwnerRound) {
        if (id == 4)
            return false;

        return TFastVDiskSetup::SetUp(vdisk, pdisks, id, d, j, pDiskID, slotId, runRepl, initOwnerRound);
    }
};

struct TNoVDiskSetup : public TFastVDiskSetup {
    bool SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j, ui32 pDiskID,
            ui32 slotId, bool runRepl, ui64 initOwnerRound) {
        TFastVDiskSetup::SetUp(vdisk, pdisks, id, d, j, pDiskID, slotId, runRepl, initOwnerRound);
        return false;
    }
};

struct TDefragVDiskSetup : public TDefaultVDiskSetup {
    TDefragVDiskSetup() {
        auto modifier = [] (NKikimr::TVDiskConfig *cfg) {
            cfg->HugeBlobsFreeChunkReservation = 0;
        };
        AddConfigModifier(modifier);
    }
};
