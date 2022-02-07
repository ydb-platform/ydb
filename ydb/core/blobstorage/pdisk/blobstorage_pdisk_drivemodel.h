#pragma once
#include "defs.h"
#include <ydb/core/protos/drivemodel.pb.h>
#include <cmath>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Drive Model
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TDriveModel : public TThrRefBase {
public:
    enum EOperationType {
        OP_TYPE_READ = 0,
        OP_TYPE_WRITE = 1,
        OP_TYPE_AVG = 2,
        OP_TYPE_COUNT = 3,
    };

protected:
    // Model data
    ui64 SeekTimeNsec;
    ui64 BulkReadBlockSizeBytes;
    ui64 BulkWriteBlockSizeBytes;
    ui64 TrimSpeedBps;
    ui32 TotalChunksCount;
    ui64 SpeedBps[OP_TYPE_COUNT];
    ui64 SpeedBpsMin[OP_TYPE_COUNT];
    ui64 SpeedBpsMax[OP_TYPE_COUNT];
    TVector<ui64> ChunkSpeedBps[OP_TYPE_COUNT];
    ui32 OptimalQueueDepth[OP_TYPE_COUNT];
    ui32 GlueingDeadline[OP_TYPE_COUNT];
    // Model metadata
    NKikimrBlobStorage::TDriveModel::EModelSource ModelSource;
    TString SourceModelNumber;
    TString SourceFirmwareRevision;
    TString SourceSerialNumber;
    bool IsSourceWriteCacheEnabled;
    bool IsSourceSharedWithOs;

    friend class TDriveEstimator;
public:
    TDriveModel() {
        Clear();
    }

    TDriveModel(ui64 seekTimeNs, ui64 speedBps, ui64 bulkWriteBlockSize, ui64 trimSpeedBps,
                ui64 speedBpsMin, ui64 speedBpsMax, ui32 queueDepth) {
        Clear();
        SeekTimeNsec = seekTimeNs;
        BulkReadBlockSizeBytes = bulkWriteBlockSize;
        BulkWriteBlockSizeBytes = bulkWriteBlockSize;
        TrimSpeedBps = trimSpeedBps;
        for (ui32 type = OP_TYPE_READ; type <= OP_TYPE_WRITE; ++type) {
            SpeedBps[type] = speedBps;
            SpeedBpsMin[type] = speedBpsMin > 100ull ? speedBpsMin : 100ull;
            SpeedBpsMax[type] = speedBpsMax < 1000000000000ull ? speedBpsMax : 1000000000000ull;
            OptimalQueueDepth[type] = queueDepth;
        }

        CalculateAvgs();
    }

    void CalculateAvgs() {
        SpeedBps[OP_TYPE_AVG] = (SpeedBps[OP_TYPE_READ] + SpeedBps[OP_TYPE_WRITE]) / 2;
        SpeedBpsMin[OP_TYPE_AVG] = (SpeedBpsMin[OP_TYPE_READ] + SpeedBpsMin[OP_TYPE_WRITE]) / 2;
        SpeedBpsMax[OP_TYPE_AVG] = (SpeedBpsMax[OP_TYPE_READ] + SpeedBpsMax[OP_TYPE_WRITE]) / 2;
        OptimalQueueDepth[OP_TYPE_AVG] = (OptimalQueueDepth[OP_TYPE_READ] + OptimalQueueDepth[OP_TYPE_WRITE]) / 2;
        GlueingDeadline[OP_TYPE_AVG] = (GlueingDeadline[OP_TYPE_READ] + GlueingDeadline[OP_TYPE_WRITE]) / 2;
    }

    void Clear() {
        SeekTimeNsec = 0;
        BulkReadBlockSizeBytes = 0;
        BulkWriteBlockSizeBytes = 0;
        TrimSpeedBps = 0;
        TotalChunksCount = 0;
        for (ui32 type = OP_TYPE_READ; type <= OP_TYPE_AVG; ++type) {
            SpeedBps[type] = 0;
            SpeedBpsMin[type] = 0;
            SpeedBpsMax[type] = 0;
            ChunkSpeedBps[type].clear();
            OptimalQueueDepth[type] = 0;
            GlueingDeadline[type] = 0;
        }
        ModelSource = NKikimrBlobStorage::TDriveModel::SourceClear;
        SourceModelNumber = "";
        SourceFirmwareRevision = "";
        SourceSerialNumber = "";
        IsSourceWriteCacheEnabled = false;
        IsSourceSharedWithOs = false;
    }

    void Apply(const NKikimrBlobStorage::TDriveModel *cfg) {
        if (!cfg) {
            return;
        }
        SeekTimeNsec = cfg->GetSeekTimeNsec();
        BulkReadBlockSizeBytes = cfg->GetBulkReadBlockSizeBytes();
        BulkWriteBlockSizeBytes = cfg->GetBulkWriteBlockSizeBytes();
        TrimSpeedBps = cfg->GetTrimSpeedBps();
        TotalChunksCount = cfg->GetTotalChunksCount();
        SpeedBps[OP_TYPE_READ] = cfg->GetSpeedBpsRead();
        SpeedBps[OP_TYPE_WRITE] = cfg->GetSpeedBpsWrite();
        SpeedBpsMin[OP_TYPE_READ] = cfg->GetSpeedBpsMinRead();
        SpeedBpsMin[OP_TYPE_WRITE] = cfg->GetSpeedBpsMinWrite();
        SpeedBpsMax[OP_TYPE_READ] = cfg->GetSpeedBpsMaxRead();
        SpeedBpsMax[OP_TYPE_WRITE] = cfg->GetSpeedBpsMaxWrite();
        OptimalQueueDepth[OP_TYPE_READ] = cfg->GetOptimalQueueDepthRead();
        OptimalQueueDepth[OP_TYPE_WRITE] = cfg->GetOptimalQueueDepthWrite();
        GlueingDeadline[OP_TYPE_READ] = cfg->GetGlueingDeadlineRead();
        GlueingDeadline[OP_TYPE_WRITE] = cfg->GetGlueingDeadlineWrite();

        // Model metadata
        ModelSource = cfg->GetModelSource();
        SourceModelNumber = cfg->GetSourceModelNumber();
        SourceFirmwareRevision = cfg->GetSourceFirmwareRevision();
        SourceSerialNumber = cfg->GetSourceSerialNumber();
        IsSourceWriteCacheEnabled = cfg->GetIsSourceWriteCacheEnabled();
        IsSourceSharedWithOs = cfg->GetIsSourceSharedWithOs();
    }

    void ToProto(NKikimrBlobStorage::TDriveModel *outCfg) {
        if (!outCfg) {
            return;
        }
        // Model data
        outCfg->SetSeekTimeNsec(SeekTimeNsec);
        outCfg->SetBulkReadBlockSizeBytes(BulkReadBlockSizeBytes);
        outCfg->SetBulkWriteBlockSizeBytes(BulkWriteBlockSizeBytes);
        outCfg->SetTrimSpeedBps(TrimSpeedBps);
        outCfg->SetTotalChunksCount(TotalChunksCount);
        outCfg->SetSpeedBpsRead(SpeedBps[OP_TYPE_READ]);
        outCfg->SetSpeedBpsWrite(SpeedBps[OP_TYPE_WRITE]);
        outCfg->SetSpeedBpsMinRead(SpeedBpsMin[OP_TYPE_READ]);
        outCfg->SetSpeedBpsMinWrite(SpeedBpsMin[OP_TYPE_WRITE]);
        outCfg->SetSpeedBpsMaxRead(SpeedBpsMax[OP_TYPE_READ]);
        outCfg->SetSpeedBpsMaxWrite(SpeedBpsMax[OP_TYPE_WRITE]);
        outCfg->SetOptimalQueueDepthRead(OptimalQueueDepth[OP_TYPE_READ]);
        outCfg->SetOptimalQueueDepthWrite(OptimalQueueDepth[OP_TYPE_WRITE]);
        outCfg->SetGlueingDeadlineRead(GlueingDeadline[OP_TYPE_READ]);
        outCfg->SetGlueingDeadlineWrite(GlueingDeadline[OP_TYPE_WRITE]);

        // Model metadata
        outCfg->SetModelSource(ModelSource);
        outCfg->SetSourceModelNumber(SourceModelNumber);
        outCfg->SetSourceFirmwareRevision(SourceFirmwareRevision);
        outCfg->SetSourceSerialNumber(SourceSerialNumber);
        outCfg->SetIsSourceWriteCacheEnabled(IsSourceWriteCacheEnabled);
        outCfg->SetIsSourceSharedWithOs(IsSourceSharedWithOs);
    }

    void SetTotalChunksCount(ui32 totalChunksCount) {
        TotalChunksCount = totalChunksCount;
        for (ui32 type = OP_TYPE_READ; type <= OP_TYPE_AVG; ++type) {
            ChunkSpeedBps[type].resize(totalChunksCount);
            double SpeedPerChunkSqr;
            for (ui32 i = 0; i < totalChunksCount; ++i) {
                SpeedPerChunkSqr = ((double)SpeedBpsMax[type] * SpeedBpsMax[type] -
                        (double)SpeedBpsMin[type] * SpeedBpsMin[type]) / totalChunksCount;
                ChunkSpeedBps[type][i] = std::sqrt((double)SpeedBpsMax[type] * SpeedBpsMax[type] - i * SpeedPerChunkSqr);
            }
        }
    }

    ui64 SeekTimeNs() const {
        return SeekTimeNsec;
    }

    ui64 Speed(EOperationType type) const {
        return SpeedBps[type];
    }

    ui64 Speed(ui32 chunkIdx, EOperationType type) const {
        if (chunkIdx < TotalChunksCount) {
            return ChunkSpeedBps[type][chunkIdx];
        } else {
            return SpeedBps[type];
        }
    }

    ui64 TimeForSizeNs(ui64 bytesToProcess, EOperationType type) const {
        return (ui64)bytesToProcess * 1000000000ull / SpeedBps[type];
    }

    ui64 TimeForSizeNs(ui64 bytesToProcess, ui32 chunkIdx, EOperationType type) const {
        return (ui64)bytesToProcess * 1000000000ull / Speed(chunkIdx, type);
    }

    ui64 TrimTimeForSizeNs(ui64 bytesToTrim) const {
        return (ui64)bytesToTrim * 1000000000ull / TrimSpeedBps;
    }

    bool IsTrimSupported() const {
        return TrimSpeedBps > 0;
    }

    ui64 SizeForTimeNs(ui64 durationNs, ui32 chunkIdx, EOperationType type) const {
        return Speed(chunkIdx, type) * durationNs / 1000000000ull;
    }

    ui64 BulkWriteBlockSize() const {
        return BulkWriteBlockSizeBytes;
    }

    ui32 IOPS() const {
        return 1000000000ULL / SeekTimeNsec;
    }

    TString ToString() const {
        return ToString(false);
    }

    TString ToString(bool isMultiline) const {
        TStringStream str;
        const char *x = isMultiline ? "\n" : "";
        str << "{TDriveModel" << x;
        str << " SeekTimeNsec# " << SeekTimeNsec << x;
        str << " TrimSpeedBps# " << TrimSpeedBps << x;
        str << " BulkWriteBlockSizeBytes# " << BulkWriteBlockSizeBytes << x;
        str << " SpeedBps[OP_TYPE_READ]# " << SpeedBps[OP_TYPE_READ] << x;
        str << " SpeedBps[OP_TYPE_WRITE]# " << SpeedBps[OP_TYPE_WRITE] << x;
        str << " SpeedBpsMin[OP_TYPE_READ]# " << SpeedBpsMin[OP_TYPE_READ] << x;
        str << " SpeedBpsMin[OP_TYPE_WRITE]# " << SpeedBpsMin[OP_TYPE_WRITE] << x;
        str << " SpeedBpsMax[OP_TYPE_READ]# " << SpeedBpsMax[OP_TYPE_READ] << x;
        str << " SpeedBpsMax[OP_TYPE_WRITE]# " << SpeedBpsMax[OP_TYPE_WRITE] << x;
        str << " OptimalQueueDepth[OP_TYPE_READ]# " << OptimalQueueDepth[OP_TYPE_READ] << x;
        str << " OptimalQueueDepth[OP_TYPE_WRITE]# " << OptimalQueueDepth[OP_TYPE_WRITE] << x;
        str << " GlueingDeadline[OP_TYPE_READ]# " << GlueingDeadline[OP_TYPE_READ] << x;
        str << " GlueingDeadline[OP_TYPE_WRITE]# " << GlueingDeadline[OP_TYPE_WRITE] << x;
        str << " ModelSource# " << (ui64)ModelSource << x;
        str << " SourceModelNumber# \"" << SourceModelNumber << "\"" << x;
        str << " SourceFirmwareRevision# \"" << SourceFirmwareRevision << "\"" << x;
        str << " SourceSerialNumber# \"" << SourceSerialNumber << "\"" << x;
        str << " IsSourceWriteCacheEnabled# " << IsSourceWriteCacheEnabled << x;
        str << " IsSourceSharedWithOs# " << IsSourceSharedWithOs << x;

        str << "}";
        return str.Str();
    }
};

}
}
