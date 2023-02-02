#pragma once
#include <util/datetime/base.h>
#include <util/system/types.h>
#include <optional>
#include <map>

namespace NKikimr::NKqp::NComputeActor {

class TChunkStatistics {
private:
    TInstant StartInstant;
    TInstant FinishInstant;
public:
    TChunkStatistics& SetStartInstant(const TInstant value);
    TChunkStatistics& SetFinishInstant(const TInstant value);
    TInstant GetStartInstant() const {
        return StartInstant;
    }
    TInstant GetFinishInstant() const {
        return FinishInstant;
    }
    TDuration GetDuration() const {
        return FinishInstant - StartInstant;
    }
};

class TShardStatistics {
private:
    std::map<ui32, TChunkStatistics> Statistics;
    std::optional<ui32> MaxPackSize;
    std::optional<ui32> MinPackSize;
    ui32 PacksCount = 0;
    ui32 TotalRowsCount = 0;
    ui32 TotalBytesCount = 0;
public:
    void AddPack(const ui32 rowsCount, const ui64 bytes);
    TChunkStatistics& MutableStatistics(const ui32 scannerIdx) {
        return Statistics[scannerIdx];
    }
    ui32 GetTotalRowsCount() const {
        return TotalRowsCount;
    }
    TString StatisticsToString() const;
};

class TScanShardsStatistics {
private:
    TInstant PredDiff = Now();
    std::map<ui32, TDuration> DurationByScansCount;
    std::map<ui32, TDuration> DurationByShardsCount;
    std::map<ui64, TShardStatistics> Statistics;
protected:
    void OnScansDiff(const ui32 shardsCount, const ui32 scansCount) {
        DurationByShardsCount[shardsCount] += Now() - PredDiff;
        DurationByScansCount[scansCount] += Now() - PredDiff;
        PredDiff = Now();
    }
public:
    TShardStatistics& MutableStatistics(const ui64 shardId) {
        return Statistics[shardId];
    }
    ui32 GetTotalRowsCount() const;
    TString StatisticsToString() const;
    TString GetDurationStats() const;

};
}
