#include "kqp_scan_compute_stat.h"
#include <util/string/builder.h>

namespace NKikimr::NKqp::NComputeActor {

TChunkStatistics& TChunkStatistics::SetStartInstant(const TInstant value) {
    StartInstant = value;
    return *this;
}

TChunkStatistics& TChunkStatistics::SetFinishInstant(const TInstant value) {
    FinishInstant = value;
    return *this;
}

void TShardStatistics::AddPack(const ui32 rowsCount, const ui64 bytes) {
    if (!MaxPackSize) {
        MaxPackSize = rowsCount;
    } else if (*MaxPackSize < rowsCount) {
        MaxPackSize = rowsCount;
    }
    if (!MinPackSize) {
        MinPackSize = rowsCount;
    } else if (*MinPackSize < rowsCount) {
        MinPackSize = rowsCount;
    }
    PacksCount += 1;
    TotalRowsCount += rowsCount;
    TotalBytesCount += bytes;
}

TString TShardStatistics::StatisticsToString() const {
    TStringBuilder sb;
    std::optional<TInstant> minInstant;
    std::optional<TInstant> maxInstant;
    TDuration dMin;
    TDuration dMax;
    TDuration dAvg;
    for (auto&& i : Statistics) {
        if (!dMin) {
            dMin = i.second.GetDuration();
        } else {
            dMin = Min(i.second.GetDuration(), dMin);
        }
        if (!dMax) {
            dMax = i.second.GetDuration();
        } else {
            dMax = Max(i.second.GetDuration(), dMax);
        }
        dAvg += i.second.GetDuration();
        if (!minInstant || !maxInstant) {
            minInstant = i.second.GetStartInstant();
            maxInstant = i.second.GetFinishInstant();
        } else {
            minInstant = Min(i.second.GetStartInstant(), *minInstant);
            maxInstant = Max(i.second.GetFinishInstant(), *maxInstant);
        }
    }
    sb << "CHUNKS=" << Statistics.size() << ";";
    if (minInstant && maxInstant) {
        sb << "D=" << *maxInstant - *minInstant << ";";
    }
    if (Statistics.size()) {
        sb << "PacksCount=" << PacksCount << ";";
        if (PacksCount) {
            sb << "RowsCount=" << TotalRowsCount << ";";
            sb << "BytesCount=" << TotalBytesCount << ";";
            sb << "MinPackSize=" << *MinPackSize << ";";
            sb << "MaxPackSize=" << *MaxPackSize << ";";
        }
        sb << "CAVG=" << dAvg / Statistics.size() << ";";
        sb << "CMIN=" << dMin << ";";
        sb << "CMAX=" << dMax << ";";
    }
    return sb;
}

TString TScanShardsStatistics::StatisticsToString() const {
    TStringBuilder sb;
    for (auto&& i : Statistics) {
        sb << "{SHARD(" << i.first << "):";
        sb << i.second.StatisticsToString() << "};";
    }
    return sb;
}

TString TScanShardsStatistics::GetDurationStats() const {
    TStringBuilder sb;
    double sumDuration = 0;
    for (auto&& i : DurationByShardsCount) {
        sumDuration += i.second.MicroSeconds();
    }
    if (sumDuration > 10) {
        sb << "InFlightScans:";
        double wScans = 0;
        for (auto&& i : DurationByScansCount) {
            wScans += i.first * 1.0 * i.second.MicroSeconds();
        }
        sb << "InFlightShards:";
        double wShards = 0;
        for (auto&& i : DurationByShardsCount) {
            wShards += i.first * 1.0 * i.second.MicroSeconds();
        }
        sb << ";wScans=" << wScans / sumDuration << ";wShards=" << wShards / sumDuration << ";";
    } else {
        sb << "so fast";
    }
    return sb;
}

ui32 TScanShardsStatistics::GetTotalRowsCount() const {
    ui32 result = 0;
    for (auto&& i : Statistics) {
        result += i.second.GetTotalRowsCount();
    }
    return result;
}

}