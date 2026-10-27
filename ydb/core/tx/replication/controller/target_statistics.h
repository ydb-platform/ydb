#pragma once

#include <util/datetime/base.h>

#include <algorithm>
#include <cmath>
#include <deque>
#include <optional>

namespace NKikimr::NReplication::NController {

// Controller-local observations. Neither byte deltas nor source scan samples
// survive controller restart. All clocks are supplied by the caller.
class TTargetStatistics {
public:
    void AddWriteBytes(TInstant now, ui64 bytes) {
        if (!WriteStart) {
            WriteStart = now;
        }
        ExpireWrites(now);
        if (bytes) {
            if (!Writes.empty() && Writes.back().Time == now) {
                Writes.back().Bytes += bytes;
            } else {
                Writes.push_back({now, bytes});
            }
            WriteBytes += bytes;
        }
    }

    std::optional<double> GetWriteSpeed(TInstant now) {
        if (!WriteStart || now <= *WriteStart) {
            return std::nullopt;
        }
        ExpireWrites(now);
        const auto elapsed = std::min(now - *WriteStart, WriteWindow);
        return double(WriteBytes) / elapsed.SecondsFloat();
    }

    std::optional<TDuration> ObserveScan(TInstant time, ui32 total, ui32 completed, bool complete) {
        if (LastScanTime && time <= *LastScanTime) {
            return std::nullopt;
        }
        LastScanTime = time;

        if (complete || (total && completed == total)) {
            Scans.clear();
            return TDuration::Zero();
        }
        if (!total || completed > total) {
            return std::nullopt;
        }

        if (!Scans.empty() && (total != ScanTotal || completed < Scans.back().Completed)) {
            Scans.clear();
        }
        ScanTotal = total;
        while (!Scans.empty() && time - Scans.front().Time > ScanWindow) {
            Scans.pop_front();
        }
        Scans.push_back({time, completed});
        const auto& first = Scans.front();
        const auto progress = completed - first.Completed;
        if (!progress || time <= first.Time) {
            return std::nullopt;
        }

        const long double micros = static_cast<long double>((time - first.Time).MicroSeconds())
            * (total - completed) / progress;
        // google.protobuf.Duration supports at most 10,000 years.
        static constexpr ui64 MaxDurationMicros = 315576000000ULL * 1000000;
        if (micros > MaxDurationMicros) {
            return std::nullopt;
        }
        return TDuration::MicroSeconds(static_cast<ui64>(std::ceil(micros)));
    }

    void ResetScan() {
        LastScanTime.reset();
        Scans.clear();
    }

private:
    static constexpr auto WriteWindow = TDuration::Seconds(60);
    static constexpr auto ScanWindow = TDuration::Minutes(5);

    struct TWriteSample {
        TInstant Time;
        ui64 Bytes;
    };
    struct TScanSample {
        TInstant Time;
        ui32 Completed;
    };

    void ExpireWrites(TInstant now) {
        while (!Writes.empty() && now >= Writes.front().Time + WriteWindow) {
            WriteBytes -= Writes.front().Bytes;
            Writes.pop_front();
        }
    }

    std::optional<TInstant> WriteStart;
    std::deque<TWriteSample> Writes;
    ui64 WriteBytes = 0;
    std::optional<TInstant> LastScanTime;
    ui32 ScanTotal = 0;
    std::deque<TScanSample> Scans;
};

// Parallel scans finish when the slowest target finishes. Missing measurements
// must propagate rather than silently treating queued targets as complete.
class TReplicationStatistics {
public:
    explicit TReplicationStatistics(bool discoveryComplete)
        : ScanEta(discoveryComplete ? std::make_optional(TDuration::Zero()) : std::nullopt)
    {}

    void Add(std::optional<double> speed, std::optional<TDuration> eta) {
        if (WriteSpeed && speed) {
            *WriteSpeed += *speed;
        } else {
            WriteSpeed.reset();
        }
        if (ScanEta && eta) {
            ScanEta = std::max(*ScanEta, *eta);
        } else {
            ScanEta.reset();
        }
    }

    std::optional<double> WriteSpeed = 0;
    std::optional<TDuration> ScanEta;
};

}
