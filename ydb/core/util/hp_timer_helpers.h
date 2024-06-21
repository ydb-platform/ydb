#pragma once

#include <util/system/hp_timer.h>
#include <ydb/library/actors/util/datetime.h>

namespace NKikimr {

inline NHPTimer::STime HPNow() {
    NHPTimer::STime ret;
    GetTimeFast(&ret);
    return ret;
}

inline double HPSecondsFloat(i64 cycles) {
    if (cycles > 0) {
        return double(cycles) / NHPTimer::GetClockRate();
    } else {
        return 0.0;
    }
}

inline double HPMilliSecondsFloat(i64 cycles) {
    if (cycles > 0) {
        return double(cycles) * 1000.0 / NHPTimer::GetClockRate();
    } else {
        return 0;
    }
}

inline ui64 HPMilliSeconds(i64 cycles) {
    return (ui64)HPMilliSecondsFloat(cycles);
}

inline ui64 HPMicroSecondsFloat(i64 cycles) {
    if (cycles > 0) {
        return double(cycles) * 1000000.0 / NHPTimer::GetClockRate();
    } else {
        return 0;
    }
}

inline ui64 HPMicroSeconds(i64 cycles) {
    return (ui64)HPMicroSecondsFloat(cycles);
}

inline ui64 HPNanoSeconds(i64 cycles) {
    if (cycles > 0) {
        return ui64(double(cycles) * 1000000000.0 / NHPTimer::GetClockRate());
    } else {
        return 0;
    }
}

inline ui64 HPCyclesNs(ui64 ns) {
    return ui64(NHPTimer::GetClockRate() * double(ns) / 1000000000.0);
}

inline ui64 HPCyclesUs(ui64 us) {
    return ui64(NHPTimer::GetClockRate() * double(us) / 1000000.0);
}

inline ui64 HPCyclesMs(ui64 ms) {
    return ui64(NHPTimer::GetClockRate() * double(ms) / 1000.0);
}

} // namespace NKikimr
