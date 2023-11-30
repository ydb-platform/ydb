#pragma once

#include "log_metrics.h"
#include "log_iface.h"
#include "log_settings.h"

#include <util/generic/intrlist.h>

namespace NActors {
class TLogBuffer {
    static const size_t LOG_STRUCTURE_BYTES = sizeof(NLog::TEvLog);
    static const ui16 LOG_PRIORITIES_NUMBER = 9;
    
    ILoggerMetrics &Metrics;
    const NLog::TSettings &Settings;

    TIntrusiveListWithAutoDelete<NLog::TEvLog, TDelete, NLog::TEvLogBufferMainListTag> Logs;
    TIntrusiveList<NLog::TEvLog, NLog::TEvLogBufferLevelListTag> PrioLogsList[LOG_PRIORITIES_NUMBER];

    ui64 SizeBytes = 0;    
    ui64 IgnoredCount = 0;
    ui16 IgnoredHighestPrio = LOG_PRIORITIES_NUMBER - 1;

    size_t GetLogCostInBytes(NLog::TEvLog *log) const;
    void HandleIgnoredLog(NLog::TEvLog *log);
    bool CheckSize(NLog::TEvLog *log);
    static inline ui16 GetPrioIndex(NLog::EPrio);
    inline TIntrusiveList<NLog::TEvLog, NLog::TEvLogBufferLevelListTag> &GetPrioLogs(NLog::EPrio);

    public:
    TLogBuffer(ILoggerMetrics &metrics, const NLog::TSettings &Settings);
    void AddLog(NLog::TEvLog *log);
    NLog::TEvLog *Pop();
    bool IsEmpty() const;
    bool CheckLogIgnoring() const;
    ui64 GetIgnoredCount();
    NLog::EPrio GetIgnoredHighestPrio();
    void ClearIgnoredCount();
};
}
