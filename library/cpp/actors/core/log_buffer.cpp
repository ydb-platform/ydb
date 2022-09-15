#include "log_buffer.h"

#include <util/system/yassert.h>
#include <algorithm>

using namespace NActors::NLog;

namespace NActors {
TLogBuffer::TLogBuffer(ILoggerMetrics &metrics, const NLog::TSettings &settings)
    : Metrics(metrics)
    , Settings(settings)
{}

size_t TLogBuffer::GetLogCostInBytes(NLog::TEvLog *log) const {
    return LOG_STRUCTURE_BYTES + log->Line.length();
}

ui16 TLogBuffer::GetPrioIndex(NLog::EPrio prio) {
    return Min(ui16(prio), ui16(LOG_PRIORITIES_NUMBER - 1));
}

TIntrusiveList<NLog::TEvLog, NLog::TEvLogBufferLevelListTag> &TLogBuffer::GetPrioLogs(NLog::EPrio prio) {
    return PrioLogsList[GetPrioIndex(prio)];
}

void TLogBuffer::AddLog(NLog::TEvLog *log) {
    NLog::EPrio prio = log->Level.ToPrio();
    if (!CheckSize(log) && prio > NLog::EPrio::Emerg) { // always keep logs with prio Emerg = 0
        HandleIgnoredLog(log);
        return;
    }

    SizeBytes += GetLogCostInBytes(log);
    Logs.PushBack(log);
    GetPrioLogs(prio).PushBack(log);
}

NLog::TEvLog* TLogBuffer::Pop() {
    NLog::TEvLog* log = Logs.PopFront();
    static_cast<TIntrusiveListItem<TEvLog, TEvLogBufferLevelListTag>&>(*log).Unlink();

    SizeBytes -= GetLogCostInBytes(log);

    return log;
}

bool TLogBuffer::IsEmpty() const {
    return Logs.Empty();
}

bool TLogBuffer::CheckLogIgnoring() const {
    return IgnoredCount > 0;
}

bool TLogBuffer::CheckSize(NLog::TEvLog *log) {
    size_t startSizeBytes = SizeBytes;

    size_t logSize = GetLogCostInBytes(log);
    if (SizeBytes + logSize <= Settings.BufferSizeLimitBytes) {
        return true;
    }

    ui16 scanHighestPrio = Max((ui16)1, GetPrioIndex(log->Level.ToPrio())); // always keep logs with prio Emerg = 0
    for (ui16 scanPrio = LOG_PRIORITIES_NUMBER - 1; scanPrio >= scanHighestPrio; scanPrio--) {
        TIntrusiveList<NLog::TEvLog, NLog::TEvLogBufferLevelListTag> &scanLogs = PrioLogsList[scanPrio];
        while (!scanLogs.Empty()) {
            NLog::TEvLog* log = scanLogs.PopFront();
            SizeBytes -= GetLogCostInBytes(log);
            HandleIgnoredLog(log);
            
            if (SizeBytes + logSize <= Settings.BufferSizeLimitBytes) {
                return true;
            }
        }
    }

    if (startSizeBytes > SizeBytes) {
        return true;
    }

    return false;
}

void TLogBuffer::HandleIgnoredLog(NLog::TEvLog *log) {
    ui16 logPrio = GetPrioIndex(log->Level.ToPrio());
    Metrics.IncIgnoredMsgs();
    if (IgnoredHighestPrio > logPrio) {
        IgnoredHighestPrio = logPrio;
    }
    IgnoredCount++;
    delete log;
}

ui64 TLogBuffer::GetIgnoredCount() {
    return IgnoredCount;
}

NLog::EPrio TLogBuffer::GetIgnoredHighestPrio() {
    NLog::EPrio prio = static_cast<NLog::EPrio>(IgnoredHighestPrio);
    return prio;
}

void TLogBuffer::ClearIgnoredCount() {
    IgnoredHighestPrio = LOG_PRIORITIES_NUMBER - 1;
    IgnoredCount = 0;
}

}
