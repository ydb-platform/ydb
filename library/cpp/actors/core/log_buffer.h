#pragma once

#include "log_metrics.h"
#include "log_iface.h"

#include <deque>

namespace NActors {

struct TLogBufferMessage {
    TString Formatted;
    TInstant Time;
    NLog::EComponent Component;
    NLog::EPrio Priority; 

    TLogBufferMessage(NLog::TEvLog::TPtr& ev);
};

class TLogBuffer {
    ILoggerMetrics *Metrics;
    std::deque<TLogBufferMessage> Buffer;
    ui64 BufferSize = 0;
    std::map<NLog::EPrio, ui32> PrioStats;

    void FilterByLogPriority(NLog::EPrio prio);
    bool inline CheckMessagesNumberEnoughForClearing(ui32 number);

    public:
    TLogBuffer(ILoggerMetrics *metrics);
    bool TryAddMessage(TLogBufferMessage message);
    TLogBufferMessage GetMessage();
    bool IsEmpty() const;
    size_t GetLogsNumber() const;
    ui64 GetSizeBytes() const;
    bool TryReduceLogsNumber();
};
}
