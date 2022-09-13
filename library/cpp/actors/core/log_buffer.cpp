#include "log_buffer.h"

#include <util/system/yassert.h>
#include <algorithm>

using namespace NActors::NLog;

namespace NActors {
TLogBufferMessage::TLogBufferMessage(NLog::TEvLog::TPtr& ev)
    : Formatted(ev->Get()->Line)
    , Time(ev->Get()->Stamp)
    , Component(ev->Get()->Component)
    , Priority(ev->Get()->Level.ToPrio())
{}

TLogBuffer::TLogBuffer(ILoggerMetrics *metrics)
    : Metrics(metrics)
{}

bool TLogBuffer::TryAddMessage(TLogBufferMessage message) {
    Buffer.push_back(std::move(message));
    BufferSize += sizeof(message);
    PrioStats[message.Priority]++;

    return true;
}

TLogBufferMessage TLogBuffer::GetMessage() {
    auto message = Buffer.front();

    ui64 messageSize = sizeof(message);
    BufferSize -= messageSize;
    Buffer.pop_front();
    PrioStats[message.Priority]--;

    return message;
}

bool TLogBuffer::IsEmpty() const {
    return Buffer.empty();
}

size_t TLogBuffer::GetLogsNumber() const {
    return Buffer.size();
}

ui64 TLogBuffer::GetSizeBytes() const {
    return BufferSize;
}

void TLogBuffer::FilterByLogPriority(NLog::EPrio prio) {
    bool isFirstRemoving = true;
    auto it = Buffer.begin();
    while (it != Buffer.end())
    {
        if (it->Priority >= prio) {
            ui64 messageSize = sizeof(*it);
            BufferSize -= messageSize;
            Metrics->IncIgnoredMsgs();
            PrioStats[it->Priority]--;

            if (isFirstRemoving && prio > NLog::EPrio::Error) {
                it->Priority = NLog::EPrio::Error;
                it->Formatted = Sprintf("Ignored log records due to log buffer overflow! IgnoredCount# %" PRIu32 " ", PrioStats[prio]);

                PrioStats[NLog::EPrio::Error]++;
                BufferSize += sizeof(*it);
                it++;
                isFirstRemoving = false;    
            }
            else {
                it = Buffer.erase(it);
            }
        }
        else {
            it++;
        }
    }
}

bool inline TLogBuffer::CheckMessagesNumberEnoughForClearing(ui32 number) {
    return number * 10 > Buffer.size();
}

bool TLogBuffer::TryReduceLogsNumber() {
    ui32 removeLogsNumber = 0;
    for (ui16 p = ui16(NLog::EPrio::Trace); p > ui16(NLog::EPrio::Alert); p--)
    {
        NLog::EPrio prio = static_cast<NLog::EPrio>(p);
        removeLogsNumber += PrioStats[prio];
        if (CheckMessagesNumberEnoughForClearing(removeLogsNumber)) {
            FilterByLogPriority(prio);
            return true;
        }
    }
    return false;
}
}
