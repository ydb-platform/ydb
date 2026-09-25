#pragma once

#include "structured_message.h"

#include <ydb/library/actors/core/log_iface.h>
#include <util/datetime/base.h>

#include <memory>

namespace NActors::NStructuredLog {

struct TLogMessage {
    TInstant Time;
    NLog::EPrio Priority {NLog::EPrio::Emerg};
    NLog::EComponent Component {0};
    ui32 NodeId {0};
    const char* FileName {nullptr};
    ui64 LineNumber {0};
    TString TextMessage;
    TStructuredMessage StructuredMessage;
};

class ILogSink {
public:
    virtual bool Write(const TLogMessage&) = 0;
    virtual void Flush() = 0;
    virtual ~ILogSink() = default;
};
using ILogSinkSPtr = std::shared_ptr<ILogSink>;
}


