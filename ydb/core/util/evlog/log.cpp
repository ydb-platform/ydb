#include "log.h"

#include <util/string/builder.h>
#include <util/system/guard.h>

namespace NKikimr::NEvLog {

TString TEvent::DebugString(const TMonotonic start) const {
    return TStringBuilder() << "{" << Text << ":" << Instant - start << "}";
}

void TLogsThread::AddEvent(const TString& text) {
    const i64 pos = Position.Inc();
    if (pos < Size.Val()) {
        Events[pos] = TEvent(text);
    } else {
        {
            TGuard<TMutex> g(Mutex);
            if ((i64)Events.size() <= pos) {
                Events.resize(std::max<ui32>(Events.size() * 2, Events.size() + 8));
            }
            Size = Events.size();
        }
        Events[pos] = TEvent(text);
    }
}

TString TLogsThread::DebugString() const {
    TStringBuilder sb;
    sb << "[";
    if (Header) {
        sb << "{" << Header << "}:";
    }
    bool first = true;
    for (auto&& i : Events) {
        if (!i) {
            break;
        }
        if (!first) {
            sb << ",";
        }
        sb << i->DebugString(Events.front()->GetInstant());
        first = false;
    }
    sb << "]";
    return sb;
}

TLogsThread::TEvWriter::TEvWriter(TLogsThread& owner, const TString& evName /*= Default<TString>()*/): Owner(owner) {
    if (evName) {
        sb << evName << ";";
    }
}

TLogsThread::TEvWriter& TLogsThread::TEvWriter::operator()(const TString& key, const TString& value) {
    sb << key << "=" << value << ";";
    return *this;
}

}