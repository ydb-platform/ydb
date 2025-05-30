#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/monotonic.h>

#include <util/generic/noncopyable.h>
#include <util/generic/refcount.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <optional>
#include <deque>

namespace NKikimr::NEvLog {

class TEvent {
private:
    YDB_READONLY_DEF(TString, Text);
    YDB_READONLY(TMonotonic, Instant, TMonotonic::Now());

public:
    TEvent(const TString& text)
        : Text(text) {

    }

    TString DebugString(const TMonotonic start) const;
};

class TLogsThread {
private:
    const TString Header;
    TMutex Mutex;
    std::deque<std::optional<TEvent>> Events;
    TAtomicCounter Position = -1;
    TAtomicCounter Size = 0;
public:

    class TEvWriter: TMoveOnly {
    private:
        TStringBuilder sb;
        TLogsThread& Owner;
    public:
        TEvWriter(TLogsThread& owner, const TString& evName = Default<TString>());

        TEvWriter& operator()(const TString& key, const TString& value);
        ~TEvWriter() {
            Owner.AddEvent(sb);
        }
    };

    TLogsThread() = default;
    TLogsThread(const TString& header)
        : Header(header) {

    }

    TEvWriter AddEventStream(const TString& evName) {
        return TEvWriter(*this, evName);
    }

    void AddEvent(const TString& evName);

    TString DebugString() const;
};
}

#define FOR_PRIORITY_LOG(component, priority, action) if (!(IS_LOG_PRIORITY_ENABLED(priority, component))) ;else {action;};

#define FOR_TRACE_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_TRACE, action);
#define FOR_DEBUG_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_DEBUG, action);
#define FOR_INFO_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_INFO, action);
#define FOR_NOTICE_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_NOTICE, action);
#define FOR_WARN_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_WARN, action);
#define FOR_ERROR_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_ERROR, action);
#define FOR_CRIT_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_CRIT, action);
#define FOR_ALERT_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_ALERT, action);
#define FOR_EMERG_LOG(component, action) FOR_PRIORITY_LOG(component, NActors::NLog::PRI_EMERG, action);
