#include "trace.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/string_utils/helpers/helpers.h>

#include <mutex>


namespace NYdb::inline Dev::NTopic::NTests {

std::string_view SkipSpaces(std::string_view& b) {
    while (b.starts_with(' ')) {
        b.remove_prefix(1);
    }
    return b;
}

std::string_view SkipSpaces(std::string_view&& b) {
    return SkipSpaces(b);
}

std::string_view NextToken(std::string_view& b, char delim) {
    SkipSpaces(b);
    return NYdb::NUtils::NextTok(b, delim);
}

std::string_view NextToken(std::string_view&& b, char delim) {
    return NextToken(b, delim);
}

TTraceEvent TTraceEvent::FromString(const std::string& s) {
    std::string_view b(s);
    std::string_view event(NextToken(b));
    if (event.empty()) {
        throw TTraceException("Wrong tracing log (format), event token not found");
    }
    std::unordered_map<std::string, std::string> keyValues;
    for (std::string_view pair, key; ; ) {
        pair = NextToken(b);
        if (pair.empty()) {
            break;
        }
        key = NYdb::NUtils::NextTok(pair, '=');
        keyValues.emplace(key, pair);
    }
    return {std::string(event), std::move(keyValues)};
}

bool TExpectedTraceEvent::Matches(const TTraceEvent& event) const {
    if (Event != event.Event) {
        return false;
    }
    for (auto const& k : DeniedKeys) {
        auto it = event.KeyValues.find(k);
        if (it != end(event.KeyValues)) {
            return false;
        }
    }
    for (auto const& [expectedKey, expectedValue] : KeyValues) {
        auto it = event.KeyValues.find(expectedKey);
        if (it == end(event.KeyValues) || !(expectedValue.empty() || expectedValue == it->second)) {
            return false;
        }
    }
    return true;
}

TExpectedTraceEvent TExpectedTraceEvent::FromString(const std::string& s) {
    std::string_view b(s);
    std::string_view event(NextToken(b));
    if (event.empty()) {
        throw TTraceException("Wrong tracing log format, event token not found");
    }
    std::unordered_map<std::string, std::string> keyValues;
    std::vector<std::string> missingKeys;
    for (std::string_view pair, key; ; ) {
        pair = NextToken(b);
        if (pair.empty()) {
            break;
        }
        key = NYdb::NUtils::NextTok(pair, '=');
        if (key.starts_with('!')) {
            key.remove_prefix(1);
            missingKeys.emplace_back(key);
        } else {
            keyValues.emplace(key, pair);
        }
    }
    return {std::string(event), std::move(keyValues), std::move(missingKeys)};
}

TExpectedTrace::TExpectedTrace(std::initializer_list<std::string> expected) {
    Expected.reserve(expected.size());
    for (auto const& e : expected) {
        Expected.emplace_back(TExpectedTraceEvent::FromString(e));
    }
}

bool TExpectedTrace::Matches(std::span<TTraceEvent const> events) const {
    std::span<TExpectedTraceEvent const> expected(Expected);
    while (!expected.empty() && expected.size() <= events.size()) {
        if (expected[0].Matches(events[0])) {
            expected = expected.subspan(1);
        }
        events = events.subspan(1);
    }
    return expected.empty();
}

std::string_view SkipPrefixLog(std::string_view& b) {
    NextToken(b); // Skip the timestamp
    NextToken(b); // Skip the log level
    NextToken(b); // Skip database path
    SkipSpaces(b);
    return b;
}

bool CleanTraceEventBuf(std::string_view& b, std::string_view traceEventMarker) {
    SkipPrefixLog(b);
    if (!b.starts_with(traceEventMarker)) {
        return false;
    }
    while (b.ends_with('\n')) {
        b.remove_suffix(1);
    }
    b.remove_prefix(traceEventMarker.size());
    return true;
}

void TTracingBackend::WriteData(const TLogRecord& rec) {
    std::lock_guard lg(Lock);
    if (rec.Priority != TLOG_RESOURCES) {
        return;
    }
    if (std::string_view b(rec.Data, rec.Len); CleanTraceEventBuf(b, TraceEventMarker)) {
        std::string s(b);
        TTraceEvent e(EventParser(s));
        if (EventPromise.Initialized() && !EventPromise.HasValue() && ExpectedEvent.Matches(e)) {
            EventPromise.SetValue();
        }
        Log.emplace_back(std::move(s));
        Events.emplace_back(std::move(e));
    }
}

std::vector<std::string> TTracingBackend::GetLog() const { 
    std::lock_guard lg(Lock);
    return Log;
}

std::vector<TTraceEvent> TTracingBackend::GetEvents() const { 
    std::lock_guard lg(Lock);
    return Events; 
}

NThreading::TFuture<void> TTracingBackend::WaitForEvent(const std::string& eventName) {
    EventPromise = NThreading::NewPromise();
    ExpectedEvent = {eventName, {}, {}};
    return EventPromise.GetFuture();
}

}
