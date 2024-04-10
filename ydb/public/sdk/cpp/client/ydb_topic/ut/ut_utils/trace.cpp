#include <mutex>
#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/trace.h>

#include "library/cpp/testing/unittest/registar.h"


namespace NYdb::NTopic::NTests {

TStringBuf SkipSpaces(TStringBuf& b) {
    while (b.starts_with(' ')) b.Skip(1);
    return b;
}
TStringBuf SkipSpaces(TStringBuf&& b) {
    return SkipSpaces(b);
}

TStringBuf NextToken(TStringBuf& b, TStringBuf::TChar const delim) {
    SkipSpaces(b);
    return b.NextTok(delim);
}
TStringBuf NextToken(TStringBuf&& b, TStringBuf::TChar const delim) {
    return NextToken(b, delim);
}

TTraceEvent TTraceEvent::FromString(TString const& s) {
    TStringBuf b(s);
    TString const event(NextToken(b));
    UNIT_ASSERT_C(!event.empty(), "Wrong tracing log (format), event token not found");
    THashMap<TString, TString> keyValues;
    for (TStringBuf pair, key; ; ) {
        pair = NextToken(b);
        if (pair.empty()) break;
        key = pair.NextTok('=');
        keyValues.emplace(key, pair);
    }
    return {event, std::move(keyValues)};
}

bool TExpectedTraceEvent::Matches(TTraceEvent const& event) const {
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

TExpectedTraceEvent TExpectedTraceEvent::FromString(TString const& s) {
    TStringBuf b(s);
    TString const event(NextToken(b));
    UNIT_ASSERT_C(!event.empty(), "Wrong tracing log format, event token not found");
    THashMap<TString, TString> keyValues;
    TVector<TString> missingKeys;
    for (TStringBuf pair, key; ; ) {
        pair = NextToken(b);
        if (pair.empty()) break;
        key = pair.NextTok('=');
        if (key.starts_with('!')) {
            key.remove_prefix(1);
            missingKeys.emplace_back(key);
        } else {
            keyValues.emplace(key, pair);
        }
    }
    return {event, std::move(keyValues), std::move(missingKeys)};
}

TExpectedTrace::TExpectedTrace(std::initializer_list<TString> expected) {
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

TStringBuf SkipPrefixLog(TStringBuf& b) {
    NextToken(b); // Skip the timestamp
    NextToken(b); // Skip the log level
    NextToken(b); // Skip database path
    SkipSpaces(b);
    return b;
}

bool CleanTraceEventBuf(TStringBuf& b, TStringBuf traceEventMarker) {
    SkipPrefixLog(b);
    if (!b.starts_with(traceEventMarker)) {
        return false;
    }
    while (b.ends_with('\n')) b.remove_suffix(1);
    b.Skip(traceEventMarker.size());
    return true;
}

void TTracingBackend::WriteData(const TLogRecord& rec) {
    std::lock_guard lg(Lock);
    if (rec.Priority != TLOG_RESOURCES) {
        return;
    }
    if (TStringBuf b(rec.Data, rec.Len); CleanTraceEventBuf(b, TraceEventMarker)) {
        TString s(b);
        TTraceEvent e(EventParser(s));
        if (EventPromise.Initialized() && !EventPromise.HasValue() && ExpectedEvent.Matches(e)) {
            EventPromise.SetValue();
        }
        Log.emplace_back(std::move(s));
        Events.emplace_back(std::move(e));
    }
}

TVector<TString> TTracingBackend::GetLog() const { 
    std::lock_guard lg(Lock);
    return Log;
}

TVector<TTraceEvent> TTracingBackend::GetEvents() const { 
    std::lock_guard lg(Lock);
    return Events; 
}

NThreading::TFuture<void> TTracingBackend::WaitForEvent(TString const& eventName) {
    EventPromise = NThreading::NewPromise();
    ExpectedEvent = {eventName, {}, {}};
    return EventPromise.GetFuture();
}

}
