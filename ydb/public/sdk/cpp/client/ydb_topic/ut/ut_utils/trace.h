#pragma once

#include "ydb/public/sdk/cpp/client/ydb_topic/common/trace_lazy.h"

#include "library/cpp/logger/backend.h"
#include "library/cpp/logger/record.h"
#include "library/cpp/threading/future/core/future.h"
#include "util/generic/hash.h"

#include <span>


namespace NYdb::NTopic::NTests {

TStringBuf SkipSpaces(TStringBuf& b);
TStringBuf SkipSpaces(TStringBuf&& b);

TStringBuf NextToken(TStringBuf& b, TStringBuf::TChar const delim = ' ');
TStringBuf NextToken(TStringBuf&& b, TStringBuf::TChar const delim = ' ');

struct TTraceEvent {
    TString Event;
    THashMap<TString, TString> KeyValues;

    // Expects "event [key[=value]]*" kind of string. No spaces around the = sign. Values are optional.
    static TTraceEvent FromString(TString const& s);
};

struct TExpectedTraceEvent {
    TString Event;
    THashMap<TString, TString> KeyValues;  // These key-value pairs should be in the event as is.
    TVector<TString> DeniedKeys;  // These keys should NOT appear in the event.

    bool Matches(TTraceEvent const& event) const;

    // Expects "event [[!]key[=value]]*" kind of string. No spaces around the = sign. Values are optional.
    // The bang symbol right before a key denies the key in events.
    static TExpectedTraceEvent FromString(TString const& s);
};

struct TExpectedTrace {
    TVector<TExpectedTraceEvent> Expected;

    TExpectedTrace(std::initializer_list<TExpectedTraceEvent> expected) : Expected(expected) {}
    TExpectedTrace(std::initializer_list<TString> expected);

    // Check if the Expected events are a subsequence of the events.
    bool Matches(std::span<TTraceEvent const> events) const;
};

// The log formatter is configured in TDriverConfig::SetDatabase using the GetPrefixLogFormatter function.
// SkipPrefixLog skips the prefix that currently looks like "2024-02-13T07:51:07.979754Z :INFO: [/Root]".
// It'd be better if TTracingBackend received strings before formatter does, but I don't know how to do it.
TStringBuf SkipPrefixLog(TStringBuf& b);

// If possible, transforms the buffer to the form of "event [key[=value]]*".
bool CleanTraceEventBuf(TStringBuf& b, TStringBuf traceEventMarker);

// Log backend for tracing events. Expects "... <traceEventMarker> <event-string>" kind of strings.
// The "<event-string>" substring is parsed by the Parser function that returns a TTraceEvent object.
// To get the trace, call GetEvents method.
class TTracingBackend : public TLogBackend {
public:
    TTracingBackend(const TString& traceEventMarker = TRACE_EVENT_MARKER, std::function<TTraceEvent(TString const&)> parser = TTraceEvent::FromString)
        : TraceEventMarker(traceEventMarker)
        , EventParser(parser) {}

    // Only logs strings on TRACE log level that start with the TraceEventMarker (after we strip the log prefix).
    void WriteData(const TLogRecord& rec) override;
    void ReopenLog() override {}
    ELogPriority FiltrationLevel() const override { return TLOG_RESOURCES; }
public:
    TVector<TString> GetLog() const;
    TVector<TTraceEvent> GetEvents() const;

    // Returns a feature that is fulfilled when an event with a particular name gets logged.
    NThreading::TFuture<void> WaitForEvent(TString const& eventName);
private:
    mutable TAdaptiveLock Lock;
    TString TraceEventMarker;  // Log strings that start with this marker are parsed and stored.
    std::function<TTraceEvent(TString const&)> EventParser;
    TVector<TString> Log;  // Stores clean strings from log records that contained TraceEventMarker.
    TVector<TTraceEvent> Events;  // Stores parsed trace events.
    NThreading::TPromise<void> EventPromise;
    TExpectedTraceEvent ExpectedEvent;
};

}
