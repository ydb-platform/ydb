#pragma once

#include <ydb/public/sdk/cpp/src/client/topic/common/trace_lazy.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>
#include <library/cpp/threading/future/core/future.h>

#include <span>


namespace NYdb::inline Dev::NTopic::NTests {

class TTraceException : public std::exception {
public:
    TTraceException(const std::string& message)
        : Message(message)
    {}

    const char* what() const noexcept override {
        return Message.c_str();
    }

private:
    std::string Message;
};

std::string_view SkipSpaces(std::string_view& b);
std::string_view SkipSpaces(std::string_view&& b);

std::string_view NextToken(std::string_view& b, char delim = ' ');
std::string_view NextToken(std::string_view&& b, char delim = ' ');

struct TTraceEvent {
    std::string Event;
    std::unordered_map<std::string, std::string> KeyValues;

    // Expects "event [key[=value]]*" kind of string. No spaces around the = sign. Values are optional.
    static TTraceEvent FromString(const std::string& s);
};

struct TExpectedTraceEvent {
    std::string Event;
    std::unordered_map<std::string, std::string> KeyValues;  // These key-value pairs should be in the event as is.
    std::vector<std::string> DeniedKeys;  // These keys should NOT appear in the event.

    bool Matches(TTraceEvent const& event) const;

    // Expects "event [[!]key[=value]]*" kind of string. No spaces around the = sign. Values are optional.
    // The bang symbol right before a key denies the key in events.
    static TExpectedTraceEvent FromString(const std::string& s);
};

struct TExpectedTrace {
    std::vector<TExpectedTraceEvent> Expected;

    TExpectedTrace(std::initializer_list<TExpectedTraceEvent> expected) : Expected(expected) {}
    TExpectedTrace(std::initializer_list<std::string> expected);

    // Check if the Expected events are a subsequence of the events.
    bool Matches(std::span<TTraceEvent const> events) const;
};

// The log formatter is configured in TDriverConfig::SetDatabase using the GetPrefixLogFormatter function.
// SkipPrefixLog skips the prefix that currently looks like "2024-02-13T07:51:07.979754Z :INFO: [/Root]".
// It'd be better if TTracingBackend received strings before formatter does, but I don't know how to do it.
std::string_view SkipPrefixLog(std::string_view& b);

// If possible, transforms the buffer to the form of "event [key[=value]]*".
bool CleanTraceEventBuf(std::string_view& b, std::string_view traceEventMarker);

// Log backend for tracing events. Expects "... <traceEventMarker> <event-string>" kind of strings.
// The "<event-string>" substring is parsed by the Parser function that returns a TTraceEvent object.
// To get the trace, call GetEvents method.
class TTracingBackend : public TLogBackend {
public:
    TTracingBackend(const std::string& traceEventMarker = TRACE_EVENT_MARKER, std::function<TTraceEvent(const std::string&)> parser = TTraceEvent::FromString)
        : TraceEventMarker(traceEventMarker)
        , EventParser(parser) {}

    // Only logs strings on TRACE log level that start with the TraceEventMarker (after we strip the log prefix).
    void WriteData(const TLogRecord& rec) override;
    void ReopenLog() override {}
    ELogPriority FiltrationLevel() const override { return TLOG_RESOURCES; }
public:
    std::vector<std::string> GetLog() const;
    std::vector<TTraceEvent> GetEvents() const;

    // Returns a feature that is fulfilled when an event with a particular name gets logged.
    NThreading::TFuture<void> WaitForEvent(const std::string& eventName);
private:
    mutable TAdaptiveLock Lock;
    std::string TraceEventMarker;  // Log strings that start with this marker are parsed and stored.
    std::function<TTraceEvent(const std::string&)> EventParser;
    std::vector<std::string> Log;  // Stores clean strings from log records that contained TraceEventMarker.
    std::vector<TTraceEvent> Events;  // Stores parsed trace events.
    NThreading::TPromise<void> EventPromise;
    TExpectedTraceEvent ExpectedEvent;
};

class TCompositeLogBackend : public TLogBackend {
public:
    TCompositeLogBackend(std::vector<std::unique_ptr<TLogBackend>>&& underlyingBackends)
        : UnderlyingBackends(std::move(underlyingBackends))
    {
    }

    void WriteData(const TLogRecord& rec) override {
        for (auto& b: UnderlyingBackends) {
            b->WriteData(rec);
        }
    }

    void ReopenLog() override {
    }

private:
    std::vector<std::unique_ptr<TLogBackend>> UnderlyingBackends;
};

}
