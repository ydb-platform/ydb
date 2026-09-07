#include "critical_events.h"

#include "public.h"

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/builder.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;

namespace {

NMonitoring::TDynamicCountersPtr CriticalEvents;
TLog Log;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void SetCriticalEventsLog(TLog log)
{
    Log = std::move(log);
}

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
    CriticalEvents = std::move(counters);

#define STORAGE_INIT_CRITICAL_EVENT_COUNTER(name)                              \
    *CriticalEvents->GetCounter(GetCriticalEventFor##name(), true) = 0;        \
    // STORAGE_INIT_CRITICAL_EVENT_COUNTER

    STORAGE_CRITICAL_EVENTS(STORAGE_INIT_CRITICAL_EVENT_COUNTER)
#undef STORAGE_INIT_CRITICAL_EVENT_COUNTER

#define STORAGE_INIT_IMPOSSIBLE_EVENT_COUNTER(name)                            \
    *CriticalEvents->GetCounter(GetImpossibleEventFor##name(), true) = 0;      \
    // STORAGE_INIT_IMPOSSIBLE_EVENT_COUNTER

    STORAGE_IMPOSSIBLE_EVENTS(STORAGE_INIT_IMPOSSIBLE_EVENT_COUNTER)
#undef STORAGE_INIT_IMPOSSIBLE_EVENT_COUNTER
}

TString GetCriticalEventFullName(const TString& name)
{
    return "AppCriticalEvents/" + name;
}

TString GetImpossibleEventFullName(const TString& name)
{
    return "AppImpossibleEvents/" + name;
}

TString ReportCriticalEvent(
    const TString& sensorName,
    const TString& message,
    bool verifyDebug)
{
    if (verifyDebug) {
        Y_DEBUG_ABORT_UNLESS(
            false,
            "sensorName: \"%s\"; message: \"%s\"",
            sensorName.c_str(),
            message.c_str());
    }

    ReportCriticalEventWithoutLogging(sensorName);

    return LogCriticalEvent(sensorName, message);
}

void ReportCriticalEventWithoutLogging(const TString& sensorName)
{
    if (CriticalEvents) {
        auto counter = CriticalEvents->GetCounter(sensorName, true);
        counter->Inc();
    }
}

TString LogCriticalEvent(const TString& sensorName, const TString& message)
{
    TStringBuilder fullMessage;
    fullMessage << "CRITICAL_EVENT:" << sensorName;
    if (message) {
        fullMessage << ": " << message;
    }

    if (Log.IsNotNullLog()) {
        Log.AddLog("%s", fullMessage.c_str());
    } else {
        // Write message and \n in one call. This will reduce the chance of
        // shuffling with writings of other threads.
        Cerr << fullMessage + '\n';
        Cerr.Flush();
    }

    return fullMessage;
}

#define STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE(name)                            \
    TString Report##name(const TString& message)                               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            message,                                                           \
            false); /* verifyDebug */                                          \
    }                                                                          \
                                                                               \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "AppCriticalEvents/" #name;                                     \
    }                                                                          \
    // STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE

STORAGE_CRITICAL_EVENTS(STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE)
#undef STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE

#define STORAGE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE(name)                          \
    TString Report##name(const TString& message)                               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetImpossibleEventFor##name(),                                     \
            message,                                                           \
            true); /* verifyDebug */                                           \
    }                                                                          \
                                                                               \
    const TString GetImpossibleEventFor##name()                                \
    {                                                                          \
        return "AppImpossibleEvents/" #name;                                   \
    }                                                                          \
    // STORAGE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE

STORAGE_IMPOSSIBLE_EVENTS(STORAGE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE)
#undef STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE

////////////////////////////////////////////////////////////////////////////////

void ReportPreconditionFailed(
    TStringBuf file,
    int line,
    TStringBuf func,
    TStringBuf expr,
    TStringBuf message)
{
    ReportCriticalEvent(
        "PreconditionFailed",
        TStringBuilder() << file << ":" << line << " " << func
                         << "(): requirement " << expr << " failed. "
                         << message,
        true   // verifyDebug
    );
}

}   // namespace NYdb::NBS
