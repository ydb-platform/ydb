#include "test_utils.h"

#include <ydb/core/base/backtrace.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NTestUtils {

namespace {

std::terminate_handler DefaultTerminateHandler;

void TerminateHandler() {
    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

    Cerr << colors.Red() << "======= terminate() call stack ========" << colors.Default() << Endl;
    FormatBackTrace(&Cerr);
    if (const auto& backtrace = TBackTrace::FromCurrentException(); backtrace.size() > 0) {
        Cerr << colors.Red() << "======== exception call stack =========" << colors.Default() << Endl;
        backtrace.PrintTo(Cerr);
    }
    Cerr << colors.Red() << "=======================================" << colors.Default() << Endl;

    if (std::current_exception()) {
        Cerr << colors.Red() << "Uncaught exception: " << CurrentExceptionMessage() << colors.Default() << Endl;
    } else {
        Cerr << colors.Red() << "Terminate for unknown reason (no current exception)" << colors.Default() << Endl;
    }

    if (DefaultTerminateHandler) {
        DefaultTerminateHandler();
    } else {
        abort();
    }
}

TString SignalToString(int signal) {
#ifndef _unix_
    return TStringBuilder() << "signal " << signal;
#else
    return strsignal(signal);
#endif
}

void BackTraceSignalHandler(int signal) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

    Cerr << colors.Red() << "======= " << SignalToString(signal) << " call stack ========" << colors.Default() << Endl;
    FormatBackTrace(&Cerr);
    Cerr << colors.Red() << "===============================================" << colors.Default() << Endl;

    abort();
}

TMaybe<NActors::NLog::EPriority> ParseLogLevel(const TString& level) {
    static const THashMap<TString, NActors::NLog::EPriority> levels = {
        { "TRACE", NActors::NLog::PRI_TRACE },
        { "DEBUG", NActors::NLog::PRI_DEBUG },
        { "INFO", NActors::NLog::PRI_INFO },
        { "NOTICE", NActors::NLog::PRI_NOTICE },
        { "WARN", NActors::NLog::PRI_WARN },
        { "ERROR", NActors::NLog::PRI_ERROR },
        { "CRIT", NActors::NLog::PRI_CRIT },
        { "ALERT", NActors::NLog::PRI_ALERT },
        { "EMERG", NActors::NLog::PRI_EMERG },
    };

    TString l = level;
    l.to_upper();
    if (const auto levelIt = levels.find(l); levelIt != levels.end()) {
        return levelIt->second;
    }

    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);
    Cerr << colors.Red() << "Failed to parse log level [" << level << "]" << colors.Default() << Endl;
    return Nothing();
}

} // anonymous namespace

void WaitFor(TDuration timeout, const TString& description, std::function<bool(TString&)> predicate) {
    const TInstant start = TInstant::Now();
    TString errorString;
    while (TInstant::Now() - start <= timeout) {
        if (predicate(errorString)) {
            return;
        }

        Cerr << "Wait " << description << " " << TInstant::Now() - start << ": " << errorString << "\n";
        Sleep(TDuration::Seconds(1));
    }

    UNIT_FAIL("Waiting " << description << " timeout. Spent time " << TInstant::Now() - start << " exceeds limit " << timeout << ", last error: " << errorString);
}

void WaitFor(TDuration timeout, const TString& description, std::function<bool()> predicate) {
    WaitFor(timeout, description, [predicate](TString&) {
        return predicate();
    });
}

void SetupSignalHandlers() {
    NKikimr::EnableYDBBacktraceFormat();

    DefaultTerminateHandler = std::set_terminate(&TerminateHandler);
    for (auto sig : {SIGFPE, SIGILL, SIGSEGV}) {
        signal(sig, &BackTraceSignalHandler);
    }
}

TTestLogSettings& TTestLogSettings::AddLogPriority(NKikimrServices::EServiceKikimr service, NActors::NLog::EPriority priority) {
    if (!Freeze) {
        LogPriorities.emplace(service, priority);
    }

    return *this;
}

bool SetupLogLevelFromTestParam(NActors::TTestActorRuntimeBase& runtime, NKikimrServices::EServiceKikimr service, const TString& prefix) {
    if (const auto& paramForService = GetTestParam(TStringBuilder() << prefix << "_LOG_" << NKikimrServices::EServiceKikimr_Name(service))) {
        if (const auto level = ParseLogLevel(paramForService)) {
            runtime.SetLogPriority(service, *level);
            return true;
        }
    }

    if (const auto& commonParam = GetTestParam(TStringBuilder() << prefix << "_LOG")) {
        if (const auto level = ParseLogLevel(commonParam)) {
            runtime.SetLogPriority(service, *level);
            return true;
        }
    }

    return false;
}

void SetupLogLevel(NActors::TTestActorRuntimeBase& runtime, const std::optional<TTestLogSettings>& logSettings, const TString& paramPrefix) {
    auto descriptor = NKikimrServices::EServiceKikimr_descriptor();
    for (i32 i = 0; i < descriptor->value_count(); ++i) {
        const auto service = static_cast<NKikimrServices::EServiceKikimr>(descriptor->value(i)->number());
        if (SetupLogLevelFromTestParam(runtime, service, paramPrefix)) {
            continue;
        }

        if (logSettings) {
            if (const auto it = logSettings->LogPriorities.find(service); it != logSettings->LogPriorities.end()) {
                runtime.SetLogPriority(service, it->second);
            } else {
                runtime.SetLogPriority(service, logSettings->DefaultLogPriority);
            }
        }
    }
}

}  // namespace NTestUtils
