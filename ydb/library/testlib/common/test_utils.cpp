#include "test_utils.h"

<<<<<<< HEAD
=======
#include <ydb/core/base/backtrace.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/common/env.h>
>>>>>>> 9e64063a051 (YQ-5164 fix streaming query info not found after create (#35787))
#include <library/cpp/testing/unittest/registar.h>

namespace NTestUtils {

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

<<<<<<< HEAD
=======
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

void RestartTablet(const NActors::TActorSystem& runtime, ui64 tabletId) {
    runtime.Send(NKikimr::MakeTabletResolverID(), new NKikimr::TEvTabletResolver::TEvForward(
        tabletId,
        new NActors::IEventHandle(NActors::TActorId(), NActors::TActorId(), new NActors::TEvents::TEvPoisonPill()),
        {},
        NKikimr::TEvTabletResolver::TEvForward::EActor::Tablet
    ));
    runtime.Send(NKikimr::MakeTabletResolverID(), new NKikimr::TEvTabletResolver::TEvTabletProblem(tabletId, NActors::TActorId()));
}

>>>>>>> 9e64063a051 (YQ-5164 fix streaming query info not found after create (#35787))
}  // namespace NTestUtils
