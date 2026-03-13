#pragma once

<<<<<<< HEAD
=======
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/services/services.pb.h>

>>>>>>> 9e64063a051 (YQ-5164 fix streaming query info not found after create (#35787))
#include <util/datetime/base.h>

namespace NTestUtils {

// Wait until predicate is true or timeout is reached 
void WaitFor(TDuration timeout, const TString& description, std::function<bool(TString& info)> predicate);
void WaitFor(TDuration timeout, const TString& description, std::function<bool()> predicate);

<<<<<<< HEAD
=======
void SetupSignalHandlers();

// You can enable logging for these services in test using test option:
// `--test-param TEST_LOG=<level>`
// or `--test-param TEST_LOG_<service>=<level>`
// For example:
// --test-param TEST_LOG=TRACE
// --test-param TEST_LOG_FLAT_TX_SCHEMESHARD=debug
struct TTestLogSettings {
    NActors::NLog::EPriority DefaultLogPriority = NActors::NLog::PRI_WARN;
    std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> LogPriorities;
    bool Freeze = false;

    TTestLogSettings& AddLogPriority(NKikimrServices::EServiceKikimr service, NActors::NLog::EPriority priority);
};
bool SetupLogLevelFromTestParam(NActors::TTestActorRuntimeBase& runtime, NKikimrServices::EServiceKikimr service, const TString& prefix = "TEST");
void SetupLogLevel(NActors::TTestActorRuntimeBase& runtime, const std::optional<TTestLogSettings>& logSettings = std::nullopt, const TString& paramPrefix = "TEST");

void RestartTablet(const NActors::TActorSystem& runtime, ui64 tabletId);

>>>>>>> 9e64063a051 (YQ-5164 fix streaming query info not found after create (#35787))
}  // namespace NTestUtils
