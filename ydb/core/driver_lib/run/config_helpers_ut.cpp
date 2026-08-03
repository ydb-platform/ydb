#include "config_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ActorSystemConfigHelpers) {

using namespace NKikimr;

Y_UNIT_TEST(HarmonizerNeedyCpuWindow) {
    NKikimrConfig::TActorSystemConfig systemConfig;
    NActors::TCpuManagerConfig cpuManager;

    auto* defaultExecutor = systemConfig.AddExecutor();
    defaultExecutor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    defaultExecutor->SetName("System");
    defaultExecutor->SetThreads(1);
    defaultExecutor->SetMaxThreads(2);
    NActorSystemConfigHelpers::AddExecutorPool(cpuManager, *defaultExecutor, systemConfig, 0, nullptr);

    auto* configuredExecutor = systemConfig.AddExecutor();
    configuredExecutor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    configuredExecutor->SetName("User");
    configuredExecutor->SetThreads(1);
    configuredExecutor->SetMaxThreads(2);
    configuredExecutor->SetHarmonizerNeedyCpuWindowSeconds(30);
    NActorSystemConfigHelpers::AddExecutorPool(cpuManager, *configuredExecutor, systemConfig, 1, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[0].HarmonizerNeedyCpuWindowSeconds, 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[1].HarmonizerNeedyCpuWindowSeconds, 30);
}

} // ActorSystemConfigHelpers
