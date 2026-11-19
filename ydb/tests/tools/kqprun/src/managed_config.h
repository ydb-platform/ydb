#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>

namespace NKqpRun {

void InitializeManagedConfig(NActors::TTestActorRuntime& runtime, const NKikimrConfig::TAppConfig& startup, TDuration timeout);

}   // namespace NKqpRun
