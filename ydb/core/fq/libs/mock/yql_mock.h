#pragma once

#include <ydb/core/fq/libs/shared_resources/interface/shared_resources.h>
#include <ydb/core/testlib/actors/test_runtime.h>

namespace NFq {

NActors::IActor* CreateYqlMockActor(int grpcPort);
void InitTest(NActors::TTestActorRuntime* runtime, int httpPort, int grpcPort, const IYqSharedResources::TPtr& yqSharedResources);

}   // namespace NFq
