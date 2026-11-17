#pragma once

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NActors {

// Fixed test-node topology, without the production dynamic-node discovery stack.
class TTestTabletRuntime : public TTestActorRuntime {
public:
    using TTestActorRuntime::TTestActorRuntime;
    std::function<TNodeLocation(ui32)> LocationCallback;
    void Initialize(TEgg egg) override;
private:
    void AddICStuff();
};

}
