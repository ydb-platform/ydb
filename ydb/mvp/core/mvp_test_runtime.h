#pragma once

#include <ydb/library/actors/testlib/test_runtime.h>

class TMvpTestRuntime : public NActors::TTestActorRuntimeBase {
    using NActors::TTestActorRuntimeBase::TTestActorRuntimeBase;

    void InitNodeImpl(TNodeDataBase*, size_t) override;
};
