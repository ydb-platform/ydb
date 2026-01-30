#pragma once

#include <ydb/library/actors/testlib/test_runtime.h>

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data);

class TMvpTestRuntime : public NActors::TTestActorRuntimeBase {
    using NActors::TTestActorRuntimeBase::TTestActorRuntimeBase;

    void InitNodeImpl(TNodeDataBase*, size_t) override;
};
