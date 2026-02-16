#pragma once

#include <ydb/library/actors/testlib/test_runtime.h>
#include <util/generic/string.h>

#include <algorithm>
#include <cstring>

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

class TMvpTestRuntime : public NActors::TTestActorRuntimeBase {
    using NActors::TTestActorRuntimeBase::TTestActorRuntimeBase;

    void InitNodeImpl(TNodeDataBase*, size_t) override;
};
