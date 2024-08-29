#pragma once

#include <ydb/library/yql/minikql/computation/mkql_spiller_factory.h>
#include <ydb/library/yql/minikql/computation/mock_spiller_ut.h>

namespace NKikimr::NMiniKQL {

using namespace NActors;

class TMockSpillerFactory : public ISpillerFactory
{
public:
    void SetTaskCounters(TIntrusivePtr<NYql::NDq::TSpillingTaskCounters> /*spillingTaskCounters*/) override {
    }

    ISpiller::TPtr CreateSpiller() override {
        return CreateMockSpiller();
    }
};

} // namespace NKikimr::NMiniKQL