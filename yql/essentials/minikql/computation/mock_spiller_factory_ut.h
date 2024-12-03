#pragma once

#include <yql/essentials/minikql/computation/mkql_spiller_factory.h>
#include <yql/essentials/minikql/computation/mock_spiller_ut.h>

namespace NKikimr::NMiniKQL {

class TMockSpillerFactory : public ISpillerFactory
{
public:
    void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& /*spillingTaskCounters*/) override {
    }

    ISpiller::TPtr CreateSpiller() override {
        auto new_spiller = CreateMockSpiller();
        Spillers_.push_back(new_spiller);
        return new_spiller;
    }

    const std::vector<ISpiller::TPtr>& GetCreatedSpillers() const {
        return Spillers_;
    }

private:
    std::vector<ISpiller::TPtr> Spillers_;
};

} // namespace NKikimr::NMiniKQL
