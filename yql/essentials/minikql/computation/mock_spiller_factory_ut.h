#pragma once

#include <yql/essentials/minikql/computation/mkql_spiller_factory.h>
#include <yql/essentials/minikql/computation/mock_spiller_ut.h>

namespace NKikimr::NMiniKQL {

class TMockSpillerFactory: public ISpillerFactory {
public:
    void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& /*spillingTaskCounters*/) override {
    }

    void SetMemoryReportingCallbacks(ISpiller::TMemoryReportCallback reportAlloc, ISpiller::TMemoryReportCallback reportFree) override {
        ReportAllocCallback_ = reportAlloc;
        ReportFreeCallback_ = reportFree;
    }

    ISpiller::TPtr CreateSpiller() override {
        auto new_spiller = CreateMockSpiller(ReportAllocCallback_, ReportFreeCallback_);
        Spillers_.push_back(new_spiller);
        return new_spiller;
    }

    const std::vector<ISpiller::TPtr>& GetCreatedSpillers() const {
        return Spillers_;
    }

private:
    std::vector<ISpiller::TPtr> Spillers_;
    ISpiller::TMemoryReportCallback ReportAllocCallback_;
    ISpiller::TMemoryReportCallback ReportFreeCallback_;
};

} // namespace NKikimr::NMiniKQL
