#pragma once

#include <yql/essentials/minikql/computation/mkql_spiller_factory.h>
#include <yql/essentials/minikql/computation/mock_spiller_ut.h>

namespace NKikimr::NMiniKQL {

class TMockSpillerFactory : public ISpillerFactory
{
public:
    void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& /*spillingTaskCounters*/) override {
    }

    void SetMemoryReportingCallbacks(std::function<bool(ui64)> reportAlloc, std::function<void(ui64)> reportFree) override {
        ReportAllocCallback_ = std::move(reportAlloc);
        ReportFreeCallback_ = std::move(reportFree);
    }

    ISpiller::TPtr CreateSpiller() override {
        auto new_spiller = CreateMockSpiller();
        Spillers_.push_back(new_spiller);
        return new_spiller;
    }

    const std::vector<ISpiller::TPtr>& GetCreatedSpillers() const {
        return Spillers_;
    }

    const std::function<bool(ui64)>& GetReportAllocCallback() const {
        return ReportAllocCallback_;
    }

    const std::function<void(ui64)>& GetReportFreeCallback() const {
        return ReportFreeCallback_;
    }

private:
    std::vector<ISpiller::TPtr> Spillers_;
    std::function<bool(ui64)> ReportAllocCallback_;
    std::function<void(ui64)> ReportFreeCallback_;
};

} // namespace NKikimr::NMiniKQL
