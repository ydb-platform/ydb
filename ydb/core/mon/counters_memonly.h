#pragma once

#include <library/cpp/monlib/dynamic_counters/page.h>

// Shim until monlib dynamic counters get the memOnly flag. Every name here mirrors the upstream one.
// Adoption: call GetCounter/GetHistogram on the group, drop the NMemOnlyShim:: qualifier,
// delete the counters_memonly* files. Adopt in the monlib sync commit: until then this page drops memOnly
// set through the upstream API on histograms.
namespace NMonitoring::NMemOnlyShim {

// group->GetCounter/GetHistogram, plus a process-wide memOnly mark on (group, value).
// Unlike upstream (first registration wins), marks an existing or re-created counter too: matters if registrations disagree.
// A scrape uses the marks as of its start, so a new mark applies from the next scrape on.
TDynamicCounters::TCounterPtr GetCounter(const TIntrusivePtr<TDynamicCounters>& group, const TString& value,
    bool derivative, TCountableBase::EVisibility visibility, bool memOnly);

THistogramPtr GetHistogram(const TIntrusivePtr<TDynamicCounters>& group, const TString& value,
    IHistogramCollectorPtr collector, bool derivative, TCountableBase::EVisibility visibility, bool memOnly);

// NMonitoring::CreateEncoder, plus the memOnly flag on marked metrics in JSON and SPACK.
THolder<ICountableConsumer> CreateEncoder(IOutputStream* out, EFormat format, TStringBuf nameLabel = "sensor",
    TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

class TDynamicCountersPage : public NMonitoring::TDynamicCountersPage {
public:
    using NMonitoring::TDynamicCountersPage::TDynamicCountersPage;

protected:
    THolder<ICountableConsumer> CreateEncoder(IOutputStream* out, EFormat format, TStringBuf nameLabel,
        TCountableBase::EVisibility visibility) const override;
};

} // namespace NMonitoring::NMemOnlyShim
