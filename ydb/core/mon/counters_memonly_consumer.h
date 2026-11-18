#pragma once

#include "counters_memonly_encoder.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/is_in.h>
#include <util/generic/vector.h>

#include <memory>

namespace NMonitoring::NMemOnlyShim {

// Metrics registered with memOnly in one group.
struct TMemOnlyGroupMarks {
    TIntrusivePtr<TDynamicCounters> Group; // keeps the address from being reused by another group
    TVector<TString> Names;
};

using TMemOnlyMarksSnapshot = THashMap<const TDynamicCounters*, std::shared_ptr<const TMemOnlyGroupMarks>>;

// Looks up whether each metric is memOnly and tells TMemOnlyEncoder.
// The lookup is here because only the tree walk passes the group,
// and the marks are keyed by group.
// Chain: TDynamicCounters::Accept -> TMemOnlyLookupConsumer
//        -> TConsumer (monlib) -> TMemOnlyEncoder -> JSON/SPACK encoder.
class TMemOnlyLookupConsumer final : public ICountableConsumer {
public:
    TMemOnlyLookupConsumer(THolder<ICountableConsumer> next, TMemOnlyEncoder* memOnlyEncoder,
            std::shared_ptr<const TMemOnlyMarksSnapshot> marks)
        : Next(std::move(next))
        , MemOnlyEncoder(memOnlyEncoder)
        , Marks(std::move(marks))
    {
    }

    void OnCounter(const TString& labelName, const TString& labelValue, const TCounterForPtr* counter) override {
        MemOnlyEncoder->SetMemOnly(IsMemOnly(labelName, labelValue));
        Next->OnCounter(labelName, labelValue, counter);
    }

    void OnHistogram(const TString& labelName, const TString& labelValue, IHistogramSnapshotPtr snapshot,
            bool derivative) override {
        MemOnlyEncoder->SetMemOnly(IsMemOnly(labelName, labelValue));
        Next->OnHistogram(labelName, labelValue, std::move(snapshot), derivative);
    }

    void OnGroupBegin(const TString& labelName, const TString& labelValue, const TDynamicCounters* group) override {
        const auto* const marks = Marks->FindPtr(group);
        OpenGroups.push_back(marks ? marks->get() : nullptr);
        Next->OnGroupBegin(labelName, labelValue, group);
    }

    void OnGroupEnd(const TString& labelName, const TString& labelValue, const TDynamicCounters* group) override {
        OpenGroups.pop_back();
        Next->OnGroupEnd(labelName, labelValue, group);
    }

    TCountableBase::EVisibility Visibility() const override {
        return Next->Visibility();
    }

private:
    // GetCounter/GetHistogram register under the sensor label.
    bool IsMemOnly(const TString& labelName, const TString& labelValue) const {
        const TMemOnlyGroupMarks* const marks = OpenGroups.empty() ? nullptr : OpenGroups.back();
        return marks && labelName == "sensor" && IsIn(marks->Names, labelValue);
    }

    const THolder<ICountableConsumer> Next;
    TMemOnlyEncoder* const MemOnlyEncoder; // owned by Next
    const std::shared_ptr<const TMemOnlyMarksSnapshot> Marks;
    // Marks of each open group, nullptr if none.
    TVector<const TMemOnlyGroupMarks*> OpenGroups;
};

} // namespace NMonitoring::NMemOnlyShim
