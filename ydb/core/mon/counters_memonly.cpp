#include "counters_memonly.h"
#include "counters_memonly_consumer.h"
#include "counters_memonly_encoder.h"

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>

#include <util/generic/is_in.h>
#include <util/generic/singleton.h>
#include <util/system/mutex.h>

#include <memory>

namespace NMonitoring::NMemOnlyShim {

namespace {

// Metrics registered with memOnly, kept as their group and their name.
class TMemOnlyMarks {
public:
    void Mark(TIntrusivePtr<TDynamicCounters> group, const TString& name) {
        TGuard<TMutex> guard(Lock);
        auto& marks = Groups[group.Get()];
        if (marks && IsIn(marks->Names, name)) {
            return;
        }
        // Copy on write: snapshots share the marks.
        auto updated = std::make_shared<TMemOnlyGroupMarks>(marks ? *marks : TMemOnlyGroupMarks{std::move(group), {}});
        updated->Names.push_back(name);
        marks = std::move(updated);
        Cached.reset();
    }

    // Rebuilt only after Mark adds a name.
    std::shared_ptr<const TMemOnlyMarksSnapshot> Snapshot() {
        TGuard<TMutex> guard(Lock);
        if (!Cached) {
            Cached = std::make_shared<const TMemOnlyMarksSnapshot>(Groups);
        }
        return Cached;
    }

private:
    TMutex Lock;
    TMemOnlyMarksSnapshot Groups;
    std::shared_ptr<const TMemOnlyMarksSnapshot> Cached;
};

} // namespace

TDynamicCounters::TCounterPtr GetCounter(const TIntrusivePtr<TDynamicCounters>& group, const TString& value,
        bool derivative, TCountableBase::EVisibility visibility, bool memOnly) {
    if (memOnly) {
        Singleton<TMemOnlyMarks>()->Mark(group, value);
    }
    return group->GetCounter(value, derivative, visibility);
}

THistogramPtr GetHistogram(const TIntrusivePtr<TDynamicCounters>& group, const TString& value,
        IHistogramCollectorPtr collector, bool derivative, TCountableBase::EVisibility visibility, bool memOnly) {
    if (memOnly) {
        Singleton<TMemOnlyMarks>()->Mark(group, value);
    }
    return group->GetHistogram(value, std::move(collector), derivative, visibility);
}

THolder<ICountableConsumer> CreateEncoder(IOutputStream* out, EFormat format, TStringBuf nameLabel,
        TCountableBase::EVisibility visibility) {
    if (format != EFormat::JSON && format != EFormat::SPACK) {
        return NMonitoring::CreateEncoder(out, format, nameLabel, visibility);
    }
    // The same encoders as NMonitoring::CreateEncoder builds for these formats.
    auto encoder = MakeHolder<TMemOnlyEncoder>(format == EFormat::JSON
        ? EncoderJson(out)
        : EncoderSpackV1(out, ETimePrecision::SECONDS, ECompression::ZSTD));
    TMemOnlyEncoder* const memOnlyEncoder = encoder.Get();
    // Marks as of scrape start: a metric first marked during the scrape gets memOnly from the next scrape on.
    return MakeHolder<TMemOnlyLookupConsumer>(AsCountableConsumer(std::move(encoder), visibility), memOnlyEncoder,
        Singleton<TMemOnlyMarks>()->Snapshot());
}

THolder<ICountableConsumer> TDynamicCountersPage::CreateEncoder(IOutputStream* out, EFormat format,
        TStringBuf nameLabel, TCountableBase::EVisibility visibility) const {
    return NMemOnlyShim::CreateEncoder(out, format, nameLabel, visibility);
}

} // namespace NMonitoring::NMemOnlyShim
