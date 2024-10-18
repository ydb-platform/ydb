#include "dynamic_counters_page.h"

namespace NActors {

class TFilteredDynamicCountersPage::TFilterCountableConsumer: public NMonitoring::ICountableConsumer {
public:
    TFilterCountableConsumer(const TMon::TConfig& config, THolder<ICountableConsumer>&& slave)
        : Config(config)
        , Slave(std::move(slave))
    {}

    void OnCounter(const TString& labelName, const TString& labelValue, const NMonitoring::TCounterForPtr* counter) override {
        if (!Config.FilterMetrics || counter->Val()) {
            Slave->OnCounter(labelName, labelValue, counter);
        }
    }

    void OnHistogram(const TString& labelName, const TString& labelValue, NMonitoring::IHistogramSnapshotPtr snapshot, bool derivative) override {
        if (Config.FilterMetrics) {
            NMonitoring::TBucketBounds bounds;
            NMonitoring::TBucketValues values;
            const auto c = snapshot->Count();
            bounds.reserve(c);
            values.reserve(c);
            constexpr NMonitoring::TBucketBound NO_BOUND = Max<NMonitoring::TBucketBound>();
            auto lastBound = NO_BOUND;
            for (ui32 i = 0; i < c; ++i) {
                const auto v = snapshot->Value(i);
                if (v > 0) {
                    if (lastBound != NO_BOUND) {
                        bounds.emplace_back(lastBound);
                        values.emplace_back(0);
                        lastBound = NO_BOUND;
                    }
                    bounds.emplace_back(snapshot->UpperBound(i));
                    values.emplace_back(v);
                } else {
                    lastBound = snapshot->UpperBound(i);
                }
            }
            if (bounds.empty()) {
                return;
            }
            snapshot = NMonitoring::ExplicitHistogramSnapshot(bounds, values);
        }
        Slave->OnHistogram(labelName, labelValue, snapshot, derivative);
    }

    void OnGroupBegin(const TString& labelName, const TString& labelValue, const NMonitoring::TDynamicCounters* group) override {
        Slave->OnGroupBegin(labelName, labelValue, group);
    }

    void OnGroupEnd(const TString& labelName, const TString& labelValue, const NMonitoring::TDynamicCounters* group) override {
        Slave->OnGroupEnd(labelName, labelValue, group);
    }

    NMonitoring::TCountableBase::EVisibility Visibility() const override {
        return Slave->Visibility();
    }

private:
    const TMon::TConfig& Config;
    THolder<ICountableConsumer> Slave;
};

THolder<NMonitoring::ICountableConsumer> TFilteredDynamicCountersPage::CreateEncoder(IOutputStream* out, NMonitoring::EFormat format, TStringBuf nameLabel, NMonitoring::TCountableBase::EVisibility visibility) const {
    return MakeHolder<TFilterCountableConsumer>(Config, TDynamicCountersPage::CreateEncoder(out, format, nameLabel, visibility));
}

}
