#pragma once

#include <library/cpp/monlib/encode/encoder.h>

#include <util/datetime/base.h>

#include <utility>

namespace NMonitoring::NMemOnlyShim {

// Forwards the metric events and adds memOnly to the metrics TMemOnlyLookupConsumer reports.
// Below TConsumer: memOnly is an IMetricConsumer event (OnMemOnly), ICountableConsumer has none.
class TMemOnlyEncoder final : public IMetricEncoder {
public:
    explicit TMemOnlyEncoder(IMetricEncoderPtr next)
        : Next(std::move(next))
    {
    }

    // For the next metric: OnMetricEnd resets it.
    void SetMemOnly(bool memOnly) {
        MemOnly = memOnly;
    }

    void OnStreamBegin() override {
        Next->OnStreamBegin();
    }

    void OnStreamEnd() override {
        Next->OnStreamEnd();
    }

    void OnCommonTime(TInstant time) override {
        Next->OnCommonTime(time);
    }

    void OnMetricBegin(EMetricType type) override {
        Next->OnMetricBegin(type);
    }

    // Keeps a counter's memOnly set through the upstream API. Written once in OnMetricEnd: the JSON encoder writes the key per call.
    void OnMemOnly(bool memOnly) override {
        MemOnly |= memOnly;
    }

    void OnMetricEnd() override {
        if (std::exchange(MemOnly, false)) {
            Next->OnMemOnly(true);
        }
        Next->OnMetricEnd();
    }

    void OnLabelsBegin() override {
        Next->OnLabelsBegin();
    }

    void OnLabelsEnd() override {
        Next->OnLabelsEnd();
    }

    void OnLabel(TStringBuf name, TStringBuf value) override {
        Next->OnLabel(name, value);
    }

    void OnDouble(TInstant time, double value) override {
        Next->OnDouble(time, value);
    }

    void OnInt64(TInstant time, i64 value) override {
        Next->OnInt64(time, value);
    }

    void OnUint64(TInstant time, ui64 value) override {
        Next->OnUint64(time, value);
    }

    void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override {
        Next->OnHistogram(time, std::move(snapshot));
    }

    void OnLogHistogram(TInstant time, TLogHistogramSnapshotPtr snapshot) override {
        Next->OnLogHistogram(time, std::move(snapshot));
    }

    void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) override {
        Next->OnSummaryDouble(time, std::move(snapshot));
    }

    void Close() override {
        Next->Close();
    }

private:
    const IMetricEncoderPtr Next;
    bool MemOnly = false;
};

} // namespace NMonitoring::NMemOnlyShim
