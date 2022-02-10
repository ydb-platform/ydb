#pragma once

#include "metric_type.h"
#include "histogram_collector.h"
#include "summary_collector.h"
#include "log_histogram_snapshot.h"

class TInstant;

namespace NMonitoring {
    class IMetricConsumer {
    public:
        virtual ~IMetricConsumer() = default;

        virtual void OnStreamBegin() = 0;
        virtual void OnStreamEnd() = 0;

        virtual void OnCommonTime(TInstant time) = 0;

        virtual void OnMetricBegin(EMetricType type) = 0;
        virtual void OnMetricEnd() = 0;

        virtual void OnLabelsBegin() = 0;
        virtual void OnLabelsEnd() = 0;
        virtual void OnLabel(TStringBuf name, TStringBuf value) = 0;
        virtual void OnLabel(ui32 name, ui32 value);
        virtual std::pair<ui32, ui32> PrepareLabel(TStringBuf name, TStringBuf value);

        virtual void OnDouble(TInstant time, double value) = 0;
        virtual void OnInt64(TInstant time, i64 value) = 0;
        virtual void OnUint64(TInstant time, ui64 value) = 0;

        virtual void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) = 0;
        virtual void OnLogHistogram(TInstant time, TLogHistogramSnapshotPtr snapshot) = 0;
        virtual void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) = 0;
    };

    using IMetricConsumerPtr = THolder<IMetricConsumer>;

}
