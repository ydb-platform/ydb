#include "fake.h"

#include <util/datetime/base.h>

namespace NMonitoring {
    class TFakeEncoder: public IMetricEncoder {
    public:
        void OnStreamBegin() override {
        }
        void OnStreamEnd() override {
        }

        void OnCommonTime(TInstant) override {
        }

        void OnMetricBegin(EMetricType) override {
        }
        void OnMetricEnd() override {
        }

        void OnLabelsBegin() override {
        }
        void OnLabelsEnd() override {
        }
        void OnLabel(const TStringBuf, const TStringBuf) override {
        }

        void OnDouble(TInstant, double) override {
        }
        void OnInt64(TInstant, i64) override {
        }
        void OnUint64(TInstant, ui64) override {
        }

        void OnHistogram(TInstant, IHistogramSnapshotPtr) override {
        }

        void OnSummaryDouble(TInstant, ISummaryDoubleSnapshotPtr) override {
        }

        void OnLogHistogram(TInstant, TLogHistogramSnapshotPtr) override {
        }

        void Close() override {
        }
    };

    IMetricEncoderPtr EncoderFake() {
        return MakeHolder<TFakeEncoder>();
    }
}
