#include "protobuf.h"

#include <util/datetime/base.h>

namespace NMonitoring {
    namespace {
        NProto::EMetricType ConvertMetricType(EMetricType type) {
            switch (type) {
                case EMetricType::GAUGE:
                    return NProto::GAUGE;
                case EMetricType::COUNTER:
                    return NProto::COUNTER;
                case EMetricType::RATE:
                    return NProto::RATE;
                case EMetricType::IGAUGE:
                    return NProto::IGAUGE;
                case EMetricType::HIST:
                    return NProto::HISTOGRAM;
                case EMetricType::HIST_RATE:
                    return NProto::HIST_RATE;
                case EMetricType::DSUMMARY:
                    return NProto::DSUMMARY;
                case EMetricType::LOGHIST:
                    return NProto::LOGHISTOGRAM;
                case EMetricType::UNKNOWN:
                    return NProto::UNKNOWN;
            }
        }

        void FillHistogram(
                const IHistogramSnapshot& snapshot,
                NProto::THistogram* histogram)
        {
            for (ui32 i = 0; i < snapshot.Count(); i++) {
                histogram->AddBounds(snapshot.UpperBound(i));
                histogram->AddValues(snapshot.Value(i));
            }
        }

        void FillSummaryDouble(const ISummaryDoubleSnapshot& snapshot, NProto::TSummaryDouble* summary) {
            summary->SetSum(snapshot.GetSum());
            summary->SetMin(snapshot.GetMin());
            summary->SetMax(snapshot.GetMax());
            summary->SetLast(snapshot.GetLast());
            summary->SetCount(snapshot.GetCount());
        }

        void FillLogHistogram(const TLogHistogramSnapshot& snapshot, NProto::TLogHistogram* logHist) {
            logHist->SetBase(snapshot.Base());
            logHist->SetZerosCount(snapshot.ZerosCount());
            logHist->SetStartPower(snapshot.StartPower());
            for (ui32 i = 0; i < snapshot.Count(); ++i) {
                logHist->AddBuckets(snapshot.Bucket(i));
            }
        }

        ///////////////////////////////////////////////////////////////////////////////
        // TSingleamplesEncoder
        ///////////////////////////////////////////////////////////////////////////////
        class TSingleSamplesEncoder final: public IMetricEncoder {
        public:
            TSingleSamplesEncoder(NProto::TSingleSamplesList* samples)
                : Samples_(samples)
                , Sample_(nullptr)
            {
            }

        private:
            void OnStreamBegin() override {
            }
            void OnStreamEnd() override {
            }

            void OnCommonTime(TInstant time) override {
                Samples_->SetCommonTime(time.MilliSeconds());
            }

            void OnMetricBegin(EMetricType type) override {
                Sample_ = Samples_->AddSamples();
                Sample_->SetMetricType(ConvertMetricType(type));
            }

            void OnMetricEnd() override {
                Sample_ = nullptr;
            }

            void OnLabelsBegin() override {
            }
            void OnLabelsEnd() override {
            }

            void OnLabel(TStringBuf name, TStringBuf value) override {
                NProto::TLabel* label = (Sample_ == nullptr)
                                            ? Samples_->AddCommonLabels()
                                            : Sample_->AddLabels();
                label->SetName(TString{name});
                label->SetValue(TString{value});
            }

            void OnDouble(TInstant time, double value) override {
                Y_ENSURE(Sample_, "metric not started");
                Sample_->SetTime(time.MilliSeconds());
                Sample_->SetFloat64(value);
            }

            void OnInt64(TInstant time, i64 value) override {
                Y_ENSURE(Sample_, "metric not started");
                Sample_->SetTime(time.MilliSeconds());
                Sample_->SetInt64(value);
            }

            void OnUint64(TInstant time, ui64 value) override {
                Y_ENSURE(Sample_, "metric not started");
                Sample_->SetTime(time.MilliSeconds());
                Sample_->SetUint64(value);
            }

            void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override {
                Y_ENSURE(Sample_, "metric not started");
                Sample_->SetTime(time.MilliSeconds());
                FillHistogram(*snapshot, Sample_->MutableHistogram());
            }

            void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) override {
                Y_ENSURE(Sample_, "metric not started");
                Sample_->SetTime(time.MilliSeconds());
                FillSummaryDouble(*snapshot, Sample_->MutableSummaryDouble());
            }

            void OnLogHistogram(TInstant time, TLogHistogramSnapshotPtr snapshot) override {
                Y_ENSURE(Sample_, "metric not started");
                Sample_->SetTime(time.MilliSeconds());
                FillLogHistogram(*snapshot, Sample_->MutableLogHistogram());
            }

            void Close() override {
            }

        private:
            NProto::TSingleSamplesList* Samples_;
            NProto::TSingleSample* Sample_;
        };

        ///////////////////////////////////////////////////////////////////////////////
        // TMultiSamplesEncoder
        ///////////////////////////////////////////////////////////////////////////////
        class TMultiSamplesEncoder final: public IMetricEncoder {
        public:
            TMultiSamplesEncoder(NProto::TMultiSamplesList* samples)
                : Samples_(samples)
                , Sample_(nullptr)
            {
            }

        private:
            void OnStreamBegin() override {
            }
            void OnStreamEnd() override {
            }

            void OnCommonTime(TInstant time) override {
                Samples_->SetCommonTime(time.MilliSeconds());
            }

            void OnMetricBegin(EMetricType type) override {
                Sample_ = Samples_->AddSamples();
                Sample_->SetMetricType(ConvertMetricType(type));
            }

            void OnMetricEnd() override {
                Sample_ = nullptr;
            }

            void OnLabelsBegin() override {
            }
            void OnLabelsEnd() override {
            }

            void OnLabel(TStringBuf name, TStringBuf value) override {
                NProto::TLabel* label = (Sample_ == nullptr)
                                            ? Samples_->AddCommonLabels()
                                            : Sample_->AddLabels();

                label->SetName(TString{name});
                label->SetValue(TString{value});
            }

            void OnDouble(TInstant time, double value) override {
                Y_ENSURE(Sample_, "metric not started");
                NProto::TPoint* point = Sample_->AddPoints();
                point->SetTime(time.MilliSeconds());
                point->SetFloat64(value);
            }

            void OnInt64(TInstant time, i64 value) override {
                Y_ENSURE(Sample_, "metric not started");
                NProto::TPoint* point = Sample_->AddPoints();
                point->SetTime(time.MilliSeconds());
                point->SetInt64(value);
            }

            void OnUint64(TInstant time, ui64 value) override {
                Y_ENSURE(Sample_, "metric not started");
                NProto::TPoint* point = Sample_->AddPoints();
                point->SetTime(time.MilliSeconds());
                point->SetUint64(value);
            }

            void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override {
                Y_ENSURE(Sample_, "metric not started");
                NProto::TPoint* point = Sample_->AddPoints();
                point->SetTime(time.MilliSeconds());
                FillHistogram(*snapshot, point->MutableHistogram());
            }

            void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) override {
                Y_ENSURE(Sample_, "metric not started");
                NProto::TPoint* point = Sample_->AddPoints();
                point->SetTime(time.MilliSeconds());
                FillSummaryDouble(*snapshot, point->MutableSummaryDouble());
            }

            void OnLogHistogram(TInstant time, TLogHistogramSnapshotPtr snapshot) override {
                Y_ENSURE(Sample_, "metric not started");
                NProto::TPoint* point = Sample_->AddPoints();
                point->SetTime(time.MilliSeconds());
                FillLogHistogram(*snapshot, point->MutableLogHistogram());
            }

            void Close() override {
            }

        private:
            NProto::TMultiSamplesList* Samples_;
            NProto::TMultiSample* Sample_;
        };

    }

    IMetricEncoderPtr EncoderProtobuf(NProto::TSingleSamplesList* samples) {
        return MakeHolder<TSingleSamplesEncoder>(samples);
    }

    IMetricEncoderPtr EncoderProtobuf(NProto::TMultiSamplesList* samples) {
        return MakeHolder<TMultiSamplesEncoder>(samples);
    }

}
