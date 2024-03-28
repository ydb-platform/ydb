#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYdb::NTopic {

using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

struct TWriterCounters : public TThrRefBase {
    using TSelf = TWriterCounters;
    using TPtr = TIntrusivePtr<TSelf>;

    explicit TWriterCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
        Errors = counters->GetCounter("errors", true);
        CurrentSessionLifetimeMs = counters->GetCounter("currentSessionLifetimeMs", false);
        BytesWritten = counters->GetCounter("bytesWritten", true);
        MessagesWritten = counters->GetCounter("messagesWritten", true);
        BytesWrittenCompressed = counters->GetCounter("bytesWrittenCompressed", true);
        BytesInflightUncompressed = counters->GetCounter("bytesInflightUncompressed", false);
        BytesInflightCompressed = counters->GetCounter("bytesInflightCompressed", false);
        BytesInflightTotal = counters->GetCounter("bytesInflightTotal", false);
        MessagesInflight = counters->GetCounter("messagesInflight", false);

#define TOPIC_COUNTERS_HISTOGRAM_SETUP ::NMonitoring::ExplicitHistogram({0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100})
        TotalBytesInflightUsageByTime = counters->GetHistogram("totalBytesInflightUsageByTime", TOPIC_COUNTERS_HISTOGRAM_SETUP);
        UncompressedBytesInflightUsageByTime = counters->GetHistogram("uncompressedBytesInflightUsageByTime", TOPIC_COUNTERS_HISTOGRAM_SETUP);
        CompressedBytesInflightUsageByTime = counters->GetHistogram("compressedBytesInflightUsageByTime", TOPIC_COUNTERS_HISTOGRAM_SETUP);
#undef TOPIC_COUNTERS_HISTOGRAM_SETUP
    }

    TCounterPtr Errors;
    TCounterPtr CurrentSessionLifetimeMs;

    TCounterPtr BytesWritten;
    TCounterPtr MessagesWritten;
    TCounterPtr BytesWrittenCompressed;

    TCounterPtr BytesInflightUncompressed;
    TCounterPtr BytesInflightCompressed;
    TCounterPtr BytesInflightTotal;
    TCounterPtr MessagesInflight;

    //! Histograms reporting % usage of memory limit in time.
    //! Provides a histogram looking like: 10% : 100ms, 20%: 300ms, ... 50%: 200ms, ... 100%: 50ms
    //! Which means that < 10% memory usage was observed for 100ms during the period and 50% usage was observed for 200ms
    //! Used to monitor if the writer successfully deals with data flow provided. Larger values in higher buckets
    //! mean that writer is close to overflow (or being overflown) for major periods of time
    //! 3 histograms stand for:
    //! Total memory usage:
    ::NMonitoring::THistogramPtr TotalBytesInflightUsageByTime;
    //! Memory usage by messages waiting for comression:
    ::NMonitoring::THistogramPtr UncompressedBytesInflightUsageByTime;
    //! Memory usage by compressed messages pending for write:
    ::NMonitoring::THistogramPtr CompressedBytesInflightUsageByTime;
};

struct TReaderCounters: public TThrRefBase {
    using TSelf = TReaderCounters;
    using TPtr = TIntrusivePtr<TSelf>;

    TReaderCounters() = default;
    explicit TReaderCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
        Errors = counters->GetCounter("errors", true);
        CurrentSessionLifetimeMs = counters->GetCounter("currentSessionLifetimeMs", false);
        BytesRead = counters->GetCounter("bytesRead", true);
        MessagesRead = counters->GetCounter("messagesRead", true);
        BytesReadCompressed = counters->GetCounter("bytesReadCompressed", true);
        BytesInflightUncompressed = counters->GetCounter("bytesInflightUncompressed", false);
        BytesInflightCompressed = counters->GetCounter("bytesInflightCompressed", false);
        BytesInflightTotal = counters->GetCounter("bytesInflightTotal", false);
        MessagesInflight = counters->GetCounter("messagesInflight", false);

#define TOPIC_COUNTERS_HISTOGRAM_SETUP ::NMonitoring::ExplicitHistogram({0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100})
        TotalBytesInflightUsageByTime = counters->GetHistogram("totalBytesInflightUsageByTime", TOPIC_COUNTERS_HISTOGRAM_SETUP);
        UncompressedBytesInflightUsageByTime = counters->GetHistogram("uncompressedBytesInflightUsageByTime", TOPIC_COUNTERS_HISTOGRAM_SETUP);
        CompressedBytesInflightUsageByTime = counters->GetHistogram("compressedBytesInflightUsageByTime", TOPIC_COUNTERS_HISTOGRAM_SETUP);
#undef TOPIC_COUNTERS_HISTOGRAM_SETUP
    }

    TCounterPtr Errors;
    TCounterPtr CurrentSessionLifetimeMs;

    TCounterPtr BytesRead;
    TCounterPtr MessagesRead;
    TCounterPtr BytesReadCompressed;

    TCounterPtr BytesInflightUncompressed;
    TCounterPtr BytesInflightCompressed;
    TCounterPtr BytesInflightTotal;
    TCounterPtr MessagesInflight;

    //! Histograms reporting % usage of memory limit in time.
    //! Provides a histogram looking like: 10% : 100ms, 20%: 300ms, ... 50%: 200ms, ... 100%: 50ms
    //! Which means < 10% memory usage was observed for 100ms during the period and 50% usage was observed for 200ms.
    //! Used to monitor if the read session successfully deals with data flow provided. Larger values in higher buckets
    //! mean that read session is close to overflow (or being overflown) for major periods of time.
    //!
    //! Total memory usage.
    ::NMonitoring::THistogramPtr TotalBytesInflightUsageByTime;
    //! Memory usage by messages waiting that are ready to be received by user.
    ::NMonitoring::THistogramPtr UncompressedBytesInflightUsageByTime;
    //! Memory usage by compressed messages pending for decompression.
    ::NMonitoring::THistogramPtr CompressedBytesInflightUsageByTime;
};

}  // namespace NYdb::NTopic
