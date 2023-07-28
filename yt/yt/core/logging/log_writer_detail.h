#pragma once

#include "private.h"
#include "log_writer.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TRateLimitCounter
{
public:
    TRateLimitCounter(
        std::optional<i64> rateLimit,
        NProfiling::TCounter bytesCounter,
        NProfiling::TCounter skippedEventsCounter);

    void SetRateLimit(std::optional<i64> rateLimit);
    bool IsLimitReached();
    bool IsIntervalPassed();
    i64 GetAndResetLastSkippedEventsCount();

    void UpdateCounter(i64 bytesWritten);

private:
    std::optional<i64> RateLimit_;

    NProfiling::TCounter BytesCounter_;
    NProfiling::TCounter SkippedEventsCounter_;

    i64 BytesWritten_ = 0;
    i64 SkippedEvents_ = 0;

    TInstant LastUpdate_;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLogWriterBase
    : public virtual ILogWriter
{
public:
    TStreamLogWriterBase(
        std::unique_ptr<ILogFormatter> formatter,
        TString name);

    void Write(const TLogEvent& event) override;
    void Flush() override;
    void Reload() override;

    void SetRateLimit(std::optional<i64> limit) override;
    void SetCategoryRateLimits(const THashMap<TString, i64>& categoryRateLimits) override;

protected:
    virtual IOutputStream* GetOutputStream() const noexcept = 0;
    virtual void OnException(const std::exception& ex);

    void ResetCurrentSegment(i64 size);

    TRateLimitCounter* GetCategoryRateLimitCounter(TStringBuf category);

    const std::unique_ptr<ILogFormatter> Formatter_;
    const TString Name_;

    TRateLimitCounter RateLimit_;
    THashMap<TStringBuf, TRateLimitCounter> CategoryToRateLimit_;

    i64 CurrentSegmentSize_ = 0;
    NProfiling::TGauge CurrentSegmentSizeGauge_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
