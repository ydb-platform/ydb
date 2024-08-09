#include "log_writer_detail.h"

#include "log.h"
#include "formatter.h"

#include <library/cpp/yt/system/exit.h>
#include <library/cpp/yt/system/handle_eintr.h>

namespace NYT::NLogging {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto RateLimitUpdatePeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TRateLimitCounter::TRateLimitCounter(
    std::optional<i64> rateLimit,
    NProfiling::TCounter bytesCounter,
    NProfiling::TCounter skippedEventsCounter)
    : RateLimit_(rateLimit)
    , BytesCounter_(std::move(bytesCounter))
    , SkippedEventsCounter_(std::move(skippedEventsCounter))
    , LastUpdate_(TInstant::Now())
{ }

void TRateLimitCounter::SetRateLimit(std::optional<i64> rateLimit)
{
    RateLimit_ = rateLimit;
    LastUpdate_ = TInstant::Now();
    BytesWritten_ = 0;
}

bool TRateLimitCounter::IsLimitReached()
{
    if (!RateLimit_) {
        return false;
    }

    if (BytesWritten_ >= *RateLimit_) {
        SkippedEvents_++;
        SkippedEventsCounter_.Increment();
        return true;
    } else {
        return false;
    }
}

bool TRateLimitCounter::IsIntervalPassed()
{
    auto now = TInstant::Now();
    if (now - LastUpdate_ >= RateLimitUpdatePeriod) {
        LastUpdate_ = now;
        BytesWritten_ = 0;
        return true;
    }
    return false;
}

void TRateLimitCounter::UpdateCounter(i64 bytesWritten)
{
    BytesWritten_ += bytesWritten;
    BytesCounter_.Increment(bytesWritten);
}

i64 TRateLimitCounter::GetAndResetLastSkippedEventsCount()
{
    i64 old = SkippedEvents_;
    SkippedEvents_ = 0;
    return old;
}

////////////////////////////////////////////////////////////////////////////////

TStreamLogWriterBase::TStreamLogWriterBase(
    std::unique_ptr<ILogFormatter> formatter,
    TString name)
    : Formatter_(std::move(formatter))
    , Name_(std::move(name))
    , RateLimit_(
        std::nullopt,
        {},
        TProfiler{"/logging"}.WithSparse().WithTag("writer", Name_).Counter("/events_skipped_by_global_limit"))
    , CurrentSegmentSizeGauge_(
        TProfiler{"/logging"}.WithSparse().WithTag("writer", Name_).Gauge("/current_segment_size"))
{ }

void TStreamLogWriterBase::Write(const TLogEvent& event)
{
    auto* stream = GetOutputStream();
    if (!stream) {
        return;
    }

    try {
        auto* categoryRateLimit = GetCategoryRateLimitCounter(event.Category->Name);
        if (RateLimit_.IsIntervalPassed()) {
            auto eventsSkipped = RateLimit_.GetAndResetLastSkippedEventsCount();
            if (eventsSkipped > 0) {
                Formatter_->WriteLogSkippedEvent(stream, eventsSkipped, Name_);
            }
        }
        if (categoryRateLimit->IsIntervalPassed()) {
            auto eventsSkipped = categoryRateLimit->GetAndResetLastSkippedEventsCount();
            if (eventsSkipped > 0) {
                Formatter_->WriteLogSkippedEvent(stream, eventsSkipped, event.Category->Name);
            }
        }
        if (!RateLimit_.IsLimitReached() && !categoryRateLimit->IsLimitReached()) {
            auto bytesWritten = Formatter_->WriteFormatted(stream, event);
            CurrentSegmentSize_ += bytesWritten;
            CurrentSegmentSizeGauge_.Update(CurrentSegmentSize_);
            RateLimit_.UpdateCounter(bytesWritten);
            categoryRateLimit->UpdateCounter(bytesWritten);
        }
    } catch (const std::exception& ex) {
        OnException(ex);
    }
}

void TStreamLogWriterBase::Flush()
{
    auto* stream = GetOutputStream();
    if (!stream) {
        return;
    }

    try {
        stream->Flush();
    } catch (const std::exception& ex) {
        OnException(ex);
    }
}

void TStreamLogWriterBase::Reload()
{ }

void TStreamLogWriterBase::OnException(const std::exception& ex)
{
    // Fail with drama by default.
    TRawFormatter<1024> formatter;
    formatter.AppendString("\n*** Unhandled exception in log writer: ");
    formatter.AppendString(ex.what());
    formatter.AppendString("\n*** Aborting ***\n");
#ifdef _unix_
    HandleEintr(::write, 2, formatter.GetData(), formatter.GetBytesWritten());
#else
    ::WriteFile(
        GetStdHandle(STD_ERROR_HANDLE),
        formatter.GetData(),
        formatter.GetBytesWritten(),
        /*lpNumberOfBytesWritten*/ nullptr,
        /*lpOverlapped*/ nullptr);
#endif
    AbortProcess(ToUnderlying(EProcessExitCode::IOError));
}

void TStreamLogWriterBase::ResetCurrentSegment(i64 size)
{
    CurrentSegmentSize_ = size;
    CurrentSegmentSizeGauge_.Update(CurrentSegmentSize_);
}

void TStreamLogWriterBase::SetRateLimit(std::optional<i64> limit)
{
    RateLimit_.SetRateLimit(limit);
}

void TStreamLogWriterBase::SetCategoryRateLimits(const THashMap<TString, i64>& categoryRateLimits)
{
    CategoryToRateLimit_.clear();
    for (const auto& it : categoryRateLimits) {
        GetCategoryRateLimitCounter(it.first)->SetRateLimit(it.second);
    }
}

TRateLimitCounter* TStreamLogWriterBase::GetCategoryRateLimitCounter(TStringBuf category)
{
    auto it = CategoryToRateLimit_.find(category);
    if (it == CategoryToRateLimit_.end()) {
        auto r = TProfiler{"/logging"}
            .WithSparse()
            .WithTag("writer", Name_)
            .WithTag("category", TString{category}, -1);

        // TODO(prime@): optimize sensor count
        it = CategoryToRateLimit_.insert({category, TRateLimitCounter(
            std::nullopt,
            r.Counter("/bytes_written"),
            r.Counter("/events_skipped_by_category_limit"))}).first;
    }
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
