#include "log_writer_detail.h"

#include "log.h"
#include "formatter.h"
#include "system_log_event_provider.h"

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

TRateLimitingLogWriterBase::TRateLimitingLogWriterBase(
    std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
    TString name,
    TLogWriterConfigPtr config)
    : SystemEventProvider_(std::move(systemEventProvider))
    , Name_(std::move(name))
    , Config_(std::move(config))
    , Profiler_(TProfiler("/logging")
        .WithSparse()
        .WithTag("writer", Name_))
    , RateLimit_(
        std::nullopt,
        {},
        Profiler_.Counter("/events_skipped_by_global_limit"))
{
    Profiler_.AddFuncGauge("/current_segment_size", MakeStrong(this), [this] {
        return CurrentSegmentSize_.load(std::memory_order::relaxed);
    });
}

void TRateLimitingLogWriterBase::Write(const TLogEvent& event)
{
    auto* categoryRateLimit = GetCategoryRateLimitCounter(event.Category->Name);
    if (RateLimit_.IsIntervalPassed()) {
        auto eventsSkipped = RateLimit_.GetAndResetLastSkippedEventsCount();
        OnLogSkipped(eventsSkipped, Name_);
    }
    if (categoryRateLimit->IsIntervalPassed()) {
        auto eventsSkipped = categoryRateLimit->GetAndResetLastSkippedEventsCount();
        OnLogSkipped(eventsSkipped, event.Category->Name);
    }

    if (!RateLimit_.IsLimitReached() && !categoryRateLimit->IsLimitReached()) {
        auto bytesWritten = WriteImpl(event);
        RateLimit_.UpdateCounter(bytesWritten);
        categoryRateLimit->UpdateCounter(bytesWritten);
    }
}

void TRateLimitingLogWriterBase::IncrementSegmentSize(i64 size)
{
    CurrentSegmentSize_.fetch_add(size, std::memory_order::relaxed);
}

void TRateLimitingLogWriterBase::ResetSegmentSize(i64 size)
{
    CurrentSegmentSize_.store(size, std::memory_order::relaxed);
}

void TRateLimitingLogWriterBase::SetRateLimit(std::optional<i64> limit)
{
    RateLimit_.SetRateLimit(limit);
}

void TRateLimitingLogWriterBase::SetCategoryRateLimits(const THashMap<TString, i64>& categoryRateLimits)
{
    CategoryToRateLimit_.clear();
    for (const auto& it : categoryRateLimits) {
        GetCategoryRateLimitCounter(it.first)->SetRateLimit(it.second);
    }
}

void TRateLimitingLogWriterBase::OnLogSkipped(i64 eventsSkipped, TStringBuf skippedBy)
{
    if (auto logSkippedEvent = SystemEventProvider_->GetSkippedLogEvent(eventsSkipped, skippedBy)) {
        WriteImpl(*logSkippedEvent);
    }
}

TRateLimitCounter* TRateLimitingLogWriterBase::GetCategoryRateLimitCounter(TStringBuf category)
{
    auto it = CategoryToRateLimit_.find(category);
    if (it == CategoryToRateLimit_.end()) {
        auto categoryProfiler = Profiler_
            .WithTag("category", TString{category}, -1);

        // TODO(prime@): optimize sensor count
        it = CategoryToRateLimit_.insert({category, TRateLimitCounter(
            std::nullopt,
            categoryProfiler.Counter("/bytes_written"),
            categoryProfiler.Counter("/events_skipped_by_category_limit"))}).first;
    }
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

TStreamLogWriterBase::TStreamLogWriterBase(
    std::unique_ptr<ILogFormatter> formatter,
    std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
    TString name,
    TLogWriterConfigPtr config)
    : TRateLimitingLogWriterBase(
        std::move(systemEventProvider),
        std::move(name),
        std::move(config))
    , Formatter_(std::move(formatter))
{ }

i64 TStreamLogWriterBase::WriteImpl(const TLogEvent& event)
{
    auto* stream = GetOutputStream();
    if (!stream) {
        return 0;
    }

    try {
        auto bytesWritten = Formatter_->WriteFormatted(stream, event);
        IncrementSegmentSize(bytesWritten);
        return bytesWritten;
    } catch (const std::exception& ex) {
        OnException(ex);
    }

    return 0;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
