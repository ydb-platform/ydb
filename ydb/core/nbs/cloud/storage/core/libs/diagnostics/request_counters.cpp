#include "request_counters.h"

#include "counters_helper.h"
#include "histogram_types.h"
#include "max_calculator.h"
#include "weighted_percentile.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/disjoint_interval_map.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/verify.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <utility>

namespace NYdb::NBS {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
struct THistBase
{
    const TString Name;
    const TString Units;
    const double PercentileMultiplier;
    const TBucketBounds HistBounds;
    const EHistogramCounterOptions CounterOptions;

    THistogramPtr Hist;
    std::array<TDynamicCounters::TCounterPtr, TDerived::BUCKETS_COUNT> Counters;

    THistBase(TString name, EHistogramCounterOptions counterOptions)
        : Name(std::move(name))
        , Units(TDerived::Units)
        , PercentileMultiplier(TDerived::PercentileMultiplier)
        , HistBounds(ConvertToHistBounds(TDerived::Buckets))
        , CounterOptions(counterOptions)
        , Hist(
              new THistogramCounter(NMonitoring::ExplicitHistogram(HistBounds)))
    {
        std::fill(Counters.begin(), Counters.end(), new TCounterForPtr(true));
    }

    void Register(
        TDynamicCounters& counters,
        TCountableBase::EVisibility vis = TCountableBase::EVisibility::Public)
    {
        auto subgroup =
            MakeVisibilitySubgroup(counters, "histogram", Name, vis);
        if (GetUnits()) {
            subgroup = subgroup->GetSubgroup("units", GetUnits());
        }
        if (CounterOptions & EHistogramCounterOption::ReportSingleCounter) {
            Hist = subgroup->GetHistogram(
                Name,
                NMonitoring::ExplicitHistogram(HistBounds),
                true,
                vis);
        }
        if (CounterOptions & EHistogramCounterOption::ReportMultipleCounters) {
            const auto names = TDerived::MakeNames();
            for (size_t i = 0; i < Counters.size(); ++i) {
                Counters[i] = subgroup->GetCounter(names[i], true, vis);
            }
        }
    }

    void Increment(double value, ui64 count)
    {
        Hist->Collect(value, count);

        auto it = LowerBound(
            TDerived::Buckets.begin(),
            TDerived::Buckets.end(),
            value);
        STORAGE_VERIFY(it != TDerived::Buckets.end(), "Bucket", value);
        size_t index = std::distance(TDerived::Buckets.begin(), it);
        Counters[index]->Add(count);
    }

    TVector<TBucketInfo> GetBuckets() const
    {
        const auto snapshot = Hist->Snapshot();

        TVector<TBucketInfo> result(snapshot->Count());
        for (size_t i = 0; i < snapshot->Count(); ++i) {
            result.emplace_back(snapshot->UpperBound(i), snapshot->Value(i));
        }
        return result;
    }

    const TString& GetUnits() const
    {
        return Units;
    }

    const TString& GetName() const
    {
        return Name;
    }

    double GetPercentileMultiplier() const
    {
        return PercentileMultiplier;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBucket>
struct TTimeHist: public THistBase<TBucket>
{
    using THistBase<TBucket>::THistBase;

    void Increment(TDuration requestTime, ui64 count = 1)
    {
        THistBase<TBucket>::Increment(
            requestTime.MicroSeconds() /
                THistBase<TBucket>::GetPercentileMultiplier(),
            count);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TUsTimeHist: public TTimeHist<TRequestUsTimeBuckets>
{
    using TTimeHist<TRequestUsTimeBuckets>::TTimeHist;
};

////////////////////////////////////////////////////////////////////////////////

struct TMsTimeHist: public TTimeHist<TRequestMsTimeBuckets>
{
    using TTimeHist<TRequestMsTimeBuckets>::TTimeHist;
};

////////////////////////////////////////////////////////////////////////////////

// Allows to use either microseconds or milliseconds for time histograms.
class TAdaptiveTimeHist
{
private:
    using TTimeHistVariant = std::variant<TUsTimeHist, TMsTimeHist>;
    std::unique_ptr<TTimeHistVariant> TimeHistPtr;

public:
    TAdaptiveTimeHist(TString name, EHistogramCounterOptions counterOptions)
    {
        if (counterOptions &
            EHistogramCounterOption::UseMsUnitsForTimeHistogram)
        {
            TimeHistPtr = std::make_unique<TTimeHistVariant>(
                std::in_place_type<TMsTimeHist>,
                std::move(name),
                counterOptions);
        } else {
            TimeHistPtr = std::make_unique<TTimeHistVariant>(
                std::in_place_type<TUsTimeHist>,
                std::move(name),
                counterOptions);
        }
    }

    ~TAdaptiveTimeHist() = default;

    TAdaptiveTimeHist(const TAdaptiveTimeHist&) = delete;
    TAdaptiveTimeHist(TAdaptiveTimeHist&&) = default;
    TAdaptiveTimeHist& operator=(const TAdaptiveTimeHist&) = delete;
    TAdaptiveTimeHist& operator=(TAdaptiveTimeHist&&) = default;

    void Increment(TDuration requestTime, ui64 count = 1)
    {
        std::visit(
            [&](auto& timeHist) { timeHist.Increment(requestTime, count); },
            *TimeHistPtr);
    }

    template <typename... Args>
    void Register(Args&&... args)
    {
        std::visit(
            [&](auto& timeHist)
            { timeHist.Register(std::forward<Args>(args)...); },
            *TimeHistPtr);
    }

    [[nodiscard]] TVector<TBucketInfo> GetBuckets() const
    {
        return std::visit(
            [&](auto& timeHist) -> TVector<TBucketInfo>
            { return timeHist.GetBuckets(); },
            *TimeHistPtr);
    }

    [[nodiscard]] const TString& GetUnits() const
    {
        return std::visit(
            [&](auto& timeHist) -> const TString&
            { return timeHist.GetUnits(); },
            *TimeHistPtr);
    }

    [[nodiscard]] const TString& GetName() const
    {
        return std::visit(
            [&](auto& timeHist) -> const TString&
            { return timeHist.GetName(); },
            *TimeHistPtr);
    }

    [[nodiscard]] double GetPercentileMultiplier() const
    {
        return std::visit(
            [&](auto& timeHist) -> double
            { return timeHist.GetPercentileMultiplier(); },
            *TimeHistPtr);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSizeHist: public THistBase<TKbSizeBuckets>
{
    using THistBase::THistBase;

    void Increment(double requestBytes, ui64 count = 1)
    {
        THistBase::Increment(requestBytes / 1024, count);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TSrcHistogram>
class TRequestPercentiles
{
    using TDynamicCounterPtr = TDynamicCounters::TCounterPtr;

private:
    TVector<TDynamicCounterPtr> Counters;

    TVector<ui64> Prev;

    const TSrcHistogram& SrcHistogram;

public:
    explicit TRequestPercentiles(const TSrcHistogram& srcHistogram)
        : SrcHistogram(srcHistogram)
    {}

    void Register(TDynamicCounters& counters)
    {
        auto subgroup =
            counters.GetSubgroup("percentiles", SrcHistogram.GetName());
        if (SrcHistogram.GetUnits()) {
            subgroup = subgroup->GetSubgroup("units", SrcHistogram.GetUnits());
        }
        const auto& percentiles = GetDefaultPercentiles();
        for (const auto& [value, name]: percentiles) {
            Counters.emplace_back(subgroup->GetCounter(name, false));
        }
    }

    void Update()
    {
        const auto update = SrcHistogram.GetBuckets();
        if (Prev.size() < update.size()) {
            Prev.resize(update.size());
        }

        TVector<TBucketInfo> delta(Reserve(update.size()));
        for (size_t i = 0; i < update.size(); ++i) {
            const auto& [bound, value] = update[i];
            delta.emplace_back(bound, value - Prev[i]);
            Prev[i] = value;
        }

        auto result =
            CalculateWeightedPercentiles(delta, GetDefaultPercentiles());

        for (ui32 i = 0; i < Min(Counters.size(), result.size()); ++i) {
            *Counters[i] =
                std::lround(result[i] * SrcHistogram.GetPercentileMultiplier());
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TRequestCounters::TSpecialCounters
{
    TDynamicCounters::TCounterPtr HwProblems;

    TSpecialCounters() = default;

    TSpecialCounters(const TSpecialCounters&) = delete;
    TSpecialCounters(TSpecialCounters&&) = default;

    TSpecialCounters& operator=(const TSpecialCounters&) = delete;
    TSpecialCounters& operator=(TSpecialCounters&&) = default;

    void Init(TDynamicCounters& counters)
    {
        HwProblems = counters.GetCounter("HwProblems", true);
    }

    void AddStats(EDiagnosticsErrorKind errorKind, ui32 errorFlags)
    {
        if ((errorKind != EDiagnosticsErrorKind::Success) &&
            HasProtoFlag(
                errorFlags,
                NYdb::NBS::NProto::EF_HW_PROBLEMS_DETECTED))
        {
            HwProblems->Inc();
        }
    }

    void AddRetryStats(EDiagnosticsErrorKind errorKind, ui32 errorFlags)
    {
        AddStats(errorKind, errorFlags);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestCounters::TStatCounters
{
    template <typename Type>
    struct TFailedAndSuccess
    {
        Type Success;
        Type Failed;
    };

    struct TSizeClassCounters
    {
        TAdaptiveTimeHist ExecutionTimeHist;
        TRequestPercentiles<TAdaptiveTimeHist> ExecutionTimePercentiles;

        explicit TSizeClassCounters(
            EHistogramCounterOptions histogramCounterOptions)
            : ExecutionTimeHist("ExecutionTime", histogramCounterOptions)
            , ExecutionTimePercentiles(ExecutionTimeHist)
        {}
    };

    bool IsReadWriteRequest = false;
    bool ReportDataPlaneHistogram = false;
    bool ReportControlPlaneHistogram = false;
    bool ThrottlingHistogramsDisabled = false;

    TIntrusivePtr<TDynamicCounters> CountersGroup;

    TDynamicCounters::TCounterPtr Count;
    TDynamicCounters::TCounterPtr MaxCount;
    TDynamicCounters::TCounterPtr UnalignedCount;
    TDynamicCounters::TCounterPtr Time;
    TDynamicCounters::TCounterPtr MaxTime;
    TDynamicCounters::TCounterPtr MaxTotalTime;
    TDynamicCounters::TCounterPtr MaxSize;
    TDynamicCounters::TCounterPtr RequestBytes;
    TDynamicCounters::TCounterPtr MaxRequestBytes;
    TDynamicCounters::TCounterPtr InProgress;
    TDynamicCounters::TCounterPtr MaxInProgress;
    TDynamicCounters::TCounterPtr InProgressBytes;
    TDynamicCounters::TCounterPtr MaxInProgressBytes;
    TDynamicCounters::TCounterPtr PostponedQueueSize;
    TDynamicCounters::TCounterPtr MaxPostponedQueueSize;
    TDynamicCounters::TCounterPtr PostponedCount;
    TDynamicCounters::TCounterPtr PostponedQueueSizeGrpc;
    TDynamicCounters::TCounterPtr MaxPostponedQueueSizeGrpc;
    TDynamicCounters::TCounterPtr PostponedCountGrpc;
    TDynamicCounters::TCounterPtr FastPathHits;

    TDynamicCounters::TCounterPtr Errors;
    TDynamicCounters::TCounterPtr ErrorsAborted;
    TDynamicCounters::TCounterPtr ErrorsFatal;
    TDynamicCounters::TCounterPtr ErrorsRetriable;
    TDynamicCounters::TCounterPtr ErrorsThrottling;
    TDynamicCounters::TCounterPtr ErrorsWriteRejectdByCheckpoint;
    TDynamicCounters::TCounterPtr ErrorsSession;
    TDynamicCounters::TCounterPtr ErrorsSilent;

    TDynamicCounters::TCounterPtr Retries;

    TSizeHist SizeHist;
    TRequestPercentiles<TSizeHist> SizePercentiles;

    TAdaptiveTimeHist TimeHist;
    TAdaptiveTimeHist TimeHistUnaligned;
    TRequestPercentiles<TAdaptiveTimeHist> TimePercentiles;

    TAdaptiveTimeHist ExecutionTimeHist;
    TAdaptiveTimeHist ExecutionTimeHistUnaligned;
    TRequestPercentiles<TAdaptiveTimeHist> ExecutionTimePercentiles;

    TDisjointIntervalMap<ui64, std::unique_ptr<TSizeClassCounters>>
        ExecutionTimeSizeClasses;

    TAdaptiveTimeHist RequestCompletionTimeHist;
    TRequestPercentiles<TAdaptiveTimeHist> RequestCompletionTimePercentiles;

    TAdaptiveTimeHist PostponedTimeHist;
    TRequestPercentiles<TAdaptiveTimeHist> PostponedTimePercentiles;

    TAdaptiveTimeHist BackoffTimeHist;
    TRequestPercentiles<TAdaptiveTimeHist> BackoffTimePercentiles;

    TAdaptiveTimeHist ShapingTimeHist;
    TRequestPercentiles<TAdaptiveTimeHist> ShapingTimePercentiles;

    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxTimeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxTotalTimeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxSizeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxInProgressCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxInProgressBytesCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxPostponedQueueSizeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxPostponedQueueSizeGrpcCalc;
    TMaxPerSecondCalculator<DEFAULT_BUCKET_COUNT> MaxCountCalc;
    TMaxPerSecondCalculator<DEFAULT_BUCKET_COUNT> MaxRequestBytesCalc;

    TMutex FullInitLock;
    TAtomic FullyInitialized = false;

    explicit TStatCounters(
        ITimerPtr timer,
        EHistogramCounterOptions histogramCounterOptions,
        const TVector<TSizeInterval>& executionTimeSizeClasses)
        : SizeHist("Size", histogramCounterOptions)
        , SizePercentiles(SizeHist)
        , TimeHist("Time", histogramCounterOptions)
        , TimeHistUnaligned("Time", histogramCounterOptions)
        , TimePercentiles(TimeHist)
        , ExecutionTimeHist("ExecutionTime", histogramCounterOptions)
        , ExecutionTimeHistUnaligned("ExecutionTime", histogramCounterOptions)
        , ExecutionTimePercentiles(ExecutionTimeHist)
        , RequestCompletionTimeHist(
              "RequestCompletionTime",
              histogramCounterOptions)
        , RequestCompletionTimePercentiles(RequestCompletionTimeHist)
        , PostponedTimeHist("ThrottlerDelay", histogramCounterOptions)
        , PostponedTimePercentiles(PostponedTimeHist)
        , BackoffTimeHist("BackoffTime", histogramCounterOptions)
        , BackoffTimePercentiles(BackoffTimeHist)
        , ShapingTimeHist("ShapingTime", histogramCounterOptions)
        , ShapingTimePercentiles(ShapingTimeHist)
        , MaxTimeCalc(timer)
        , MaxTotalTimeCalc(timer)
        , MaxSizeCalc(timer)
        , MaxInProgressCalc(timer)
        , MaxInProgressBytesCalc(timer)
        , MaxPostponedQueueSizeCalc(timer)
        , MaxPostponedQueueSizeGrpcCalc(timer)
        , MaxCountCalc(timer)
        , MaxRequestBytesCalc(timer)
    {
        for (auto [start, end]: executionTimeSizeClasses) {
            ExecutionTimeSizeClasses.Add(
                start,
                end,
                std::make_unique<TSizeClassCounters>(histogramCounterOptions));
        }
    }

    TStatCounters(const TStatCounters&) = delete;
    TStatCounters(TStatCounters&&) = default;

    TStatCounters& operator=(const TStatCounters&) = delete;
    TStatCounters& operator=(TStatCounters&&) = default;

    void Init(
        TDynamicCountersPtr countersGroup,
        bool isReadWriteRequest,
        bool reportDataPlaneHistogram,
        bool reportControlPlaneHistogram,
        bool throttlingHistogramsDisabled)
    {
        CountersGroup = std::move(countersGroup);
        auto& counters = *CountersGroup;

        IsReadWriteRequest = isReadWriteRequest;
        ReportDataPlaneHistogram = reportDataPlaneHistogram;
        ReportControlPlaneHistogram = reportControlPlaneHistogram;
        ThrottlingHistogramsDisabled = throttlingHistogramsDisabled;

        // always reporting the most important counters even when lazy
        // initialization is enabled and the values are zeroes
        Count = counters.GetCounter("Count", true);
        ErrorsFatal = counters.GetCounter("Errors/Fatal", true);
        Time = counters.GetCounter("Time", true);

        if (IsReadWriteRequest) {
            RequestBytes = counters.GetCounter("RequestBytes", true);
        }
    }

    void FullInitIfNeeded()
    {
        if (AtomicGet(FullyInitialized)) {
            return;
        }

        auto g = Guard(FullInitLock);
        if (AtomicGet(FullyInitialized)) {
            return;
        }

        auto& counters = *CountersGroup;

        MaxTime = counters.GetCounter("MaxTime");
        MaxTotalTime = counters.GetCounter("MaxTotalTime");

        InProgress = counters.GetCounter("InProgress");
        MaxInProgress = counters.GetCounter("MaxInProgress");

        Errors = counters.GetCounter("Errors", true);
        ErrorsAborted = counters.GetCounter("Errors/Aborted", true);
        ErrorsRetriable = counters.GetCounter("Errors/Retriable", true);
        ErrorsThrottling = counters.GetCounter("Errors/Throttling", true);
        ErrorsWriteRejectdByCheckpoint =
            counters.GetCounter("Errors/CheckpointReject", true);
        ErrorsSession = counters.GetCounter("Errors/Session", true);
        Retries = counters.GetCounter("Retries", true);

        if (!IsReadWriteRequest) {
            if (ReportControlPlaneHistogram) {
                TimeHist.Register(counters);
            } else {
                TimePercentiles.Register(counters);
            }
        }

        if (IsReadWriteRequest) {
            ErrorsSilent = counters.GetCounter("Errors/Silent", true);

            MaxSize = counters.GetCounter("MaxSize");
            MaxCount = counters.GetCounter("MaxCount");

            MaxRequestBytes = counters.GetCounter("MaxRequestBytes");

            InProgressBytes = counters.GetCounter("InProgressBytes");
            MaxInProgressBytes = counters.GetCounter("MaxInProgressBytes");

            UnalignedCount = counters.GetCounter("UnalignedCount", true);

            if (ReportDataPlaneHistogram) {
                auto unalignedClassGroup =
                    counters.GetSubgroup("sizeclass", "Unaligned");

                SizeHist.Register(counters);
                TimeHistUnaligned.Register(*unalignedClassGroup);
                if (!ThrottlingHistogramsDisabled) {
                    ExecutionTimeHist.Register(counters);
                    ExecutionTimeHistUnaligned.Register(*unalignedClassGroup);
                }
            } else {
                SizePercentiles.Register(counters);
                if (!ThrottlingHistogramsDisabled) {
                    ExecutionTimePercentiles.Register(counters);
                    PostponedTimePercentiles.Register(counters);
                    BackoffTimePercentiles.Register(counters);
                    ShapingTimePercentiles.Register(counters);
                }
                TimePercentiles.Register(counters);
            }

            if (!ThrottlingHistogramsDisabled) {
                for (auto& [_, sizeClass]: ExecutionTimeSizeClasses) {
                    auto sizeClassCounters = counters.GetSubgroup(
                        "sizeclass",
                        ToString(
                            TSizeInterval{sizeClass.Begin, sizeClass.End}));
                    if (ReportDataPlaneHistogram) {
                        sizeClass.Value->ExecutionTimeHist.Register(
                            *sizeClassCounters);
                    } else {
                        sizeClass.Value->ExecutionTimePercentiles.Register(
                            *sizeClassCounters);
                    }
                }
            }

            const auto visibleHistogram =
                ReportDataPlaneHistogram ? TCountableBase::EVisibility::Public
                                         : TCountableBase::EVisibility::Private;

            if (!ThrottlingHistogramsDisabled) {
                PostponedTimeHist.Register(counters, visibleHistogram);
                BackoffTimeHist.Register(counters, visibleHistogram);
                ShapingTimeHist.Register(counters, visibleHistogram);
            }
            TimeHist.Register(counters, visibleHistogram);

            // Always enough only percentiles.
            RequestCompletionTimeHist.Register(
                counters,
                TCountableBase::EVisibility::Private);
            RequestCompletionTimePercentiles.Register(counters);

            PostponedQueueSize = counters.GetCounter("PostponedQueueSize");
            MaxPostponedQueueSize =
                counters.GetCounter("MaxPostponedQueueSize");
            PostponedCount = counters.GetCounter("PostponedCount", true);

            PostponedQueueSizeGrpc =
                counters.GetCounter("PostponedQueueSizeGrpc");
            MaxPostponedQueueSizeGrpc =
                counters.GetCounter("MaxPostponedQueueSizeGrpc");
            PostponedCountGrpc =
                counters.GetCounter("PostponedCountGrpc", true);

            FastPathHits = counters.GetCounter("FastPathHits", true);
        }

        AtomicSet(FullyInitialized, true);
    }

    void Started(ui64 requestBytes)
    {
        MaxInProgressCalc.Add(InProgress->Inc());

        if (IsReadWriteRequest) {
            MaxInProgressBytesCalc.Add(InProgressBytes->Add(requestBytes));
        }
    }

    void Completed(ui64 requestBytes)
    {
        InProgress->Dec();

        if (IsReadWriteRequest) {
            InProgressBytes->Sub(requestBytes);
        }
    }

    void AddStats(
        TDuration totalTime,
        TDuration completionTime,
        TDuration time,
        TDuration execTime,
        TDuration postponedTime,
        TDuration backoffTime,
        TDuration shapingTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        bool unaligned,
        ECalcMaxTime calcMaxTime)
    {
        const bool failed = errorKind != EDiagnosticsErrorKind::Success &&
                            (errorKind != EDiagnosticsErrorKind::ErrorSilent ||
                             !IsReadWriteRequest);

        if (failed) {
            Errors->Inc();
        } else {
            Count->Inc();
        }

        switch (errorKind) {
            case EDiagnosticsErrorKind::Success:
                break;
            case EDiagnosticsErrorKind::ErrorAborted:
                ErrorsAborted->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorFatal:
                ErrorsFatal->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorRetriable:
                ErrorsRetriable->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorThrottling:
                ErrorsThrottling->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint:
                ErrorsWriteRejectdByCheckpoint->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorSession:
                ErrorsSession->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorSilent:
                if (IsReadWriteRequest) {
                    ErrorsSilent->Inc();
                }
                break;
            case EDiagnosticsErrorKind::Max:
                Y_DEBUG_ABORT_UNLESS(false);
                return;
        }

        if (calcMaxTime == ECalcMaxTime::ENABLE) {
            MaxTimeCalc.Add(execTime.MicroSeconds());
        }
        MaxTotalTimeCalc.Add(totalTime.MicroSeconds());

        Time->Add(time.MicroSeconds());
        TimeHist.Increment(time);

        if (completionTime != TDuration::Zero()) {
            RequestCompletionTimeHist.Increment(completionTime);
        }

        if (IsReadWriteRequest) {
            MaxCountCalc.Add(1);
            RequestBytes->Add(requestBytes);
            MaxRequestBytesCalc.Add(requestBytes);

            SizeHist.Increment(requestBytes);
            MaxSizeCalc.Add(requestBytes);

            if (unaligned) {
                UnalignedCount->Inc();
                TimeHistUnaligned.Increment(time);
                if (!ThrottlingHistogramsDisabled) {
                    ExecutionTimeHistUnaligned.Increment(execTime);
                }
            }

            if (!ThrottlingHistogramsDisabled) {
                ExecutionTimeHist.Increment(execTime);

                ExecutionTimeSizeClasses.VisitOverlapping(
                    requestBytes,
                    requestBytes + 1,
                    [&](TDisjointIntervalMap<
                        ui64,
                        std::unique_ptr<TSizeClassCounters>>::TIterator it) {
                        it->second.Value->ExecutionTimeHist.Increment(execTime);
                    });

                PostponedTimeHist.Increment(postponedTime);
                BackoffTimeHist.Increment(backoffTime);
                ShapingTimeHist.Increment(shapingTime);
            }
        }
    }

    void BatchCompleted(
        ui64 requestCount,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist,
        bool unaligned)
    {
        Count->Add(requestCount);
        Errors->Add(errors);

        for (auto [dt, count]: timeHist) {
            Time->Add(dt.MicroSeconds());
            TimeHist.Increment(dt, count);
            MaxTimeCalc.Add(dt.MicroSeconds());
        }

        if (IsReadWriteRequest) {
            MaxCountCalc.Add(requestCount);
            RequestBytes->Add(bytes);
            MaxRequestBytesCalc.Add(bytes);

            for (auto [size, count]: sizeHist) {
                SizeHist.Increment(size, count);
                MaxSizeCalc.Add(size);
            }

            if (unaligned) {
                UnalignedCount->Add(requestCount);
                UnalignedCount->Add(errors);
            }

            for (auto [dt, count]: timeHist) {
                ExecutionTimeHist.Increment(dt, count);

                if (unaligned) {
                    TimeHistUnaligned.Increment(dt, count);
                    ExecutionTimeHistUnaligned.Increment(dt, count);
                }
            }
        }
    }

    void AddRetryStats(EDiagnosticsErrorKind errorKind)
    {
        switch (errorKind) {
            case EDiagnosticsErrorKind::ErrorRetriable:
                ErrorsRetriable->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorThrottling:
                ErrorsThrottling->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint:
                ErrorsWriteRejectdByCheckpoint->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorSession:
                ErrorsSession->Inc();
                break;
            case EDiagnosticsErrorKind::Success:
            case EDiagnosticsErrorKind::ErrorAborted:
            case EDiagnosticsErrorKind::ErrorFatal:
            case EDiagnosticsErrorKind::ErrorSilent:
                Y_DEBUG_ABORT_UNLESS(false);
                return;
            case EDiagnosticsErrorKind::Max:
                Y_DEBUG_ABORT_UNLESS(false);
                return;
        }

        Errors->Inc();
        Retries->Inc();
    }

    void RequestPostponed()
    {
        if (IsReadWriteRequest) {
            PostponedCount->Inc();
            MaxPostponedQueueSizeCalc.Add(PostponedQueueSize->Inc());
        }
    }

    void RequestPostponedServer()
    {
        if (IsReadWriteRequest) {
            PostponedCountGrpc->Inc();
            MaxPostponedQueueSizeGrpcCalc.Add(PostponedQueueSizeGrpc->Inc());
        }
    }

    void RequestFastPathHit()
    {
        if (IsReadWriteRequest) {
            FastPathHits->Inc();
        }
    }

    void RequestAdvanced()
    {
        if (IsReadWriteRequest) {
            PostponedQueueSize->Dec();
        }
    }

    void RequestAdvancedServer()
    {
        if (IsReadWriteRequest) {
            PostponedQueueSizeGrpc->Dec();
        }
    }

    void AddIncompleteStats(
        TDuration executionTime,
        TDuration totalTime,
        ECalcMaxTime calcMaxTime)
    {
        if (calcMaxTime == ECalcMaxTime::ENABLE) {
            MaxTimeCalc.Add(executionTime.MicroSeconds());
        }
        MaxTotalTimeCalc.Add(totalTime.MicroSeconds());
    }

    void UpdateStats(bool updatePercentiles)
    {
        *MaxInProgress = MaxInProgressCalc.NextValue();
        *MaxTime = MaxTimeCalc.NextValue();
        *MaxTotalTime = MaxTotalTimeCalc.NextValue();

        if (IsReadWriteRequest) {
            *MaxCount = MaxCountCalc.NextValue();
            *MaxSize = MaxSizeCalc.NextValue();
            *MaxRequestBytes = MaxRequestBytesCalc.NextValue();
            *MaxInProgressBytes = MaxInProgressBytesCalc.NextValue();
            *MaxPostponedQueueSize = MaxPostponedQueueSizeCalc.NextValue();
            *MaxPostponedQueueSizeGrpc =
                MaxPostponedQueueSizeGrpcCalc.NextValue();
            if (updatePercentiles && !ReportDataPlaneHistogram) {
                SizePercentiles.Update();
                TimePercentiles.Update();
                RequestCompletionTimePercentiles.Update();
                if (!ThrottlingHistogramsDisabled) {
                    ExecutionTimePercentiles.Update();
                    PostponedTimePercentiles.Update();
                    BackoffTimePercentiles.Update();
                    ShapingTimePercentiles.Update();

                    for (auto& [_, sizeClass]: ExecutionTimeSizeClasses) {
                        sizeClass.Value->ExecutionTimePercentiles.Update();
                    }
                }
            }
        } else if (updatePercentiles && !ReportControlPlaneHistogram) {
            TimePercentiles.Update();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TRequestCounters::TRequestCounters(
    ITimerPtr timer,
    ui32 requestCount,
    std::function<TString(TRequestType)> requestType2Name,
    std::function<bool(TRequestType)> isReadWriteRequestType,
    std::function<bool(TRequestType)> isStartEndpointRequestType,
    EOptions options,
    EHistogramCounterOptions histogramCounterOptions,
    const TVector<TSizeInterval>& executionTimeSizeClasses)
    : RequestType2Name(std::move(requestType2Name))
    , IsReadWriteRequestType(std::move(isReadWriteRequestType))
    , IsStartEndpointRequestType(std::move(isStartEndpointRequestType))
    , Options(options)
{
    if (Options & EOption::AddSpecialCounters) {
        SpecialCounters = MakeHolder<TSpecialCounters>();
    }

    CountersByRequest.reserve(requestCount);
    for (ui32 i = 0; i < requestCount; ++i) {
        CountersByRequest.emplace_back(
            timer,
            histogramCounterOptions,
            executionTimeSizeClasses);
    }
}

TRequestCounters::~TRequestCounters()
{}

TRequestCounters::TStatCounters& TRequestCounters::AccessRequestStats(
    TRequestType t)
{
    auto& statCounters = CountersByRequest[t];
    if (statCounters.CountersGroup) {
        statCounters.FullInitIfNeeded();
    }
    return statCounters;
}

void TRequestCounters::Register(TDynamicCounters& counters)
{
    if (SpecialCounters) {
        SpecialCounters->Init(counters);
    }

    for (TRequestType t = 0; t < CountersByRequest.size(); ++t) {
        if (ShouldReport(t)) {
            auto requestGroup =
                counters.GetSubgroup("request", RequestType2Name(t));

            CountersByRequest[t].Init(
                std::move(requestGroup),
                IsReadWriteRequestType(t),
                Options & EOption::ReportDataPlaneHistogram,
                Options & EOption::ReportControlPlaneHistogram,
                Options & EOption::ThrottlingHistogramsDisabled);

            // ReadWrite counters are usually the most important ones so let's
            // report zeroes for them instead of not reporting anything at all
            const bool lazyInit =
                Options & EOption::LazyRequestInitialization &&
                !IsReadWriteRequestType(t);

            if (!lazyInit) {
                CountersByRequest[t].FullInitIfNeeded();
            }
        }
    }
}

void TRequestCounters::Subscribe(TRequestCountersPtr subscriber)
{
    Subscribers.push_back(std::move(subscriber));
}

ui64 TRequestCounters::RequestStarted(
    TRequestType requestType,
    ui64 requestBytes)
{
    RequestStartedImpl(requestType, requestBytes);
    return GetCycleCount();
}

TRequestCounters::TRequestTime TRequestCounters::RequestCompleted(
    TRequestType requestType,
    ui64 requestStarted,
    TDuration postponedTime,
    TDuration backoffTime,
    TDuration shapingTime,
    ui64 requestBytes,
    EDiagnosticsErrorKind errorKind,
    ui32 errorFlags,
    bool unaligned,
    ECalcMaxTime calcMaxTime,
    ui64 responseSent)
{
    const ui64 requestCompleted = GetCycleCount();
    const TDuration totalTime =
        CyclesToDurationSafe(requestCompleted - requestStarted);
    const TDuration completionTime =
        responseSent ? CyclesToDurationSafe(requestCompleted - responseSent)
                     : TDuration::Zero();

    const TDuration time = totalTime - completionTime;
    const TDuration execTime = time - postponedTime - backoffTime - shapingTime;

    RequestCompletedImpl(
        requestType,
        totalTime,
        completionTime,
        time,
        execTime,
        postponedTime,
        backoffTime,
        shapingTime,
        requestBytes,
        errorKind,
        errorFlags,
        unaligned,
        calcMaxTime);

    return {.ExecutionTime = execTime, .Time = totalTime};
}

void TRequestCounters::AddRetryStats(
    TRequestType requestType,
    EDiagnosticsErrorKind errorKind,
    ui32 errorFlags)
{
    if (SpecialCounters) {
        SpecialCounters->AddRetryStats(errorKind, errorFlags);
    }

    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).AddRetryStats(errorKind);
    }
    NotifySubscribers(
        &TRequestCounters::AddRetryStats,
        requestType,
        errorKind,
        errorFlags);
}

void TRequestCounters::RequestPostponed(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestPostponed();
    }
    NotifySubscribers(&TRequestCounters::RequestPostponed, requestType);
}

void TRequestCounters::RequestPostponedServer(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestPostponedServer();
    }
    NotifySubscribers(&TRequestCounters::RequestPostponedServer, requestType);
}

void TRequestCounters::RequestFastPathHit(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestFastPathHit();
    }
    NotifySubscribers(&TRequestCounters::RequestFastPathHit, requestType);
}

void TRequestCounters::RequestAdvanced(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestAdvanced();
    }
    NotifySubscribers(&TRequestCounters::RequestAdvanced, requestType);
}

void TRequestCounters::RequestAdvancedServer(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestAdvancedServer();
    }
    NotifySubscribers(&TRequestCounters::RequestAdvancedServer, requestType);
}

void TRequestCounters::AddIncompleteStats(
    TRequestType requestType,
    TDuration executionTime,
    TDuration totalTime,
    ECalcMaxTime calcMaxTime)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType)
            .AddIncompleteStats(executionTime, totalTime, calcMaxTime);
    }
    NotifySubscribers(
        &TRequestCounters::AddIncompleteStats,
        requestType,
        executionTime,
        totalTime,
        calcMaxTime);
}

void TRequestCounters::BatchCompleted(
    TRequestType requestType,
    ui64 count,
    ui64 bytes,
    ui64 errors,
    std::span<TTimeBucket> timeHist,
    std::span<TSizeBucket> sizeHist)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType)
            .BatchCompleted(
                count,
                bytes,
                errors,
                timeHist,
                sizeHist,
                false);   // unaligned
    }
    NotifySubscribers(
        &TRequestCounters::BatchCompleted,
        requestType,
        count,
        bytes,
        errors,
        timeHist,
        sizeHist);
}

void TRequestCounters::UpdateStats(bool updatePercentiles)
{
    for (auto& statCounters: CountersByRequest) {
        if (AtomicGet(statCounters.FullyInitialized)) {
            statCounters.UpdateStats(updatePercentiles);
        }
    }
    // NOTE subscribers are updated by their owners
}

void TRequestCounters::RequestStartedImpl(
    TRequestType requestType,
    ui64 requestBytes)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).Started(requestBytes);
    }
    NotifySubscribers(
        &TRequestCounters::RequestStartedImpl,
        requestType,
        requestBytes);
}

void TRequestCounters::RequestCompletedImpl(
    TRequestType requestType,
    TDuration totalTime,
    TDuration completionTime,
    TDuration time,
    TDuration execTime,
    TDuration postponedTime,
    TDuration backoffTime,
    TDuration shapingTime,
    ui64 requestBytes,
    EDiagnosticsErrorKind errorKind,
    ui32 errorFlags,
    bool unaligned,
    ECalcMaxTime calcMaxTime)
{
    if (SpecialCounters) {
        SpecialCounters->AddStats(errorKind, errorFlags);
    }

    if (ShouldReport(requestType)) {
        auto& statCounters = AccessRequestStats(requestType);
        statCounters.Completed(requestBytes);
        statCounters.AddStats(
            totalTime,
            completionTime,
            time,
            execTime,
            postponedTime,
            backoffTime,
            shapingTime,
            requestBytes,
            errorKind,
            unaligned,
            calcMaxTime);
    }
    NotifySubscribers(
        &TRequestCounters::RequestCompletedImpl,
        requestType,
        totalTime,
        completionTime,
        time,
        execTime,
        postponedTime,
        backoffTime,
        shapingTime,
        requestBytes,
        errorKind,
        errorFlags,
        unaligned,
        calcMaxTime);
}

bool TRequestCounters::ShouldReport(TRequestType requestType) const
{
    if (Options & EOption::DisaggregatedCountersDisabled) {
        return false;
    }

    if (requestType >= CountersByRequest.size()) {
        return false;
    }

    if (Options & EOption::OnlyReadWriteRequests) {
        return IsReadWriteRequestType(requestType);
    }

    if (Options & EOption::OnlyStartEndpointRequests) {
        return IsStartEndpointRequestType(requestType);
    }

    return true;
}

template <typename TMethod, typename... TArgs>
void TRequestCounters::NotifySubscribers(TMethod&& m, TArgs&&... args)
{
    for (auto& s: Subscribers) {
        (s.get()->*m)(std::forward<TArgs>(args)...);
    }
}

}   // namespace NYdb::NBS
