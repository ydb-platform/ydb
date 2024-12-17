#pragma once

///
/// @file yt/cpp/mapreduce/interface/job_statistics.h
///
/// Header containing classes and utility functions to work with
/// [job statistics](https://ytsaurus.tech/docs/ru/user-guide/problems/jobstatistics).

#include "fwd.h"

#include <library/cpp/yson/node/node.h>

#include <util/system/defaults.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Convert i64 representation of statistics to other type.
///
/// Library defines this template for types TDuration and i64.
/// Users may define it for their types.
///
/// @see @ref NYT::TJobStatistics::GetStatisticsAs method.
template <typename T>
T ConvertJobStatisticsEntry(i64 value);

////////////////////////////////////////////////////////////////////////////////

/// Class representing a collection of job statistics.
class TJobStatistics
{
public:
    ///
    /// Construct empty statistics.
    TJobStatistics();

    ///
    /// Construct statistics from statistics node.
    TJobStatistics(const NYT::TNode& statistics);

    TJobStatistics(const TJobStatistics& jobStatistics);
    TJobStatistics(TJobStatistics&& jobStatistics);

    TJobStatistics& operator=(const TJobStatistics& jobStatistics);
    TJobStatistics& operator=(TJobStatistics&& jobStatistics);

    ~TJobStatistics();

    ///
    /// @brief Filter statistics by task name.
    ///
    /// @param taskNames What task names to include (empty means all).
    TJobStatistics TaskName(TVector<TTaskName> taskNames) const;

    ///
    /// @brief Filter statistics by job state.
    ///
    /// @param filter What job states to include (empty means all).
    ///
    /// @note Default statistics include only (successfully) completed jobs.
    TJobStatistics JobState(TVector<EJobState> filter) const;

    ///
    /// @brief Filter statistics by job type.
    ///
    /// @param filter What job types to include (empty means all).
    ///
    /// @deprecated Use @ref TJobStatistics::TaskName instead.
    ///
    /// @see https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/jobs#obshaya-shema
    TJobStatistics JobType(TVector<EJobType> filter) const;

    ///
    /// @brief Check that given statistics exist.
    ///
    /// @param name Slash separated statistics name, e.g. "time/total" (like it appears in web interface).
    bool HasStatistics(TStringBuf name) const;

    ///
    /// @brief Get statistics by name.
    ///
    /// @param name Slash separated statistics name, e.g. "time/total" (like it appears in web interface).
    ///
    /// @note If statistics is missing an exception is thrown. If because of filters
    /// no fields remain the returned value is empty (all fields are `Nothing`).
    ///
    /// @note We don't use `TMaybe<TJobStatisticsEntry>` here;
    /// instead, @ref NYT::TJobStatisticsEntry methods return `TMaybe<i64>`,
    /// so user easier use `.GetOrElse`:
    /// ```
    ///     jobStatistics.GetStatistics("some/statistics/name").Max().GetOrElse(0);
    /// ```
    TJobStatisticsEntry<i64> GetStatistics(TStringBuf name) const;

    ///
    /// @brief Get statistics by name.
    ///
    /// @param name Slash separated statistics name, e.g. "time/total" (like it appears in web interface).
    ///
    /// @note In order to use `GetStatisticsAs` method, @ref NYT::ConvertJobStatisticsEntry function must be defined
    /// (the library defines it for `i64` and `TDuration`, user may define it for other types).
    template <typename T>
    TJobStatisticsEntry<T> GetStatisticsAs(TStringBuf name) const;

    ///
    /// Get (slash separated) names of statistics.
    TVector<TString> GetStatisticsNames() const;

    ///
    /// @brief Check if given custom statistics exists.
    ///
    /// @param name Slash separated custom statistics name.
    bool HasCustomStatistics(TStringBuf name) const;

    ///
    /// @brief Get custom statistics (those the user can write in job with @ref NYT::WriteCustomStatistics).
    ///
    /// @param name Slash separated custom statistics name.
    TJobStatisticsEntry<i64> GetCustomStatistics(TStringBuf name) const;

    ///
    /// @brief Get custom statistics (those the user can write in job with @ref NYT::WriteCustomStatistics).
    ///
    /// @param name Slash separated custom statistics name.
    template <typename T>
    TJobStatisticsEntry<T> GetCustomStatisticsAs(TStringBuf name) const;

    ///
    /// Get names of all custom statistics.
    TVector<TString> GetCustomStatisticsNames() const;

private:
    class TData;
    struct TFilter;

    struct TDataEntry {
        i64 Max;
        i64 Min;
        i64 Sum;
        i64 Count;
    };

    static const TString CustomStatisticsNamePrefix_;

private:
    TJobStatistics(::TIntrusivePtr<TData> data, ::TIntrusivePtr<TFilter> filter);

    TMaybe<TDataEntry> GetStatisticsImpl(TStringBuf name) const;

private:
    ::TIntrusivePtr<TData> Data_;
    ::TIntrusivePtr<TFilter> Filter_;

private:
    template<typename T>
    friend class TJobStatisticsEntry;
};

////////////////////////////////////////////////////////////////////////////////

/// Class representing single statistic.
template <typename T>
class TJobStatisticsEntry
{
public:
    TJobStatisticsEntry(TMaybe<TJobStatistics::TDataEntry> data)
        : Data_(std::move(data))
    { }

    /// Sum of the statistic over all jobs.
    TMaybe<T> Sum() const
    {
        if (Data_) {
            return ConvertJobStatisticsEntry<T>(Data_->Sum);
        }
        return Nothing();
    }

    /// @brief Average of the statistic over all jobs.
    ///
    /// @note Only jobs that emitted statistics are taken into account.
    TMaybe<T> Avg() const
    {
        if (Data_ && Data_->Count) {
            return ConvertJobStatisticsEntry<T>(Data_->Sum / Data_->Count);
        }
        return Nothing();
    }

    /// @brief Number of jobs that emitted this statistic.
    TMaybe<T> Count() const
    {
        if (Data_) {
            return ConvertJobStatisticsEntry<T>(Data_->Count);
        }
        return Nothing();
    }

    /// @brief Maximum value of the statistic over all jobs.
    TMaybe<T> Max() const
    {
        if (Data_) {
            return ConvertJobStatisticsEntry<T>(Data_->Max);
        }
        return Nothing();
    }

    /// @brief Minimum value of the statistic over all jobs.
    TMaybe<T> Min() const
    {
        if (Data_) {
            return ConvertJobStatisticsEntry<T>(Data_->Min);
        }
        return Nothing();
    }

private:
    TMaybe<TJobStatistics::TDataEntry> Data_;

private:
    friend class TJobStatistics;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TJobStatisticsEntry<T> TJobStatistics::GetStatisticsAs(TStringBuf name) const
{
    return TJobStatisticsEntry<T>(GetStatisticsImpl(name));
}

template <typename T>
TJobStatisticsEntry<T> TJobStatistics::GetCustomStatisticsAs(TStringBuf name) const
{
    return TJobStatisticsEntry<T>(GetStatisticsImpl(CustomStatisticsNamePrefix_ + name));
}

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Write [custom statistics](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/jobs#user_stats).
///
/// @param path Slash-separated path (length must not exceed 512 bytes).
/// @param value Value of the statistic.
///
/// @note The function must be called in job.
/// Total number of statistics (with different paths) must not exceed 128.
void WriteCustomStatistics(TStringBuf path, i64 value);

///
/// @brief Write several [custom statistics](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/jobs#user_stats) at once.
///
/// @param statistics A tree of map nodes with leaves of type `i64`.
///
/// @note The call is equivalent to calling @ref NYT::WriteCustomStatistics(TStringBuf, i64) for every path in the given map.
void WriteCustomStatistics(const TNode& statistics);

///
/// @brief Flush [custom statistics stream](https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/jobs#user_stats)
///
void FlushCustomStatisticsStream();
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
