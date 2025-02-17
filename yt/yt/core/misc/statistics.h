#pragma once

#include "public.h"

#include <yt/yt/core/yson/forwarding_consumer.h>
#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/building_consumer.h>

#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/property.h>

#include <util/generic/iterator_range.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSummary
{
public:
    TSummary();

    TSummary(i64 sum, i64 count, i64 min, i64 max, std::optional<i64> last);

    void AddSample(i64 sample);

    void Merge(const TSummary& summary);

    void Reset();

    DEFINE_BYVAL_RO_PROPERTY(i64, Sum);
    DEFINE_BYVAL_RO_PROPERTY(i64, Count);
    DEFINE_BYVAL_RO_PROPERTY(i64, Min);
    DEFINE_BYVAL_RO_PROPERTY(i64, Max);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<i64>, Last);

    void Persist(const TStreamPersistenceContext& context);

    bool operator == (const TSummary& other) const;

    friend class TStatisticsBuildingConsumer;
};

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TStatistics
{
public:
    using TSummaryMap = std::map<NStatisticPath::TStatisticPath, TSummary>;
    using TSummaryRange = TIteratorRange<TSummaryMap::const_iterator>;
    DEFINE_BYREF_RO_PROPERTY(TSummaryMap, Data);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TInstant>, Timestamp);

public:
    void AddSample(const NStatisticPath::TStatisticPath& path, i64 sample);

    void AddSample(const NStatisticPath::TStatisticPath& path, const NYTree::INodePtr& sample);

    template <class T>
    void AddSample(const NStatisticPath::TStatisticPath& path, const T& sample);

    void ReplacePathWithSample(const NStatisticPath::TStatisticPath& path, i64 sample);

    void ReplacePathWithSample(const NStatisticPath::TStatisticPath& path, const NYTree::INodePtr& sample);

    template <class T>
    void ReplacePathWithSample(const NStatisticPath::TStatisticPath& path, const T& sample);

    //! Merge statistics by merging summaries for each common statistics path.
    void Merge(const TStatistics& statistics);
    //! Merge statistics by taking summary from #statistics for each common statistics path.
    void MergeWithOverride(const TStatistics& statistics);

    //! Get range of all elements whose path starts with a given strict prefix path (possibly empty).
    /*!
     * Pre-requisites: `prefixPath` must not have terminating slash.
     * Examples: /a/b is a prefix path for /a/b/hij but not for /a/bcd/efg nor /a/b itself.
     */
    TSummaryRange GetRangeByPrefix(const NStatisticPath::TStatisticPath& prefixPath) const;

    //! Remove all the elements starting from prefixPath.
    //! The requirements for prefixPath are the same as in GetRangeByPrefix.
    void RemoveRangeByPrefix(const NStatisticPath::TStatisticPath& prefixPath);

    void Persist(const TStreamPersistenceContext& context);

private:
    template <class TCallback>
    void ProcessNodeWithCallback(const NStatisticPath::TStatisticPath& path, const NYTree::INodePtr& sample, TCallback callback);

    TSummary& GetSummary(const NStatisticPath::TStatisticPath& path);

    friend class TStatisticsBuildingConsumer;
};

i64 GetNumericValue(const TStatistics& statistics, const NStatisticPath::TStatisticPath& path);

std::optional<i64> FindNumericValue(const TStatistics& statistics, const NStatisticPath::TStatisticPath& path);
std::optional<TSummary> FindSummary(const TStatistics& statistics, const NStatisticPath::TStatisticPath& path);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStatisticPathConflictType,
    (HasPrefix)
    (IsPrefix)
    (Exists)
    (None)
);

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer);

void CreateBuildingYsonConsumer(std::unique_ptr<NYson::IBuildingYsonConsumer<TStatistics>>* buildingConsumer, NYson::EYsonType ysonType);

////////////////////////////////////////////////////////////////////////////////

class TStatisticsConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    using TSampleHandler = TCallback<void(const NYTree::INodePtr& sample)>;
    explicit TStatisticsConsumer(TSampleHandler consumer);

private:
    const std::unique_ptr<NYTree::ITreeBuilder> TreeBuilder_;
    const TSampleHandler SampleHandler_;

    void OnMyListItem() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TTags>
class TTaggedStatistics
{
public:
    using TTaggedSummaries = THashMap<TTags, TSummary>;
    using TSummaryMap = std::map<NStatisticPath::TStatisticPath, TTaggedSummaries>;

    void AppendStatistics(const TStatistics& statistics, TTags tags);
    void AppendTaggedSummary(const NStatisticPath::TStatisticPath& path, const TTaggedSummaries& taggedSummaries);

    const TTaggedSummaries* FindTaggedSummaries(const NStatisticPath::TStatisticPath& path) const;
    const TSummaryMap& GetData() const;

    void Persist(const TStreamPersistenceContext& context);

private:
    TSummaryMap Data_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TTags>
void Serialize(const TTaggedStatistics<TTags>& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
void SerializeStatisticPathsMap(
    const std::map<NStatisticPath::TStatisticPath, TValue>& map,
    NYson::IYsonConsumer* consumer,
    const std::function<void(const TValue&, NYson::IYsonConsumer*)>& valueSerializer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define STATISTICS_INL_H_
#include "statistics-inl.h"
#undef STATISTICS_INL_H_
