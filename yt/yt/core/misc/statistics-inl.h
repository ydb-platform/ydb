#ifndef STATISTICS_INL_H_
#error "Direct inclusion of this file is not allowed, include statistics.h"
// For the sake of sane code completion.
#include "statistics.h"
#endif

#include "statistic_path.h"

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TStatistics::AddSample(const NStatisticPath::TStatisticPath& path, const T& sample)
{
    AddSample(path, NYTree::ConvertToNode(sample));
}

template <class T>
void TStatistics::ReplacePathWithSample(const NStatisticPath::TStatisticPath& path, const T& sample)
{
    ReplacePathWithSample(path, NYTree::ConvertToNode(sample));
}

////////////////////////////////////////////////////////////////////////////////

/*! Checks if the existing statistics in TSummaryMap are compatible with a statistic
 * at |path|. Returns a pair of the conflict type (has a prefix in existing
 * statistics, is a prefix of an existing statistic, or no conflict at all) and
 * an iterator. If there is a conflict, iterator points to the conflicting statistic,
 * otherwise it is a hint. Assumes that the |existingStatistics|
 * are compatible.
 */
template <typename TSummaryMap>
std::pair<EStatisticPathConflictType, typename TSummaryMap::iterator> IsCompatibleStatistic(
    TSummaryMap& existingStatistics,
    const NStatisticPath::TStatisticPath& path)
{
    auto it = existingStatistics.lower_bound(path);
    if (it != existingStatistics.end()) {
        if (it->first == path) {
            return {EStatisticPathConflictType::Exists, it};
        }
        // TODO(pavook) std::ranges::starts_with(it->first, path) when C++23 arrives.
        if (it->first.StartsWith(path)) {
            return {EStatisticPathConflictType::IsPrefix, it};
        }
    }
    if (it != existingStatistics.begin()) {
        auto prev = std::prev(it);
        // TODO(pavook) std::ranges::starts_with(path, prev->first) when C++23 arrives.
        if (path.StartsWith(prev->first)) {
            return {EStatisticPathConflictType::HasPrefix, prev};
        }
    }
    return {EStatisticPathConflictType::None, it};
}

////////////////////////////////////////////////////////////////////////////////

//! Tries to emplace statistic into TSummaryMap, and checks if it is valid and compatible.
template <typename TSummaryMap, typename... Ts>
std::pair<typename TSummaryMap::iterator, bool> CheckedEmplaceStatistic(
    TSummaryMap& existingStatistics,
    const NStatisticPath::TStatisticPath& path,
    Ts&&... args)
{
    auto [conflictType, hintIt] = IsCompatibleStatistic(existingStatistics, path);
    if (conflictType == EStatisticPathConflictType::Exists) {
        return {hintIt, false};
    }
    if (conflictType != EStatisticPathConflictType::None) {
        auto prefixPath = hintIt->first;
        auto conflictPath = path;

        if (conflictType == EStatisticPathConflictType::IsPrefix) {
            std::swap(prefixPath, conflictPath);
        }

        THROW_ERROR_EXCEPTION("Statistic path cannot be a prefix of another statistic path")
            << TErrorAttribute("prefix_path", prefixPath)
            << TErrorAttribute("contained_in_path", conflictPath);
    }
    auto emplacedIt = existingStatistics.emplace_hint(hintIt, path, std::forward<Ts>(args)...);
    return {emplacedIt, true};
}

////////////////////////////////////////////////////////////////////////////////

template <class TTags>
void TTaggedStatistics<TTags>::AppendStatistics(const TStatistics& statistics, TTags tags)
{
    for (const auto& [path, summary] : statistics.Data()) {
        auto [emplacedIterator, _] = CheckedEmplaceStatistic(Data_, path, TTaggedSummaries{});
        auto& pathSummaries = emplacedIterator->second;
        auto it = pathSummaries.find(tags);
        if (it == pathSummaries.end()) {
            pathSummaries.emplace(tags, summary);
        } else {
            it->second.Merge(summary);
        }
    }
}

template <class TTags>
void TTaggedStatistics<TTags>::AppendTaggedSummary(const NStatisticPath::TStatisticPath& path, const TTaggedStatistics<TTags>::TTaggedSummaries& taggedSummaries)
{
    auto [taggedSummariesIt, emplaceHappened] = CheckedEmplaceStatistic(Data_, path, taggedSummaries);
    if (emplaceHappened) {
        return;
    }

    auto& currentTaggedSummaries = taggedSummariesIt->second;
    for (const auto& [tags, summary] : taggedSummaries) {
        if (auto summaryIt = currentTaggedSummaries.find(tags); summaryIt == currentTaggedSummaries.end()) {
            currentTaggedSummaries.insert(std::pair(tags, summary));
        } else {
            summaryIt->second.Merge(summary);
        }
    }
}

template <class TTags>
const THashMap<TTags, TSummary>* TTaggedStatistics<TTags>::FindTaggedSummaries(const NStatisticPath::TStatisticPath& path) const
{
    auto it = Data_.find(path);
    if (it != Data_.end()) {
        return &it->second;
    }
    return nullptr;
}

template <class TTags>
const typename TTaggedStatistics<TTags>::TSummaryMap& TTaggedStatistics<TTags>::GetData() const
{
    return Data_;
}

template <class TTags>
void TTaggedStatistics<TTags>::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Data_);
}

////////////////////////////////////////////////////////////////////////////////

template <class TTags>
void Serialize(const TTaggedStatistics<TTags>& statistics, NYson::IYsonConsumer* consumer)
{
    SerializeStatisticPathsMap<THashMap<TTags, TSummary>>(
        statistics.GetData(),
        consumer,
        [] (const THashMap<TTags, TSummary>& summaries, NYson::IYsonConsumer* consumer) {
            NYTree::BuildYsonFluently(consumer)
                .DoListFor(summaries, [] (NYTree::TFluentList fluentList, const auto& pair) {
                    fluentList.Item()
                        .BeginMap()
                            .Item("tags").Value(pair.first)
                            .Item("summary").Value(pair.second)
                        .EndMap();
                });
        });
}

////////////////////////////////////////////////////////////////////////////////

template <class TMapValue>
void SerializeStatisticPathsMap(
    const std::map<NStatisticPath::TStatisticPath, TMapValue>& map,
    NYson::IYsonConsumer* consumer,
    const std::function<void(const TMapValue&, NYson::IYsonConsumer*)>& valueSerializer)
{
    using NYT::Serialize;

    // Global map.
    consumer->OnBeginMap();

    // Depth of the previous key defined as a number of nested maps before the summary itself.
    size_t previousDepth = 0;
    NStatisticPath::TStatisticPath previousPath;
    for (const auto& [currentPath, value] : map) {
        auto enumeratedCurrent = Enumerate(currentPath);
        auto enumeratedPrevious = Enumerate(previousPath);

        auto [currentIt, previousIt] = std::ranges::mismatch(enumeratedCurrent, enumeratedPrevious);

        // No statistic path should be a path-prefix of another statistic path.
        YT_VERIFY(currentIt != enumeratedCurrent.end());

        // The depth of the common part of two keys, needed to determine the number of maps to close.
        size_t commonDepth = std::get<0>(*currentIt);

        // Close the maps that need to be closed.
        while (previousDepth > commonDepth) {
            consumer->OnEndMap();
            --previousDepth;
        }

        // Open all newly appeared maps.
        size_t currentDepth = commonDepth;
        auto beginIt = currentIt;
        auto endIt = enumeratedCurrent.end();
        for (; currentIt != endIt; ++currentIt) {
            if (currentIt != beginIt) {
                consumer->OnBeginMap();
                ++currentDepth;
            }
            consumer->OnKeyedItem(std::get<1>(*currentIt));
        }

        // Serialize value.
        valueSerializer(value, consumer);

        previousDepth = currentDepth;
        previousPath = currentPath;
    }
    while (previousDepth > 0) {
        consumer->OnEndMap();
        --previousDepth;
    }

    // This OnEndMap is complementary to the OnBeginMap before the main loop.
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
