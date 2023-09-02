#pragma once

#include "public.h"

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/misc/range.h>

// TODO(lukyan): Checks denoted by YT_QL_CHECK are heavy. Change them to YT_ASSERT after some time.
#define YT_QL_CHECK(expr) YT_VERIFY(expr)

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TRow WidenKeySuccessor(TRow key, size_t prefix, const TRowBufferPtr& rowBuffer, bool captureValues);

TRow WidenKeySuccessor(TRow key, const TRowBufferPtr& rowBuffer, bool captureValues);

size_t GetSignificantWidth(TRow row);

size_t Step(size_t current, size_t source, size_t target);

struct TRangeFormatter
{
    void operator()(TStringBuilderBase* builder, TRowRange source) const;
};

using TRangeIt = TRange<TRowRange>::iterator;
using TSampleIt = TRange<TRow>::iterator;

// Ranges must be cropped before using ForEachRange

template <class T, class TCallback>
void ForEachRange(TRange<std::pair<T, T>> ranges, std::pair<T, T> limit, const TCallback& callback)
{
    YT_VERIFY(!ranges.Empty());

    auto it = ranges.begin();
    auto lower = limit.first;

    while (true) {
        auto next = it;
        ++next;

        if (next == ranges.end()) {
            break;
        }

        auto upper = it->second;
        callback(std::make_pair(lower, upper));

        it = next;
        lower = it->first;
    }

    auto upper = limit.second;
    callback(std::make_pair(lower, upper));
}

////////////////////////////////////////////////////////////////////////////////

template <class TItem, class TLess, class TNotGreater>
TRange<TItem> CropItems(TRange<TItem> items, TLess less, TNotGreater notGreater)
{
    auto itemsBegin = BinarySearch(items.begin(), items.end(), less);
    auto itemsEnd = BinarySearch(itemsBegin, items.end(), notGreater);
    return TRange<TItem>(itemsBegin, itemsEnd);
}

// TPredicate implements operator():

// operator(itemIt, pivot) ~ itemIt PRECEDES pivot:
// ranges: !(pivot < itemIt->upper)
// points: *itemIt < pivot

// operator(pivot, itemIt) ~ it FOLLOWS pivot:
// ranges, key: !(pivot < itemIt->lower)
// points: !(pivot < *itemIt)

// Properties:
// item PRECEDES shard ==> item NOT FOLLOWS shard
// item FOLLOWS shard ==> item NOT PRECEDES shard


// SplitByPivots does not repeat items in callbacks.

// For input:  [..) [..)   [.|..|..|..)
// OnItems     [..) [..)     |
// OnShards                [.|..|..|..)

// For input: [..) [..) [.|.) [..)  |
// OnItems    [..) [..)   |
// OnShards             [.|.)
// OnItems                    [..)  |

template <class TItem, class TShard, class TPredicate, class TOnItemsFunctor, class TOnShardsFunctor>
void SplitByPivots(
    TRange<TItem> items,
    TRange<TShard> shards,
    TPredicate pred,
    TOnItemsFunctor onItemsFunctor,
    TOnShardsFunctor onShardsFunctor)
{
    auto shardIt = shards.begin();
    auto itemIt = items.begin();

    while (itemIt != items.end()) {
        // Run binary search to find the relevant shards.
        // First shard such that !Follows(itemIt, shard)

        // First shard: item NOT FOLLOWS shard
        shardIt = ExponentialSearch(shardIt, shards.end(), [&] (auto it) {
            // For interval: *shardIt <= itemIt->lower
            // For points: *shardIt <= *itemIt
            return pred(it, itemIt); // item FOLLOWS shard
        });

        // For interval: itemIt->upper <= *shardIt
        // For points: *itemIt < shardIt is always true: *shardIt <= *itemIt ~ shardIt > *itemIt

        if (shardIt == shards.end()) {
            onItemsFunctor(itemIt, items.end(), shardIt);
            return;
        }

        // item PRECEDES shard
        if (pred(itemIt, shardIt)) { // PRECEDES
            // First item: item NOT PRECEDES shard
            auto nextItemsIt = ExponentialSearch(itemIt, items.end(), [&] (auto it) {
                // For interval: itemIt->upper <= *shardIt
                // For points: *itemIt < shardIt
                return pred(it, shardIt); // item PRECEDES shard
            });

            onItemsFunctor(itemIt, nextItemsIt, shardIt);
            itemIt = nextItemsIt;
        } else {
            // First shard: item PRECEDES shard
            auto endShardIt = ExponentialSearch(shardIt, shards.end(), [&] (auto it) {
                // For interval: !(itemIt->upper <= *shardIt) ~ *shardIt < itemIt->upper
                // For points: *itemIt < shardIt
                return !pred(itemIt, it); // item PRECEDES shard
            });

            onShardsFunctor(shardIt, endShardIt, itemIt);
            shardIt = endShardIt;
            ++itemIt;
        }
    }
}

// GroupByShards does not repeat shards in callbacks.

// For input:  [..|..|..|..) [..) [..) |
// OnShards    [..|..|..|  )
// OnItems     [         ..) [..) [..) |

// For input:  [..) [..|..|..|..) [..) |
// OnItems     [..) [..|        )
// OnShards         [   ..|..|  )
// OnItems          [         ..) [..) |

template <class TItem, class TShard, class TPredicate, class TGroupFunctor>
void GroupByShards(
    TRange<TItem> items,
    TRange<TShard> shards,
    TPredicate pred,
    TGroupFunctor onGroupFunctor)
{
    auto shardIt = shards.begin();
    auto itemIt = items.begin();

    while (itemIt != items.end()) {
        // Run binary search to find the relevant shards.

        // First shard: item NOT FOLLOWS shard
        auto shardItStart = ExponentialSearch(shardIt, shards.end(), [&] (auto it) {
            // For interval: *shardIt <= itemIt->lower
            // For points: *shardIt <= *itemIt
            return pred(it, itemIt); // item FOLLOWS shard
        });

        // pred(shardIt, itemIt)
        // !pred(itemIt, shardIt)
        // pred(itemIt, shardIt)
        // !pred(shardIt, itemIt)

        // For interval: itemIt->upper <= *shardIt
        // For points: *itemIt < shardIt is allways true: *shardIt <= *itemIt ~ shardIt > *itemIt

        // First shard: item PRECEDES shard
        shardIt = ExponentialSearch(shardItStart, shards.end(), [&] (auto it) {
            // For interval: !(itemIt->upper <= *shardIt) ~ *shardIt < itemIt->upper
            // For points: *itemIt < shardIt
            return !pred(itemIt, it); // item PRECEDES shard
        });

        if (shardIt != shards.end()) {
            // First item: item NOT PRECEDES shard
            auto itemsItNext = ExponentialSearch(itemIt, items.end(), [&] (auto it) {
                // For interval: itemIt->upper <= *shardIt
                // For points: *itemIt < shardIt
                return pred(it, shardIt); // item PRECEDES shard
            });

#if 0
            auto itemsItEnd = ExponentialSearch(itemsItNext, items.end(), [&] (auto it) {
                return !pred(shardIt, it); // item FOLLOWS shard
            });

            YT_VERIFY(itemsItNext == itemsItEnd || itemsItNext + 1 == itemsItEnd);
#else
            auto itemsItEnd = itemsItNext;
            if (itemsItEnd != items.end() && !pred(shardIt, itemsItEnd)) { // item FOLLOWS shard
                ++itemsItEnd;
            }
#endif

            onGroupFunctor(itemIt, itemsItEnd, shardItStart, shardIt);

            itemIt = itemsItNext;
            ++shardIt;

            // TODO(lukyan): Reduce comparisons.
            // There are three cases for the next iteration:
            // 0. [ ) [ | | | ) [ )    // itemsItNext != itemsItEnd
            // 1. [ ) [ | ) [ ) [ ) |  // itemsItNext != itemsItEnd
            // 2. [ ) | | | [ )        // itemsItNext == itemsItEnd
            // In cases 0 and 1 no need to call `auto shardItStart = ExponentialSearch`.

        } else {
            onGroupFunctor(itemIt, items.end(), shardItStart, shardIt);
            return;
        }
    }
}

template <class TItem, class TShard, class TPredicate, class TOnItemsFunctor>
void GroupItemsByShards(
    TRange<TItem> items,
    TRange<TShard> shards,
    TPredicate pred,
    TOnItemsFunctor onItemsFunctor)
{
    GroupByShards(
        items,
        shards,
        pred,
        [&] (auto itemsIt, auto itemsItEnd, auto shardIt, auto shardItEnd) {
            YT_VERIFY(itemsIt != itemsItEnd);
            // shardItEnd can invalid.
            while (shardIt != shardItEnd) {
                onItemsFunctor(shardIt++, itemsIt, itemsIt + 1);
            }

            onItemsFunctor(shardIt, itemsIt, itemsItEnd);
        });
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TRow GetPivotKey(const T& shard);

template <class T>
TRow GetNextPivotKey(const T& shard);

template <class T>
TRange<TRow> GetSampleKeys(const T& shard);

////////////////////////////////////////////////////////////////////////////////

template <class T, class TOnItemsFunctor>
void SplitRangesByTablets(
    TRange<TRowRange> ranges,
    TRange<T> tablets,
    TRow lowerCapBound,
    TRow upperCapBound,
    TOnItemsFunctor onItemsFunctor)
{
    using TShardIt = typename TRange<T>::iterator;

    struct TPredicate
    {
        TRow GetKey(TShardIt shardIt) const
        {
            return GetPivotKey(*shardIt);
        }

        // itemIt PRECEDES shardIt
        bool operator() (const TRowRange* itemIt, TShardIt shardIt) const
        {
            return itemIt->second <= GetKey(shardIt);
        }

        // itemIt FOLLOWS shardIt
        bool operator() (TShardIt shardIt, const TRowRange* itemIt) const
        {
            return GetKey(shardIt) <= itemIt->first;
        }
    };

    auto croppedRanges = CropItems(
        ranges,
        [&] (const TRowRange* itemIt) {
            return !(lowerCapBound < itemIt->second);
        },
        [&] (const TRowRange* itemIt) {
            return !(upperCapBound < itemIt->first);
        });

    YT_VERIFY(!tablets.Empty());

    GroupItemsByShards(
        croppedRanges,
        tablets.Slice(1, tablets.size()),
        TPredicate{},
        onItemsFunctor);
}

template <class T, class TOnItemsFunctor>
void SplitKeysByTablets(
    TRange<TRow> keys,
    size_t keyWidth,
    size_t fullKeySize,
    TRange<T> tablets,
    TRow lowerCapBound,
    TRow upperCapBound,
    TOnItemsFunctor onItemsFunctor)
{
    using TShardIt = typename TRange<T>::iterator;

    struct TPredicate
    {
        size_t KeySize;
        bool IsFullKey;

        TRow GetKey(TShardIt shardIt) const
        {
            return GetPivotKey(*shardIt);
        }

        bool Less(TRow lhs, TRow rhs) const
        {
            return CompareRows(lhs, rhs, KeySize) < 0;
        }

        bool LessOrEqual(TRow lhs, TRow rhs) const
        {
            return CompareRows(lhs, rhs, KeySize) < IsFullKey;
        }

        // itemIt PRECEDES shardIt
        bool operator() (const TRow* itemIt, TShardIt shardIt) const
        {
            return Less(*itemIt, GetKey(shardIt));
        }

        // itemIt FOLLOWS shardIt
        bool operator() (TShardIt shardIt, const TRow* itemIt) const
        {
            // Less?
            return LessOrEqual(GetKey(shardIt), *itemIt);
        }
    };

    auto croppedKeys = CropItems(
        keys,
        [&] (const TRow* itemIt) {
            return *itemIt < lowerCapBound;
        },
        [&] (const TRow* itemIt) {
            return !(upperCapBound < *itemIt);
        });

    YT_VERIFY(!tablets.Empty());

    GroupItemsByShards(
        croppedKeys,
        tablets.Slice(1, tablets.size()),
        TPredicate{keyWidth, keyWidth == fullKeySize},
        onItemsFunctor);
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TOnGroup>
void GroupRangesByPartition(TRange<TRowRange> ranges, TRange<T> partitions, const TOnGroup& onGroup)
{
    using TShardIt = typename TRange<T>::iterator;

    if (!ranges.Empty()) {
        TRow lower = GetPivotKey(partitions.Front());
        TRow upper = GetNextPivotKey(partitions.Back());

        YT_VERIFY(lower < ranges.Front().second);
        YT_VERIFY(ranges.Back().first < upper);
    }

    struct TPredicate
    {
        TRow GetKey(TShardIt shardIt) const
        {
            return GetNextPivotKey(*shardIt);
        }

        // itemIt PRECEDES shardIt
        bool operator() (TRangeIt itemIt, TShardIt shardIt) const
        {
            return itemIt->second <= GetKey(shardIt);
        }

        // itemIt FOLLOWS shardIt
        bool operator() (TShardIt shardIt, TRangeIt itemIt) const
        {
            return GetKey(shardIt) <= itemIt->first;
        }
    };

    GroupItemsByShards(ranges, partitions, TPredicate{}, onGroup);
}

template <class T>
std::vector<TSharedRange<TRowRange>> SplitTablet(
    TRange<T> partitions,
    TSharedRange<TRowRange> ranges,
    TRowBufferPtr rowBuffer,
    size_t maxSubsplitsPerTablet,
    bool verboseLogging,
    const NLogging::TLogger& Logger)
{
    using TShardIt = typename TRange<T>::iterator;
    using TItemIt = TRange<TRowRange>::iterator;

    struct TGroup
    {
        TShardIt PartitionIt;
        TItemIt BeginIt;
        TItemIt EndIt;
    };

    std::vector<TGroup> groupedByPartitions;

    GroupRangesByPartition(ranges, MakeRange(partitions), [&] (TShardIt shardIt, TItemIt itemIt, TItemIt itemItEnd) {
        YT_VERIFY(itemIt != itemItEnd);

        if (shardIt == partitions.end()) {
            YT_VERIFY(itemIt + 1 == ranges.end());
            return;
        }

        YT_VERIFY(groupedByPartitions.empty() || groupedByPartitions.back().PartitionIt != shardIt);
        groupedByPartitions.push_back(TGroup{shardIt, itemIt, itemItEnd});
    });

    struct TPredicate
    {
        TRow GetKey(const TRow* shardIt) const
        {
            return *shardIt;
        }

        // itemIt PRECEDES shardIt
        bool operator() (const TRowRange* itemIt, const TRow* shardIt) const
        {
            return itemIt->second <= GetKey(shardIt);
        }

        // itemIt FOLLOWS shardIt
        bool operator() (const TRow* shardIt, const TRowRange* itemIt) const
        {
            return GetKey(shardIt) <= itemIt->first;
        }
    };

    size_t allShardCount = 0;

    // Calculate touched shards (partitions an) count.
    for (auto [partitionIt, beginIt, endIt] : groupedByPartitions) {
        GroupByShards(
            MakeRange(beginIt, endIt),
            GetSampleKeys(*partitionIt),
            TPredicate{},
            [&] (TRangeIt /*rangesIt*/, TRangeIt /*rangesItEnd*/, TSampleIt sampleIt, TSampleIt sampleItEnd) {
                allShardCount += 1 + std::distance(sampleIt, sampleItEnd);
            });
    }
    size_t targetSplitCount = std::min(maxSubsplitsPerTablet, allShardCount);

    YT_VERIFY(targetSplitCount > 0);

    YT_LOG_DEBUG_IF(verboseLogging, "AllShardCount: %v, TargetSplitCount: %v",
        allShardCount,
        targetSplitCount);

    std::vector<TSharedRange<TRowRange>> groupedSplits;
    std::vector<TRowRange> group;

    auto holder = MakeSharedRangeHolder(ranges.GetHolder(), rowBuffer);

    size_t currentShardCount = 0;
    size_t lastSampleCount = 0;
    auto addGroup = [&] (size_t count) {
        YT_VERIFY(!group.empty());

        size_t nextStep = Step(currentShardCount, allShardCount, targetSplitCount);

        YT_VERIFY(count <= nextStep);
        YT_VERIFY(currentShardCount  <= allShardCount);

        currentShardCount += count;

        if (count == nextStep) {
            YT_LOG_DEBUG_IF(verboseLogging, "(%v, %v) make batch [%v .. %v] from %v ranges",
                lastSampleCount,
                currentShardCount,
                group.front().first,
                group.back().second,
                group.size());

            for (size_t i = 0; i + 1 < group.size(); ++i) {
                YT_QL_CHECK(group[i].second <= group[i + 1].first);
            }

            for (size_t i = 0; i < group.size(); ++i) {
                YT_QL_CHECK(group[i].first < group[i].second);
            }

            groupedSplits.push_back(MakeSharedRange(std::move(group), holder));
            lastSampleCount = currentShardCount;
        }
    };

    for (auto [partitionIt, beginIt, endIt] : groupedByPartitions) {
        const auto& partition = *partitionIt;
        TRowRange partitionBounds(GetPivotKey(partition), GetNextPivotKey(partition));

        YT_LOG_DEBUG_IF(verboseLogging, "Iterating over partition %v: [%v .. %v]",
            partitionBounds,
            beginIt - begin(ranges),
            endIt - begin(ranges));


        auto slice = MakeRange(beginIt, endIt);

        // Do not need to crop. Already cropped in GroupRangesByPartition.

        auto minBound = std::max<TRow>(slice.Front().first, rowBuffer->CaptureRow(GetPivotKey(partition)));
        auto maxBound = std::min<TRow>(slice.Back().second, rowBuffer->CaptureRow(GetNextPivotKey(partition)));

        auto samples = GetSampleKeys(partition);

        TRangeIt rangesItLast = nullptr;

        GroupByShards(
            slice,
            samples,
            TPredicate{},
            [&] (TRangeIt rangesIt, TRangeIt rangesItEnd, TSampleIt sampleIt, TSampleIt sampleItEnd) {
                YT_VERIFY(rangesIt != rangesItEnd);

                if (sampleIt != sampleItEnd) {
                    TRow start = minBound;
                    if (rangesIt == rangesItLast) {
                        if (sampleIt != samples.begin()) {
                            start = *(sampleIt - 1);
                        }
                    } else {
                        if (rangesIt != slice.begin()) {
                            start = rangesIt->first;
                        }
                    }

                    {
                        auto upper = rangesIt + 1 == slice.end() ? maxBound : rangesIt->second;
                        YT_QL_CHECK(*(sampleItEnd - 1) <= upper);
                    }

                    auto currentBound = start;

                    while (sampleIt != sampleItEnd) {
                        size_t nextStep = std::min<size_t>(
                            Step(currentShardCount, allShardCount, targetSplitCount),
                            sampleItEnd - sampleIt);
                        YT_VERIFY(nextStep > 0);

                        sampleIt += nextStep - 1;

                        auto nextBound = rowBuffer->CaptureRow(*sampleIt);
                        YT_QL_CHECK(currentBound < nextBound);
                        group.emplace_back(currentBound, nextBound);

                        addGroup(nextStep);
                        currentBound = nextBound;
                        ++sampleIt;
                    }
                }

                // TODO: Capture *sampleIt ?
                auto lower = sampleIt == samples.begin() ? minBound : *(sampleIt - 1);
                auto upper = sampleIt == samples.end() ? maxBound : *sampleIt;

                lower = std::max<TRow>(lower, rangesIt->first);
                upper = std::min<TRow>(upper, (rangesItEnd - 1)->second);

                ForEachRange(MakeRange(rangesIt, rangesItEnd), TRowRange(lower, upper), [&] (auto item) {
                    group.push_back(item);
                });

                addGroup(1);

                rangesItLast = rangesItEnd - 1;
            });
    }

    YT_VERIFY(currentShardCount == allShardCount);

    return groupedSplits;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
