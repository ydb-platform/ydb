#include "key_trie.h"
#include "query_helpers.h"

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <deque>
#include <tuple>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TKeyTriePtr ReduceKeyTrie(TKeyTriePtr keyTrie)
{
    // TODO(lukyan): If keyTrie is too big, reduce its size
    return keyTrie;
}

struct TKeyTrieComparer
{
    bool operator () (const std::pair<TValue, TKeyTriePtr>& element, TValue pivot) const
    {
        return element.first < pivot;
    }

    bool operator () (TValue pivot, const std::pair<TValue, TKeyTriePtr>& element) const
    {
        return pivot < element.first;
    }

    bool operator () (const std::pair<TValue, TKeyTriePtr>& lhs, const std::pair<TValue, TKeyTriePtr>& rhs) const
    {
        return lhs.first < rhs.first;
    }
};

int CompareBound(const TBound& lhs, const TBound& rhs, bool lhsDir, bool rhsDir)
{
    auto rank = [] (bool direction, bool included) {
        // <  - (false, fasle)
        // >  - (true, false)
        // <= - (false, true)
        // >= - (true, true)

        // (< x) < (>= x) < (<= x) < (> x)
        return (included ? -1 : 2) * (direction ? 1 : -1);
    };

    int result = CompareRowValues(lhs.Value, rhs.Value);
    return result == 0
        ? rank(lhsDir, lhs.Included) - rank(rhsDir, rhs.Included)
        : result;
}

template <class TEachCallback>
void MergeBounds(const std::vector<TBound>& lhs, const std::vector<TBound>& rhs, TEachCallback eachCallback)
{
    auto first = lhs.begin();
    auto second = rhs.begin();

    bool firstIsOpen = true;
    bool secondIsOpen = true;

    while (first != lhs.end() && second != rhs.end()) {
        if (CompareBound(*first, *second, firstIsOpen, secondIsOpen) < 0) {
            eachCallback(*first, firstIsOpen);
            ++first;
            firstIsOpen = !firstIsOpen;
        } else {
            eachCallback(*second, secondIsOpen);
            ++second;
            secondIsOpen = !secondIsOpen;
        }
    }

    while (first != lhs.end()) {
        eachCallback(*first, firstIsOpen);
        ++first;
        firstIsOpen = !firstIsOpen;
    }

    while (second != rhs.end()) {
        eachCallback(*second, secondIsOpen);
        ++second;
        secondIsOpen = !secondIsOpen;
    }
}

std::vector<TBound> UniteBounds(const std::vector<TBound>& lhs, const std::vector<TBound>& rhs)
{
    int cover = 0;
    std::vector<TBound> result;
    bool resultIsOpen = false;

    MergeBounds(lhs, rhs, [&] (TBound bound, bool isOpen) {
        if ((isOpen ? cover++ : --cover) == 0) {
            if (result.empty() || !(result.back() == bound && isOpen == resultIsOpen)) {
                result.push_back(bound);
                resultIsOpen = !resultIsOpen;
            }
        }
    });

    return result;
}

void UniteBounds(std::vector<std::vector<TBound>>* bounds)
{
    while (bounds->size() > 1) {
        size_t i = 0;
        while (2 * i + 1 < bounds->size()) {
            (*bounds)[i] = UniteBounds((*bounds)[2 * i], (*bounds)[2 * i + 1]);
            ++i;
        }
        if (2 * i < bounds->size()) {
            (*bounds)[i] = (*bounds)[2 * i];
            ++i;
        }
        bounds->resize(i);
    }
}

std::vector<TBound> IntersectBounds(const std::vector<TBound>& lhs, const std::vector<TBound>& rhs)
{
    int cover = 0;
    std::vector<TBound> result;
    bool resultIsOpen = false;

    MergeBounds(lhs, rhs, [&] (TBound bound, bool isOpen) {
        if ((isOpen ? cover++ : --cover) == 1) {
            if (result.empty() || !(result.back() == bound && isOpen == resultIsOpen)) {
                result.push_back(bound);
                resultIsOpen = !resultIsOpen;
            }
        }
    });

    return result;
}

TKeyTriePtr UniteKeyTrie(const std::vector<TKeyTriePtr>& tries)
{
    if (tries.empty()) {
        return TKeyTrie::Empty();
    } else if (tries.size() == 1) {
        return tries.front();
    }

    std::vector<TKeyTriePtr> maxTries;
    size_t offset = 0;
    for (const auto& trie : tries) {
        if (!trie) {
            return TKeyTrie::Universal();
        }

        if (trie->Offset > offset) {
            maxTries.clear();
            offset = trie->Offset;
        }

        if (trie->Offset == offset) {
            maxTries.push_back(trie);
        }
    }

    std::vector<std::pair<TValue, TKeyTriePtr>> groups;
    for (const auto& trie : maxTries) {
        for (auto& next : trie->Next) {
            groups.push_back(std::move(next));
        }
    }

    std::sort(groups.begin(), groups.end(), TKeyTrieComparer());

    auto result = New<TKeyTrie>(offset);
    std::vector<TKeyTriePtr> unique;

    auto it = groups.begin();
    auto end = groups.end();
    while (it != end) {
        unique.clear();
        auto same = it;
        for (; same != end && same->first == it->first; ++same) {
            unique.push_back(same->second);
        }
        result->Next.emplace_back(it->first, UniteKeyTrie(unique));
        it = same;
    }

    std::vector<std::vector<TBound>> bounds;
    for (const auto& trie : maxTries) {
        if (!trie->Bounds.empty()) {
            bounds.push_back(std::move(trie->Bounds));
        }
    }

    UniteBounds(&bounds);

    YT_VERIFY(bounds.size() <= 1);
    if (!bounds.empty()) {
        std::vector<TBound> deletedPoints;

        deletedPoints.emplace_back(MakeUnversionedSentinelValue(EValueType::Min), true);
        for (const auto& next : result->Next) {
            deletedPoints.emplace_back(next.first, false);
            deletedPoints.emplace_back(next.first, false);
        }
        deletedPoints.emplace_back(MakeUnversionedSentinelValue(EValueType::Max), true);

        result->Bounds = IntersectBounds(bounds.front(), deletedPoints);
    }

    return result;
}

TKeyTriePtr UniteKeyTrie(TKeyTriePtr lhs, TKeyTriePtr rhs)
{
    return UniteKeyTrie({lhs, rhs});
}

bool Covers(const std::vector<TBound>& bounds, const TValue& point)
{
    YT_VERIFY(!(bounds.size() & 1));

    auto index = BinarySearch(
        0,
        bounds.size() / 2,
        [&] (int index) -> bool {
            const auto& bound = bounds[index * 2 + 1];
            return bound.Value == point ? !bound.Included : bound.Value < point;
        });

    if (index < bounds.size() / 2) {
        const auto& bound = bounds[index * 2];
        return bound.Value == point ? bound.Included : bound.Value < point;
    } else {
        return false;
    }
}

TKeyTriePtr IntersectKeyTrie(TKeyTriePtr lhs, TKeyTriePtr rhs)
{
    auto lhsOffset = lhs ? lhs->Offset : std::numeric_limits<size_t>::max();
    auto rhsOffset = rhs ? rhs->Offset : std::numeric_limits<size_t>::max();

    if (lhsOffset < rhsOffset) {
        auto result = New<TKeyTrie>(*lhs);
        for (auto& next : result->Next) {
            next.second = IntersectKeyTrie(next.second, rhs);
        }
        return result;
    }

    if (lhsOffset > rhsOffset) {
        auto result = New<TKeyTrie>(*rhs);
        for (auto& next : result->Next) {
            next.second = IntersectKeyTrie(next.second, lhs);
        }
        return result;
    }

    if (!lhs && !rhs) {
        return nullptr;
    }

    YT_VERIFY(lhs);
    YT_VERIFY(rhs);

    auto result = New<TKeyTrie>(lhs->Offset);
    result->Bounds = IntersectBounds(lhs->Bounds, rhs->Bounds);

    // Iterate through resulting bounds and convert singleton ranges into
    // new edges in the trie. This enables futher range limiting.
    auto it = result->Bounds.begin();
    auto jt = result->Bounds.begin();
    auto kt = result->Bounds.end();
    while (it < kt) {
        const auto& lhs = *it++;
        const auto& rhs = *it++;
        if (lhs == rhs) {
            result->Next.emplace_back(lhs.Value, TKeyTrie::Universal());
        } else {
            if (std::distance(jt, it) > 2) {
                *jt++ = lhs;
                *jt++ = rhs;
            } else {
                ++jt; ++jt;
            }
        }
    }

    result->Bounds.erase(jt, kt);

    for (const auto& next : lhs->Next) {
        if (Covers(rhs->Bounds, next.first)) {
            result->Next.push_back(next);
        }
    }

    for (const auto& next : rhs->Next) {
        if (Covers(lhs->Bounds, next.first)) {
            result->Next.push_back(next);
        }
    }

    for (const auto& next : lhs->Next) {
        auto eq = std::equal_range(rhs->Next.begin(), rhs->Next.end(), next.first, TKeyTrieComparer());
        if (eq.first != eq.second) {
            result->Next.emplace_back(next.first, IntersectKeyTrie(eq.first->second, next.second));
        }
    }

    std::sort(result->Next.begin(), result->Next.end(), TKeyTrieComparer());
    return result;
}

void GetRangesFromTrieWithinRangeImpl(
    const TRowRange& keyRange,
    TKeyTriePtr trie,
    std::vector<TMutableRowRange>* result,
    TRowBufferPtr rowBuffer,
    bool insertUndefined,
    ui64 rangeCountLimit,
    std::vector<TValue> prefix = std::vector<TValue>(),
    bool refineLower = true,
    bool refineUpper = true)
{
    auto lowerBoundSize = keyRange.first.GetCount();
    auto upperBoundSize = keyRange.second.GetCount();

    struct TState
    {
        TKeyTriePtr Trie;
        std::vector<TValue> Prefix;
        bool RefineLower;
        bool RefineUpper;
    };

    std::vector<std::tuple<TBound, bool>> resultBounds;
    std::vector<std::tuple<TValue, TKeyTriePtr, bool, bool>> nextValues;

    std::deque<TState> states;
    states.push_back(TState{trie, prefix, refineLower, refineUpper});

    while (!states.empty()) {
        auto state = std::move(states.front());
        states.pop_front();
        const auto& trie = state.Trie;
        auto prefix = std::move(state.Prefix);
        auto refineLower = state.RefineLower;
        auto refineUpper = state.RefineUpper;

        size_t offset = prefix.size();

        if (offset >= lowerBoundSize) {
            refineLower = false;
        }

        if (refineUpper && offset >= upperBoundSize) {
            // NB: prefix is exactly the upper bound, which is non-inlusive.
            continue;
        }

        YT_VERIFY(!refineLower || offset < lowerBoundSize);
        YT_VERIFY(!refineUpper || offset < upperBoundSize);

        TUnversionedRowBuilder builder(offset);

        auto trieOffset = trie ? trie->Offset : std::numeric_limits<size_t>::max();

        auto makeValue = [] (TUnversionedValue value, int id) {
            value.Id = id;
            return value;
        };

        if (trieOffset > offset) {
            if (refineLower && refineUpper && keyRange.first[offset] == keyRange.second[offset]) {
                prefix.emplace_back(keyRange.first[offset]);
                states.push_back(TState{trie, std::move(prefix), true, true});
            } else if (trie && insertUndefined) {
                prefix.emplace_back(MakeUnversionedSentinelValue(EValueType::TheBottom));
                states.push_back(TState{trie, std::move(prefix), false, false});
            } else {
                TMutableRowRange range;
                for (size_t i = 0; i < offset; ++i) {
                    builder.AddValue(makeValue(prefix[i], i));
                }

                if (refineLower) {
                    for (size_t i = offset; i < lowerBoundSize; ++i) {
                        builder.AddValue(makeValue(keyRange.first[i], i));
                    }
                }
                range.first = rowBuffer->CaptureRow(builder.GetRow());
                builder.Reset();

                for (size_t i = 0; i < offset; ++i) {
                    builder.AddValue(makeValue(prefix[i], i));
                }

                if (refineUpper) {
                    for (size_t i = offset; i < upperBoundSize; ++i) {
                        builder.AddValue(makeValue(keyRange.second[i], i));
                    }
                } else {
                    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
                }
                range.second = rowBuffer->CaptureRow(builder.GetRow());
                builder.Reset();

                if (insertUndefined || !IsEmpty(range)) {
                     result->push_back(range);
                }
            }
            continue;
        }

        YT_VERIFY(trie);
        YT_VERIFY(trie->Offset == offset);

        YT_VERIFY(!(trie->Bounds.size() & 1));

        resultBounds.clear();
        resultBounds.reserve(trie->Bounds.size());

        for (size_t i = 0; i + 1 < trie->Bounds.size(); i += 2) {
            auto lower = trie->Bounds[i];
            auto upper = trie->Bounds[i + 1];

            YT_VERIFY(CompareBound(lower, upper, true, false) < 0);

            bool lowerBoundRefined = false;
            bool upperBoundRefined = false;

            if (offset < lowerBoundSize) {
                auto keyRangeLowerBound = TBound(keyRange.first[offset], true);
                if (CompareBound(upper, keyRangeLowerBound, false, true) < 0) {
                    continue;
                } else if (refineLower && CompareBound(lower, keyRangeLowerBound, true, true) <= 0) {
                    lowerBoundRefined = true;
                }
            }

            if (offset < upperBoundSize) {
                auto keyRangeUpperBound = TBound(keyRange.second[offset], offset + 1 < upperBoundSize);
                if (CompareBound(lower, keyRangeUpperBound, true, false) > 0) {
                    continue;
                } else if (refineUpper && CompareBound(upper, keyRangeUpperBound, false, false) >= 0) {
                    upperBoundRefined = true;
                }
            }

            resultBounds.emplace_back(lower, lowerBoundRefined);
            resultBounds.emplace_back(upper, upperBoundRefined);
        }

        nextValues.clear();
        nextValues.reserve(trie->Next.size());

        for (const auto& next : trie->Next) {
            auto value = next.first;

            bool refineLowerNext = false;
            bool refineUpperNext = false;

            if (refineLower) {
                if (value < keyRange.first[offset]) {
                    continue;
                } else if (value == keyRange.first[offset]) {
                    refineLowerNext = true;
                }
            }

            if (refineUpper) {
                if (value > keyRange.second[offset]) {
                    continue;
                } else if (value == keyRange.second[offset]) {
                    refineUpperNext = true;
                }
            }

            nextValues.emplace_back(value, next.second, refineLowerNext, refineUpperNext);
        }

        ui64 subrangeCount = resultBounds.size() / 2 + nextValues.size();

        if (subrangeCount > rangeCountLimit) {
            auto min = TBound(MakeUnversionedSentinelValue(EValueType::Max), false);
            auto max = TBound(MakeUnversionedSentinelValue(EValueType::Min), true);

            auto updateMinMax = [&] (const TBound& lower, const TBound& upper) {
                if (CompareBound(lower, min, true, true) < 0) {
                    min = lower;
                }
                if (CompareBound(upper, max, false, false) > 0) {
                    max = upper;
                }
            };

            for (size_t i = 0; i + 1 < resultBounds.size(); i += 2) {
                auto lower = std::get<0>(resultBounds[i]);
                auto upper = std::get<0>(resultBounds[i + 1]);
                updateMinMax(lower, upper);
            }

            for (const auto& next : nextValues) {
                auto value = TBound(std::get<0>(next), true);
                updateMinMax(value, value);
            }

            TMutableRowRange range;

            for (size_t j = 0; j < offset; ++j) {
                builder.AddValue(makeValue(prefix[j], j));
            }

            if (refineLower && min.Included && min.Value == keyRange.first[offset]) {
                for (size_t j = offset; j < lowerBoundSize; ++j) {
                    builder.AddValue(makeValue(keyRange.first[j], j));
                }
            } else {
                builder.AddValue(makeValue(min.Value, offset));

                if (!min.Included) {
                    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
                }
            }

            range.first = rowBuffer->CaptureRow(builder.GetRow());
            builder.Reset();

            for (size_t j = 0; j < offset; ++j) {
                builder.AddValue(makeValue(prefix[j], j));
            }

            if (refineUpper && max.Included && max.Value == keyRange.second[offset]) {
                for (size_t j = offset; j < upperBoundSize; ++j) {
                    builder.AddValue(makeValue(keyRange.second[j], j));
                }
            } else {
                builder.AddValue(makeValue(max.Value, offset));

                if (max.Included) {
                    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
                }
            }

            range.second = rowBuffer->CaptureRow(builder.GetRow());
            builder.Reset();
            result->push_back(range);

            continue;
        }

        rangeCountLimit -= subrangeCount;

        for (size_t i = 0; i + 1 < resultBounds.size(); i += 2) {
            auto lower = std::get<0>(resultBounds[i]);
            auto upper = std::get<0>(resultBounds[i + 1]);
            bool lowerBoundRefined = std::get<1>(resultBounds[i]);
            bool upperBoundRefined = std::get<1>(resultBounds[i + 1]);

            TMutableRowRange range;
            for (size_t j = 0; j < offset; ++j) {
                builder.AddValue(makeValue(prefix[j], j));
            }

            if (lowerBoundRefined) {
                for (size_t j = offset; j < lowerBoundSize; ++j) {
                    builder.AddValue(makeValue(keyRange.first[j], j));
                }
            } else {
                builder.AddValue(makeValue(lower.Value, offset));

                if (!lower.Included) {
                    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
                }
            }

            range.first = rowBuffer->CaptureRow(builder.GetRow());
            builder.Reset();

            for (size_t j = 0; j < offset; ++j) {
                builder.AddValue(makeValue(prefix[j], j));
            }

            if (upperBoundRefined) {
                for (size_t j = offset; j < upperBoundSize; ++j) {
                    builder.AddValue(makeValue(keyRange.second[j], j));
                }
            } else {
                builder.AddValue(makeValue(upper.Value, offset));

                if (upper.Included) {
                    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
                }
            }

            range.second = rowBuffer->CaptureRow(builder.GetRow());
            builder.Reset();
            result->push_back(range);
        }

        prefix.emplace_back();

        for (const auto& next : nextValues) {
            auto value = std::get<0>(next);
            auto trie = std::get<1>(next);
            bool refineLowerNext = std::get<2>(next);
            bool refineUpperNext = std::get<3>(next);
            prefix.back() = value;
            states.push_back(TState{trie, prefix, refineLowerNext, refineUpperNext});
        }
    }
}

TMutableRowRanges GetRangesFromTrieWithinRange(
    const TRowRange& keyRange,
    TKeyTriePtr trie,
    TRowBufferPtr rowBuffer,
    bool insertUndefined,
    ui64 rangeCountLimit)
{
    TMutableRowRanges result;
    GetRangesFromTrieWithinRangeImpl(keyRange, trie, &result, rowBuffer, insertUndefined, rangeCountLimit);

    if (insertUndefined) {
        return result;
    }

    std::sort(result.begin(), result.end());
    result.erase(MergeOverlappingRanges(result.begin(), result.end()), result.end());
    return result;
}

TString ToString(TKeyTriePtr node) {
    auto printOffset = [](int offset) {
        TString str;
        for (int i = 0; i < offset; ++i) {
            str += "  ";
        }
        return str;
    };

    std::function<TString(TKeyTriePtr, size_t)> printNode =
        [&] (TKeyTriePtr node, size_t offset) {
            TString str;
            str += printOffset(offset);

            if (!node) {
                str += "(universe)";
            } else {
                str += "(key";
                str += NYT::ToString(node->Offset);
                str += ", { ";

                for (int i = 0; i < std::ssize(node->Bounds); i += 2) {
                    str += node->Bounds[i].Included ? "[" : "(";
                    str += Format("%k", node->Bounds[i].Value);
                    str += ":";
                    str += Format("%k", node->Bounds[i+1].Value);
                    str += node->Bounds[i+1].Included ? "]" : ")";
                    if (i + 2 < std::ssize(node->Bounds)) {
                        str += ", ";
                    }
                }

                str += " })";

                for (const auto& next : node->Next) {
                    str += "\n";
                    str += printOffset(node->Offset);
                    str += Format("%k", next.first);
                    str += ":\n";
                    str += printNode(next.second, offset + 1);
                }
            }
            return str;
        };

    return printNode(node, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
