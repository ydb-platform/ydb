#ifndef HEAP_INL_H_
#error "Direct inclusion of this file is not allowed, include heap.h"
// For the sake of sane code completion.
#include "heap.h"
#endif

#include <iterator>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TIterator, class TComparer, class TOnAssign>
void SiftDown(TIterator begin, TIterator end, TIterator current, TComparer comparer, TOnAssign onAssign)
{
    size_t size = std::distance(begin, end);
    size_t offset = std::distance(begin, current);

    auto value = std::move(begin[offset]);
    while (true) {
        size_t left = 2 * offset + 1;

        if (left >= size) {
            break;
        }

        size_t right = left + 1;
        size_t min;

        if (right >= size) {
            min = left;
        } else {
            min = comparer(begin[left], begin[right]) ? left : right;
        }

        auto&& minValue = begin[min];
        if (comparer(value, minValue)) {
            break;
        }

        begin[offset] = std::move(minValue);
        onAssign(offset);
        offset = min;
    }
    begin[offset] = std::move(value);
    onAssign(offset);
}

template <class TIterator, class TComparer>
void SiftDown(TIterator begin, TIterator end, TIterator current, TComparer comparer)
{
    SiftDown(std::move(begin), std::move(end), std::move(current), comparer, [] (size_t) {});
}

template <class TIterator>
void SiftDown(TIterator begin, TIterator end, TIterator current)
{
    SiftDown(std::move(begin), std::move(end), std::move(current), std::less<>(), [] (size_t) {});
}

template <class TIterator, class TComparer, class TOnAssign>
void SiftUp(TIterator begin, TIterator /*end*/, TIterator current, TComparer comparer, TOnAssign onAssign)
{
    auto value = std::move(*current);
    while (current != begin) {
        size_t dist = std::distance(begin, current);
        auto parent = begin + (dist - 1) / 2;
        auto&& parentValue = *parent;
        if (comparer(parentValue, value)) {
            break;
        }

        *current = std::move(parentValue);
        onAssign(dist);
        current = parent;
    }
    *current = std::move(value);
    onAssign(std::distance(begin, current));
}

template <class TIterator, class TComparer>
void SiftUp(TIterator begin, TIterator end, TIterator current, TComparer comparer)
{
    SiftUp(std::move(begin), std::move(end), std::move(current), comparer, [] (size_t) {});
}

template <class TIterator>
void SiftUp(TIterator begin, TIterator end, TIterator current)
{
    SiftUp(std::move(begin), std::move(end), std::move(current), std::less<>(), [] (size_t) {});
}

template <class TIterator, class TComparer, class TOnAssign>
void MakeHeap(TIterator begin, TIterator end, TComparer comparer, TOnAssign onAssign)
{
    size_t size = std::distance(begin, end);
    if (size > 1) {
        for (size_t current = size / 2; current > 0; ) {
            --current;
            SiftDown(begin, end, begin + current, comparer, onAssign);
        }
    }
}

template <class TIterator, class TComparer>
void MakeHeap(TIterator begin, TIterator end, TComparer comparer)
{
    MakeHeap(std::move(begin), std::move(end), std::move(comparer), [] (size_t) {});
}

template <class TIterator>
void MakeHeap(TIterator begin, TIterator end)
{
    MakeHeap(std::move(begin), std::move(end), std::less<>(), [] (size_t) {});
}

template <class TIterator, class TComparer, class TOnAssign>
void AdjustHeapFront(TIterator begin, TIterator end, TComparer comparer, TOnAssign onAssign)
{
    if (std::distance(begin, end) > 1) {
        SiftDown(begin, end, begin, std::move(comparer), std::move(onAssign));
    }
}

template <class TIterator, class TComparer>
void AdjustHeapFront(TIterator begin, TIterator end, TComparer comparer)
{
    AdjustHeapFront(std::move(begin), std::move(end), std::move(comparer), [] (size_t) {});
}

template <class TIterator>
void AdjustHeapFront(TIterator begin, TIterator end)
{
    AdjustHeapFront(std::move(begin), std::move(end), std::less<>(), [] (size_t) {});
}

template <class TIterator, class TComparer, class TOnAssign>
void AdjustHeapBack(TIterator begin, TIterator end, TComparer comparer, TOnAssign onAssign)
{
    if (std::distance(begin, end) > 1) {
        SiftUp(begin, end, end - 1, std::move(comparer), std::move(onAssign));
    }
}

template <class TIterator, class TComparer>
void AdjustHeapBack(TIterator begin, TIterator end, TComparer comparer)
{
    AdjustHeapBack(std::move(begin), std::move(end), std::move(comparer), [] (size_t) {});
}

template <class TIterator>
void AdjustHeapBack(TIterator begin, TIterator end)
{
    AdjustHeapBack(std::move(begin), std::move(end), std::less<>(), [] (size_t) {});
}

template <class TIterator, class TComparer, class TOnAssign>
void ExtractHeap(TIterator begin, TIterator end, TIterator current, TComparer comparer, TOnAssign onAssign)
{
    YT_ASSERT(begin != end);
    auto newEnd = end - 1;
    if (current == newEnd) {
        return;
    }
    std::swap(*current, *newEnd);
    onAssign(std::distance(begin, current));
    onAssign(std::distance(begin, newEnd));
    SiftDown(std::move(begin), std::move(newEnd), std::move(current), comparer, onAssign);
}

template <class TIterator, class TComparer, class TOnAssign>
void ExtractHeap(TIterator begin, TIterator end, TComparer comparer, TOnAssign onAssign)
{
    auto current = begin;
    ExtractHeap(std::move(begin), std::move(end), std::move(current), comparer, onAssign);
}

template <class TIterator, class TComparer>
void ExtractHeap(TIterator begin, TIterator end, TComparer comparer)
{
    auto current = begin;
    ExtractHeap(std::move(begin), std::move(end), std::move(current), comparer, [] (size_t) {});
}

template <class TIterator>
void ExtractHeap(TIterator begin, TIterator end)
{
    auto current = begin;
    ExtractHeap(std::move(begin), std::move(end), std::move(current), std::less<>(), [] (size_t) {});
}

template <class TIterator, class TComparer, class TOnAssign>
void AdjustHeapItem(TIterator begin, TIterator end, TIterator current, TComparer comparer, TOnAssign onAssign)
{
    // It intentionally duplicates SiftUp and SiftDown code for optimization reasons.
    bool hasSiftedUp = false;
    {
        auto value = std::move(*current);
        while (current != begin) {
            size_t dist = std::distance(begin, current);
            auto parent = begin + (dist - 1) / 2;
            auto&& parentValue = *parent;
            if (comparer(parentValue, value)) {
                break;
            }

            hasSiftedUp = true;

            *current = std::move(parentValue);
            onAssign(dist);
            current = parent;
        }
        *current = std::move(value);
    }

    if (hasSiftedUp) {
        onAssign(std::distance(begin, current));
    } else {
        size_t size = std::distance(begin, end);
        size_t offset = std::distance(begin, current);

        auto value = std::move(begin[offset]);
        while (true) {
            size_t left = 2 * offset + 1;

            if (left >= size) {
                break;
            }

            size_t right = left + 1;
            size_t min;

            if (right >= size) {
                min = left;
            } else {
                min = comparer(begin[left], begin[right]) ? left : right;
            }

            auto&& minValue = begin[min];
            if (comparer(value, minValue)) {
                break;
            }

            begin[offset] = std::move(minValue);
            onAssign(offset);
            offset = min;
        }
        begin[offset] = std::move(value);
        onAssign(offset);
    }
}

template <class TIterator, class TComparer>
void AdjustHeapItem(TIterator begin, TIterator end, TIterator current, TComparer comparer)
{
    AdjustHeapItem(std::move(begin), std::move(end), std::move(current), comparer, [] (size_t) {});
}

template <class TIterator>
void AdjustHeapItem(TIterator begin, TIterator end, TIterator current)
{
    AdjustHeapItem(std::move(begin), std::move(end), std::move(current), std::less<>(), [] (size_t) {});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
