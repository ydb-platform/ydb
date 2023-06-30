#include "helpers.h"

#include <algorithm>
#include <iterator>

namespace NErasure {

TPartIndexList MakeSegment(int begin, int end) {
    TPartIndexList result(end - begin);
    for (int i = begin; i < end; ++i) {
        result[i - begin] = i;
    }
    return result;
}

TPartIndexList MakeSingleton(int elem) {
    TPartIndexList result;
    result.push_back(elem);
    return result;
}

TPartIndexList Difference(int begin, int end, const TPartIndexList& subtrahend) {
    size_t pos = 0;
    TPartIndexList result;
    for (int i = begin; i < end; ++i) {
        while (pos < subtrahend.size() && subtrahend[pos] < i) {
            pos += 1;
        }
        if (pos == subtrahend.size() || subtrahend[pos] != i) {
            result.push_back(i);
        }
    }
    return result;
}

TPartIndexList Difference(const TPartIndexList& first, const TPartIndexList& second) {
    TPartIndexList result;
    std::set_difference(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

TPartIndexList Difference(const TPartIndexList& set, int subtrahend) {
    return Difference(set, MakeSingleton(subtrahend));
}

TPartIndexList Intersection(const TPartIndexList& first, const TPartIndexList& second) {
    TPartIndexList result;
    std::set_intersection(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

TPartIndexList Union(const TPartIndexList& first, const TPartIndexList& second) {
    TPartIndexList result;
    std::set_union(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

bool Contains(const TPartIndexList& set, int elem) {
    return std::binary_search(set.begin(), set.end(), elem);
}

TPartIndexList UniqueSortedIndices(const TPartIndexList& indices) {
    TPartIndexList copy = indices;
    std::sort(copy.begin(), copy.end());
    copy.erase(std::unique(copy.begin(), copy.end()), copy.end());
    return copy;
}

TPartIndexList ExtractRows(const TPartIndexList& matrix, int width, const TPartIndexList& rows) {
    Y_ASSERT(matrix.size() % width == 0);
    TPartIndexList result(width * rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        auto start = matrix.begin() + rows[i] * width;
        std::copy(start, start + width, result.begin() + i * width);
    }
    return result;
}

} // namespace NErasure

