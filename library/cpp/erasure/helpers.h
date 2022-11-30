#pragma once

#include "public.h"

namespace NErasure {

// All vectors here are assumed to be sorted.

TPartIndexList MakeSegment(int begin, int end);

TPartIndexList MakeSingleton(int elem);

TPartIndexList Difference(int begin, int end, const TPartIndexList& subtrahend);

TPartIndexList Difference(const TPartIndexList& first, const TPartIndexList& second);

TPartIndexList Difference(const TPartIndexList& first, int elem);

TPartIndexList Intersection(const TPartIndexList& first, const TPartIndexList& second);

TPartIndexList Union(const TPartIndexList& first, const TPartIndexList& second);

bool Contains(const TPartIndexList& set, int elem);

TPartIndexList UniqueSortedIndices(const TPartIndexList& indices);

TPartIndexList ExtractRows(const TPartIndexList& matrix, int width, const TPartIndexList& rows);

} // namespace NErasure

