#pragma once

#include "algorithm_helpers.h"
#include "binary_search.h"
#include "piecewise_linear_function.h"

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <vector>
#include <algorithm>
#include <random>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

static constexpr int MergeArity = 8;
using TPivotsVector = TCompactVector<int, MergeArity + 1>;

void SortOrMergeImpl(
    std::vector<double>* vec,
    std::vector<double>* buffer,
    TPivotsVector* mergePivots,
    TPivotsVector* newPivots);

bool FindMergePivots(const std::vector<double>* vec, TPivotsVector* pivots) noexcept;

void SortOrMerge(std::vector<double>* vec);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
