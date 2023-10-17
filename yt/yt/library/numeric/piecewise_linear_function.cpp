#include "piecewise_linear_function.h"
#include "piecewise_linear_function-test.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void SortOrMergeImpl(
    std::vector<double>* vec,
    std::vector<double>* buffer,
    TPivotsVector* mergePivots,
    TPivotsVector* newPivots)
{
    std::vector<double>* buffer1 = vec;
    std::vector<double>* buffer2 = buffer;

    while (mergePivots->size() > 2) {
        newPivots->clear();

        for (int startPivot = 0; startPivot < std::ssize(*mergePivots) - 1; startPivot += 2) {
            newPivots->push_back((*mergePivots)[startPivot]);
            if (startPivot + 2 < std::ssize(*mergePivots)) {
                std::merge(
                    /* first1 */ begin(*buffer1) + (*mergePivots)[startPivot],
                    /* last1 */ begin(*buffer1) + (*mergePivots)[startPivot + 1],
                    /* first2 */ begin(*buffer1) + (*mergePivots)[startPivot + 1],
                    /* last2 */ begin(*buffer1) + (*mergePivots)[startPivot + 2],
                    /* result */ begin(*buffer2) + (*mergePivots)[startPivot]);
            } else {
                std::copy(
                    /* first */ begin(*buffer1) + (*mergePivots)[startPivot],
                    /* last */ begin(*buffer1) + (*mergePivots)[startPivot + 1],
                    /* result */ begin(*buffer2) + (*mergePivots)[startPivot]);
            }
        }
        newPivots->push_back(vec->size());

        // Swap the pointers.
        std::swap(buffer1, buffer2);
        std::swap(mergePivots, newPivots);
    }

    if (buffer1 != vec) {
        // Swap the contents (takes only constant number of operations).
        std::swap(*vec, *buffer);
    }
}

bool FindMergePivots(const std::vector<double>* vec, TPivotsVector* pivots) noexcept
{
    pivots->clear();
    pivots->push_back(0);

    for (int i = 1; i < std::ssize(*vec); i++) {
        if ((*vec)[i] < (*vec)[i - 1]) {
            if (pivots->size() < MergeArity) {
                pivots->push_back(i);
            } else {
                return false;
            }
        }
    }

    pivots->push_back(vec->size());

    return true;
}

void SortOrMerge(std::vector<double>* vec)
{
    // If |*vec| is small, |std::sort| works fine.
    if (vec->size() <= 1000) {
        std::sort(begin(*vec), end(*vec));
        return;
    }

    TPivotsVector mergePivots;

    if (!FindMergePivots(vec, &mergePivots)) {
        // Fall back to |std::sort|.
        std::sort(begin(*vec), end(*vec));
        return;
    }

    if (mergePivots.size() == 2) {
        // *vec is already sorted.
        return;
    }

    std::vector<double> mergeBuffer(vec->size());
    TPivotsVector pivotsBuffer;

    SortOrMergeImpl(vec, &mergeBuffer, &mergePivots, &pivotsBuffer);
    Y_DEBUG_ABORT_UNLESS(std::is_sorted(begin(*vec), end(*vec)));
}

} // namespace NDetail

// Removes duplicates and the point outside the range [|leftBound|, |rightBound|].
void ClearAndSortCriticalPoints(std::vector<double>* vec, double leftBound, double rightBound)
{
    auto removePredicate = [&] (double point) {
        return point < leftBound || point > rightBound;
    };

    vec->erase(std::remove_if(begin(*vec), end(*vec), removePredicate), end(*vec));
    // NB(antonkikh): This method is often used where simple merge can be used.
    // To avoid complicating the callers code, we make dynamic optimization here.
    NDetail::SortOrMerge(vec);
    vec->erase(std::unique(begin(*vec), end(*vec)), end(*vec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
