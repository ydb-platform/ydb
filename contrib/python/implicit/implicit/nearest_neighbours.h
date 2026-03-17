// Copyright 2017 Ben Frederickson

#ifndef IMPLICIT_NEAREST_NEIGHBOURS_H_
#define IMPLICIT_NEAREST_NEIGHBOURS_H_

#include <algorithm>
#include <vector>
#include <utility>
#include <functional>

namespace implicit {

/** Functor that stores the Top K (Value/Index) pairs
 passed to it in and stores in its results member
 */
template <typename Index, typename Value>
struct TopK {
    explicit TopK(size_t K) : K(K) {}

    void operator()(Index index, Value score) {
        if ((results.size() < K) || (score > results[0].first)) {
            if (results.size() >= K) {
                std::pop_heap(results.begin(), results.end(), heap_order);
                results.pop_back();
            }

            results.push_back(std::make_pair(score, index));
            std::push_heap(results.begin(), results.end(), heap_order);
        }
    }

    size_t K;
    std::vector<std::pair<Value, Index> > results;
    std::greater<std::pair<Value, Index> > heap_order;
};

/** A utility class to multiply rows of a sparse matrix
 Implements the sparse matrix multiplication algorithm
 described in the paper 'Sparse Matrix Multiplication Package (SMMP)'
 http://www.i2m.univ-amu.fr/~bradji/multp_sparse.pdf
*/
template <typename Index, typename Value>
class SparseMatrixMultiplier {
 public:
    explicit SparseMatrixMultiplier(Index item_count)
        : sums(item_count, 0), nonzeros(item_count, -1), head(-2), length(0) {
    }

    /** Adds value to the item at index */
    void add(Index index, Value value) {
        sums[index] += value;

        if (nonzeros[index] == -1) {
            nonzeros[index] = head;
            head = index;
            length += 1;
        }
    }

    /** Calls a function once per non-zero entry, also clears state for next run*/
    template <typename Function>
    void foreach(Function & f) {  // NOLINT(*)
        for (int i = 0; i < length; ++i) {
            Index index = head;
            f(index, sums[index]);

            // clear up memory and advance linked list
            head = nonzeros[head];
            sums[index] = 0;
            nonzeros[index] = -1;
        }

        length = 0;
        head = -2;
    }

    Index nnz() const { return length; }

    std::vector<Value> sums;

 protected:
    std::vector<Index> nonzeros;
    Index head, length;
};
}  // namespace implicit
#endif  // IMPLICIT_NEAREST_NEIGHBOURS_H_
