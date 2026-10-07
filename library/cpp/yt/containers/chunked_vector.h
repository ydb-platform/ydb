#pragma once

#include <memory>
#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A two-level chunked vector.
/*!
 *  The interface is pretty minimalistic, feel free to extend it when needed.
 *
 *  As long as the capacity of the first (top) level does not change,
 *  instances of TChunkedVector can be safely read from arbitrary reader threads
 *  while being concurrently appended from a single writer thread.
 */
template <class T, size_t ChunkSize>
class TChunkedVector
{
public:
    size_t Size() const;
    bool Empty() const;

    void ReserveChunks(size_t capacity);

    void PushBack(T value);
    void PopBack();

    T& operator[](size_t index);
    const T& operator[](size_t index) const;

    // STL interop.
    size_t size() const;
    bool empty() const;

private:
    struct TChunk
    {
        T Elements[ChunkSize];
    };

    std::vector<std::unique_ptr<TChunk>> Chunks_;
    size_t Size_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CHUNKED_VECTOR_INL_H_
#include "chunked_vector-inl.h"
#undef CHUNKED_VECTOR_INL_H_
