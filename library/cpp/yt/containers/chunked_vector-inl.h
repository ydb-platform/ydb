#ifndef CHUNKED_VECTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include chunked_vector.h"
// For the sake of sane code completion.
#include "chunked_vector.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t ChunkSize>
size_t TChunkedVector<T, ChunkSize>::Size() const
{
    return Size_;
}

template <class T, size_t ChunkSize>
bool TChunkedVector<T, ChunkSize>::Empty() const
{
    return Size_ == 0;
}

template <class T, size_t ChunkSize>
void TChunkedVector<T, ChunkSize>::ReserveChunks(size_t capacity)
{
    Chunks_.reserve(capacity);
}

template <class T, size_t ChunkSize>
void TChunkedVector<T, ChunkSize>::PushBack(T value)
{
    if (Size_ + 1 > Chunks_.size() * ChunkSize) {
        Chunks_.push_back(std::make_unique<TChunk>());
    }
    (*this)[Size_++] = std::move(value);
}

template <class T, size_t ChunkSize>
void TChunkedVector<T, ChunkSize>::PopBack()
{
    (*this)[--Size_] = T();
}

template <class T, size_t ChunkSize>
T& TChunkedVector<T, ChunkSize>::operator[](size_t index)
{
    return Chunks_[index / ChunkSize]->Elements[index % ChunkSize];
}

template <class T, size_t ChunkSize>
const T& TChunkedVector<T, ChunkSize>::operator[](size_t index) const
{
    return Chunks_[index / ChunkSize]->Elements[index % ChunkSize];
}

template <class T, size_t ChunkSize>
size_t TChunkedVector<T, ChunkSize>::size() const
{
    return Size();
}

template <class T, size_t ChunkSize>
bool TChunkedVector<T, ChunkSize>::empty() const
{
    return Empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
