#ifndef THREE_LEVEL_STABLE_VECTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include three_level_stable_vector.h"
// For the sake of sane code completion.
#include "three_level_stable_vector.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::TThreeLevelStableVector()
{ }

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
size_t TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::Size() const
{
    return Size_;
}

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
bool TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::Empty() const
{
    return Size_ == 0;
}

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
void TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::PushBack(T value)
{
    YT_VERIFY(Size_ + 1 <= MaxSize);

    auto newElementIndex = Size_;

    auto shallowChunkIndex = ShallowChunkIndex(newElementIndex);
    YT_ASSERT(shallowChunkIndex <= CurrentShallowChunkCount_);
    if (shallowChunkIndex == CurrentShallowChunkCount_) {
        YT_VERIFY(CurrentDeepChunkCount_ == 0 || CurrentDeepChunkCount_ == ShallowChunkSize);
        CurrentDeepChunkCount_ = 0;
        ShallowChunks_[CurrentShallowChunkCount_++] = std::make_unique<TShallowChunk>();
    }

    auto deepChunkIndex = DeepChunkIndex(newElementIndex);
    YT_ASSERT(deepChunkIndex <= CurrentDeepChunkCount_);
    if (deepChunkIndex == CurrentDeepChunkCount_) {
        ShallowChunks_[shallowChunkIndex]->DeepChunks[CurrentDeepChunkCount_++] = std::make_unique<TDeepChunk>();
    }

    (*this)[newElementIndex] = std::move(value);
    ++Size_;
}

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
T& TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::operator[](size_t index)
{
    return ShallowChunks_[ShallowChunkIndex(index)]
        ->DeepChunks[DeepChunkIndex(index)]
        ->Elements[ElementIndexInDeepChunk(index)];
}

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
const T& TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::operator[](size_t index) const
{
    return ShallowChunks_[ShallowChunkIndex(index)]
        ->DeepChunks[DeepChunkIndex(index)]
        ->Elements[ElementIndexInDeepChunk(index)];
}

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
size_t TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::ShallowChunkIndex(size_t index)
{
    return index / ElementsPerShallowChunk_;
}

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
size_t TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::DeepChunkIndex(size_t index)
{
    return (index % ElementsPerShallowChunk_) / ElementsPerDeepChunk_;
}

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
size_t TThreeLevelStableVector<T, DeepChunkSize, ShallowChunkSize, MaxSize>::ElementIndexInDeepChunk(size_t index)
{
    return index % ElementsPerDeepChunk_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
