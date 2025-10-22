#pragma once

#include <array>
#include <memory>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t DeepChunkSize, size_t ShallowChunkSize, size_t MaxSize>
class TThreeLevelStableVector
{
public:
    TThreeLevelStableVector();

    size_t Size() const;
    bool Empty() const;

    void PushBack(T value);

    T& operator [] (size_t index);
    const T& operator [] (size_t index) const;

private:
    struct TDeepChunk
    {
        std::array<T, DeepChunkSize> Elements;
    };

    struct TShallowChunk
    {
        std::array<std::unique_ptr<TDeepChunk>, ShallowChunkSize> DeepChunks;
    };

    static constexpr size_t ElementsPerDeepChunk_ = DeepChunkSize;
    static constexpr size_t ElementsPerShallowChunk_ = ElementsPerDeepChunk_ * ShallowChunkSize;
    static constexpr size_t MaxShallowChunkCount_ = MaxSize / ElementsPerShallowChunk_;

    static_assert(MaxSize % ElementsPerShallowChunk_ == 0);

    std::array<std::unique_ptr<TShallowChunk>, MaxShallowChunkCount_> ShallowChunks_;
    size_t Size_ = 0;
    size_t CurrentShallowChunkCount_ = 0;
    size_t CurrentDeepChunkCount_ = 0;

    static size_t ShallowChunkIndex(size_t index);
    static size_t DeepChunkIndex(size_t index);
    static size_t ElementIndexInDeepChunk(size_t index);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define THREE_LEVEL_STABLE_VECTOR_INL_H_
#include "three_level_stable_vector-inl.h"
#undef THREE_LEVEL_STABLE_VECTOR_INL_H_
