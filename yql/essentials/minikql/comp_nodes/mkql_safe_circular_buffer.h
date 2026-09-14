#pragma once

#include <yql/essentials/utils/yql_panic.h>

#include <util/system/yassert.h>
#include <util/generic/maybe.h>

#include <vector>

namespace NKikimr::NMiniKQL {

template <class T>
class TSafeCircularBuffer {
public:
    TSafeCircularBuffer(TMaybe<size_t> size, T emptyValue, size_t initSize = 0)
        : Buffer_(size ? *size : initSize, emptyValue)
        , EmptyValue_(emptyValue)
        , Unbounded_(!size.Defined())
        , Size_(initSize)
    {
        if (!Unbounded_) {
            Y_ABORT_UNLESS(initSize <= *size);
        }
    }

    bool IsUnbounded() const {
        return Unbounded_;
    }

    void PushBack(T&& data) {
        MutationCount_++;
        if (IsFull()) {
            Grow();
        }
        Size_++;
        Buffer_[RealIndex(Size_ - 1)] = std::move(data);
    }

    const T& Get(size_t index) const {
        if (index < Size_) {
            return Buffer_[RealIndex(index)];
        } else {
            // Circular buffer out of bounds
            return EmptyValue_;
        }
    }

    void PopFront() {
        MutationCount_++;
        if (!Size_) {
            // Circular buffer not have elements for pop, no elements, no problem.
            return;
        }
        Buffer_[RealIndex(0)] = EmptyValue_;
        Head_ = RealIndex(1, /*mayOutOfBounds=*/true);
        Size_--;
    }

    // Returns the actual allocated size of the underlying buffer vector.
    // This is the total capacity that can hold elements including empty slots.
    size_t Capacity() const {
        return Buffer_.size();
    }

    // Returns the number of elements currently stored in the circular buffer.
    // These are the active elements between push and pop operations.
    size_t Size() const {
        return Size_;
    }

    // Return current generation of a queue.
    // Generation grows each time some modification happens.
    ui64 Generation() const {
        return MutationCount_;
    }

    // Set all values to the |EmptyValue_|.
    void Clean() {
        MutationCount_++;
        for (size_t index = 0; index < Size(); ++index) {
            Buffer_[RealIndex(index)] = EmptyValue_;
        }
    }

    // Clear all queue. Reset size to 0. Free buffer allocated resources.
    // Queue in general not usable after that.
    void Clear() {
        MutationCount_++;
        Head_ = Size_ = 0;
        Buffer_.clear();
        Buffer_.shrink_to_fit();
    }

    void Reserve(size_t capacity) {
        if (capacity <= Capacity()) {
            return;
        }
        Grow(capacity);
    }

private:
    static inline constexpr size_t GrowFactor = 2;

    bool IsFull() const {
        return Size() == Capacity();
    }

    void Grow(size_t to) {
        YQL_ENSURE(Unbounded_, "Cannot reallocate buffer in Bounded mode");
        // Rotate elements so that logical first element is at position 0.
        std::rotate(Buffer_.begin(), Buffer_.begin() + Head_, Buffer_.end());
        Buffer_.resize(to, EmptyValue_);
        // Reset Head since elements now start at position 0.
        Head_ = 0;
    }

    void Grow() {
        if (Capacity() == 0) {
            Grow(1);
        }
        // Double buffer size.
        Grow(Capacity() * GrowFactor);
    }

    size_t RealIndex(size_t index, bool mayOutOfBounds = false) const {
        auto capacity = Capacity();
        Y_ABORT_UNLESS(capacity);
        if (!mayOutOfBounds) {
            Y_ABORT_UNLESS(index < Size());
        }
        return (Head_ + index) % capacity;
    }

    std::vector<T> Buffer_;
    const T EmptyValue_;
    const bool Unbounded_;
    size_t Head_ = 0;
    size_t Size_;
    ui64 MutationCount_ = 0;
};

} // namespace NKikimr::NMiniKQL
