#pragma once

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <optional>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TRingBuffer
{
public:
    TRingBuffer(size_t capacity, std::optional<T> defaultValue = std::nullopt)
        : DefaultValue(std::move(defaultValue))
        , Buffer(capacity)
    {}

    void Reset(size_t capacity)
    {
        Buffer = TVector<T>(capacity);
        Clear();
    }

    std::optional<T> PopFront()
    {
        if (IsEmpty()) {
            return std::nullopt;
        }
        std::optional<T> result = std::move(Buffer[BeginIndex]);
        if (BeginIndex == EndIndex) {
            Clear();
        } else {
            BeginIndex = GetNext(BeginIndex);
        }
        return result;
    }

    std::optional<T> PopBack()
    {
        if (IsEmpty()) {
            return std::nullopt;
        }
        std::optional<T> result = std::move(Buffer[EndIndex]);
        if (BeginIndex == EndIndex) {
            Clear();
        } else {
            EndIndex = GetPrevious(EndIndex);
        }
        return result;
    }

    std::optional<T> PushFront(T val)
    {
        if (Capacity() == 0) {
            return val;
        }

        if (EndIndex == InvalidIndex) {
            BeginIndex = EndIndex = 0;
            Buffer[EndIndex] = std::move(val);
            return std::nullopt;
        }

        Y_DEBUG_ABORT_UNLESS(EndIndex != InvalidIndex);
        Y_DEBUG_ABORT_UNLESS(BeginIndex != InvalidIndex);

        BeginIndex = GetPrevious(BeginIndex);
        std::optional<T> result = std::nullopt;
        if (BeginIndex == EndIndex) {
            result = std::move(Buffer[EndIndex]);
            EndIndex = GetPrevious(EndIndex);
        }
        Buffer[BeginIndex] = std::move(val);
        return result;
    }

    std::optional<T> PushBack(T val)
    {
        if (Capacity() == 0) {
            return val;
        }

        if (EndIndex == InvalidIndex) {
            BeginIndex = EndIndex = 0;
            Buffer[EndIndex] = std::move(val);
            return std::nullopt;
        }

        Y_DEBUG_ABORT_UNLESS(EndIndex != InvalidIndex);
        Y_DEBUG_ABORT_UNLESS(BeginIndex != InvalidIndex);

        EndIndex = GetNext(EndIndex);
        std::optional<T> result = std::nullopt;
        if (BeginIndex == EndIndex) {
            result = std::move(Buffer[BeginIndex]);
            BeginIndex = GetNext(BeginIndex);
        }
        Buffer[EndIndex] = std::move(val);
        return result;
    }

    [[nodiscard]] const T& Front(size_t offset = 0) const
    {
        if (IsEmpty() || offset >= Size()) {
            if (!DefaultValue) {
                Y_ABORT();
            }
            return *DefaultValue;
        }
        return Buffer[RealIndex(BeginIndex + offset)];
    }

    [[nodiscard]] const T& Back(size_t offset = 0) const
    {
        if (IsEmpty() || offset >= Size()) {
            if (!DefaultValue) {
                Y_ABORT();
            }
            return *DefaultValue;
        }
        return Buffer[RealIndex(EndIndex + Capacity() - offset)];
    }

    void Clear()
    {
        BeginIndex = EndIndex = InvalidIndex;
    }

    [[nodiscard]] bool IsEmpty() const
    {
        return EndIndex == InvalidIndex;
    }

    [[nodiscard]] bool IsFull() const
    {
        return Size() == Capacity();
    }

    [[nodiscard]] size_t Size() const
    {
        if (EndIndex == InvalidIndex) {
            return 0;
        }

        Y_DEBUG_ABORT_UNLESS(BeginIndex != InvalidIndex);

        if (EndIndex >= BeginIndex) {
            return EndIndex - BeginIndex + 1;
        } else {
            return (Capacity() - BeginIndex) + (EndIndex + 1);
        }
    }

    [[nodiscard]] size_t Capacity() const
    {
        return Buffer.size();
    }

private:
    static constexpr size_t InvalidIndex = std::numeric_limits<size_t>::max();

    size_t RealIndex(size_t index) const
    {
        return index % Capacity();
    }

    size_t GetNext(size_t index) const
    {
        return RealIndex(index + 1);
    }

    size_t GetPrevious(size_t index) const
    {
        return RealIndex(index + Capacity() - 1);
    }

    std::optional<T> DefaultValue;
    TVector<T> Buffer;
    size_t BeginIndex = InvalidIndex;
    size_t EndIndex = InvalidIndex;
};

}   // namespace NYdb::NBS
