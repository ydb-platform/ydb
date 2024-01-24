#pragma once

#include <cstddef>
#include <limits>

class TConstBuffer;
class TMutableBuffer;

class TBufferBase {
public:
    size_t GetSize() const noexcept;

    void SetSize(size_t newSize) noexcept;

protected:
    TBufferBase(size_t size = 0) noexcept;

    size_t Size;
};

template <typename PointerType>
class TBufferBaseT: public TBufferBase {
public:
    PointerType GetPointer() const noexcept;

    void Cut(size_t offset) noexcept;

    void Assign(PointerType data = nullptr, size_t size = 0U) noexcept;

protected:
    TBufferBaseT(PointerType data, size_t size) noexcept;

    PointerType Data;
};

/// Represents constant memory buffer, but do not owns it.
class TConstBuffer: public TBufferBaseT<const void*> {
    friend class TMutableBuffer;

public:
    TConstBuffer(const TMutableBuffer& buffer) noexcept;

    TConstBuffer(const void* data = nullptr, size_t size = 0U) noexcept;

    TConstBuffer Offset(ptrdiff_t offset, size_t size = std::numeric_limits<size_t>::max()) const noexcept;
};

/// Represents mutable memory buffer, but do not owns it.
class TMutableBuffer: public TBufferBaseT<void*> {
    friend class TConstBuffer;

public:
    TMutableBuffer(void* data = nullptr, size_t size = 0U) noexcept;

    TMutableBuffer(const TMutableBuffer& value) noexcept
        : TBufferBaseT<void*>(value)
    {
    }

    TMutableBuffer Offset(ptrdiff_t offset, size_t size = std::numeric_limits<size_t>::max()) const noexcept;

    size_t CopyFrom(const TConstBuffer& buffer) const noexcept;
};
