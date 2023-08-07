#ifndef ZEROCOPY_OUTPUT_WRITER_INL_H_
#error "Direct inclusion of this file is not allowed, include zerocopy_output_writer.h"
// For the sake of sane code completion.
#include "zerocopy_output_writer.h"
#endif

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/coding/varint.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

char* TZeroCopyOutputStreamWriter::Current() const
{
    return Current_;
}

ui64 TZeroCopyOutputStreamWriter::RemainingBytes() const
{
    return RemainingBytes_;
}

void TZeroCopyOutputStreamWriter::Advance(size_t bytes)
{
    YT_VERIFY(bytes <= RemainingBytes_);
    Current_ += bytes;
    RemainingBytes_ -= bytes;
}

void TZeroCopyOutputStreamWriter::Write(const void* buffer, size_t length)
{
    if (length > RemainingBytes_) {
        UndoRemaining();
        Output_->Write(buffer, length);
        TotalWrittenBlockSize_ += length;
        ObtainNextBlock();
    } else {
        memcpy(Current_, buffer, length);
        Advance(length);
    }
}

ui64 TZeroCopyOutputStreamWriter::GetTotalWrittenSize() const
{
    return TotalWrittenBlockSize_ - RemainingBytes_;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
Y_FORCE_INLINE int WriteVarInt(char* output, T value)
{
    if constexpr (std::is_same_v<T, i32>) {
        return WriteVarInt32(output, value);
    } else if constexpr (std::is_same_v<T, ui32>) {
        return WriteVarUint32(output, value);
    } else if constexpr (std::is_same_v<T, i64>) {
        return WriteVarInt64(output, value);
    } else if constexpr (std::is_same_v<T, ui64>) {
        return WriteVarUint64(output, value);
    } else {
        static_assert(TDependentFalse<T>);
    }
}

template <typename T>
constexpr auto MaxVarIntSize = [] {
    if constexpr (std::is_same_v<T, i32>) {
        return MaxVarInt32Size;
    } else if constexpr (std::is_same_v<T, ui32>) {
        return MaxVarUint32Size;
    } else if constexpr (std::is_same_v<T, i64>) {
        return MaxVarInt64Size;
    } else if constexpr (std::is_same_v<T, ui64>) {
        return MaxVarUint64Size;
    } else {
        static_assert(TDependentFalse<T>);
    }
}();

template <typename T>
int WriteVarInt(TZeroCopyOutputStreamWriter* writer, T value)
{
    int size;
    if (writer->RemainingBytes() >= MaxVarIntSize<T>) {
        size = WriteVarInt(writer->Current(), value);
        writer->Advance(size);
    } else {
        char result[MaxVarIntSize<T>];
        size = WriteVarInt(result, value);
        writer->Write(result, size);
    }
    return size;
}

int WriteVarUint32(TZeroCopyOutputStreamWriter* writer, ui32 value)
{
    return WriteVarInt(writer, value);
}

int WriteVarUint64(TZeroCopyOutputStreamWriter* writer, ui64 value)
{
    return WriteVarInt(writer, value);
}

int WriteVarInt32(TZeroCopyOutputStreamWriter* writer, i32 value)
{
    return WriteVarInt(writer, value);
}

int WriteVarInt64(TZeroCopyOutputStreamWriter* writer, i64 value)
{
    return WriteVarInt(writer, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
