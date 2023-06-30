#pragma once

#include <util/stream/zerocopy_output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Simple wrapper around
class TZeroCopyOutputStreamWriter
    : private TNonCopyable
{
public:
    explicit TZeroCopyOutputStreamWriter(IZeroCopyOutput* output);

    ~TZeroCopyOutputStreamWriter();

    Y_FORCE_INLINE char* Current() const;
    Y_FORCE_INLINE ui64 RemainingBytes() const;
    Y_FORCE_INLINE void Advance(size_t bytes);
    void UndoRemaining();
    Y_FORCE_INLINE void Write(const void* buffer, size_t length);
    Y_FORCE_INLINE ui64 GetTotalWrittenSize() const;

private:
    void ObtainNextBlock();

private:
    IZeroCopyOutput* Output_;
    char* Current_ = nullptr;
    ui64 RemainingBytes_ = 0;
    ui64 TotalWrittenBlockSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Writes varint to |writer| and returned number of written bytes.
template <typename T>
Y_FORCE_INLINE int WriteVarInt(TZeroCopyOutputStreamWriter* writer, T value);

Y_FORCE_INLINE int WriteVarUint32(TZeroCopyOutputStreamWriter* writer, ui32 value);
Y_FORCE_INLINE int WriteVarUint64(TZeroCopyOutputStreamWriter* writer, ui64 value);
Y_FORCE_INLINE int WriteVarInt32(TZeroCopyOutputStreamWriter* writer, i32 value);
Y_FORCE_INLINE int WriteVarInt64(TZeroCopyOutputStreamWriter* writer, i64 value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ZEROCOPY_OUTPUT_WRITER_INL_H_
#include "zerocopy_output_writer-inl.h"
#undef ZEROCOPY_OUTPUT_WRITER_INL_H_
