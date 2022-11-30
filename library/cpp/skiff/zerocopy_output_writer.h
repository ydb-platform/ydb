#pragma once

#include <util/stream/zerocopy_output.h>

namespace NSkiff {

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

} // namespace NSkiff

#define ZEROCOPY_OUTPUT_WRITER_INL_H_
#include "zerocopy_output_writer-inl.h"
#undef ZEROCOPY_OUTPUT_WRITER_INL_H_
