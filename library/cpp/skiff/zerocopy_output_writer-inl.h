#pragma once
#ifndef ZEROCOPY_OUTPUT_WRITER_INL_H_
#error "Direct inclusion of this file is not allowed, include zerocopy_output_writer.h"
// For the sake of sane code completion.
#include "zerocopy_output_writer.h"
#endif

#include <util/system/yassert.h>

namespace NSkiff {

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
    Y_ABORT_UNLESS(bytes <= RemainingBytes_);
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

} // namespace NSkiff
