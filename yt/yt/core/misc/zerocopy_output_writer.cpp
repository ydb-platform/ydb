#include "zerocopy_output_writer.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TZeroCopyOutputStreamWriter::TZeroCopyOutputStreamWriter(IZeroCopyOutput* output)
    : Output_(output)
{
    ObtainNextBlock();
    // Need to have a "not dirty" stream after creating TZeroCopyOutputStreamWriter.
    // This is for when we want to reuse a writer and clear the stream at the beginning of each iteration.
    // But we want to have a valid (not null) Current_ pointer as not to break the current behavior.
    UndoRemaining();
}

TZeroCopyOutputStreamWriter::~TZeroCopyOutputStreamWriter()
{
    if (RemainingBytes_ > 0) {
        UndoRemaining();
    }
}

void TZeroCopyOutputStreamWriter::ObtainNextBlock()
{
    if (RemainingBytes_ > 0) {
        UndoRemaining();
    }
    RemainingBytes_ = Output_->Next(&Current_);
    TotalWrittenBlockSize_ += RemainingBytes_;
}

void TZeroCopyOutputStreamWriter::UndoRemaining()
{
    Output_->Undo(RemainingBytes_);
    TotalWrittenBlockSize_ -= RemainingBytes_;
    RemainingBytes_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
