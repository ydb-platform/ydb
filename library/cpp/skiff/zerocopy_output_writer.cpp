#include "zerocopy_output_writer.h"

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

TZeroCopyOutputStreamWriter::TZeroCopyOutputStreamWriter(IZeroCopyOutput* output)
    : Output_(output)
{
    ObtainNextBlock();
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

} // namespace NSkiff
