#include "counting_raw_reader.h"

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

bool TCountingRawTableReader::Retry(
    const TMaybe<ui32>& rangeIndex,
    const TMaybe<ui64>& rowIndex,
    const std::exception_ptr& error)
{
    return Reader_->Retry(rangeIndex, rowIndex, error);
}

void TCountingRawTableReader::ResetRetries()
{
    Reader_->ResetRetries();
}

bool TCountingRawTableReader::HasRangeIndices() const
{
    return Reader_->HasRangeIndices();
}

size_t TCountingRawTableReader::GetReadByteCount() const
{
    return ReadByteCount_;
}

size_t TCountingRawTableReader::DoRead(void* buf, size_t len)
{
    auto readLen = Reader_->Read(buf, len);
    ReadByteCount_ += readLen;
    return readLen;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
