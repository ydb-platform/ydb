#include "yamr_table_reader.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

////////////////////////////////////////////////////////////////////

static void CheckedSkip(IInputStream* input, size_t byteCount)
{
    size_t skipped = input->Skip(byteCount);
    Y_ENSURE(skipped == byteCount, "Premature end of YaMR stream");
}

////////////////////////////////////////////////////////////////////

namespace NYT {

using namespace NYT::NDetail::NRawClient;

////////////////////////////////////////////////////////////////////////////////

TYaMRTableReader::TYaMRTableReader(::TIntrusivePtr<TRawTableReader> input)
    : TLenvalTableReader(std::move(input))
{ }

TYaMRTableReader::~TYaMRTableReader()
{ }

const TYaMRRow& TYaMRTableReader::GetRow() const
{
    CheckValidity();
    if (!RowTaken_) {
        const_cast<TYaMRTableReader*>(this)->ReadRow();
    }
    return Row_;
}

bool TYaMRTableReader::IsValid() const
{
    return Valid_;
}

void TYaMRTableReader::Next()
{
    TLenvalTableReader::Next();
}

void TYaMRTableReader::NextKey()
{
    TLenvalTableReader::NextKey();
}

ui32 TYaMRTableReader::GetTableIndex() const
{
    return TLenvalTableReader::GetTableIndex();
}

ui32 TYaMRTableReader::GetRangeIndex() const
{
    return TLenvalTableReader::GetRangeIndex();
}

ui64 TYaMRTableReader::GetRowIndex() const
{
    return TLenvalTableReader::GetRowIndex();
}

TMaybe<size_t> TYaMRTableReader::GetReadByteCount() const
{
    return TLenvalTableReader::GetReadByteCount();
}

bool TYaMRTableReader::IsEndOfStream() const
{
    return TLenvalTableReader::IsEndOfStream();
}

bool TYaMRTableReader::IsRawReaderExhausted() const
{
    return TLenvalTableReader::IsRawReaderExhausted();
}

void TYaMRTableReader::ReadField(TString* result, i32 length)
{
    result->resize(length);
    size_t count = Input_.Load(result->begin(), length);
    Y_ENSURE(count == static_cast<size_t>(length), "Premature end of YaMR stream");
}

void TYaMRTableReader::ReadRow()
{
    while (true) {
        try {
            i32 value = static_cast<i32>(Length_);
            ReadField(&Key_, value);
            Row_.Key = Key_;

            ReadInteger(&value);
            ReadField(&SubKey_, value);
            Row_.SubKey = SubKey_;

            ReadInteger(&value);
            ReadField(&Value_, value);
            Row_.Value = Value_;

            RowTaken_ = true;

            // We successfully parsed one more row from the stream,
            // so reset retry count to their initial value.
            Input_.ResetRetries();

            break;
        } catch (const std::exception& ex) {
            if (!TLenvalTableReader::Retry(std::make_exception_ptr(ex))) {
                throw;
            }
        }
    }
}

void TYaMRTableReader::SkipRow()
{
    while (true) {
        try {
            i32 value = static_cast<i32>(Length_);
            CheckedSkip(&Input_, value);

            ReadInteger(&value);
            CheckedSkip(&Input_, value);

            ReadInteger(&value);
            CheckedSkip(&Input_, value);
            break;
        } catch (const std::exception& ex) {
            if (!TLenvalTableReader::Retry(std::make_exception_ptr(ex))) {
                throw;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
