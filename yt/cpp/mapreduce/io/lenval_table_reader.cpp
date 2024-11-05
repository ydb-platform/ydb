#include "lenval_table_reader.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <util/string/printf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const i32 CONTROL_ATTR_TABLE_INDEX   = -1;
const i32 CONTROL_ATTR_KEY_SWITCH    = -2;
const i32 CONTROL_ATTR_RANGE_INDEX   = -3;
const i32 CONTROL_ATTR_ROW_INDEX     = -4;
const i32 CONTROL_ATTR_END_OF_STREAM = -5;
const i32 CONTROL_ATTR_TABLET_INDEX  = -6;

////////////////////////////////////////////////////////////////////////////////

TLenvalTableReader::TLenvalTableReader(::TIntrusivePtr<TRawTableReader> input)
    : Input_(std::move(input))
{
    TLenvalTableReader::Next();
}

TLenvalTableReader::~TLenvalTableReader()
{ }

void TLenvalTableReader::CheckValidity() const
{
    if (!IsValid()) {
        ythrow yexception() << "Iterator is not valid";
    }
}

bool TLenvalTableReader::IsValid() const
{
    return Valid_;
}

void TLenvalTableReader::Next()
{
    if (!RowTaken_) {
        SkipRow();
    }

    CheckValidity();

    if (RowIndex_) {
        ++*RowIndex_;
    }

    while (true) {
        try {
            i32 value = 0;
            if (!ReadInteger(&value, true)) {
                return;
            }

            while (value < 0 && !IsEndOfStream_) {
                switch (value) {
                    case CONTROL_ATTR_KEY_SWITCH:
                        if (!AtStart_) {
                            Valid_ = false;
                            return;
                        } else {
                            ReadInteger(&value);
                        }
                        break;

                    case CONTROL_ATTR_TABLE_INDEX: {
                        ui32 tmp = 0;
                        ReadInteger(&tmp);
                        TableIndex_ = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    case CONTROL_ATTR_ROW_INDEX: {
                        ui64 tmp = 0;
                        ReadInteger(&tmp);
                        RowIndex_ = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    case CONTROL_ATTR_RANGE_INDEX: {
                        ui32 tmp = 0;
                        ReadInteger(&tmp);
                        RangeIndex_ = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    case CONTROL_ATTR_TABLET_INDEX: {
                        ui64 tmp = 0;
                        ReadInteger(&tmp);
                        TabletIndex_ = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    case CONTROL_ATTR_END_OF_STREAM: {
                        IsEndOfStream_ = true;
                        break;
                    }
                    default:
                        ythrow yexception() <<
                            Sprintf("Invalid control integer %d in lenval stream", value);
                }
            }

            Length_ = static_cast<ui32>(value);
            RowTaken_ = false;
            AtStart_ = false;
        } catch (const std::exception& ex) {
            if (!PrepareRetry(std::make_exception_ptr(ex))) {
                throw;
            }
            continue;
        }
        break;
    }
}

bool TLenvalTableReader::Retry(const std::exception_ptr& error)
{
    if (PrepareRetry(error)) {
        RowTaken_ = true;
        Next();
        return true;
    }
    return false;
}

void TLenvalTableReader::NextKey()
{
    while (Valid_) {
        Next();
    }

    if (Finished_) {
        return;
    }

    Valid_ = true;

    if (RowIndex_) {
        --*RowIndex_;
    }

    RowTaken_ = true;
}

ui32 TLenvalTableReader::GetTableIndex() const
{
    CheckValidity();
    return TableIndex_;
}

ui32 TLenvalTableReader::GetRangeIndex() const
{
    CheckValidity();
    return RangeIndex_.GetOrElse(0) + RangeIndexShift_;
}

ui64 TLenvalTableReader::GetRowIndex() const
{
    CheckValidity();
    return RowIndex_.GetOrElse(0UL);
}

TMaybe<size_t> TLenvalTableReader::GetReadByteCount() const
{
    return Input_.GetReadByteCount();
}

bool TLenvalTableReader::IsEndOfStream() const
{
    return IsEndOfStream_;
}

bool TLenvalTableReader::IsRawReaderExhausted() const
{
    return Finished_;
}

bool TLenvalTableReader::PrepareRetry(const std::exception_ptr& error)
{
    if (Input_.Retry(RangeIndex_, RowIndex_, error)) {
        if (RangeIndex_) {
            RangeIndexShift_ += *RangeIndex_;
        }
        RowIndex_.Clear();
        RangeIndex_.Clear();
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
