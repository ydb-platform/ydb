#include "skiff_row_table_reader.h"

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/interface/skiff_row.h>

#include <library/cpp/skiff/skiff.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSkiffRowTableReader::TSkiffRowTableReader(
    ::TIntrusivePtr<TRawTableReader> input,
    const NSkiff::TSkiffSchemaPtr& schema,
    TVector<ISkiffRowSkipperPtr>&& skippers,
    NDetail::TCreateSkiffSchemaOptions&& options)
    : Input_(std::move(input))
    , BufferedInput_(&Input_)
    , Parser_({schema, &BufferedInput_})
    , Skippers_(std::move(skippers))
    , Options_(std::move(options))
{
    Next();
}

TSkiffRowTableReader::~TSkiffRowTableReader()
{ }

bool TSkiffRowTableReader::Retry(const std::exception_ptr& error)
{
    if (PrepareRetry(error)) {
        RowTaken_ = true;
        Next();
        return true;
    }
    return false;
}

bool TSkiffRowTableReader::PrepareRetry(const std::exception_ptr& error)
{
    if (Input_.Retry(RangeIndex_, RowIndex_, error)) {
        if (RangeIndex_) {
            RangeIndexShift_ += *RangeIndex_;
        }
        RowIndex_.Clear();
        RangeIndex_.Clear();
        BufferedInput_ = TBufferedInput(&Input_);
        Parser_.emplace(&BufferedInput_);
        return true;
    }
    return false;
}

void TSkiffRowTableReader::ReadRow(const ISkiffRowParserPtr& parser)
{
    while (true) {
        try {
            parser->Parse(&Parser_.value());
            RowTaken_ = true;

            // We successfully parsed one more row from the stream,
            // so reset retry count to their initial value.
            Input_.ResetRetries();

            break;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR("Read error during parsing: %v", ex.what());

            if (!Retry(std::make_exception_ptr(ex))) {
                throw;
            }
        }
    }
}

bool TSkiffRowTableReader::IsValid() const
{
    return Valid_;
}

void TSkiffRowTableReader::SkipRow()
{
    CheckValidity();
    while (true) {
        try {
            Skippers_[TableIndex_]->SkipRow(&Parser_.value());

            break;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR("Read error during skipping row: %v", ex.what());

            if (!Retry(std::make_exception_ptr(ex))) {
                throw;
            }
        }
    }
}

void TSkiffRowTableReader::CheckValidity() const {
    if (!IsValid()) {
        ythrow yexception() << "Iterator is not valid";
    }
}

void TSkiffRowTableReader::Next()
{
    if (!RowTaken_) {
        SkipRow();
    }

    CheckValidity();

    if (Y_UNLIKELY(Finished_ || !Parser_->HasMoreData())) {
        Finished_ = true;
        Valid_ = false;
        return;
    }

    if (AfterKeySwitch_) {
        AfterKeySwitch_ = false;
        return;
    }

    if (RowIndex_) {
        ++*RowIndex_;
    }

    while (true) {
        try {
            auto tag = Parser_->ParseVariant16Tag();
            if (tag == NSkiff::EndOfSequenceTag<ui16>()) {
                IsEndOfStream_ = true;
                break;
            } else {
                TableIndex_ = tag;
            }

            if (TableIndex_ >= Skippers_.size()) {
                ythrow TIOException() <<
                    "Table index " << TableIndex_ <<
                    " is out of range [0, " << Skippers_.size() <<
                    ") in read";
            }

            if (Options_.HasKeySwitch_) {
                auto keySwitch = Parser_->ParseBoolean();
                if (keySwitch) {
                    AfterKeySwitch_ = true;
                    Valid_ = false;
                }
            }

            auto tagRowIndex = Parser_->ParseVariant8Tag();
            if (tagRowIndex == 1) {
                RowIndex_ = Parser_->ParseInt64();
            } else {
                Y_ENSURE(tagRowIndex == 0, "Tag for row_index was expected to be 0 or 1, got " << tagRowIndex);
            }

            if (Options_.HasRangeIndex_) {
                auto tagRangeIndex = Parser_->ParseVariant8Tag();
                if (tagRangeIndex == 1) {
                    RangeIndex_ = Parser_->ParseInt64();
                } else {
                    Y_ENSURE(tagRangeIndex == 0, "Tag for range_index was expected to be 0 or 1, got " << tagRangeIndex);
                }
            }

            break;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR("Read error: %v", ex.what());

            if (!PrepareRetry(std::make_exception_ptr(ex))) {
                throw;
            }
        }
    }

    RowTaken_ = false;
}

ui32 TSkiffRowTableReader::GetTableIndex() const
{
    CheckValidity();
    return TableIndex_;
}

ui32 TSkiffRowTableReader::GetRangeIndex() const
{
    CheckValidity();
    return RangeIndex_.GetOrElse(0) + RangeIndexShift_;
}

ui64 TSkiffRowTableReader::GetRowIndex() const
{
    CheckValidity();
    return RowIndex_.GetOrElse(0ULL);
}

void TSkiffRowTableReader::NextKey() {
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

TMaybe<size_t> TSkiffRowTableReader::GetReadByteCount() const {
    return Input_.GetReadByteCount();
}

bool TSkiffRowTableReader::IsEndOfStream() const {
    return IsEndOfStream_;
}

bool TSkiffRowTableReader::IsRawReaderExhausted() const {
    return Finished_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
