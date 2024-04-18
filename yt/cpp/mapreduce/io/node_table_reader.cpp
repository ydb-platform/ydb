#include "node_table_reader.h"

#include <yt/cpp/mapreduce/common/node_builder.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/parser.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRowBuilder
    : public ::NYson::TYsonConsumerBase
{
public:
    explicit TRowBuilder(TMaybe<TRowElement>* resultRow);

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnBeginList() override;
    void OnEntity() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;

    void Finalize();

private:
    THolder<TNodeBuilder> Builder_;
    TRowElement Row_;
    int Depth_ = 0;
    bool Started_ = false;
    TMaybe<TRowElement>* ResultRow_;

    void SaveResultRow();
};

TRowBuilder::TRowBuilder(TMaybe<TRowElement>* resultRow)
    : ResultRow_(resultRow)
{ }

void TRowBuilder::OnStringScalar(TStringBuf value)
{
    Row_.Size += sizeof(TNode) + sizeof(TString) + value.size();
    Builder_->OnStringScalar(value);
}

void TRowBuilder::OnInt64Scalar(i64 value)
{
    Row_.Size += sizeof(TNode);
    Builder_->OnInt64Scalar(value);
}

void TRowBuilder::OnUint64Scalar(ui64 value)
{
    Row_.Size += sizeof(TNode);
    Builder_->OnUint64Scalar(value);
}

void TRowBuilder::OnDoubleScalar(double value)
{
    Row_.Size += sizeof(TNode);
    Builder_->OnDoubleScalar(value);
}

void TRowBuilder::OnBooleanScalar(bool value)
{
    Row_.Size += sizeof(TNode);
    Builder_->OnBooleanScalar(value);
}

void TRowBuilder::OnBeginList()
{
    ++Depth_;
    Builder_->OnBeginList();
}

void TRowBuilder::OnEntity()
{
    Row_.Size += sizeof(TNode);
    Builder_->OnEntity();
}

void TRowBuilder::OnListItem()
{
    if (Depth_ == 0) {
        SaveResultRow();
    } else {
        Builder_->OnListItem();
    }
}

void TRowBuilder::OnEndList()
{
    --Depth_;
    Builder_->OnEndList();
}

void TRowBuilder::OnBeginMap()
{
    ++Depth_;
    Builder_->OnBeginMap();
}

void TRowBuilder::OnKeyedItem(TStringBuf key)
{
    Row_.Size += sizeof(TString) + key.size();
    Builder_->OnKeyedItem(key);
}

void TRowBuilder::OnEndMap()
{
    --Depth_;
    Builder_->OnEndMap();
}

void TRowBuilder::OnBeginAttributes()
{
    ++Depth_;
    Builder_->OnBeginAttributes();
}

void TRowBuilder::OnEndAttributes()
{
    --Depth_;
    Builder_->OnEndAttributes();
}

void TRowBuilder::SaveResultRow()
{
    if (!Started_) {
        Started_ = true;
    } else {
        *ResultRow_ = std::move(Row_);
    }
    Row_.Reset();
    Builder_.Reset(new TNodeBuilder(&Row_.Node));
}

void TRowBuilder::Finalize()
{
    if (Started_) {
        *ResultRow_ = std::move(Row_);
    }
}

////////////////////////////////////////////////////////////////////////////////

TNodeTableReader::TNodeTableReader(::TIntrusivePtr<TRawTableReader> input)
    : Input_(std::move(input))
{
    PrepareParsing();
    Next();
}

TNodeTableReader::~TNodeTableReader()
{
}

void TNodeTableReader::ParseListFragmentItem() {
    if (!Parser_->Parse()) {
        Builder_->Finalize();
        IsLast_ = true;
    }
}

const TNode& TNodeTableReader::GetRow() const
{
    CheckValidity();
    if (!Row_) {
        ythrow yexception() << "Row is moved";
    }
    return Row_->Node;
}

void TNodeTableReader::MoveRow(TNode* result)
{
    CheckValidity();
    if (!Row_) {
        ythrow yexception() << "Row is moved";
    }
    *result = std::move(Row_->Node);
    Row_.Clear();
}

bool TNodeTableReader::IsValid() const
{
    return Valid_;
}

void TNodeTableReader::Next()
{
    try {
        NextImpl();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR("TNodeTableReader::Next failed: %v", ex.what());
        throw;
    }
}

void TNodeTableReader::NextImpl()
{
    CheckValidity();

    if (RowIndex_) {
        ++*RowIndex_;
    }

    // At the begin of stream parser doesn't return a finished row.
    ParseFirstListFragmentItem();

    while (true) {
        if (IsLast_) {
            Finished_ = true;
            Valid_ = false;
            break;
        }

        try {
            ParseListFragmentItem();
        } catch (std::exception& ex) {
            NeedParseFirst_ = true;
            OnStreamError(std::current_exception(), ex.what());
            ParseFirstListFragmentItem();
            continue;
        }

        Row_ = std::move(*NextRow_);
        if (!Row_) {
            throw yexception() << "No row in NextRow_";
        }

        // We successfully parsed one more row from the stream,
        // so reset retry count to their initial value.
        Input_.ResetRetries();

        if (!Row_->Node.IsNull()) {
            AtStart_ = false;
            break;
        }

        for (auto& entry : Row_->Node.GetAttributes().AsMap()) {
            if (entry.first == "key_switch") {
                if (!AtStart_) {
                    Valid_ = false;
                }
            } else if (entry.first == "table_index") {
                TableIndex_ = static_cast<ui32>(entry.second.AsInt64());
            } else if (entry.first == "row_index") {
                RowIndex_ = static_cast<ui64>(entry.second.AsInt64());
            } else if (entry.first == "range_index") {
                RangeIndex_ = static_cast<ui32>(entry.second.AsInt64());
            } else if (entry.first == "tablet_index") {
                TabletIndex_ = entry.second.AsInt64();
            } else if (entry.first == "end_of_stream") {
                IsEndOfStream_ = true;
            }
        }

        if (!Valid_) {
            break;
        }
    }
}

void TNodeTableReader::ParseFirstListFragmentItem()
{
    while (NeedParseFirst_) {
        try {
            ParseListFragmentItem();
            NeedParseFirst_ = false;
            break;
        } catch (std::exception& ex) {
            OnStreamError(std::current_exception(), ex.what());
        }
    }
}

ui32 TNodeTableReader::GetTableIndex() const
{
    CheckValidity();
    return TableIndex_;
}

ui32 TNodeTableReader::GetRangeIndex() const
{
    CheckValidity();
    return RangeIndex_.GetOrElse(0) + RangeIndexShift_;
}

ui64 TNodeTableReader::GetRowIndex() const
{
    CheckValidity();
    return RowIndex_.GetOrElse(0UL);
}

i64 TNodeTableReader::GetTabletIndex() const
{
    CheckValidity();
    return TabletIndex_.GetOrElse(0L);
}

void TNodeTableReader::NextKey()
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
}

TMaybe<size_t> TNodeTableReader::GetReadByteCount() const
{
    return Input_.GetReadByteCount();
}

bool TNodeTableReader::IsEndOfStream() const
{
    return IsEndOfStream_;
}

bool TNodeTableReader::IsRawReaderExhausted() const
{
    return Finished_;
}

////////////////////////////////////////////////////////////////////////////////

void TNodeTableReader::PrepareParsing()
{
    NextRow_.Clear();
    Builder_.Reset(new TRowBuilder(&NextRow_));
    Parser_.Reset(new ::NYson::TYsonListParser(Builder_.Get(), &Input_));
}

void TNodeTableReader::OnStreamError(std::exception_ptr exception, TString error)
{
    YT_LOG_ERROR("Read error (RangeIndex: %v, RowIndex: %v, Error: %v)", RangeIndex_, RowIndex_, error);
    Exception_ = exception;
    if (Input_.Retry(RangeIndex_, RowIndex_, exception)) {
        if (RangeIndex_) {
            RangeIndexShift_ += *RangeIndex_;
        }
        RowIndex_.Clear();
        RangeIndex_.Clear();
        PrepareParsing();
    } else {
        std::rethrow_exception(Exception_);
    }
}

void TNodeTableReader::CheckValidity() const
{
    if (!Valid_) {
        ythrow yexception() << "Iterator is not valid";
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
