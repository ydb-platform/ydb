#include "versioned_writer.h"
#include "config.h"

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TVersionedWriter::TVersionedWriter(
    NConcurrency::IAsyncOutputStreamPtr stream,
    NTableClient::TTableSchemaPtr schema,
    const std::function<std::unique_ptr<IFlushableYsonConsumer>(IZeroCopyOutput*)>& consumerBuilder)
    : Stream_(std::move(stream))
    , Schema_(std::move(schema))
    , Consumer_(consumerBuilder(&Buffer_))
{ }

TFuture<void> TVersionedWriter::Close()
{
    return Result_;
}

bool TVersionedWriter::Write(TRange<TVersionedRow> rows)
{
    Buffer_.Clear();

    auto consumeUnversionedData = [&] (const TUnversionedValue& value) {
        switch (value.Type) {
            case EValueType::Int64:
                Consumer_->OnInt64Scalar(value.Data.Int64);
                return;
            case EValueType::Uint64:
                Consumer_->OnUint64Scalar(value.Data.Uint64);
                return;
            case EValueType::Double:
                Consumer_->OnDoubleScalar(value.Data.Double);
                return;
            case EValueType::Boolean:
                Consumer_->OnBooleanScalar(value.Data.Boolean);
                return;
            case EValueType::String:
                Consumer_->OnStringScalar(value.AsStringBuf());
                return;
            case EValueType::Null:
                Consumer_->OnEntity();
                return;
            case EValueType::Any:
                Consumer_->OnRaw(value.AsStringBuf(), EYsonType::Node);
                return;
            case EValueType::Composite:
            case EValueType::Min:
            case EValueType::Max:
            case EValueType::TheBottom:
                break;
        }
        ThrowUnexpectedValueType(value.Type);
    };

    for (auto row : rows) {
        if (!row) {
            Consumer_->OnEntity();
            continue;
        }

        Consumer_->OnBeginAttributes();
        {
            Consumer_->OnKeyedItem("write_timestamps");
            Consumer_->OnBeginList();
            for (auto timestamp : row.WriteTimestamps()) {
                Consumer_->OnListItem();
                Consumer_->OnUint64Scalar(timestamp);
            }
            Consumer_->OnEndList();
        }
        {
            Consumer_->OnKeyedItem("delete_timestamps");
            Consumer_->OnBeginList();
            for (auto timestamp : row.DeleteTimestamps()) {
                Consumer_->OnListItem();
                Consumer_->OnUint64Scalar(timestamp);
            }
            Consumer_->OnEndList();
        }
        Consumer_->OnEndAttributes();

        Consumer_->OnBeginMap();
        for (const auto& value : row.Keys()) {
            const auto& column = Schema_->Columns()[value.Id];
            Consumer_->OnKeyedItem(column.Name());
            consumeUnversionedData(value);
        }
        for (auto valuesBeginIt = row.BeginValues(), valuesEndIt = row.EndValues(); valuesBeginIt != valuesEndIt; /**/) {
            auto columnBeginIt = valuesBeginIt;
            auto columnEndIt = columnBeginIt;
            while (columnEndIt < valuesEndIt && columnEndIt->Id == columnBeginIt->Id) {
                ++columnEndIt;
            }

            const auto& column = Schema_->Columns()[columnBeginIt->Id];
            Consumer_->OnKeyedItem(column.Name());
            Consumer_->OnBeginList();
            while (columnBeginIt != columnEndIt) {
                Consumer_->OnListItem();
                Consumer_->OnBeginAttributes();
                Consumer_->OnKeyedItem("timestamp");
                Consumer_->OnUint64Scalar(columnBeginIt->Timestamp);
                Consumer_->OnKeyedItem("aggregate");
                Consumer_->OnBooleanScalar(Any(columnBeginIt->Flags & EValueFlags::Aggregate));
                Consumer_->OnEndAttributes();
                consumeUnversionedData(*columnBeginIt);
                ++columnBeginIt;
            }
            Consumer_->OnEndList();

            valuesBeginIt = columnEndIt;
        }
        Consumer_->OnEndMap();
    }

    Consumer_->Flush();
    auto buffer = Buffer_.Flush();
    Result_ = Stream_->Write(buffer);
    return Result_.IsSet() && Result_.Get().IsOK();
}

TFuture<void> TVersionedWriter::GetReadyEvent()
{
    return Result_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
