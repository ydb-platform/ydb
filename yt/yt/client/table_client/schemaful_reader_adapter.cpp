#include "schemaful_reader_adapter.h"
#include "schemaless_row_reorderer.h"

#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {

using namespace NYson;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchemafulReaderAdapter)

struct TSchemafulReaderAdapterPoolTag { };

class TSchemafulReaderAdapter
    : public ISchemafulUnversionedReader
{
public:
    TSchemafulReaderAdapter(
        ISchemalessUnversionedReaderPtr underlyingReader,
        TTableSchemaPtr schema,
        TKeyColumns keyColumns,
        bool ignoreRequired)
        : UnderlyingReader_(std::move(underlyingReader))
        , ReaderSchema_(std::move(schema))
        , RowReorderer_(TNameTable::FromSchema(*ReaderSchema_), RowBuffer_, /*deepCapture*/ false, std::move(keyColumns))
        , IgnoreRequired_(ignoreRequired)
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (ErrorPromise_.IsSet()) {
            return CreateEmptyUnversionedRowBatch();
        }

        std::vector<TUnversionedRow> schemafulRows;
        schemafulRows.reserve(options.MaxRowsPerRead);
        RowBuffer_->Clear();

        CurrentBatch_ = UnderlyingReader_->Read(options);
        if (!CurrentBatch_) {
            return nullptr;
        }

        try {
            for (auto schemalessRow : CurrentBatch_->MaterializeRows()) {
                if (!schemalessRow) {
                    schemafulRows.push_back(TUnversionedRow());
                    continue;
                }

                auto schemafulRow = RowReorderer_.ReorderKey(schemalessRow);

                for (int valueIndex = 0; valueIndex < std::ssize(ReaderSchema_->Columns()); ++valueIndex) {
                    const auto& value = schemafulRow[valueIndex];
                    ValidateDataValue(value);
                    // The underlying schemaless reader may unpack typed scalar values even
                    // if the schema has "any" type. For schemaful reader, this is not an expected behavior
                    // so we have to convert such values back into YSON.
                    // Cf. YT-5396
                    if (ReaderSchema_->Columns()[valueIndex].GetWireType() == EValueType::Any &&
                        value.Type != EValueType::Any &&
                        value.Type != EValueType::Null)
                    {
                        schemafulRow[valueIndex] = MakeAnyFromScalar(value);
                    } else {
                        ValidateValueType(value, *ReaderSchema_, valueIndex, /*typeAnyAcceptsAllValues*/ false, IgnoreRequired_);
                    }
                }
                schemafulRows.push_back(schemafulRow);
            }
        } catch (const std::exception& ex) {
            ErrorPromise_.Set(ex);
            return CreateEmptyUnversionedRowBatch();
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(schemafulRows), MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        if (ErrorPromise_.IsSet()) {
            return ErrorPromise_.ToFuture();
        } else {
            return UnderlyingReader_->GetReadyEvent();
        }
    }

    TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

private:
    const ISchemalessUnversionedReaderPtr UnderlyingReader_;
    const TTableSchemaPtr ReaderSchema_;

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TSchemafulReaderAdapterPoolTag());
    TSchemalessRowReorderer RowReorderer_;

    const bool IgnoreRequired_;

    IUnversionedRowBatchPtr CurrentBatch_;
    TBlobOutput ValueBuffer_;

    const TPromise<void> ErrorPromise_ = NewPromise<void>();


    TUnversionedValue MakeAnyFromScalar(const TUnversionedValue& value)
    {
        ValueBuffer_.Clear();
        TBufferedBinaryYsonWriter writer(&ValueBuffer_);
        switch (value.Type) {
            case EValueType::Int64:
                writer.OnInt64Scalar(value.Data.Int64);
                break;
            case EValueType::Uint64:
                writer.OnUint64Scalar(value.Data.Uint64);
                break;
            case EValueType::Double:
                writer.OnDoubleScalar(value.Data.Double);
                break;
            case EValueType::Boolean:
                writer.OnBooleanScalar(value.Data.Boolean);
                break;
            case EValueType::String:
                writer.OnStringScalar(value.AsStringBuf());
                break;
            case EValueType::Null:
                writer.OnEntity();
                break;
            case EValueType::Composite:
                writer.OnRaw(value.AsStringBuf());
                break;

            case EValueType::Min:
            case EValueType::Max:
            case EValueType::TheBottom:
            default:
                YT_ABORT();
        }
        writer.Flush();
        auto ysonSize = ValueBuffer_.Size();
        auto* ysonBuffer = RowBuffer_->GetPool()->AllocateUnaligned(ysonSize);
        ::memcpy(ysonBuffer, ValueBuffer_.Begin(), ysonSize);
        return MakeUnversionedAnyValue(TStringBuf(ysonBuffer, ysonSize), value.Id);
    }
};

DEFINE_REFCOUNTED_TYPE(TSchemafulReaderAdapter)

ISchemafulUnversionedReaderPtr CreateSchemafulReaderAdapter(
    TSchemalessReaderFactory createReader,
    TTableSchemaPtr schema,
    const TColumnFilter& columnFilter,
    bool ignoreRequired)
{
    TKeyColumns keyColumns;
    for (const auto& columnSchema : schema->Columns()) {
        keyColumns.push_back(columnSchema.Name());
    }

    auto nameTable = TNameTable::FromSchema(*schema);
    auto underlyingReader = createReader(
        nameTable,
        columnFilter.IsUniversal() ? TColumnFilter(schema->GetColumnCount()) : columnFilter);

    auto result = New<TSchemafulReaderAdapter>(
        std::move(underlyingReader),
        std::move(schema),
        std::move(keyColumns),
        ignoreRequired);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
