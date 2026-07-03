#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_data_format.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/util/string_view.h>
#include <parquet/arrow/writer.h>

namespace NKikimr::NDataShard {

namespace {

class TCheckpointOutputStream : public arrow::io::OutputStream {
public:
    TCheckpointOutputStream() : Buffer_(), TotalWritten_(0), IsOpen_(true) {}

    arrow::Status Close() override {
        IsOpen_ = false;
        return arrow::Status::OK();
    }

    bool closed() const override { return !IsOpen_; }

    arrow::Result<int64_t> Tell() const override { return TotalWritten_; }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        Buffer_.Append((const char *)(data), nbytes);
        TotalWritten_ += nbytes;
        return arrow::Status::OK();
    }

    using arrow::io::Writable::Write;

    TBuffer Checkpoint() {
        TBuffer buffer;
        Buffer_.Swap(buffer);
        return buffer;
    }

    size_t GetBufferSize() const { return Buffer_.Size(); }

private:
    TBuffer Buffer_;
    int64_t TotalWritten_;
    bool IsOpen_;
};

class TDataFormatParquet: public IExportDataFormat {
    using TTagToColumn = IExport::TTableColumns;

private:

static std::shared_ptr<parquet::WriterProperties> CreateWriteProperties(const TParquetExportSettings& settings) {
    auto builder = std::make_unique<parquet::WriterProperties::Builder>();
    if (settings.CompressionSettings) {
        switch (settings.CompressionSettings->Algorithm) {
        case TParquetExportSettings::TCompressionSettings::EAlgorithm::Zstd:
            builder->compression(arrow::Compression::ZSTD);
            builder->compression_level(settings.CompressionSettings->Level);
            break;
        }
    }
    return builder->build();
}

bool FlushRowGroup(bool last) {
    if (!ErrorString.empty()) {
        return false;
    }

    arrow::Status status;

    if (!ArrowWriter) {
        auto arrowPropsBuilder = parquet::ArrowWriterProperties::Builder();
        arrowPropsBuilder.store_schema();
        auto arrowProps = arrowPropsBuilder.build();

        if (!(status = parquet::arrow::FileWriter::Open(
                *Schema, arrow::default_memory_pool(), OutStream, WriteProperties,
                    arrowProps, &ArrowWriter))
            .ok()) {

            ErrorString = TStringBuilder() << "Failed to open parquet file writer: " << status.message();
            return false;
        }
    }

    if (BatchBuilder->Rows() != 0) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        auto batch = BatchBuilder->FlushBatch(true, false);
        batches.push_back(batch);
        auto tableResult = arrow::Table::FromRecordBatches(batches);
        if (!tableResult.ok()) {
            ErrorString = TStringBuilder() << "Failed to make table from batches: " << tableResult.status().message();
            return false;
        };

        auto table = tableResult.ValueOrDie();
        if (!(status = ArrowWriter->WriteTable(*table, table->num_rows())).ok()) {
            ErrorString = TStringBuilder() << "Failed to write table to parquet file: " << status.message();
            return false;
        }
    }

    if (last) {
        if (!(status = ArrowWriter->Close()).ok()) {
            ErrorString = TStringBuilder() << "Failed to close parquet file writer: " << status.message();
            return false;
        }
    }

    return true;
}

public:

TDataFormatParquet(TParquetExportSettings&& settings)
    : Columns(std::move(settings.Columns))
    , RowGroupSize(settings.RowGroupSize)
    , WriteProperties(CreateWriteProperties(settings))
    , OutStream(std::make_shared<TCheckpointOutputStream>())
{
    Y_ENSURE(RowGroupSize > 0);
}

~TDataFormatParquet() = default;

bool ColumnsOrder(const TVector<ui32>& tags) override {
    Y_ENSURE(tags.size() == Columns.size());

    ArrowWriter.reset();

    std::vector<std::pair<TString, NScheme::TTypeInfo>> ydbColumns;
    std::set<std::string> notNullColumns;
    for (const auto& tag : tags) {
        auto it = Columns.find(tag);
        Y_ENSURE(it != Columns.end());
        auto column = it->second;

        ydbColumns.push_back({column.Name, column.Type});
        if (column.NotNull) {
            notNullColumns.insert(column.Name);
        }
    }

    auto schemaRes = NArrow::MakeArrowSchema(ydbColumns, notNullColumns);
    if (!schemaRes.ok()) {
        ErrorString = TStringBuilder() << "Failed to make arrow schema: " << schemaRes.status().message();
        return false;
    }
    Schema.swap(schemaRes.ValueOrDie());

    arrow::Status status;
    BatchBuilder = std::make_unique<NArrow::TArrowBatchBuilder>();
    if (!(status = BatchBuilder->Start(ydbColumns, Schema)).ok()) {
        ErrorString = TStringBuilder() << "Failed to start batch builder: " << status.message();
        return false;
    }

    return true;
}

TMaybe<TBuffer> Collect(const NTable::IScan::TRow& row) override {
    if (!ErrorString.empty()) {
        return Nothing();
    }

    BatchBuilder->AddRow(*row);
    if (BatchBuilder->Rows() >= RowGroupSize) {
        if(!FlushRowGroup(false)) {
            return Nothing();
        }
    }

    return TBuffer();
}

TMaybe<TBuffer> Flush(bool last) override {
    if (!ErrorString.empty()) {
        return Nothing();
    }

    if (!FlushRowGroup(last)) {
        return Nothing();
    }

    return OutStream->Checkpoint();
}

void Clear() override {
    ErrorString.clear();
    if (BatchBuilder) {
        BatchBuilder->FlushBatch(true, false);
    }
    ArrowWriter.reset();
    OutStream.reset(new TCheckpointOutputStream());
}

size_t GetReadyOutputBytes() const override {
    // Encoded row groups accumulate in the output stream until the buffer flushes them.
    return OutStream->GetBufferSize();
}

TString GetError() const override {
    return ErrorString;
}

private:
    const TTagToColumn Columns;
    const ui64 RowGroupSize;

    std::shared_ptr<parquet::WriterProperties> WriteProperties;
    std::shared_ptr<arrow::Schema> Schema;
    std::shared_ptr<TCheckpointOutputStream> OutStream;
    std::unique_ptr<parquet::arrow::FileWriter> ArrowWriter;
    std::unique_ptr<NArrow::TArrowBatchBuilder> BatchBuilder;

    TString ErrorString;
};

} // namespace

std::unique_ptr<IExportDataFormat> CreateExportDataFormat(TParquetExportSettings&& settings) {
    return std::make_unique<TDataFormatParquet>(std::move(settings));
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
