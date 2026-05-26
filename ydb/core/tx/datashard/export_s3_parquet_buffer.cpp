#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_s3_buffer.h"

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <yql/essentials/types/binary_json/read.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/util/string_view.h>
#include <parquet/arrow/writer.h>

#include <util/datetime/base.h>
#include <util/generic/buffer.h>
#include <util/stream/buffer.h>

#include <sstream>

namespace NKikimr::NDataShard {

namespace {

class ICheckpointOutputStream : public arrow::io::OutputStream {
public:
    virtual ~ICheckpointOutputStream() = default;

    static std::shared_ptr<ICheckpointOutputStream> Create();

    arrow::Status Close() override = 0;
    bool closed() const override = 0;
    arrow::Result<int64_t> Tell() const override = 0;
    arrow::Status Write(const void *data, int64_t nbytes) override = 0;
    using arrow::io::Writable::Write;

    virtual TBuffer Checkpoint() = 0;
    virtual size_t GetBufferSize() const = 0;
};

class TS3ParquetExportBuffer : public NExportScan::IBuffer {
    using TTagToColumn = IExport::TTableColumns;
    using TTagToIndex = THashMap<ui32, ui32>; // index in IScan::TRow

public:
    explicit TS3ParquetExportBuffer(TS3ExportBufferSettings &&settings);

    void ColumnsOrder(const TVector<ui32> &tags) override;
    bool Collect(const NTable::IScan::TRow &row) override;
    virtual IEventBase *
    PrepareEvent(bool last, NExportScan::IBuffer::TStats &stats) override;
    void Clear() override;
    bool IsFilled() const override;
    TString GetError() const override;

private:
    inline ui64 GetRowsLimit() const { return RowsLimit; }
    inline ui64 GetBytesLimit() const { return MaxBytes; }

    static NBackup::IChecksum *CreateChecksum(
        const TMaybe<TS3ExportBufferSettings::TChecksumSettings> &settings);

    bool Flush(bool last);

private:
    const TTagToColumn Columns;
    const ui64 ParquetRowGroupSize;
    const ui64 RowsLimit;
    const ui64 MaxBytes;
    const ui64 MinBytes;

    ui64 Rows = 0;
    ui64 BytesRead = 0;

    TMaybe<TS3ExportBufferSettings::TChecksumSettings> ChecksumSettings;
    NBackup::IChecksum::TPtr Checksum;
    std::shared_ptr<parquet::WriterProperties> WriteProperties;

    TString ErrorString;

    std::shared_ptr<arrow::Schema> Schema;
    std::shared_ptr<ICheckpointOutputStream> OutStream;
    std::unique_ptr<parquet::arrow::FileWriter> ArrowWriter;
    NArrow::TArrowBatchBuilder BatchBuilder;
}; // TS3ParquetExportBuffer

TS3ParquetExportBuffer::TS3ParquetExportBuffer(
    TS3ExportBufferSettings &&settings)
    : Columns(std::move(settings.Columns))
    , ParquetRowGroupSize(settings.ParquetRowGroupSize)
    , RowsLimit(settings.MaxRows)
    , MaxBytes(settings.MaxBytes)
    , MinBytes(settings.MinBytes)
    , ChecksumSettings(settings.ChecksumSettings)
    , Checksum(CreateChecksum(settings.ChecksumSettings))
    , OutStream(ICheckpointOutputStream::Create())
{
    auto builder = std::make_unique<parquet::WriterProperties::Builder>();
    if (settings.CompressionSettings) {
        switch (settings.CompressionSettings->Algorithm) {
        case TS3ExportBufferSettings::TCompressionSettings::EAlgorithm::Zstd:
            builder->compression(arrow::Compression::ZSTD);
            break;
        }
        if (settings.CompressionSettings->CompressionLevel != -1) {
            builder->compression_level(
                settings.CompressionSettings->CompressionLevel);
        }
    }
    WriteProperties = builder->build();
}

NBackup::IChecksum *TS3ParquetExportBuffer::CreateChecksum(const TMaybe<TS3ExportBufferSettings::TChecksumSettings> &settings) {
    if (settings) {
        switch (settings->ChecksumType) {
        case TS3ExportBufferSettings::TChecksumSettings::EChecksumType::Sha256:
            return NBackup::CreateChecksum();
        }
    }
    return nullptr;
}

void TS3ParquetExportBuffer::ColumnsOrder(const TVector<ui32> &tags) {
    Y_ENSURE(tags.size() == Columns.size());

    std::vector<std::pair<TString, NScheme::TTypeInfo>> ydbColumns;
    std::set<std::string> notNullColumns;
    for (const auto &tag : tags) {
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
        ErrorString = (std::ostringstream() << "Failed to make arrow schema: " << schemaRes.status().message()).str();
        return;
    }
    Schema.swap(schemaRes.ValueOrDie());

    arrow::Status status;
    if (!(status = BatchBuilder.Start(ydbColumns, Schema)).ok()) {
        ErrorString = (std::ostringstream() << "Failed to start batch builder: " << status.message()).str();
        return;
    }

    ArrowWriter.reset();
}

bool TS3ParquetExportBuffer::Collect(const NTable::IScan::TRow &row) {
    if (!ErrorString.empty()) {
        return false;
    }

    auto bytesBefore = BatchBuilder.Bytes();
    BatchBuilder.AddRow(*row);
    BytesRead += BatchBuilder.Bytes() - bytesBefore;
    Rows++;

    if (BatchBuilder.Rows() >= ParquetRowGroupSize) {
        return Flush(false);
    }

    return true;
}

IEventBase *TS3ParquetExportBuffer::PrepareEvent(bool last, NExportScan::IBuffer::TStats &stats) {
    stats.Rows = Rows;
    stats.BytesRead = BytesRead;

    if (!Flush(last)) {
        return nullptr;
    }

    auto buffer = OutStream->Checkpoint();
    if (Checksum) {
        Checksum->AddData(TStringBuf(buffer.Data(), buffer.Size()));
    }
    stats.BytesSent = buffer.Size();

    Rows = 0;
    BytesRead = 0;

    if (Checksum && last) {
        return new TEvExportScan::TEvBuffer<TBuffer>(std::move(buffer), last, Checksum->Finalize());
    } else {
        return new TEvExportScan::TEvBuffer<TBuffer>(std::move(buffer), last);
    }
}

bool TS3ParquetExportBuffer::Flush(bool last) {
    arrow::Status status;

    if (!ArrowWriter) {
        auto arrowPropsBuilder = parquet::ArrowWriterProperties::Builder();
        arrowPropsBuilder.store_schema();
        auto arrowProps = arrowPropsBuilder.build();

        if (!(status = parquet::arrow::FileWriter::Open(
                *Schema, arrow::default_memory_pool(), OutStream, WriteProperties,
                    arrowProps, &ArrowWriter))
            .ok()) {

            ErrorString =
                (std::ostringstream()
                    << "Failed to open parquet file writer: " << status.message())
                .str();
            return false;
        }
    }

    if (BatchBuilder.Rows() != 0) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        auto batch = BatchBuilder.FlushBatch(true, false);
        batches.push_back(batch);
        auto tableResult = arrow::Table::FromRecordBatches(batches);
        if (!tableResult.ok()) {
            ErrorString =
                (std::ostringstream()
                    << "Failed to make table from batches: " << tableResult.status().message())
                .str();
            return false;
        };

        auto table = tableResult.ValueOrDie();
        if (!(status = ArrowWriter->WriteTable(*table, table->num_rows())).ok()) {
            ErrorString =
                (std::ostringstream()
                    << "Failed to write table to parquet file: " << status.message())
                .str();
            return false;
        }
    }

    if (last) {
        if (!(status = ArrowWriter->Close()).ok()) {
            ErrorString =
                (std::ostringstream()
                    << "Failed to close parquet file writer: " << status.message())
                .str();
            return false;
        }
    }

    return true;
}

void TS3ParquetExportBuffer::Clear() {
    Rows = 0;
    BytesRead = 0;
    BatchBuilder.FlushBatch(true, false);
    ArrowWriter.reset();
    auto newOutStream = ICheckpointOutputStream::Create();
    OutStream.swap(newOutStream);
    Checksum.reset(CreateChecksum(ChecksumSettings));
}

bool TS3ParquetExportBuffer::IsFilled() const {
    return (OutStream->GetBufferSize() >= MinBytes) &&
           (Rows >= GetRowsLimit() || BytesRead >= GetBytesLimit());
}

TString TS3ParquetExportBuffer::GetError() const {
    return ErrorString;
}

class CheckpointOutputStream : public ICheckpointOutputStream {
public:
    CheckpointOutputStream() : Buffer_(), TotalWritten_(0), IsOpen_(true) {}

    // Implement the OutputStream interface
    arrow::Status Close() override {
        IsOpen_ = false;
        return arrow::Status::OK();
    }

    bool closed() const override { return !IsOpen_; }

    arrow::Result<int64_t> Tell() const override { return TotalWritten_; }

    arrow::Status Write(const void *data, int64_t nbytes) override {
        Buffer_.Append((const char *)(data), nbytes);
        TotalWritten_ += nbytes;
        return arrow::Status::OK();
    }

    using arrow::io::Writable::Write;

    TBuffer Checkpoint() override {
        TBuffer buffer;
        Buffer_.Swap(buffer);
        return buffer;
    }

    size_t GetBufferSize() const override { return Buffer_.Size(); }

private:
    TBuffer Buffer_;
    int64_t TotalWritten_;
    bool IsOpen_;
};

std::shared_ptr<ICheckpointOutputStream> ICheckpointOutputStream::Create() {
    return std::make_shared<CheckpointOutputStream>();
}

} // anonymous namespace

NExportScan::IBuffer *CreateS3ParquetExportBuffer(TS3ExportBufferSettings &&settings) {
    return new TS3ParquetExportBuffer(std::move(settings));
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
