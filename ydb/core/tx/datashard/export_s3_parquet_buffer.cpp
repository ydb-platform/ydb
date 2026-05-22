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

class IArrayBuilder {
public:
    virtual ~IArrayBuilder() = default;

    virtual arrow::Status Append(const TCell &cell) = 0;
    virtual arrow::Status AppendNull() = 0;
    virtual std::shared_ptr<arrow::Array> Finish() = 0;
    virtual void Reset() = 0;
};

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
    std::shared_ptr<IArrayBuilder> CreateArrayBuilder(NScheme::TTypeId typeId);
    std::shared_ptr<arrow::DataType> ArrowDataType(NScheme::TTypeId typeId);

private:
    const TTagToColumn Columns;
    const ui64 RowGroupSize;
    const ui64 RowsLimit;
    const ui64 MaxBytes;
    const ui64 MinBytes;

    ui64 Rows = 0;
    ui64 BytesRead = 0;

    NBackup::IChecksum::TPtr Checksum;
    std::shared_ptr<parquet::WriterProperties> WriteProperties;

    TString ErrorString;

    std::shared_ptr<arrow::Schema> Schema;
    std::shared_ptr<ICheckpointOutputStream> OutStream;
    std::unique_ptr<parquet::arrow::FileWriter> ArrowWriter;
    NArrow::TArrowBatchBuilder BatchBuilder;

    static constexpr TStringBuf LogPrefix() { return "parquet"sv; }
}; // TS3ParquetExportBuffer

TS3ParquetExportBuffer::TS3ParquetExportBuffer(
    TS3ExportBufferSettings &&settings)
    : Columns(std::move(settings.Columns))
    , RowGroupSize(settings.RowGroupSize)
    , RowsLimit(settings.MaxRows)
    , MaxBytes(settings.MaxBytes)
    , MinBytes(settings.MinBytes)
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
    for (const auto& tag : tags) {
        auto it = Columns.find(tag);
        Y_ENSURE(it != Columns.end());
        auto column = it->second;

        ydbColumns.push_back({column.Name, column.Type});
        if (column.NotNull) {
            notNullColumns.insert(column.Name);
        }
    }

    arrow::Status status;
    if (!(status = BatchBuilder.Start(ydbColumns)).ok()) {
        ErrorString = (std::ostringstream() << "Failed to start batch builder: " << status.message()).str();
        return;
    }

    auto schemaRes = NArrow::MakeArrowSchema(ydbColumns, notNullColumns);
    if (!schemaRes.ok()) {
        ErrorString = (std::ostringstream() << "Failed to make arrow schema: " << schemaRes.status().message()).str();
        return;
    }
    Schema.swap(schemaRes.ValueOrDie());

    ArrowWriter.reset();
}

bool TS3ParquetExportBuffer::Collect(const NTable::IScan::TRow &row) {
    if (!ErrorString.empty()) {
        return false;
    }

    arrow::Status status;

    auto bytesBefore = BatchBuilder.Bytes();
    BatchBuilder.AddRow(*row);
    BytesRead += BatchBuilder.Bytes() - bytesBefore;
    Rows++;

    if (BatchBuilder.Rows() >= RowGroupSize) {
        return Flush(false);
    }

    return true;
}

IEventBase* TS3ParquetExportBuffer::PrepareEvent(bool last, NExportScan::IBuffer::TStats &stats) {
    // std::cerr << "PrepareEvent; last=" << last << "; rows=" << Rows << "; bytes=" << BytesRead << std::endl;

    stats.Rows = Rows;
    stats.BytesRead = BytesRead;

    if (!Flush(last)) {
        // std::cerr << "PrepareEvent; failed" << std::endl;
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
        // std::cerr << "PrepareEvent; checksum" << std::endl;
        return new TEvExportScan::TEvBuffer<TBuffer>(std::move(buffer), last, Checksum->Finalize());
    } else {
        // std::cerr << "PrepareEvent; no checksum" << std::endl;
        return new TEvExportScan::TEvBuffer<TBuffer>(std::move(buffer), last);
    }
}

bool TS3ParquetExportBuffer::Flush(bool last) {
    // std::cerr << "Flush; batchRows=" << BatchBuilder.Rows() << "; totalRows=" << Rows << "; bytes=" << BytesRead << std::endl;

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
}

bool TS3ParquetExportBuffer::IsFilled() const {
    return (OutStream->GetBufferSize() >= MinBytes) &&
           (Rows >= GetRowsLimit() || BytesRead >= GetBytesLimit());
}

TString TS3ParquetExportBuffer::GetError() const { return ErrorString; }

// template <typename T, typename B> class TAsValueBuilder : public IArrayBuilder {
// public:
//     TAsValueBuilder() : builder_(std::make_shared<B>()) {}

//     arrow::Status Append(const TCell &cell) override {
//         return builder_->Append(cell.AsValue<T>());
//     }

//     arrow::Status AppendNull() override { return builder_->AppendNull(); }

//     std::shared_ptr<arrow::Array> Finish() override {
//         auto result = builder_->Finish().ValueOrDie();
//         builder_->Reset();
//         return result;
//     }

//     void Reset() override { builder_->Reset(); }

// private:
//     std::shared_ptr<B> builder_;
// };

// using TInt8Builder = TAsValueBuilder<i8, arrow::Int8Builder>;
// using TUInt8Builder = TAsValueBuilder<ui8, arrow::UInt8Builder>;
// using TInt16Builder = TAsValueBuilder<i16, arrow::Int16Builder>;
// using TUInt16Builder = TAsValueBuilder<ui16, arrow::UInt16Builder>;
// using TInt32Builder = TAsValueBuilder<i32, arrow::Int32Builder>;
// using TUInt32Builder = TAsValueBuilder<ui32, arrow::UInt32Builder>;
// using TInt64Builder = TAsValueBuilder<i64, arrow::Int64Builder>;
// using TUInt64Builder = TAsValueBuilder<ui64, arrow::UInt64Builder>;
// using TFloatBuilder = TAsValueBuilder<float, arrow::FloatBuilder>;
// using TDoubleBuilder = TAsValueBuilder<double, arrow::DoubleBuilder>;
// using TBoolBuilder = TAsValueBuilder<bool, arrow::BooleanBuilder>;

// template <typename B> class TAsBufBuilder : public IArrayBuilder {
// public:
//     TAsBufBuilder() : builder_(std::make_shared<B>()) {}

//     arrow::Status Append(const TCell &cell) override {
//         const auto buf = cell.AsBuf();
//         return builder_->Append(arrow::util::string_view{buf.data(), buf.size()});
//     }

//     arrow::Status AppendNull() override { return builder_->AppendNull(); }

//     std::shared_ptr<arrow::Array> Finish() override {
//         auto result = builder_->Finish().ValueOrDie();
//         builder_->Reset();
//         return result;
//     }

//     void Reset() override { builder_->Reset(); }

// private:
//     std::shared_ptr<B> builder_;
// };

// using TStringBuilder = TAsBufBuilder<arrow::StringBuilder>;

// std::shared_ptr<IArrayBuilder> TS3ParquetExportBuffer::CreateArrayBuilder(NScheme::TTypeId typeId) {
//     switch (typeId) {
//     case NScheme::NTypeIds::Int8:
//         return std::make_shared<TInt8Builder>();
//     case NScheme::NTypeIds::Uint8:
//         return std::make_shared<TUInt8Builder>();
//     case NScheme::NTypeIds::Int16:
//         return std::make_shared<TInt16Builder>();
//     case NScheme::NTypeIds::Uint16:
//     case NScheme::NTypeIds::Date:
//         return std::make_shared<TUInt16Builder>();
//     case NScheme::NTypeIds::Int32:
//         return std::make_shared<TInt32Builder>();
//     case NScheme::NTypeIds::Uint32:
//     case NScheme::NTypeIds::Datetime:
//     case NScheme::NTypeIds::Date32:
//         return std::make_shared<TUInt32Builder>();
//     case NScheme::NTypeIds::Int64:
//     case NScheme::NTypeIds::Datetime64:
//     case NScheme::NTypeIds::Timestamp64:
//     case NScheme::NTypeIds::Interval:
//     case NScheme::NTypeIds::Interval64:
//         return std::make_shared<TInt64Builder>();
//     case NScheme::NTypeIds::Uint64:
//     case NScheme::NTypeIds::Timestamp:
//         return std::make_shared<TUInt64Builder>();
//     case NScheme::NTypeIds::Float:
//         return std::make_shared<TFloatBuilder>();
//     case NScheme::NTypeIds::Double:
//         return std::make_shared<TDoubleBuilder>();
//     case NScheme::NTypeIds::Bool:
//         return std::make_shared<TBoolBuilder>();
//     case NScheme::NTypeIds::String:
//     case NScheme::NTypeIds::String4k:
//     case NScheme::NTypeIds::String2m:
//     case NScheme::NTypeIds::Utf8:
//     case NScheme::NTypeIds::Json:
//     case NScheme::NTypeIds::Yson:
//         return std::make_shared<TStringBuilder>();
//     // TODO(diseaz): JsonDocument, Pg, Uuid, DyNumber, Decimal
//     }

//     ErrorString = "Unsupported type";
//     return nullptr;
// }

// std::shared_ptr<arrow::DataType> TS3ParquetExportBuffer::ArrowDataType(NScheme::TTypeId typeId) {
//     switch (typeId) {
//     case NScheme::NTypeIds::Int8:
//         return arrow::int8();
//     case NScheme::NTypeIds::Uint8:
//         return arrow::uint8();
//     case NScheme::NTypeIds::Int16:
//         return arrow::int16();
//     case NScheme::NTypeIds::Uint16:
//     case NScheme::NTypeIds::Date:
//         return arrow::uint16();
//     case NScheme::NTypeIds::Int32:
//         return arrow::int32();
//     case NScheme::NTypeIds::Uint32:
//     case NScheme::NTypeIds::Date32:
//     case NScheme::NTypeIds::Datetime:
//         return arrow::uint32();
//     case NScheme::NTypeIds::Int64:
//         return arrow::int64();
//     case NScheme::NTypeIds::Uint64:
//         return arrow::uint64();
//     case NScheme::NTypeIds::Interval:
//     case NScheme::NTypeIds::Interval64:
//         return arrow::duration(arrow::TimeUnit::MICRO);
//     case NScheme::NTypeIds::Datetime64:
//         return arrow::timestamp(arrow::TimeUnit::SECOND);
//     case NScheme::NTypeIds::Timestamp:
//     case NScheme::NTypeIds::Timestamp64:
//         return arrow::timestamp(arrow::TimeUnit::MICRO);
//     case NScheme::NTypeIds::Float:
//         return arrow::float32();
//     case NScheme::NTypeIds::Double:
//         return arrow::float64();
//     case NScheme::NTypeIds::Bool:
//         return arrow::boolean();
//     case NScheme::NTypeIds::String:
//     case NScheme::NTypeIds::String4k:
//     case NScheme::NTypeIds::String2m:
//     case NScheme::NTypeIds::Utf8:
//     case NScheme::NTypeIds::Json:
//     case NScheme::NTypeIds::Yson:
//         return arrow::utf8();
//     // TODO(diseaz): JsonDocument, Pg, Uuid, DyNumber, Decimal
//     }

//     ErrorString = "Unsupported type";
//     return nullptr;
// }

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

NExportScan::IBuffer * CreateS3ParquetExportBuffer(TS3ExportBufferSettings &&settings) {
    return new TS3ParquetExportBuffer(std::move(settings));
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS