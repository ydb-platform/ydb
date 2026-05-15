#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_common.h"
#include "export_s3_buffer.h"

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <yql/essentials/types/binary_json/read.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

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

    virtual arrow::Status Append(const TCell& cell) = 0;
    virtual arrow::Status AppendNull() = 0;
    virtual std::shared_ptr<arrow::Array> Finish() = 0;
    virtual void Reset() = 0;
};

class TS3ParquetExportBuffer: public NExportScan::IBuffer {
    using TTagToColumn = IExport::TTableColumns;
    using TTagToIndex = THashMap<ui32, ui32>; // index in IScan::TRow

public:
    explicit TS3ParquetExportBuffer(TS3ExportBufferSettings&& settings);

    void ColumnsOrder(const TVector<ui32>& tags) override;
    bool Collect(const NTable::IScan::TRow& row) override;
    virtual IEventBase* PrepareEvent(bool last, NExportScan::IBuffer::TStats& stats) override;
    void Clear() override;
    bool IsFilled() const override;
    TString GetError() const override;

private:
    inline ui64 GetRowsLimit() const { return RowsLimit; }
    inline ui64 GetBytesLimit() const { return MaxBytes; }

    static NBackup::IChecksum* CreateChecksum(const TMaybe<TS3ExportBufferSettings::TChecksumSettings>& settings);

    TMaybe<TBuffer> Flush(bool storeSchema);
    std::shared_ptr<IArrayBuilder> CreateArrayBuilder(NScheme::TTypeId typeId);
    std::shared_ptr<arrow::DataType> ArrowDataType(NScheme::TTypeId typeId);

private:
    const TTagToColumn Columns;
    const ui64 RowsLimit;
    const ui64 MaxBytes;
    const ui64 MinBytes;

    ui64 Rows = 0;
    ui64 BytesRead = 0;
    bool StoreSchema = true;

    TTagToIndex Indices;
    NBackup::IChecksum::TPtr Checksum;
    // TMaybe<NBackup::TEncryptedFileSerializer> Encryption;
    std::shared_ptr<parquet::WriterProperties> WriteProperties;

    TString ErrorString;

    std::unordered_map<ui32, std::shared_ptr<IArrayBuilder>> ArrayBuilders;
    std::shared_ptr<arrow::Schema> Schema;

    static constexpr TStringBuf LogPrefix() {
        return "pq_buffer"sv;
    }
}; // TS3ParquetExportBuffer

TS3ParquetExportBuffer::TS3ParquetExportBuffer(TS3ExportBufferSettings&& settings)
    : Columns(std::move(settings.Columns))
    , RowsLimit(settings.MaxRows)
    , MaxBytes(settings.MaxBytes)
    , MinBytes(settings.MinBytes)
    , Checksum(CreateChecksum(settings.ChecksumSettings))
{
    // TODO(diseaz): implement encryption or drop completely
    // if (settings.EncryptionSettings) {
    //     Encryption.ConstructInPlace(
    //         std::move(settings.EncryptionSettings->Algorithm),
    //         std::move(settings.EncryptionSettings->Key),
    //         std::move(settings.EncryptionSettings->IV)
    //     );
    // }
    auto builder = std::make_unique<parquet::WriterProperties::Builder>();
    // TODO(diseaz): support MinBytes with compression
    // if (settings.CompressionSettings) {
    //     switch(settings.CompressionSettings->Algorithm) {
    //         case TS3ExportBufferSettings::TCompressionSettings::EAlgorithm::Zstd:
    //             builder->compression(arrow::Compression::ZSTD);
    //             break;
    //     }
    //     if (settings.CompressionSettings->CompressionLevel != -1) {
    //         builder->compression_level(settings.CompressionSettings->CompressionLevel);
    //     }
    // }
    WriteProperties = builder->build();
}

NBackup::IChecksum* TS3ParquetExportBuffer::CreateChecksum(const TMaybe<TS3ExportBufferSettings::TChecksumSettings>& settings) {
    if (settings) {
        switch (settings->ChecksumType) {
        case TS3ExportBufferSettings::TChecksumSettings::EChecksumType::Sha256:
            return NBackup::CreateChecksum();
        }
    }
    return nullptr;
}

void TS3ParquetExportBuffer::ColumnsOrder(const TVector<ui32>& tags) {
    Y_ENSURE(tags.size() == Columns.size());

    std::vector<std::shared_ptr<arrow::Field>> fields;
    Indices.clear();
    ArrayBuilders.clear();
    for (ui32 i = 0; i < tags.size(); ++i) {
        const ui32 tag = tags.at(i);
        auto it = Columns.find(tag);
        Y_ENSURE(it != Columns.end());
        Y_ENSURE(Indices.emplace(tag, i).second);
        auto column = it->second;
        auto typeId = column.Type.GetTypeId();
        ArrayBuilders.insert({tag, CreateArrayBuilder(typeId)});
        fields.push_back(arrow::field(column.Name, ArrowDataType(typeId)));
    }
    Schema = std::make_shared<arrow::Schema>(fields);
}

bool TS3ParquetExportBuffer::Collect(const NTable::IScan::TRow& row) {    
    arrow::Status status;

    std::cerr << "Collect row[" << Rows << "]: start" << std::endl;
    for (const auto& [tag, column] : Columns) {
        auto it = Indices.find(tag);
        Y_ENSURE(it != Indices.end());
        Y_ENSURE(it->second < (*row).size());
        auto builderIt = ArrayBuilders.find(tag);
        Y_ENSURE(builderIt != ArrayBuilders.end());

        std::shared_ptr<IArrayBuilder> builder = builderIt->second;
        if (!builder) {
            if (ErrorString.empty()) {
                ErrorString = "Array builder is null";
            }
            return false;
        }

        const auto& cell = (*row)[it->second];
        BytesRead += cell.Size();
        if (cell.IsNull()) {
            if (!(status = builder->AppendNull()).ok()) {
                ErrorString = (std::ostringstream() << "Failed to append null to array builder: " << status.message()).str();
                std::cerr << "Collect row[" << Rows << "]: error: " << ErrorString << std::endl;
                return false;
            }
            continue;
        }
        if (!(status = builder->Append(cell)).ok()) {
            ErrorString = (std::ostringstream() << "Failed to append value to array builder: " << status.message()).str();
            std::cerr << "Collect row[" << Rows << "]: error: " << ErrorString << std::endl;
            return false;
        }
    }
    std::cerr << "Collect row[" << Rows << "]: end" << std::endl;

    Rows++;
    
    return true;
}

IEventBase* TS3ParquetExportBuffer::PrepareEvent(bool last, NExportScan::IBuffer::TStats& stats) {
    std::cerr << "PrepareEvent start; last=" << last << ", rows=" << Rows << ", bytesRead=" << BytesRead << std::endl;
    stats.Rows = Rows;
    stats.BytesRead = BytesRead;

    auto buffer = Flush(StoreSchema);
    if (!buffer) {
        return nullptr;
    }
    StoreSchema = false;

    if (Checksum) {
        std::cerr << "PrepareEvent: Checksum->AddData size=" << buffer->Size() << std::endl;
        Checksum->AddData(TStringBuf(buffer->Data(), buffer->Size()));
    }
    stats.BytesSent = buffer->Size();

    std::cerr << "PrepareEvent end" << std::endl;
    if (Checksum && last) {
        return new TEvExportScan::TEvBuffer<TBuffer>(std::move(*buffer), last, Checksum->Finalize());
    } else {
        return new TEvExportScan::TEvBuffer<TBuffer>(std::move(*buffer), last);
    }
}

TMaybe<TBuffer> TS3ParquetExportBuffer::Flush(bool storeSchema) {
    std::cerr << "Flush start" << std::endl;

    arrow::Status status;

    std::cerr << "Flush: create buffer output stream" << std::endl;
    std::shared_ptr<arrow::io::BufferOutputStream> outputStream;
    auto outputStreamResult = arrow::io::BufferOutputStream::Create();
    if (!outputStreamResult.ok()) {
        ErrorString = (std::ostringstream() << "Failed to create buffer output stream: " << outputStreamResult.status().message()).str();
        std::cerr << "Flush error: " << ErrorString << std::endl;
        return Nothing();
    }
    outputStream = outputStreamResult.ValueOrDie();

    std::cerr << "Flush: create arrow props builder" << std::endl;
    auto arrowPropsBuilder = parquet::ArrowWriterProperties::Builder();
    if (storeSchema) {
        std::cerr << "Flush schema: ---/n" << Schema->ToString() << "---" << std::endl;
        arrowPropsBuilder.store_schema();
    }
    std::shared_ptr<parquet::ArrowWriterProperties> arrowProps = arrowPropsBuilder.build();

    std::cerr << "Flush: open parquet file writer" << std::endl;
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    if (!(status = parquet::arrow::FileWriter::Open(
        *Schema,
        arrow::default_memory_pool(),
        outputStream,
        WriteProperties,
        arrowProps,
        &writer)).ok()) {

        ErrorString = (std::ostringstream() << "Failed to open parquet file writer: " << status.message()).str();
        std::cerr << "Flush error: " << ErrorString << std::endl;
        return Nothing();
    }

    std::cerr << "Flush: fill arrays" << std::endl;    
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (const auto& [tag, column] : Columns) {
        auto builderIt = ArrayBuilders.find(tag);
        if (builderIt == ArrayBuilders.end()) {
            ErrorString = "Failed to find column builder";
            std::cerr << "Flush error: " << ErrorString << std::endl;
            return Nothing();
        }
        arrays.push_back(builderIt->second->Finish());
    }

    std::cerr << "Flush: create table" << std::endl;
    auto table = arrow::Table::Make(Schema, arrays);
    std::cerr << "Flush: write table" << std::endl;
    if (!(status = writer->WriteTable(*table, table->num_rows())).ok()) {
        ErrorString = (std::ostringstream() << "Failed to write table to parquet file: " << status.message()).str();
        std::cerr << "Flush error: " << ErrorString << std::endl;
        return Nothing();
    }
    std::cerr << "Flush: finish parquet file writer" << std::endl;
    auto arrowBufferResult = outputStream->Finish();
    if (!arrowBufferResult.ok()) {
        ErrorString = (std::ostringstream() << "Failed to finish parquet file writer: " << arrowBufferResult.status().message()).str();
        std::cerr << "Flush error: " << ErrorString << std::endl;
        return Nothing();
    }    
    auto arrowBuffer = arrowBufferResult.ValueOrDie();
    std::cerr << "Flush end: " << arrowBuffer->size() << std::endl;
    return TBuffer((char*)(arrowBuffer->data()), size_t(arrowBuffer->size()));
}

void TS3ParquetExportBuffer::Clear() {
    Rows = 0;
    BytesRead = 0;
    for (auto& builder : ArrayBuilders) {
        builder.second->Reset();
    }
}

bool TS3ParquetExportBuffer::IsFilled() const {
    return (BytesRead >= MinBytes)
        && (Rows >= GetRowsLimit() || BytesRead >= GetBytesLimit());
}

TString TS3ParquetExportBuffer::GetError() const {
    return ErrorString;
}

template <typename T, typename B>
class TAsValueBuilder: public IArrayBuilder {
public:
    TAsValueBuilder()
        : builder_(std::make_shared<B>())
    {}

    arrow::Status Append(const TCell& cell) override {
        return builder_->Append(cell.AsValue<T>());
    }

    arrow::Status AppendNull() override {
        return builder_->AppendNull();
    }

    std::shared_ptr<arrow::Array> Finish() override {
        auto result = builder_->Finish().ValueOrDie();
        builder_.reset();
        return result;
    }

    void Reset() override {
        builder_.reset();
    }

private:
    std::shared_ptr<B> builder_;
};

using TInt8Builder = TAsValueBuilder<i8, arrow::Int8Builder>;
using TUInt8Builder = TAsValueBuilder<ui8, arrow::UInt8Builder>;
using TInt16Builder = TAsValueBuilder<i16, arrow::Int16Builder>;
using TUInt16Builder = TAsValueBuilder<ui16, arrow::UInt16Builder>;
using TInt32Builder = TAsValueBuilder<i32, arrow::Int32Builder>;
using TUInt32Builder = TAsValueBuilder<ui32, arrow::UInt32Builder>;
using TInt64Builder = TAsValueBuilder<i64, arrow::Int64Builder>;
using TUInt64Builder = TAsValueBuilder<ui64, arrow::UInt64Builder>;
using TFloatBuilder = TAsValueBuilder<float, arrow::FloatBuilder>;
using TDoubleBuilder = TAsValueBuilder<double, arrow::DoubleBuilder>;
using TBoolBuilder = TAsValueBuilder<bool, arrow::BooleanBuilder>;

template <typename B>
class TAsBufBuilder: public IArrayBuilder {
public:
    TAsBufBuilder()
        : builder_(std::make_shared<B>())
    {}

    arrow::Status Append(const TCell& cell) override {
        const auto buf = cell.AsBuf();
        return builder_->Append(arrow::util::string_view{buf.data(), buf.size()});
    }

    arrow::Status AppendNull() override {
        return builder_->AppendNull();
    }

    std::shared_ptr<arrow::Array> Finish() override {
        auto result = builder_->Finish().ValueOrDie();
        builder_.reset();
        return result;
    }

    void Reset() override {
        builder_.reset();
    }

private:
    std::shared_ptr<B> builder_;
};

using TStringBuilder = TAsBufBuilder<arrow::StringBuilder>;

std::shared_ptr<IArrayBuilder> TS3ParquetExportBuffer::CreateArrayBuilder(NScheme::TTypeId typeId) {
    switch(typeId) {
        case NScheme::NTypeIds::Int8:
            return std::make_shared<TInt8Builder>();
        case NScheme::NTypeIds::Uint8:
            return std::make_shared<TUInt8Builder>();
        case NScheme::NTypeIds::Int16:
            return std::make_shared<TInt16Builder>();
        case NScheme::NTypeIds::Uint16:
        case NScheme::NTypeIds::Date:
            return std::make_shared<TUInt16Builder>();
        case NScheme::NTypeIds::Int32:
            return std::make_shared<TInt32Builder>();
        case NScheme::NTypeIds::Uint32:
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Date32:
            return std::make_shared<TUInt32Builder>();
        case NScheme::NTypeIds::Int64:
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval:
        case NScheme::NTypeIds::Interval64:
            return std::make_shared<TInt64Builder>();
        case NScheme::NTypeIds::Uint64:
        case NScheme::NTypeIds::Timestamp:
            return std::make_shared<TUInt64Builder>();
        case NScheme::NTypeIds::Float:
            return std::make_shared<TFloatBuilder>();
        case NScheme::NTypeIds::Double:
            return std::make_shared<TDoubleBuilder>();
        case NScheme::NTypeIds::Bool:
            return std::make_shared<TBoolBuilder>();
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::Yson:
            return std::make_shared<TStringBuilder>();
        // case NScheme::NTypeIds::JsonDocument:
        // case NScheme::NTypeIds::Pg:
        // case NScheme::NTypeIds::Uuid:
    }

    ErrorString = "Unsupported type";
    return nullptr;
}

std::shared_ptr<arrow::DataType> TS3ParquetExportBuffer::ArrowDataType(NScheme::TTypeId typeId) {
    switch(typeId) {
        case NScheme::NTypeIds::Int8:
            return arrow::int8();
        case NScheme::NTypeIds::Uint8:
            return arrow::uint8();
        case NScheme::NTypeIds::Int16:
            return arrow::int16();
        case NScheme::NTypeIds::Uint16:
        case NScheme::NTypeIds::Date:
            return arrow::uint16();
        case NScheme::NTypeIds::Int32:
            return arrow::int32();
        case NScheme::NTypeIds::Uint32:
        case NScheme::NTypeIds::Date32:
        case NScheme::NTypeIds::Datetime:
            return arrow::uint32();
        case NScheme::NTypeIds::Int64:
            return arrow::int64();
        case NScheme::NTypeIds::Uint64:
            return arrow::uint64();
        case NScheme::NTypeIds::Interval:
        case NScheme::NTypeIds::Interval64:
            return arrow::duration(arrow::TimeUnit::MICRO);
        case NScheme::NTypeIds::Datetime64:
            return arrow::timestamp(arrow::TimeUnit::SECOND);
        case NScheme::NTypeIds::Timestamp:
        case NScheme::NTypeIds::Timestamp64:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        case NScheme::NTypeIds::Float:
            return arrow::float32();
        case NScheme::NTypeIds::Double:
            return arrow::float64();
        case NScheme::NTypeIds::Bool:
            return arrow::boolean();
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::Yson:
            return arrow::utf8();
        // case NScheme::NTypeIds::JsonDocument:
        // case NScheme::NTypeIds::Pg:
        // case NScheme::NTypeIds::Uuid:
    }

    ErrorString = "Unsupported type";
    return nullptr;
}

} // anonymous namespace

NExportScan::IBuffer* CreateS3ParquetExportBuffer(TS3ExportBufferSettings&& settings) {
    return new TS3ParquetExportBuffer(std::move(settings));
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS