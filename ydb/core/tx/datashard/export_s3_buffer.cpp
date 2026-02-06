#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_s3_buffer.h"
#include "backup_restore_traits.h"
#include "export_s3.h"
#include "type_serialization.h"

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <yql/essentials/types/binary_json/read.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <util/datetime/base.h>
#include <util/generic/buffer.h>
#include <util/stream/buffer.h>

#include <contrib/libs/zstd/include/zstd.h>


namespace NKikimr::NDataShard {

namespace {

struct DestroyZCtx {
    static void Destroy(::ZSTD_CCtx* p) noexcept {
        ZSTD_freeCCtx(p);
    }
};

class TZStdCompressionProcessor {
public:
    using TPtr = THolder<TZStdCompressionProcessor>;

    explicit TZStdCompressionProcessor(const TS3ExportBufferSettings::TCompressionSettings& settings);

    TString GetError() const {
        return ZSTD_getErrorName(ErrorCode);
    }

    bool AddData(TStringBuf data);

    void Clear() {
        Reset();
    }

    TMaybe<TBuffer> Flush();

    size_t GetReadyOutputBytes() const {
        return Buffer.Size();
    }

private:
    enum ECompressionResult {
        CONTINUE,
        DONE,
        ERROR,
    };

    ECompressionResult Compress(ZSTD_inBuffer* input, ZSTD_EndDirective endOp);
    void Reset();

private:
    const int CompressionLevel;
    THolder<::ZSTD_CCtx, DestroyZCtx> Context;
    size_t ErrorCode = 0;
    TBuffer Buffer;
    ui64 BytesAdded = 0;
};

class TS3Buffer: public NExportScan::IBuffer {
    using TTagToColumn = IExport::TTableColumns;
    using TTagToIndex = THashMap<ui32, ui32>; // index in IScan::TRow

public:
    explicit TS3Buffer(TS3ExportBufferSettings&& settings);

    void ColumnsOrder(const TVector<ui32>& tags) override;
    bool Collect(const NTable::IScan::TRow& row) override;
    IEventBase* PrepareEvent(bool last, NExportScan::IBuffer::TStats& stats) override;
    void Clear() override;
    bool IsFilled() const override;
    TString GetError() const override;

private:
    inline ui64 GetRowsLimit() const { return RowsLimit; }
    inline ui64 GetBytesLimit() const { return MaxBytes; }

    bool Collect(const NTable::IScan::TRow& row, IOutputStream& out);
    virtual TMaybe<TBuffer> Flush(bool last);

    static NBackup::IChecksum* CreateChecksum(const TMaybe<TS3ExportBufferSettings::TChecksumSettings>& settings);
    static TZStdCompressionProcessor* CreateCompression(const TMaybe<TS3ExportBufferSettings::TCompressionSettings>& settings);

private:
    const TTagToColumn Columns;
    const ui64 RowsLimit;
    const ui64 MinBytes;
    const ui64 MaxBytes;

    TTagToIndex Indices;

protected:
    ui64 Rows = 0;
    ui64 BytesRead = 0;
    TBuffer Buffer;

    NBackup::IChecksum::TPtr Checksum;
    TZStdCompressionProcessor::TPtr Compression;
    TMaybe<NBackup::TEncryptedFileSerializer> Encryption;

    TString ErrorString;
}; // TS3Buffer

TS3Buffer::TS3Buffer(TS3ExportBufferSettings&& settings)
    : Columns(std::move(settings.Columns))
    , RowsLimit(settings.MaxRows)
    , MinBytes(settings.MinBytes)
    , MaxBytes(settings.MaxBytes)
    , Checksum(CreateChecksum(settings.ChecksumSettings))
    , Compression(CreateCompression(settings.CompressionSettings))
{
    if (settings.EncryptionSettings) {
        Encryption.ConstructInPlace(
            std::move(settings.EncryptionSettings->Algorithm),
            std::move(settings.EncryptionSettings->Key),
            std::move(settings.EncryptionSettings->IV)
        );
    }
}

NBackup::IChecksum* TS3Buffer::CreateChecksum(const TMaybe<TS3ExportBufferSettings::TChecksumSettings>& settings) {
    if (settings) {
        switch (settings->ChecksumType) {
        case TS3ExportBufferSettings::TChecksumSettings::EChecksumType::Sha256:
            return NBackup::CreateChecksum();
        }
    }
    return nullptr;
}

TZStdCompressionProcessor* TS3Buffer::CreateCompression(const TMaybe<TS3ExportBufferSettings::TCompressionSettings>& settings) {
    if (settings) {
        switch (settings->Algorithm) {
        case TS3ExportBufferSettings::TCompressionSettings::EAlgorithm::Zstd:
            return new TZStdCompressionProcessor(*settings);
        }
    }
    return nullptr;
}

void TS3Buffer::ColumnsOrder(const TVector<ui32>& tags) {
    Y_ENSURE(tags.size() == Columns.size());

    Indices.clear();
    for (ui32 i = 0; i < tags.size(); ++i) {
        const ui32 tag = tags.at(i);
        auto it = Columns.find(tag);
        Y_ENSURE(it != Columns.end());
        Y_ENSURE(Indices.emplace(tag, i).second);
    }
}

bool TS3Buffer::Collect(const NTable::IScan::TRow& row, IOutputStream& out) {
    bool needsComma = false;
    for (const auto& [tag, column] : Columns) {
        auto it = Indices.find(tag);
        Y_ENSURE(it != Indices.end());
        Y_ENSURE(it->second < (*row).size());
        const auto& cell = (*row)[it->second];

        BytesRead += cell.Size();

        if (needsComma) {
            out << ",";
        } else {
            needsComma = true;
        }

        if (cell.IsNull()) {
            out << "null";
            continue;
        }

        bool serialized = true;
        switch (column.Type.GetTypeId()) {
        case NScheme::NTypeIds::Int32:
            serialized = cell.ToStream<i32>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Uint32:
            serialized = cell.ToStream<ui32>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Int64:
            serialized = cell.ToStream<i64>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Uint64:
            serialized = cell.ToStream<ui64>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Uint8:
        //case NScheme::NTypeIds::Byte:
            out << static_cast<ui32>(cell.AsValue<ui8>());
            break;
        case NScheme::NTypeIds::Int8:
            out << static_cast<i32>(cell.AsValue<i8>());
            break;
        case NScheme::NTypeIds::Int16:
            serialized = cell.ToStream<i16>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Uint16:
            serialized = cell.ToStream<ui16>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Bool:
            serialized = cell.ToStream<bool>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Double:
            serialized = cell.ToStream<double>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Float:
            serialized = cell.ToStream<float>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Date:
            out << TInstant::Days(cell.AsValue<ui16>());
            break;
        case NScheme::NTypeIds::Datetime:
            out << TInstant::Seconds(cell.AsValue<ui32>());
            break;
        case NScheme::NTypeIds::Timestamp:
            out << TInstant::MicroSeconds(cell.AsValue<ui64>());
            break;
        case NScheme::NTypeIds::Interval:
            serialized = cell.ToStream<i64>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Date32:
            serialized = cell.ToStream<i32>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            serialized = cell.ToStream<i64>(out, ErrorString);
            break;
        case NScheme::NTypeIds::Decimal:
            serialized = DecimalToStream(cell.AsValue<std::pair<ui64, i64>>(), out, ErrorString, column.Type);
            break;
        case NScheme::NTypeIds::DyNumber:
            serialized = DyNumberToStream(cell.AsBuf(), out, ErrorString);
            break;
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::Yson:
            out << '"' << CGIEscapeRet(cell.AsBuf()) << '"';
            break;
        case NScheme::NTypeIds::JsonDocument:
            out << '"' << CGIEscapeRet(NBinaryJson::SerializeToJson(cell.AsBuf())) << '"';
            break;
        case NScheme::NTypeIds::Pg:
            serialized = PgToStream(cell.AsBuf(), column.Type, out, ErrorString);
            break;
        case NScheme::NTypeIds::Uuid:
            serialized = UuidToStream(cell.AsValue<std::pair<ui64, ui64>>(), out, ErrorString);
            break;
        default:
            Y_ENSURE(false, "Unsupported type");
        }

        if (!serialized) {
            return false;
        }
    }

    out << "\n";
    ++Rows;

    return true;
}

bool TS3Buffer::Collect(const NTable::IScan::TRow& row) {
    TBufferOutput out(Buffer);
    ErrorString.clear();

    size_t beforeSize = Buffer.Size();
    if (!Collect(row, out)) {
        return false;
    }

    TStringBuf data(Buffer.Data(), Buffer.Size());
    data = data.Tail(beforeSize);

    // Apply checksum
    if (Checksum) {
        Checksum->AddData(data);
    }

    // Compress
    if (Compression && !Compression->AddData(data)) {
        ErrorString = Compression->GetError();
        return false;
    }

    return true;
}

IEventBase* TS3Buffer::PrepareEvent(bool last, NExportScan::IBuffer::TStats& stats) {
    stats.Rows = Rows;
    stats.BytesRead = BytesRead;

    auto buffer = Flush(last);
    if (!buffer) {
        return nullptr;
    }

    stats.BytesSent = buffer->Size();

    if (Checksum && last) {
        return new TEvExportScan::TEvBuffer<TBuffer>(std::move(*buffer), last, Checksum->Finalize());
    } else {
        return new TEvExportScan::TEvBuffer<TBuffer>(std::move(*buffer), last);
    }
}

void TS3Buffer::Clear() {
    Rows = 0;
    BytesRead = 0;
    if (Compression) {
        Compression->Clear();
    }
}

bool TS3Buffer::IsFilled() const {
    size_t outputSize = Buffer.Size();
    if (Compression) {
        outputSize = Compression->GetReadyOutputBytes();
    }
    if (outputSize < MinBytes) {
        return false;
    }

    return Rows >= GetRowsLimit() || Buffer.Size() >= GetBytesLimit();
}

TString TS3Buffer::GetError() const {
    return ErrorString;
}

TMaybe<TBuffer> TS3Buffer::Flush(bool last) {
    Rows = 0;
    BytesRead = 0;

    // Compression finishes compression frame during Flush
    // so that last table row borders equal to compression frame borders.
    // This full finished block must then be encrypted so that encryption frame
    // has the same borders.
    // It allows to import data in batches and save its state during import.

    if (Compression) {
        TMaybe<TBuffer> compressedBuffer = Compression->Flush();
        if (!compressedBuffer) {
            return Nothing();
        }

        Buffer = std::move(*compressedBuffer);
    }

    if (Encryption) {
        TBuffer encryptedBlock = Encryption->AddBlock(TStringBuf(Buffer.Data(), Buffer.Size()), last);
        Buffer = std::move(encryptedBlock);
    }

    return std::exchange(Buffer, TBuffer());
}

TZStdCompressionProcessor::TZStdCompressionProcessor(const TS3ExportBufferSettings::TCompressionSettings& settings)
    : CompressionLevel(settings.CompressionLevel)
    , Context(ZSTD_createCCtx())
{
}

bool TZStdCompressionProcessor::AddData(TStringBuf data) {
    BytesAdded += data.size();
    auto input = ZSTD_inBuffer{data.data(), data.size(), 0};
    while (input.pos < input.size) {
        if (ERROR == Compress(&input, ZSTD_e_continue)) {
            return false;
        }
    }

    return true;
}

TMaybe<TBuffer> TZStdCompressionProcessor::Flush() {
    if (BytesAdded) {
        ECompressionResult res;
        auto input = ZSTD_inBuffer{NULL, 0, 0};

        do {
            if (res = Compress(&input, ZSTD_e_end); res == ERROR) {
                return Nothing();
            }
        } while (res != DONE);
    }

    Reset();
    return std::exchange(Buffer, TBuffer());
}

TZStdCompressionProcessor::ECompressionResult TZStdCompressionProcessor::Compress(ZSTD_inBuffer* input, ZSTD_EndDirective endOp) {
    auto output = ZSTD_outBuffer{Buffer.Data(), Buffer.Capacity(), Buffer.Size()};
    auto res = ZSTD_compressStream2(Context.Get(), &output, input, endOp);

    if (ZSTD_isError(res)) {
        ErrorCode = res;
        return ERROR;
    }

    if (res > 0) {
        Buffer.Reserve(output.pos + res);
    }

    Buffer.Proceed(output.pos);
    return res ? CONTINUE : DONE;
}

void TZStdCompressionProcessor::Reset() {
    BytesAdded = 0;
    ZSTD_CCtx_reset(Context.Get(), ZSTD_reset_session_only);
    ZSTD_CCtx_refCDict(Context.Get(), NULL);
    ZSTD_CCtx_setParameter(Context.Get(), ZSTD_c_compressionLevel, CompressionLevel);
}

} // anonymous

IExport::IBuffer* TS3Export::CreateBuffer() const {
    using namespace NBackupRestoreTraits;

    const auto& scanSettings = Task.GetScanSettings();
    const ui64 maxRows = scanSettings.GetRowsBatchSize() ? scanSettings.GetRowsBatchSize() : Max<ui64>();

    const ui64 configMaxBytes = AppData()->DataShardConfig.GetBackupBytesBatchSize();
    const ui64 maxBytes = scanSettings.HasBytesBatchSize() ? scanSettings.GetBytesBatchSize() : configMaxBytes;

    const ui64 minBytes = Task.HasS3Settings()
        ? Task.GetS3Settings().GetLimits().GetMinWriteBatchSize()
        : Task.GetFSSettings().GetLimits().GetMinWriteBatchSize();

    TS3ExportBufferSettings bufferSettings;
    bufferSettings
        .WithColumns(Columns)
        .WithMaxRows(maxRows)
        .WithMaxBytes(maxBytes)
        .WithMinBytes(minBytes); // S3 API returns EntityTooSmall error if file part is smaller that 5MB: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    if (Task.GetEnableChecksums()) {
        bufferSettings.WithChecksum(TS3ExportBufferSettings::Sha256Checksum());
    }

    switch (CodecFromTask(Task)) {
    case ECompressionCodec::None:
        break;
    case ECompressionCodec::Zstd:
        bufferSettings
            .WithCompression(TS3ExportBufferSettings::ZstdCompression(Task.GetCompression().GetLevel()));
        break;
    case ECompressionCodec::Invalid:
        Y_ENSURE(false, "unreachable");
    }

    if (Task.HasEncryptionSettings()) {
        NBackup::TEncryptionIV iv = NBackup::TEncryptionIV::Combine(
            NBackup::TEncryptionIV::FromBinaryString(Task.GetEncryptionSettings().GetIV()),
            NBackup::EBackupFileType::TableData,
            0, // already combined
            Task.GetShardNum());
        bufferSettings.WithEncryption(
            TS3ExportBufferSettings::TEncryptionSettings()
                .WithAlgorithm(Task.GetEncryptionSettings().GetEncryptionAlgorithm())
                .WithKey(NBackup::TEncryptionKey(Task.GetEncryptionSettings().GetSymmetricKey().key()))
                .WithIV(iv)
        );
    }

    return CreateS3ExportBuffer(std::move(bufferSettings));
}

NExportScan::IBuffer* CreateS3ExportBuffer(TS3ExportBufferSettings&& settings) {
    return new TS3Buffer(std::move(settings));
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
