#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_data_format.h"
#include "export_s3_buffer.h"
#include "backup_restore_traits.h"
#include "export_s3.h"
#include "type_serialization.h"

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/generated/runtime_feature_flags.h>
#include <ydb/core/protos/data_format_settings.pb.h>
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

#include <functional>

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
    using TChecksumCreator = std::function<NBackup::IChecksum*()>;
    using TEncryptionCreator = std::function<TMaybe<NBackup::TEncryptedFileSerializer>()>;
    using TTagToColumn = IExport::TTableColumns;
    using TTagToIndex = THashMap<ui32, ui32>; // index in IScan::TRow

public:
    explicit TS3Buffer(TS3ExportBufferSettings&& settings, std::unique_ptr<IExportDataFormat> dataFormat);

    void ColumnsOrder(const TVector<ui32>& tags) override;
    bool Collect(const NTable::IScan::TRow& row) override;
    IEventBase* PrepareEvent(bool last, NExportScan::IBuffer::TStats& stats) override;
    void Clear() override;
    bool IsFilled() const override;
    TString GetError() const override;

private:
    bool Append(const char *data, size_t size);

    virtual TMaybe<TBuffer> Flush(bool last);

    static TChecksumCreator GenChecksumCreator(const TMaybe<TS3ExportBufferSettings::TChecksumSettings>& settings);
    static TEncryptionCreator GenEncryptionCreator(const TMaybe<TS3ExportBufferSettings::TEncryptionSettings>& settings);
    static TZStdCompressionProcessor* CreateCompression(const TMaybe<TS3ExportBufferSettings::TCompressionSettings>& settings);

private:
    const TTagToColumn Columns;
    const ui64 RowsLimit;
    const ui64 MinBytes;
    const ui64 MaxBytes;
    std::unique_ptr<IExportDataFormat> DataFormat;

    TTagToIndex Indices;

protected:
    ui64 Rows = 0;
    ui64 BytesRead = 0;
    TBuffer Buffer;

    TChecksumCreator ChecksumCreator;
    NBackup::IChecksum::TPtr Checksum;
    TZStdCompressionProcessor::TPtr Compression;
    TEncryptionCreator EncryptionCreator;
    TMaybe<NBackup::TEncryptedFileSerializer> Encryption;

    TString ErrorString;
}; // TS3Buffer

TS3Buffer::TS3Buffer(TS3ExportBufferSettings&& settings, std::unique_ptr<IExportDataFormat> dataFormat)
    : Columns(std::move(settings.Columns))
    , RowsLimit(settings.MaxRows)
    , MinBytes(settings.MinBytes)
    , MaxBytes(settings.MaxBytes)
    , DataFormat(std::move(dataFormat))
    , ChecksumCreator(GenChecksumCreator(settings.ChecksumSettings))
    , Checksum(ChecksumCreator())
    , Compression(CreateCompression(settings.CompressionSettings))
    , EncryptionCreator(GenEncryptionCreator(settings.EncryptionSettings))
    , Encryption(EncryptionCreator())
{
}

std::function<NBackup::IChecksum*()> TS3Buffer::GenChecksumCreator(const TMaybe<TS3ExportBufferSettings::TChecksumSettings>& settings) {
    return [settings]() -> NBackup::IChecksum* {
        if (settings) {
            switch (settings->ChecksumType) {
            case TS3ExportBufferSettings::TChecksumSettings::EChecksumType::Sha256:
                return NBackup::CreateChecksum();
            }
        }
        return nullptr;
    };
}

TS3Buffer::TEncryptionCreator TS3Buffer::GenEncryptionCreator(const TMaybe<TS3ExportBufferSettings::TEncryptionSettings>& settings) {
    return [settings]() -> TMaybe<NBackup::TEncryptedFileSerializer> {
        if (settings) {
            return NBackup::TEncryptedFileSerializer(settings->Algorithm, settings->Key, settings->IV);
        }
        return Nothing();
    };
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
    if (!DataFormat->ColumnsOrder(tags)) {
        ErrorString = DataFormat->GetError();
    }
}

bool TS3Buffer::Collect(const NTable::IScan::TRow& row) {
    auto collectBuffer = DataFormat->Collect(row);
    if (!collectBuffer) {
        ErrorString = DataFormat->GetError();
        return false;
    }

    for (size_t i = 0; i < Columns.size(); ++i) {
        const auto& cell = row.Get(i);
        BytesRead += cell.Size();
    }

    if (collectBuffer->Size() > 0) {
        if (!Append(collectBuffer->Data(), collectBuffer->Size())) {
            return false;
        }
    }

    ++Rows;
    
    return true;
}

bool TS3Buffer::Append(const char *data, size_t size) {
    if (!size) {
        return true;
    }

    TStringBuf chunk(data, size);
    if (Compression) {
        if (!Compression->AddData(chunk)) {
            ErrorString = Compression->GetError();
            return false;
        }
    } else {
        Buffer.Append(data, size);
    }

    // Apply checksum
    if (Checksum) {
        Checksum->AddData(chunk);
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
    Buffer = TBuffer();
    // Reset the data format too: on a retry the scan is restarted from scratch,
    // so any format-internal state (e.g. the Parquet writer and its already
    // emitted file header) must be discarded, otherwise the regenerated file
    // would be corrupted.
    DataFormat->Clear();
    if (Checksum) {
        Checksum.reset(ChecksumCreator());
    }
    if (Compression) {
        Compression->Clear();
    }
    Encryption = EncryptionCreator();
}

bool TS3Buffer::IsFilled() const {
    size_t outputSize = Buffer.Size();
    if (Compression) {
        outputSize = Compression->GetReadyOutputBytes();
    }
    // Some formats (e.g. Parquet) keep encoded output inside the format itself
    // until a flush, so it is not yet reflected in Buffer/Compression.
    outputSize += DataFormat->GetReadyOutputBytes();
    if (outputSize < MinBytes) {
        return false;
    }
    return Rows >= RowsLimit || outputSize >= MaxBytes;
}

TString TS3Buffer::GetError() const {
    return ErrorString;
}

TMaybe<TBuffer> TS3Buffer::Flush(bool last) {
    Rows = 0;
    BytesRead = 0;

    auto dataFormatBuffer = DataFormat->Flush(last);
    if (!dataFormatBuffer) {
        ErrorString = DataFormat->GetError();
        return Nothing();
    }

    if (!Append(dataFormatBuffer->Data(), dataFormatBuffer->Size())) {
        return Nothing();
    }

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

    auto buffer = std::exchange(Buffer, TBuffer());

    Reset();
    return buffer;
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
    Buffer = TBuffer();
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

    std::unique_ptr<IExportDataFormat> dataFormat;
    switch (DataFormatFromTask(Task)) {
    case EDataFormat::YdbDump:
        {
            TYdbDumpExportSettings settings;
            settings
                .WithColumns(Columns);
            dataFormat.reset(CreateExportDataFormat(std::move(settings)));
            break;
        }
    case EDataFormat::Parquet:
        {
            bufferSettings.WithoutCompression();
            
            auto settings = ParquetExportSettingsFromTask(Task);
            settings
                .WithColumns(Columns);
            
            switch (CodecFromTask(Task)) {
            case ECompressionCodec::None:
                break;
            case ECompressionCodec::Zstd:
                settings
                    .WithCompression(TParquetExportSettings::TCompressionSettings()
                        .WithAlgorithm(TParquetExportSettings::TCompressionSettings::EAlgorithm::Zstd)
                        .WithLevel(Task.GetCompression().GetLevel()));
                break;
            case ECompressionCodec::Invalid:
                Y_ENSURE(false, "unreachable");
            }
            dataFormat.reset(CreateExportDataFormat(std::move(settings)));
            break;
        }
    case EDataFormat::Invalid:
        Y_ENSURE(false, "unreachable");
    }

    return CreateS3ExportBuffer(std::move(bufferSettings), std::move(dataFormat));
}

NExportScan::IBuffer* CreateS3ExportBuffer(TS3ExportBufferSettings&& settings, std::unique_ptr<IExportDataFormat> dataFormat) {
    return new TS3Buffer(std::move(settings), std::move(dataFormat));
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
