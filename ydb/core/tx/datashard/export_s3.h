#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"
#include "backup_restore_traits.h"
#include "export_iface.h"
#include "export_s3_buffer.h"

#include <ydb/public/api/protos/ydb_export.pb.h>

namespace NKikimr {
namespace NDataShard {

class TS3Export: public IExport {
public:
    explicit TS3Export(const TTask& task, const TTableColumns& columns)
        : Task(task)
        , Columns(columns)
    {
        Y_ABORT_UNLESS(task.HasS3Settings());
    }

    IActor* CreateUploader(const TActorId& dataShard, ui64 txId) const override;

    IBuffer* CreateBuffer() const override {
        using namespace NBackupRestoreTraits;

        const auto& scanSettings = Task.GetScanSettings();
        const ui64 maxRows = scanSettings.GetRowsBatchSize() ? scanSettings.GetRowsBatchSize() : Max<ui64>();
        const ui64 maxBytes = scanSettings.GetBytesBatchSize();
        const ui64 minBytes = Task.GetS3Settings().GetLimits().GetMinWriteBatchSize();

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
            Y_ABORT("unreachable");
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

    void Shutdown() const override {}

protected:
    const TTask Task;
    const TTableColumns Columns;
};

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
