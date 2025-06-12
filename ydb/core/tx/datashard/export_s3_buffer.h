#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_iface.h"
#include "export_scan.h"

#include <ydb/core/backup/common/encryption.h>

#include <util/generic/maybe.h>

namespace NKikimr {
namespace NDataShard {

struct TS3ExportBufferSettings {
    // Subsettings
    struct TChecksumSettings {
        enum class EChecksumType {
            Sha256,
        };

        // Builders
        TChecksumSettings& WithChecksumType(EChecksumType t) {
            ChecksumType = t;
            return *this;
        }

        // Fields
        EChecksumType ChecksumType = EChecksumType::Sha256;
    };

    static TChecksumSettings Sha256Checksum() {
        return TChecksumSettings().WithChecksumType(TChecksumSettings::EChecksumType::Sha256);
    }

    struct TCompressionSettings {
        enum class EAlgorithm {
            Zstd,
        };

        // Builders
        TCompressionSettings& WithAlgorithm(EAlgorithm algorithm) {
            Algorithm = algorithm;
            return *this;
        }

        TCompressionSettings& WithCompressionLevel(int level) {
            CompressionLevel = level;
            return *this;
        }

        // Fields
        EAlgorithm Algorithm = EAlgorithm::Zstd;
        int CompressionLevel = -1;
    };

    static TCompressionSettings ZstdCompression(int level) {
        return TCompressionSettings().WithAlgorithm(TCompressionSettings::EAlgorithm::Zstd).WithCompressionLevel(level);
    }

    struct TEncryptionSettings {
        // Builders
        TEncryptionSettings& WithAlgorithm(const TString& algorithm) {
            Algorithm = algorithm;
            return *this;
        }

        TEncryptionSettings& WithKey(const NBackup::TEncryptionKey& key) {
            Key = key;
            return *this;
        }

        TEncryptionSettings& WithIV(const NBackup::TEncryptionIV& iv) {
            IV = iv;
            return *this;
        }

        // Fields
        TString Algorithm;
        NBackup::TEncryptionKey Key;
        NBackup::TEncryptionIV IV;
    };

    // Builders
    TS3ExportBufferSettings& WithColumns(IExport::TTableColumns columns) {
        Columns = std::move(columns);
        return *this;
    }

    TS3ExportBufferSettings& WithMaxRows(ui64 maxRows) {
        MaxRows = maxRows;
        return *this;
    }

    TS3ExportBufferSettings& WithMinBytes(ui64 minBytes) {
        MinBytes = minBytes;
        return *this;
    }

    TS3ExportBufferSettings& WithMaxBytes(ui64 maxBytes) {
        MaxBytes = maxBytes;
        return *this;
    }

    TS3ExportBufferSettings& WithChecksum(TChecksumSettings settings) {
        ChecksumSettings.ConstructInPlace(std::move(settings));
        return *this;
    }

    TS3ExportBufferSettings& WithCompression(TCompressionSettings settings) {
        CompressionSettings.ConstructInPlace(std::move(settings));
        return *this;
    }

    TS3ExportBufferSettings& WithEncryption(TEncryptionSettings settings) {
        EncryptionSettings.ConstructInPlace(std::move(settings));
        return *this;
    }

    // Fields
    IExport::TTableColumns Columns;
    ui64 MaxRows = 0;
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;

    // Data processing
    TMaybe<TChecksumSettings> ChecksumSettings;
    TMaybe<TCompressionSettings> CompressionSettings;
    TMaybe<TEncryptionSettings> EncryptionSettings;
};

NExportScan::IBuffer* CreateS3ExportBuffer(TS3ExportBufferSettings&& settings);

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
