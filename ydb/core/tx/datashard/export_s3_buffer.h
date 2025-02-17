#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_iface.h"
#include "export_scan.h"

#include <util/generic/maybe.h>

namespace NKikimr {
namespace NDataShard {

struct TS3ExportBufferSettings {
    struct TChecksumSettings {
        enum class EChecksumType {
            Sha256,
        };

        EChecksumType ChecksumType = EChecksumType::Sha256;
    };

    struct TCompressionSettings {
        enum class ECompressionAlg {
            Zstd,
        };

        ECompressionAlg Alg = ECompressionAlg::Zstd;
        int CompressionLevel = -1;
    };

    IExport::TTableColumns Columns;
    ui64 MaxRows = 0;
    ui64 MinBytes = 0;
    ui64 MaxBytes = 0;

    // Data processing
    TMaybe<TChecksumSettings> ChecksumSettings;
    TMaybe<TCompressionSettings> CompressionSettings;
};

NExportScan::IBuffer* CreateS3ExportBuffer(TS3ExportBufferSettings&& settings);

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
