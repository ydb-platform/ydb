#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_iface.h"

#include <ydb/core/tablet_flat/flat_scan_iface.h>

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

namespace NKikimr {
namespace NDataShard {

class IExportDataFormat {
public:
    virtual bool ColumnsOrder(const TVector<ui32>& tags) = 0;
    virtual TMaybe<TBuffer> Collect(const NTable::IScan::TRow& row) = 0;
    virtual TMaybe<TBuffer> Flush(bool last) = 0;
    virtual void Clear() = 0;
    // Number of encoded output bytes the format is currently holding internally
    // (data produced but not yet returned via Collect/Flush). Formats that hand
    // their output back immediately from Collect return 0.
    virtual size_t GetReadyOutputBytes() const = 0;
    virtual TString GetError() const = 0;

    virtual ~IExportDataFormat() = default;
}; 

struct TYdbDumpExportSettings {
    TYdbDumpExportSettings& WithColumns(const IExport::TTableColumns columns) {
        Columns = std::move(columns);
        return *this;
    }

    IExport::TTableColumns Columns;
};

struct TParquetExportSettings {
    struct TCompressionSettings {
        enum class EAlgorithm {
            Zstd,
        };

        TCompressionSettings& WithAlgorithm(EAlgorithm algorithm) {
            Algorithm = algorithm;
            return *this;
        }

        TCompressionSettings& WithLevel(int level) {
            Level = level;
            return *this;
        }

        EAlgorithm Algorithm = EAlgorithm::Zstd;
        int Level = -1;
    };

    TParquetExportSettings& WithColumns(const IExport::TTableColumns& columns) {
        Columns = columns;
        return *this;
    }

    TParquetExportSettings& WithRowGroupSize(ui64 rowGroupSize) {
        RowGroupSize = rowGroupSize;
        return *this;
    }

    TParquetExportSettings& WithCompression(const TCompressionSettings& compressionSettings) {
        CompressionSettings.Emplace(compressionSettings);
        return *this;
    }

    IExport::TTableColumns Columns;
    ui64 RowGroupSize = 1000;
    TMaybe<TCompressionSettings> CompressionSettings;
};

IExportDataFormat* CreateExportDataFormat(TYdbDumpExportSettings&& settings);
IExportDataFormat* CreateExportDataFormat(TParquetExportSettings&& settings);

} // namespace NDataShard
} // namespace NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
