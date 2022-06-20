#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_iface.h"
#include "export_scan.h"

namespace NKikimr {
namespace NDataShard {

NExportScan::IBuffer* CreateS3ExportBufferRaw(
    const IExport::TTableColumns& columns, ui64 maxRows, ui64 maxBytes);

NExportScan::IBuffer* CreateS3ExportBufferZstd(int compressionLevel,
    const IExport::TTableColumns& columns, ui64 maxRows, ui64 maxBytes, ui64 minBytes);

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
