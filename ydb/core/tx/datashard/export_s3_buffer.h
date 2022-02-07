#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_iface.h"
#include "export_scan.h"

namespace NKikimr {
namespace NDataShard {

NExportScan::IBuffer* CreateS3ExportBuffer(const IExport::TTableColumns& columns, ui64 rowsLimit, ui64 bytesLimit);

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
