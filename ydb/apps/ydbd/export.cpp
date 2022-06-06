#include "export.h"

#include <ydb/core/tx/datashard/export_s3.h>

NKikimr::NDataShard::IExport* TDataShardExportFactory::CreateExportToYt(
        const IExport::TTask& task, const IExport::TTableColumns& columns) const
{
    Y_UNUSED(task);
    Y_UNUSED(columns);
    return nullptr; // not supported
}

NKikimr::NDataShard::IExport* TDataShardExportFactory::CreateExportToS3(
        const IExport::TTask& task, const IExport::TTableColumns& columns) const
{
#ifndef KIKIMR_DISABLE_S3_OPS
    return new NKikimr::NDataShard::TS3Export(task, columns);
#else
    Y_UNUSED(task);
    Y_UNUSED(columns);
    return nullptr;
#endif
}

void TDataShardExportFactory::Shutdown() {
}
