#include "export.h"

#include <kikimr/yndx/yt/export_yt.h>
#include <kikimr/yndx/yt/yt_shutdown.h>

#include <contrib/ydb/core/tx/datashard/export_s3.h>

NKikimr::NDataShard::IExport* TDataShardExportFactory::CreateExportToYt(
        const IExport::TTask& task, const IExport::TTableColumns& columns) const
{
#ifndef KIKIMR_DISABLE_YT
    return new NKikimr::NYndx::TYtExport(task, columns);
#else
    Y_UNUSED(task);
    Y_UNUSED(columns);
    return nullptr;
#endif
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
#ifndef KIKIMR_DISABLE_YT
    ShutdownYT();
#endif
}
