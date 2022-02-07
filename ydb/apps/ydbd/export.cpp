#include "export.h"

#include <ydb/core/tx/datashard/export_s3.h>

NKikimr::NDataShard::IExport* TDataShardExportFactory::CreateExportToYt(bool useTypeV3) const {
    Y_UNUSED(useTypeV3);
    return nullptr; // not supported
}

NKikimr::NDataShard::IExport* TDataShardExportFactory::CreateExportToS3() const {
#ifndef KIKIMR_DISABLE_S3_OPS
    return new NKikimr::NDataShard::TS3Export();
#else
    return nullptr;
#endif
}

void TDataShardExportFactory::Shutdown() {
}
