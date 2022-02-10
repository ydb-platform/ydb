#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"
#include "export_iface.h"
#include "export_s3_buffer.h"

namespace NKikimr {
namespace NDataShard {

class TS3Export: public IExport {
public:
    IActor* CreateUploader(
        const TActorId& dataShard,
        ui64 txId,
        const TTableColumns& columns,
        const TTask& task) const override;

    IBuffer* CreateBuffer(const TTableColumns& columns, ui64 rowsLimit, ui64 bytesLimit) const override {
        return CreateS3ExportBuffer(columns, rowsLimit, bytesLimit);
    }

    void Shutdown() const override {}
};

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
