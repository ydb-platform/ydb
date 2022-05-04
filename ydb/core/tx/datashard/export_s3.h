#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"
#include "export_iface.h"
#include "export_s3_buffer.h"

namespace NKikimr {
namespace NDataShard {

class TS3Export: public IExport {
public:
    explicit TS3Export(const TTask& task, const TTableColumns& columns)
        : Task(task)
        , Columns(columns)
    {
        Y_VERIFY(task.HasS3Settings());
    }

    IActor* CreateUploader(const TActorId& dataShard, ui64 txId) const override;

    IBuffer* CreateBuffer(ui64 rowsLimit, ui64 bytesLimit) const override {
        return CreateS3ExportBuffer(Columns, rowsLimit, bytesLimit);
    }

    void Shutdown() const override {}

protected:
    const TTask Task;
    const TTableColumns Columns;
};

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
