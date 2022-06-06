#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"
#include "backup_restore_traits.h"
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
        using namespace NBackupRestoreTraits;

        switch (CodecFromTask(Task)) {
        case ECompressionCodec::None:
            return CreateS3ExportBufferRaw(Columns, rowsLimit, bytesLimit);
        case ECompressionCodec::Zstd:
            return CreateS3ExportBufferZstd(Task.GetCompression().GetLevel(), Columns, rowsLimit, bytesLimit);
        case ECompressionCodec::Invalid:
            Y_FAIL("unreachable");
        }
    }

    void Shutdown() const override {}

protected:
    const TTask Task;
    const TTableColumns Columns;
};

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
