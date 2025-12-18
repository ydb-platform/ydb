#pragma once

#ifndef KIKIMR_DISABLE_EXPORT_OPS

#include "export_iface.h"

namespace NKikimr {
namespace NDataShard {

class TExport: public IExport {
public:
    explicit TExport(const TTask& task, const TTableColumns& columns)
        : Task(task)
        , Columns(columns)
    {
        Y_ENSURE(task.HasS3Settings());
    }

    IActor* CreateUploader(const TActorId& dataShard, ui64 txId) const override;

    IBuffer* CreateBuffer() const override;

    void Shutdown() const override {}

protected:
    const TTask Task;
    const TTableColumns Columns;
};

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_EXPORT_OPS
