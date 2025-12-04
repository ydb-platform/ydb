#pragma once

#include "export_iface.h"

namespace NKikimr {
namespace NDataShard {

class TFsExport: public IExport {
public:
    explicit TFsExport(const TTask& task, const TTableColumns& columns)
        : Task(task)
        , Columns(columns)
    {
        Y_ENSURE(task.HasFSSettings());
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


