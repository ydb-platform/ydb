#pragma once

#include "datashard_user_table.h"
#include "export_scan.h"

namespace NKikimr {
namespace NDataShard {

class IExport {
public:
    using TTableColumns = TMap<ui32, TUserTable::TUserColumn>;
    using TTask = NKikimrSchemeOp::TBackupTask;
    using IBuffer = NExportScan::IBuffer;

public:
    virtual ~IExport() = default;

    virtual IActor* CreateUploader(const TActorId& dataShard, ui64 txId) const = 0;
    virtual IBuffer* CreateBuffer() const = 0;

    virtual void Shutdown() const = 0;
};

class IExportFactory {
public:
    virtual ~IExportFactory() = default;

    virtual IExport* CreateExportToYt(const IExport::TTask& task, const IExport::TTableColumns& columns) const = 0;
    virtual IExport* CreateExportToS3(const IExport::TTask& task, const IExport::TTableColumns& columns) const = 0;
    virtual void Shutdown() = 0;
};

} // NDataShard
} // NKikimr
