#pragma once

#include <contrib/ydb/core/tx/datashard/export_iface.h>

class TDataShardExportFactory : public NKikimr::NDataShard::IExportFactory {
    using IExport = NKikimr::NDataShard::IExport;

public:
    IExport* CreateExportToYt(const IExport::TTask& task, const IExport::TTableColumns& columns) const override;
    IExport* CreateExportToS3(const IExport::TTask& task, const IExport::TTableColumns& columns) const override;
    void Shutdown() override;
};
