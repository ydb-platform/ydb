#pragma once

#include <ydb/core/tx/datashard/export_iface.h>

class TDataShardExportFactory : public NKikimr::NDataShard::IExportFactory {
public:
    NKikimr::NDataShard::IExport *CreateExportToYt(bool useTypeV3) const override;
    NKikimr::NDataShard::IExport *CreateExportToS3() const override;
    void Shutdown() override;
};
