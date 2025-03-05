#pragma once

#include "restore_impl.h"

class TLog;

namespace NYdb::NDump {

NPrivate::IDataAccumulator* CreateImportDataAccumulator(
    const NTable::TTableDescription& dumpedDesc,
    const NTable::TTableDescription& actualDesc,
    const TRestoreSettings& settings,
    const std::shared_ptr<TLog>& log);

NPrivate::IDataWriter* CreateImportDataWriter(
    const TString& path,
    const NTable::TTableDescription& desc,
    ui32 partitionCount,
    NImport::TImportClient& importClient,
    NTable::TTableClient& tableClient,
    const TVector<THolder<NPrivate::IDataAccumulator>>& accumulators,
    const TRestoreSettings& settings,
    const std::shared_ptr<TLog>& log);

} // NYdb::NDump
