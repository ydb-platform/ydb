#pragma once

#include "restore_impl.h"

namespace NYdb {
namespace NDump {

NPrivate::IDataAccumulator* CreateImportDataAccumulator(
    const NTable::TTableDescription& dumpedDesc,
    const NTable::TTableDescription& actualDesc,
    const TRestoreSettings& settings);

NPrivate::IDataWriter* CreateImportDataWriter(
    const TString& path,
    const NTable::TTableDescription& desc,
    NImport::TImportClient& importClient,
    NTable::TTableClient& tableClient,
    NPrivate::IDataAccumulator* accumulator,
    const TRestoreSettings& settings);

} // NDump
} // NYdb
