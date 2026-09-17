#pragma once

#include "restore_impl.h"

namespace NYdb::NDump {

NPrivate::IDataAccumulator* CreateCompatAccumulator(
    const TString& path,
    const NTable::TTableDescription& desc,
    const TRestoreSettings& settings);

NPrivate::IDataWriter* CreateCompatWriter(
    const TString& path,
    NTable::TTableClient& tableClient,
    NQuery::TQueryClient& queryClient,
    const NPrivate::IDataAccumulator* accumulator,
    const TRestoreSettings& settings,
    bool isColumnTable);

} // NYdb::NDump
