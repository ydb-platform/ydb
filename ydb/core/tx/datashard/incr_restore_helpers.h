#pragma once

#include <ydb/core/tablet_flat/flat_update_op.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>

#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NDataShard::NIncrRestoreHelpers {

using namespace NTable;

std::optional<TVector<TUpdateOp>> MakeRestoreUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, const TMap<ui32, TUserTable::TUserColumn>& columns);

} // namespace NKikimr::NBackup::NImpl
