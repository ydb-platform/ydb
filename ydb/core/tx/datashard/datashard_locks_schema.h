#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NDataShard {

enum class ELocksFormatVersion : ui64 {
    Legacy = 0,
    Current = 1,
};

// Called after Materialize, in the same transaction and before loading locks.
// Returns false when the format version needs to be read again after a page fault.
bool UpdateLocksSchema(NTable::TDatabase& database);

} // namespace NKikimr::NDataShard
