#include "clean_unused_tables_template.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NKikimr::NColumnShard;

template class TCleanUnusedTablesNormalizerTemplate<
    Schema::IndexColumns,
    Schema::TtlSettingsPresetInfo,
    Schema::TtlSettingsPresetVersionInfo,
    Schema::OneToOneEvictedBlobs,
    Schema::TxStates,
    Schema::TxEvents,
    Schema::LockRanges,
    Schema::LockConflicts,
    Schema::LockVolatileDependencies,
    Schema::BackgroundSessions
    // Schema::InsertTable uncomment after enabling EnableWritePortionsOnInsert
>;

} // namespace NKikimr::NOlap::NCleanUnusedTables