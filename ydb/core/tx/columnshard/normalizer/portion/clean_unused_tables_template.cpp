#include "clean_unused_tables_template.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NKikimr::NColumnShard;

template class TCleanUnusedTablesNormalizerTemplate<Schema::IndexColumns
                                                   >;

} // namespace NKikimr::NOlap::NCleanUnusedTables