#include "read_metadata.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NReader {

ISnapshotSchema::TPtr TReadMetadataBase::GetLoadSchemaVerified(const TPortionInfo& portion) const {
    auto schema = portion.GetSchema(GetIndexVersions());
    YDB_LOG_VERIFY(schema);
    return schema;
}

}   // namespace NKikimr::NOlap::NReader
