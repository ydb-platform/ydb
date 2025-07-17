#include "read_metadata.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NReader {

TDataStorageAccessor::TDataStorageAccessor(const std::unique_ptr<IColumnEngine>& index)
    : Index(index) {
}

std::shared_ptr<TSelectInfo> TDataStorageAccessor::Select(const TReadDescription& readDescription, const bool withUncommitted) const {
    if (readDescription.ReadNothing) {
        return std::make_shared<TSelectInfo>();
    }
    AFL_VERIFY(readDescription.PKRangesFilter);
    return Index->Select(readDescription.PathId.InternalPathId, readDescription.GetSnapshot(),
         *readDescription.PKRangesFilter, withUncommitted);
}

ISnapshotSchema::TPtr TReadMetadataBase::GetLoadSchemaVerified(const TPortionInfo& portion) const {
    auto schema = portion.GetSchema(GetIndexVersions());
    AFL_VERIFY(schema);
    return schema;
}

}   // namespace NKikimr::NOlap::NReader
