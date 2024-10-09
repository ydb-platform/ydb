#include "read_metadata.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NReader {

TDataStorageAccessor::TDataStorageAccessor(const std::unique_ptr<TInsertTable>& insertTable, const std::unique_ptr<IColumnEngine>& index)
    : InsertTable(insertTable)
    , Index(index) {
}

std::shared_ptr<TSelectInfo> TDataStorageAccessor::Select(const TReadDescription& readDescription) const {
    if (readDescription.ReadNothing) {
        return std::make_shared<TSelectInfo>();
    }
    AFL_VERIFY(readDescription.PKRangesFilter);
    return Index->Select(readDescription.PathId, readDescription.GetSnapshot(), *readDescription.PKRangesFilter);
}

ISnapshotSchema::TPtr TReadMetadataBase::GetLoadSchemaVerified(const TPortionInfo& portion) const {
    auto schema = portion.GetSchema(GetIndexVersions());
    AFL_VERIFY(schema);
    return schema;
}

std::vector<TCommittedBlob> TDataStorageAccessor::GetCommitedBlobs(const TReadDescription& readDescription,
    const std::shared_ptr<arrow::Schema>& pkSchema, const std::optional<ui64> lockId, const TSnapshot& reqSnapshot) const {
    AFL_VERIFY(readDescription.PKRangesFilter);
    return std::move(InsertTable->Read(readDescription.PathId, lockId, reqSnapshot, pkSchema, &*readDescription.PKRangesFilter));
}

}   // namespace NKikimr::NOlap::NReader
