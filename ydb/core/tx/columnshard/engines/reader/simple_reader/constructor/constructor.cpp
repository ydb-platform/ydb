#include "constructor.h"
#include "read_metadata.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/resolver.h>

namespace NKikimr::NOlap::NReader::NSimple {

NKikimr::TConclusionStatus TIndexScannerConstructor::ParseProgram(
    const TVersionedIndex* vIndex, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const {
    AFL_VERIFY(vIndex);
    auto& indexInfo = vIndex->GetSchemaVerified(Snapshot)->GetIndexInfo();
    NCommon::TIndexColumnResolver columnResolver(indexInfo);
    return TBase::ParseProgram(vIndex, proto.GetOlapProgramType(), proto.GetOlapProgram(), read, columnResolver);
}

std::vector<TNameTypeInfo> TIndexScannerConstructor::GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const {
    auto& indexInfo = self->TablesManager.GetIndexInfo(Snapshot);
    return indexInfo.GetPrimaryKeyColumns();
}

NKikimr::TConclusion<std::shared_ptr<TReadMetadataBase>> TIndexScannerConstructor::DoBuildReadMetadata(
    const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    auto& index = self->TablesManager.GetPrimaryIndex();
    if (!index) {
        return std::shared_ptr<TReadMetadataBase>();
    }

    if (read.GetSnapshot().GetPlanInstant() < self->GetMinReadSnapshot().GetPlanInstant()) {
        return TConclusionStatus::Fail(TStringBuilder() << "Snapshot too old: " << read.GetSnapshot() << ". CS min read snapshot: "
                                                        << self->GetMinReadSnapshot() << ". now: " << TInstant::Now());
    }

    TDataStorageAccessor dataAccessor(index);
    AFL_VERIFY(read.PathId);
    auto readMetadata = std::make_shared<TReadMetadata>(index->CopyVersionedIndexPtr(), read);

    auto initResult = readMetadata->Init(self, read, dataAccessor);
    if (!initResult) {
        return initResult;
    }
    return static_pointer_cast<TReadMetadataBase>(readMetadata);
}

std::shared_ptr<NKikimr::NOlap::IScanCursor> TIndexScannerConstructor::DoBuildCursor() const {
    switch (Sorting) {
        case ERequestSorting::ASC:
        case ERequestSorting::DESC:
            return std::make_shared<TSimpleScanCursor>();
        case ERequestSorting::NONE:
            return std::make_shared<TNotSortedSimpleScanCursor>();
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
