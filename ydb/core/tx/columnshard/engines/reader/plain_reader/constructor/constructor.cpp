#include "constructor.h"
#include "resolver.h"
#include "read_metadata.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NReader::NPlain {

NKikimr::TConclusionStatus TIndexScannerConstructor::ParseProgram(const TVersionedIndex* vIndex, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const {
    AFL_VERIFY(vIndex);
    auto& indexInfo = vIndex->GetSchema(Snapshot)->GetIndexInfo();
    TIndexColumnResolver columnResolver(indexInfo);
    return TBase::ParseProgram(vIndex, proto.GetOlapProgramType(), proto.GetOlapProgram(), read, columnResolver);
}

std::vector<TNameTypeInfo> TIndexScannerConstructor::GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const {
    auto& indexInfo = self->TablesManager.GetIndexInfo(Snapshot);
    return indexInfo.GetPrimaryKeyColumns();
}

NKikimr::TConclusion<std::shared_ptr<TReadMetadataBase>> TIndexScannerConstructor::DoBuildReadMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    auto& insertTable = self->InsertTable;
    auto& index = self->TablesManager.GetPrimaryIndex();
    if (!insertTable || !index) {
        return std::shared_ptr<TReadMetadataBase>();
    }

    if (read.GetSnapshot().GetPlanInstant() < self->GetMinReadSnapshot().GetPlanInstant()) {
        return TConclusionStatus::Fail(TStringBuilder() << "Snapshot too old: " << read.GetSnapshot());
    }

    TDataStorageAccessor dataAccessor(insertTable, index);
    auto readMetadata = std::make_shared<TReadMetadata>(index->CopyVersionedIndexPtr(), read.GetSnapshot(),
        IsReverse ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC, read.GetProgram());

    auto initResult = readMetadata->Init(read, dataAccessor);
    if (!initResult) {
        return initResult;
    }
    return static_pointer_cast<TReadMetadataBase>(readMetadata);
}

}