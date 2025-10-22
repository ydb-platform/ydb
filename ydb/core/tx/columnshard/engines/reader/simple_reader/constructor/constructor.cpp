#include "constructor.h"
#include "read_metadata.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/resolver.h>

namespace NKikimr::NOlap::NReader::NSimple {

NKikimr::TConclusionStatus TIndexScannerConstructor::ParseProgram(
    const TProgramParsingContext& context, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const {
    auto& indexInfo = read.TableMetadataAccessor->GetSnapshotSchemaVerified(context.GetVersionedSchemas(), Snapshot)->GetIndexInfo();
    NCommon::TIndexColumnResolver columnResolver(indexInfo);
    return TBase::ParseProgram(context, proto.GetOlapProgramType(), proto.GetOlapProgram(), read, columnResolver);
}

std::vector<TNameTypeInfo> TIndexScannerConstructor::GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const {
    auto& indexInfo = self->TablesManager.GetIndexInfo(Snapshot);
    return indexInfo.GetPrimaryKeyColumns();
}

TConclusion<std::shared_ptr<TReadMetadataBase>> TIndexScannerConstructor::DoBuildReadMetadata(
    const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    TVersionedPresetSchemas* schemas = nullptr;
    TVersionedPresetSchemas defaultSchemas(
        0, self->GetStoragesManager(), self->GetTablesManager().GetSchemaObjectsCache().GetObjectPtrVerified());
    auto* index = self->TablesManager.MutablePrimaryIndexAsOptional<TColumnEngineForLogs>();
    if (index) {
        schemas = &index->MutableVersionedSchemas();
    } else {
        schemas = &defaultSchemas;
    }
    if (read.TableMetadataAccessor->NeedStalenessChecker()) {
        if (read.GetSnapshot().GetPlanInstant() < self->GetMinReadSnapshot().GetPlanInstant()) {
            return TConclusionStatus::Fail(TStringBuilder() << "Snapshot too old: " << read.GetSnapshot() << ". CS min read snapshot: "
                                                            << self->GetMinReadSnapshot() << ". now: " << TInstant::Now());
        }
    }

    auto readMetadata = std::make_shared<TReadMetadata>(read.TableMetadataAccessor->GetVersionedIndexCopyVerified(*schemas), read);

    auto initResult = readMetadata->Init(self, read, false);
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
