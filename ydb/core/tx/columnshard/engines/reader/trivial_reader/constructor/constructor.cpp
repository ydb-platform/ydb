#include "constructor.h"
#include "read_metadata.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/resolver.h>

namespace NKikimr::NOlap::NReader::NTrivial {

NKikimr::TConclusionStatus TIndexScannerConstructor::ParseProgram(
    const TProgramParsingContext& context, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const {
    auto& indexInfo = read.GetTableMetadataAccessor()->GetSnapshotSchemaVerified(context.GetVersionedSchemas(), Snapshot)->GetIndexInfo();
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
    if (read.GetTableMetadataAccessor()->NeedStalenessChecker()) {
        auto pathId = read.GetTableMetadataAccessor()->GetPathIdVerified();
        if (!self->MayStartScanAt(read.GetSnapshot(), pathId.GetSchemeShardLocalPathId())) {
            return TConclusionStatus::Fail(TStringBuilder() << "Snapshot too old: " << read.GetSnapshot() << ". CS min read snapshot: "
                                                            << self->GetMinSnapshotForNewReads() << ". now: " << TInstant::Now());
        }
    }

    auto readMetadata = std::make_shared<TReadMetadata>(read.GetTableMetadataAccessor()->GetVersionedIndexCopyVerified(*schemas), read);

    auto initResult = readMetadata->Init(self, read, GetReaderClass());
    if (!initResult) {
        return initResult;
    }
    return static_pointer_cast<TReadMetadataBase>(readMetadata);
}

std::shared_ptr<IScanCursor> TIndexScannerConstructor::DoBuildCursor(const NKikimrKqp::TEvKqpScanCursor::ImplementationCase impl) const {
    switch (impl) {
        case NKikimrKqp::TEvKqpScanCursor::kColumnShardSimple:
        case NKikimrKqp::TEvKqpScanCursor::kColumnShardNotSortedSimple:
        case NKikimrKqp::TEvKqpScanCursor::IMPLEMENTATION_NOT_SET:
            return std::make_shared<TSourceIndexScanCursor>();
        case NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardSimple:
        case NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardNotSortedSimple:
            return std::make_shared<TSourceIdScanCursor>();
        case NKikimrKqp::TEvKqpScanCursor::kColumnShardPlain:
            break;
    }
    // The cursor came off the wire. A shape this reader cannot read fails the query.
    return nullptr;
}

}   // namespace NKikimr::NOlap::NReader::NTrivial
