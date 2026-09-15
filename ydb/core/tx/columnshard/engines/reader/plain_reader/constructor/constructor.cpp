#include "constructor.h"
#include "read_metadata.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/resolver.h>

namespace NKikimr::NOlap::NReader::NPlain {

NKikimr::TConclusionStatus TIndexScannerConstructor::ParseProgram(
    const TProgramParsingContext& context, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const {
    const ISnapshotSchema::TPtr schema =
        read.GetTableMetadataAccessor()->GetSnapshotSchemaVerified(context.GetVersionedSchemas(), read.GetSnapshot());
    NCommon::TIndexColumnResolver columnResolver(schema->GetIndexInfo());
    return TBase::ParseProgram(context, proto.GetOlapProgramType(), proto.GetOlapProgram(), read, columnResolver);
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

    auto pathId = read.GetTableMetadataAccessor()->GetPathIdVerified();
    if (!self->MayStartScanAt(read.GetSnapshot(), pathId.GetSchemeShardLocalPathId())) {
        return TConclusionStatus::Fail(TStringBuilder() << "Snapshot too old: " << read.GetSnapshot() << ". CS min read snapshot: "
                                                        << self->GetMinSnapshotForNewReads() << ". now: " << TInstant::Now());
    }

    auto readMetadata = std::make_shared<TReadMetadata>(index->GetVersionedIndexReadonlyCopy(), read);

    auto initResult = readMetadata->Init(self, read, GetReaderClass());
    if (!initResult) {
        return initResult;
    }
    return static_pointer_cast<TReadMetadataBase>(readMetadata);
}

std::shared_ptr<IScanCursor> TIndexScannerConstructor::DoBuildCursor(const NKikimrKqp::TEvKqpScanCursor::ImplementationCase impl) const {
    if (impl != NKikimrKqp::TEvKqpScanCursor::ImplementationCase::kColumnShardPlain &&
        impl != NKikimrKqp::TEvKqpScanCursor::ImplementationCase::IMPLEMENTATION_NOT_SET) {
        return nullptr;
    }
    return std::make_shared<TPlainScanCursor>();
}

}   // namespace NKikimr::NOlap::NReader::NPlain
