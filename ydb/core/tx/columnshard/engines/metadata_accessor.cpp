#include "metadata_accessor.h"

#include "reader/common/description.h"
#include "reader/common_reader/constructor/read_metadata.h"
#include "reader/plain_reader/iterator/constructors.h"
#include "reader/simple_reader/iterator/collections/constructors.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <util/folder/path.h>

namespace NKikimr::NOlap {
ITableMetadataAccessor::ITableMetadataAccessor(const TString& tablePath)
    : TablePath(tablePath) {
    AFL_VERIFY(!!TablePath);
}

TString ITableMetadataAccessor::GetTableName() const {
    return TFsPath(TablePath).Fix().GetName();
}

TUserTableAccessor::TUserTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId)
    : TBase(tableName)
    , PathId(pathId) {
    AFL_VERIFY(pathId.IsValid());
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TUserTableAccessor::SelectMetadata(const TSelectMetadataContext& context,
    const NReader::TReadDescription& readDescription, const NColumnShard::IResolveWriteIdToLockId& resolver, const bool isPlain) const {
    AFL_VERIFY(readDescription.PKRangesFilter);
    std::vector<std::shared_ptr<TPortionInfo>> portions =
        context.GetEngine().Select(PathId.InternalPathId, readDescription.GetSnapshot(), *readDescription.PKRangesFilter, !!readDescription.LockId);
    std::vector<TInsertWriteId> uncommitted;
    if (readDescription.LockId) {
        std::vector<std::shared_ptr<TPortionInfo>> visible;
        for (auto&& portion: portions) {
            if (portion->IsCommitted()) {
                visible.emplace_back(std::move(portion));
            } else {
                AFL_VERIFY(portion->GetPortionType() == EPortionType::Written);
                auto* written = static_cast<NKikimr::NOlap::TWrittenPortionInfo*>(portion.get());
                const auto& insertWriteId = written->GetInsertWriteId();
                uncommitted.emplace_back(insertWriteId);
                if (const auto& lockId = resolver.ResolveWriteIdToLockId(insertWriteId); lockId == readDescription.LockId) {
                    visible.emplace_back(std::move(portion));
                }
            }
        }
        portions.swap(visible);
    }
    if (!isPlain) {
        std::deque<NReader::NSimple::TSourceConstructor> sources;
        for (auto&& i : portions) {
            sources.emplace_back(NReader::NSimple::TSourceConstructor(std::move(i), readDescription.GetSorting()));
        }
        return std::make_unique<NReader::NSimple::TPortionsSources>(std::move(sources), readDescription.GetSorting(), std::move(uncommitted));
    } else {
        return std::make_unique<NReader::NPlain::TPortionSources>(std::move(portions), std::move(uncommitted));
    }
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TAbsentTableAccessor::SelectMetadata(const TSelectMetadataContext& /*context*/,
    const NReader::TReadDescription& /*readDescription*/, const NColumnShard::IResolveWriteIdToLockId& /*resolver*/,
    const bool /*isPlain*/) const {
    return NReader::NSimple::TPortionsSources::BuildEmpty();
}

}   // namespace NKikimr::NOlap
