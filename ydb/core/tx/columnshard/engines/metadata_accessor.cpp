#include "metadata_accessor.h"

#include "reader/common/description.h"
#include "reader/common_reader/constructor/read_metadata.h"
#include "reader/plain_reader/iterator/constructors.h"
#include "reader/simple_reader/iterator/collections/constructors.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

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
    const NReader::TReadDescription& readDescription, const bool withUncommitted, const bool isPlain) const {
    AFL_VERIFY(readDescription.PKRangesFilter);
    std::vector<std::shared_ptr<TPortionInfo>> portions =
        context.GetEngine().Select(PathId.InternalPathId, readDescription.GetSnapshot(), *readDescription.PKRangesFilter, withUncommitted);
    if (!isPlain) {
        std::deque<NReader::NSimple::TSourceConstructor> sources;
        for (auto&& i : portions) {
            sources.emplace_back(NReader::NSimple::TSourceConstructor(std::move(i), readDescription.GetSorting()));
        }
        return std::make_unique<NReader::NSimple::TPortionsSources>(std::move(sources), readDescription.GetSorting());
    } else {
        return std::make_unique<NReader::NPlain::TPortionSources>(std::move(portions));
    }
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TAbsentTableAccessor::SelectMetadata(const TSelectMetadataContext& /*context*/,
    const NReader::TReadDescription& /*readDescription*/, const bool /*withUncommitted*/, const bool /*isPlain*/) const {
    return NReader::NSimple::TPortionsSources::BuildEmpty();
}

}   // namespace NKikimr::NOlap
