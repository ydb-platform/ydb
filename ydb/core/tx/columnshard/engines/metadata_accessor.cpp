#include "metadata_accessor.h"

#include "reader/common/description.h"
#include "reader/common_reader/constructor/read_metadata.h"
#include "reader/simple_reader/iterator/collections/constructors.h"

#include <ydb/library/actors/core/log.h>

#include <util/folder/path.h>

namespace NKikimr::NOlap {
ITableMetadataAccessor::ITableMetadataAccessor(const TString& tablePath)
    : TablePath(tablePath) {
    AFL_VERIFY(!!TablePath);
}

TString ITableMetadataAccessor::GetTableName() const {
    return TFsPath(TablePath).Fix().GetName();
}

TSysViewTableAccessor::TSysViewTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId)
    : TBase(tableName)
    , PathId(pathId) {
    AFL_VERIFY(GetTablePath().find(".sys") != TString::npos);
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TSysViewTableAccessor::SelectMetadata(const IColumnEngine& /*engine*/,
    const NReader::TReadDescription& /*readDescription*/, const bool /*withUncommitted*/, const bool /*isPlain*/) const {
    AFL_VERIFY(false);
    return {};
}

TUserTableAccessor::TUserTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId)
    : TBase(tableName)
    , PathId(pathId) {
    AFL_VERIFY(GetTablePath().find(".sys") == TString::npos);
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TUserTableAccessor::SelectMetadata(
    const IColumnEngine& engine, const NReader::TReadDescription& readDescription, const bool withUncommitted, const bool isPlain) const {
    AFL_VERIFY(readDescription.PKRangesFilter);
    std::vector<std::shared_ptr<TPortionInfo>> portions =
        engine.Select(PathId.InternalPathId, readDescription.GetSnapshot(), *readDescription.PKRangesFilter, withUncommitted);
    if (!isPlain) {
        std::deque<NReader::NSimple::TSourceConstructor> sources;
        for (auto&& i : portions) {
            sources.emplace_back(NReader::NSimple::TSourceConstructor(std::move(i), readDescription.GetSorting()));
        }
        if (readDescription.GetSorting() != NReader::ERequestSorting::NONE) {
            return std::make_unique<NReader::NSimple::TSortedPortionsSources>(std::move(sources));
        } else {
            return std::make_unique<NReader::NSimple::TNotSortedPortionsSources>(std::move(sources));
        }
    } else {
        return std::make_unique<NReader::NPlain::TPortionSources>(std::move(portions));
    }
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TAbsentTableAccessor::SelectMetadata(const IColumnEngine& /*engine*/,
    const NReader::TReadDescription& /*readDescription*/, const bool /*withUncommitted*/, const bool /*isPlain*/) const {
    return std::make_unique<NReader::NSimple::TNotSortedPortionsSources>();
}

}   // namespace NKikimr::NOlap
