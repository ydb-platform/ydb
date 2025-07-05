#include "metadata_accessor.h"

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

TSysViewTableAccessor::TSysViewTableAccessor(const TString& tableName)
    : TBase(tableName) {
    AFL_VERIFY(GetTablePath().find(".sys") != TString::npos);
}

TUserTableAccessor::TUserTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId)
    : TBase(tableName)
    , PathId(pathId) {
    AFL_VERIFY(GetTablePath().find(".sys") == TString::npos);
}

std::unique_ptr<NKikimr::NOlap::NReader::NCommon::ISourcesConstructor> TUserTableAccessor::SelectMetadata(
    const IColumnEngine& engine, const TReadDescription& readDescription, const bool withUncommitted) {
    AFL_VERIFY(readDescription.PKRangesFilter);
    std::vector<std::shared_ptr<TPortionInfo>> portions =
        Index->Select(PathId.InternalPathId, readDescription.GetSnapshot(), *readDescription.PKRangesFilter, withUncommitted);
    std::deque<TSourceConstructor> sources;
    sources.reserve(portions.size());
    for (auto&& i : portions) {
        sources.emplace_back(TSourceConstructor(sources.size(), i, readDescription.GetSorting()));
    }
    if (readDescription.GetSorting() != ERequestSorting::NONE) {
        return std::make_unique<TSortedPortionsSources>(std::move(sources));
    } else {
        return std::make_unique<TNotSortedPortionsSources>(std::move(sources));
    }
}

}   // namespace NKikimr::NOlap
