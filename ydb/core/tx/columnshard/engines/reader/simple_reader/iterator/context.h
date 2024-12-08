#pragma once
#include "fetching.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

class IDataSource;
using TColumnsSet = NCommon::TColumnsSet;
using EStageFeaturesIndexes = NCommon::EStageFeaturesIndexes;
using TColumnsSetIds = NCommon::TColumnsSetIds;
using EMemType = NCommon::EMemType;

class TSpecialReadContext: public NCommon::TSpecialReadContext {
private:
    using TBase = NCommon::TSpecialReadContext;
    TReadMetadata::TConstPtr ReadMetadata;
    std::shared_ptr<TFetchingScript> BuildColumnsFetchingPlan(const bool needSnapshots, const bool partialUsageByPredicateExt,
        const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const;
    TMutex Mutex;
    std::array<std::array<std::array<std::array<std::array<std::optional<std::shared_ptr<TFetchingScript>>, 2>, 2>, 2>, 2>, 2>
        CacheFetchingScripts;
    std::shared_ptr<TFetchingScript> AskAccumulatorsScript;

public:
    const TReadMetadata::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    virtual TString ProfileDebugString() const override;

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);

    std::shared_ptr<TFetchingScript> GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
