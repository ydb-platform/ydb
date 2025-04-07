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
class TSourceConstructor;
using TColumnsSet = NCommon::TColumnsSet;
using TColumnsSetIds = NCommon::TColumnsSetIds;
using EMemType = NCommon::EMemType;
using TFetchingScript = NCommon::TFetchingScript;

class TSpecialReadContext: public NCommon::TSpecialReadContext, TNonCopyable {
private:
    using TBase = NCommon::TSpecialReadContext;
    YDB_READONLY_DEF(TActorId, DuplicatesManager);

private:
    std::shared_ptr<TFetchingScript> BuildColumnsFetchingPlan(const bool needSnapshots, const bool partialUsageByPredicateExt,
        const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion, const bool needFilterDuplicates) const;
    TMutex Mutex;
    std::array<std::array<std::array<std::array<std::array<std::array<NCommon::TFetchingScriptOwner, 2>, 2>, 2>, 2>, 2>, 2> CacheFetchingScripts;
    std::shared_ptr<TFetchingScript> AskAccumulatorsScript;

    virtual std::shared_ptr<TFetchingScript> DoGetColumnsFetchingPlan(const std::shared_ptr<NCommon::IDataSource>& source) override;
    virtual void DoAbort() override {
        if (DuplicatesManager) {
            NActors::TActivationContext::AsActorContext().Send(DuplicatesManager, new NActors::TEvents::TEvPoison());
            DuplicatesManager = TActorId();
        }
    }

public:
    virtual TString ProfileDebugString() const override;

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);

    ~TSpecialReadContext() {
        if (DuplicatesManager) {
            NActors::TActivationContext::AsActorContext().Send(DuplicatesManager, new NActors::TEvents::TEvPoison());
        }
    }

    void RegisterDuplicatesManager(const std::shared_ptr<TSpecialReadContext>& self);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
