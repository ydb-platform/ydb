#pragma once
#include "fetching.h"

#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetch_steps.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

class IDataSource;
class TSourceConstructor;
using TColumnsSet = NCommon::TColumnsSet;
using TColumnsSetIds = NCommon::TColumnsSetIds;
using EMemType = NCommon::EMemType;
using TFetchingScript = NCommon::TFetchingScript;

// Streaming configuration for page-based reading
struct TStreamingConfig {
    enum class EStrategy {
        Always,      // Always use page-based streaming
        Never,       // Never use streaming (read entire portion)
        Auto         // Automatically decide based on portion size
    };

    // Size of page in rows
    ui32 PageSizeRows = 10000;
    
    // Maximum page size in bytes
    ui64 PageSizeBytes = 8 * 1024 * 1024; // 8 MB
    
    // Maximum number of pages in flight
    ui32 MaxPagesInFlight = 10;
    
    // Enable page-based reading for portions larger than this
    ui32 MinRecordsForPaging = 50000;
    
    // Strategy for enabling streaming
    EStrategy Strategy = EStrategy::Auto;

    bool ShouldUseStreamingMode(ui32 recordsCount) const {
        switch (Strategy) {
            case EStrategy::Always:
                return true;
            case EStrategy::Never:
                return false;
            case EStrategy::Auto:
                return recordsCount >= MinRecordsForPaging;
        }
        return false;
    }
};

class TSpecialReadContext: public NCommon::TSpecialReadContext, TNonCopyable {
private:
    using TBase = NCommon::TSpecialReadContext;
    mutable TSpinLock DuplicatesManagerLock;
    NActors::TActorId DuplicatesManager = NActors::TActorId();

private:
    std::shared_ptr<TFetchingScript> BuildColumnsFetchingPlan(const bool needSnapshots, const bool partialUsageByPredicateExt,
        const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion,
        const bool needFilterDuplicates, const bool isFinalSyncPoint) const;
    TMutex Mutex;
    std::array<std::array<std::array<std::array<std::array<std::array<NCommon::TFetchingScriptOwner, 2>, 2>, 2>, 2>, 2>, 2> CacheFetchingScripts;

    virtual std::shared_ptr<TFetchingScript> DoGetColumnsFetchingPlan(
        const std::shared_ptr<NCommon::IDataSource>& source, const bool isFinalSyncPoint) override;
    mutable std::optional<std::shared_ptr<TFetchingScript>> SourcesAggregationScript;
    mutable std::optional<std::shared_ptr<TFetchingScript>> RestoreResultScript;

    bool NeedDuplicateFiltering() const {
        return GetReadMetadata()->GetDeduplicationPolicy() == EDeduplicationPolicy::PREVENT_DUPLICATES &&
               GetReadMetadata()->TableMetadataAccessor->NeedDuplicateFiltering();
    }

    // Streaming configuration
    TStreamingConfig StreamingConfig;

public:
    const TStreamingConfig& GetStreamingConfig() const {
        return StreamingConfig;
    }

    TStreamingConfig& MutableStreamingConfig() {
        return StreamingConfig;
    }
    std::shared_ptr<TFetchingScript> GetSourcesAggregationScript() const {
        if (!SourcesAggregationScript) {
            auto graph = GetReadMetadata()->GetProgram().GetGraphOptional();
            if (!graph || !graph->GetResultsAggregationProcessor()) {
                SourcesAggregationScript = nullptr;
            } else {
                // TODO: fix me, temporary disabled
                SourcesAggregationScript = nullptr;
                // auto aggrProc = GetReadMetadata()->GetProgram().GetChainVerified()->GetResultsAggregationProcessor();
                // NCommon::TFetchingScriptBuilder builder(*this);
                // builder.AddStep(std::make_shared<TInitializeSourceStep>());
                // builder.AddStep(std::make_shared<TStepAggregationSources>(aggrProc));
                // builder.AddStep(std::make_shared<TCleanAggregationSources>(aggrProc));
                // SourcesAggregationScript = std::move(builder).Build();
            }
        }
        return *SourcesAggregationScript;
    }

    std::shared_ptr<TFetchingScript> GetRestoreResultScript() const {
        if (!RestoreResultScript) {
            NCommon::TFetchingScriptBuilder builder(*this);
            builder.AddStep(std::make_shared<NCommon::TBuildStageResultStep>());
            builder.AddStep(std::make_shared<TPrepareResultStep>(true));
            RestoreResultScript = std::move(builder).Build();
        }
        return *RestoreResultScript;
    }

    virtual TString ProfileDebugString() const override;

    void RegisterActors(const NCommon::ISourcesConstructor& sources);
    void UnregisterActors();

    NActors::TActorId GetDuplicatesManagerVerified() const {
        TGuard<TSpinLock> g(DuplicatesManagerLock);
        AFL_VERIFY(DuplicatesManager);
        return DuplicatesManager;
    }

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
        : TBase(commonContext) {
    }

    ~TSpecialReadContext() {
        if (NActors::TActorSystem::IsStopped()) {
            return;
        }

        TGuard<TSpinLock> g(DuplicatesManagerLock);
        AFL_VERIFY(!DuplicatesManager);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
