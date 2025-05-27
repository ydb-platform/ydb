#pragma once
#include "columns_set.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TFetchingScript;
class IDataSource;

class TSpecialReadContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<TReadContext>, CommonContext);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TProcessGuard>, ProcessMemoryGuard);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TScopeGuard>, ProcessScopeGuard);

    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, SpecColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, MergeColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ShardingColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, DeletionColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, EFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PredicateColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PKColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, AllUsageColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ProgramInputColumns);

    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, MergeStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FilterStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FetchingStageMemory);

    TReadMetadata::TConstPtr ReadMetadata;

    virtual std::shared_ptr<TFetchingScript> DoGetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source) = 0;

protected:
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();

public:
    template <class T>
    std::shared_ptr<TFetchingScript> GetColumnsFetchingPlan(const std::shared_ptr<T>& source) {
        return GetColumnsFetchingPlan(std::static_pointer_cast<IDataSource>(source));
    }

    std::shared_ptr<TFetchingScript> GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source) {
        return DoGetColumnsFetchingPlan(source);
    }

    const TReadMetadata::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    template <class T>
    std::shared_ptr<T> GetReadMetadataVerifiedAs() const {
        auto result = std::dynamic_pointer_cast<T>(ReadMetadata);
        AFL_VERIFY(!!result);
        return result;
    }

    ui64 GetProcessMemoryControlId() const {
        AFL_VERIFY(ProcessMemoryGuard);
        return ProcessMemoryGuard->GetProcessId();
    }

    bool IsActive() const {
        return !CommonContext->IsAborted();
    }

    bool IsAborted() const {
        return CommonContext->IsAborted();
    }

    void Abort() {
        CommonContext->Stop();
    }

    virtual ~TSpecialReadContext() {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("fetching", DebugString());
    }

    TString DebugString() const;
    virtual TString ProfileDebugString() const = 0;

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
