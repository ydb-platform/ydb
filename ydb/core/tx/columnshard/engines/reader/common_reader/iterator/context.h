#pragma once
#include "columns_set.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NCommon {

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

    TAtomic AbortFlag = 0;

protected:
    NIndexes::TIndexCheckerContainer IndexChecker;
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();

public:
    ui64 GetProcessMemoryControlId() const {
        AFL_VERIFY(ProcessMemoryGuard);
        return ProcessMemoryGuard->GetProcessId();
    }

    bool IsAborted() const {
        return AtomicGet(AbortFlag);
    }

    void Abort() {
        AtomicSet(AbortFlag, 1);
    }

    virtual ~TSpecialReadContext() {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("fetching", DebugString());
    }

    TString DebugString() const;
    virtual TString ProfileDebugString() const = 0;

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
