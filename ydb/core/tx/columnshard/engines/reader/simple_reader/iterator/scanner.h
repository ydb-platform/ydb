#pragma once
#include "source.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scheduler.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TPlainReadData;

class TSourceFetchingSchedulerImpl {
private:
    std::deque<std::shared_ptr<IDataSource>> SortedSources;
    std::deque<std::shared_ptr<IDataSource>> FetchingSources;
    THashMap<ui32, std::shared_ptr<IDataSource>> ActiveSources;
    THashMap<ui32, std::shared_ptr<IDataSource>> BlockedSources;
    THashMap<ui32, std::shared_ptr<IDataSource>> UnblockedSources;
    YDB_READONLY(ui64, InFlightLimit, 1);

    ui64 GetInFlightSourcesCount() const {
        return ActiveSources.size();
    }

public:
    void SetUnblocked(const ui32 sourceIdx) {
        auto findSource = BlockedSources.find(sourceIdx);
        AFL_VERIFY(findSource != BlockedSources.end())("source_idx", sourceIdx);
        AFL_VERIFY(UnblockedSources.emplace(findSource->first, findSource->second).second);
        BlockedSources.erase(findSource);
        Y_UNUSED(ScheduleNextSource());
    }

    void SetBlocked(const ui32 sourceIdx) {
        auto findSource = ActiveSources.find(sourceIdx);
        AFL_VERIFY(findSource != ActiveSources.end())("source_idx", sourceIdx);
        AFL_VERIFY(BlockedSources.emplace(findSource->first, findSource->second).second);
        ActiveSources.erase(findSource);
        Y_UNUSED(ScheduleNextSource());
    }

    std::shared_ptr<IDataSource> ScheduleNextSource() {
        const ui64 limit = GetInFlightLimit();
        AFL_VERIFY(GetInFlightSourcesCount() <= limit);
        if (GetInFlightSourcesCount() == limit) {
            return nullptr;
        }

        if (UnblockedSources.size()) {
            auto nextSource = UnblockedSources.begin()->second;
            ActiveSources.emplace(nextSource->GetSourceIdx(), nextSource);
            UnblockedSources.erase(UnblockedSources.begin());
            nextSource->ContinueCursor(nextSource);
            return nextSource;
        } else if (SortedSources.size()) {
            auto nextSource = SortedSources.front();
            SortedSources.pop_front();
            FetchingSources.emplace_back(nextSource);
            ActiveSources.emplace(nextSource->GetSourceIdx(), nextSource);
            nextSource->StartProcessing(nextSource);
            return nextSource;
        }
        return nullptr;
    }

    void SetInFlightLimit(const ui64 limit) {
        AFL_VERIFY(limit >= InFlightLimit);
        InFlightLimit = limit;
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "S:";
        for (auto&& i : SortedSources) {
            sb << i->GetSourceId() << ";";
        }
        sb << "F:";
        for (auto&& i : FetchingSources) {
            sb << i->GetSourceId() << ";";
        }
        return sb;
    }

    std::shared_ptr<IDataSource> GetFirstNotStartedSourceOptional() const {
        if (SortedSources.empty()) {
            return nullptr;
        }
        return SortedSources.front();
    }

    void EraseNotStartedSources() {
        SortedSources.clear();
    }

    void AppendSource(const std::shared_ptr<IDataSource>& source) {
        SortedSources.emplace_back(source);
    }

    std::vector<std::shared_ptr<IDataSource>> ExtractSourcesWithResult();

    void Abort() {
        for (const auto& source : SortedSources) {
            source->Abort();
        }
        for (const auto& source : FetchingSources) {
            source->Abort();
        }
        SortedSources.clear();
        FetchingSources.clear();
        AFL_VERIFY(IsFinished());
    }

    bool IsFinished() const {
        return FetchingSources.empty() && SortedSources.empty();
    }
};

class TSourceFetchingScheduler: public ISourceFetchingScheduler {
private:
    TMutex Mutex;
    TSourceFetchingSchedulerImpl Impl;

    void DoSetUnblocked(const ui32 sourceIdx) override {
        TGuard g(Mutex);
        Impl.SetUnblocked(sourceIdx);
    }

    void DoSetBlocked(const ui32 sourceIdx) override {
        TGuard g(Mutex);
        Impl.SetBlocked(sourceIdx);
    }

public:
    std::shared_ptr<IDataSource> ScheduleNextSource() {
        TGuard g(Mutex);
        return Impl.ScheduleNextSource();
    }

    void SetInFlightLimit(const ui64 limit) {
        TGuard g(Mutex);
        return Impl.SetInFlightLimit(limit);
    }

    ui64 GetInFlightLimit() const {
        TGuard g(Mutex);
        return Impl.GetInFlightLimit();
    }

    TString DebugString() const {
        TGuard g(Mutex);
        return Impl.DebugString();
    }

    std::shared_ptr<IDataSource> GetFirstNotStartedSourceOptional() const {
        TGuard g(Mutex);
        return Impl.GetFirstNotStartedSourceOptional();
    }

    void EraseNotStartedSources() {
        TGuard g(Mutex);
        return Impl.EraseNotStartedSources();
    }

    void AppendSource(const std::shared_ptr<IDataSource>& source) {
        TGuard g(Mutex);
        return Impl.AppendSource(source);
    }

    std::vector<std::shared_ptr<IDataSource>> ExtractSourcesWithResult() {
        TGuard g(Mutex);
        return Impl.ExtractSourcesWithResult();
    }

    void Abort() {
        TGuard g(Mutex);
        return Impl.Abort();
    }

    bool IsFinished() const {
        TGuard g(Mutex);
        return Impl.IsFinished();
    }
};

class TScanHead {
private:
    std::shared_ptr<TSpecialReadContext> Context;
    std::shared_ptr<TSourceFetchingScheduler> Scheduler;
    std::vector<std::shared_ptr<IDataSource>> UninitializedSources;
    std::set<std::shared_ptr<IDataSource>, IDataSource::TCompareFinishForScanSequence> FinishedSources;
    std::set<std::shared_ptr<IDataSource>, IDataSource::TCompareFinishForScanSequence> FetchingInFlightSources;
    TPositiveControlInteger IntervalsInFlightCount;
    ui64 FetchedCount = 0;
    ui64 MaxInFlight = 256;

    ui32 GetInFlightIntervalsCount() const;

public:
    ~TScanHead();

    bool IsReverse() const;
    void Abort();

    bool IsFinished() const {
        return Scheduler->IsFinished() && UninitializedSources.empty();
    }

    const TReadContext& GetContext() const;

    TString DebugString() const {
        return Scheduler->DebugString();
    }

    void OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& table, const ui32 startIndex,
        const ui32 recordsCount, TPlainReadData& reader);

    TConclusionStatus Start();

    TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context);

    [[nodiscard]] TConclusion<bool> BuildNextInterval();
};
}
