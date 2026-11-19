#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/column_cache_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/cursor_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/fetching_scope_factory.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/memory_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/read_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/read_report_finalizer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <arrow/api.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/base.h>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

class TArrayWithoutRawSize: public NArrow::NAccessor::TTrivialArray {
    std::optional<ui64> DoGetRawSize() const override {
        return std::nullopt;
    }

public:
    explicit TArrayWithoutRawSize(const std::shared_ptr<arrow::Array>& array)
        : TTrivialArray(array)
    {
    }
};

NColumnShardStats::TScanIdentity Identity() {
    NColumnShardStats::TScanIdentity identity;
    identity.SetTabletId(42);
    identity.SetScanAttemptId("attempt");
    return identity;
}

class TEnvironment {
public:
    std::optional<TScanStats> Collector{ std::in_place, Identity() };
    std::shared_ptr<NColumnShardStats::TReport> Frozen;
    std::vector<TStatsUpdate> Updates;
    std::unique_ptr<TLocalStats> Root = TLocalStats::Create(Identity(), MakeOutput(), [this](TStatsUpdate&& update) {
        Updates.push_back(std::move(update));
    });

    TEnvironment() {
        Root->AddChild(Root->GetRoot(), MakeSend());
        Root->AddChild(Root->GetRoot(), MakeAck());
    }

    TOperationNode* GetSendNode() const {
        return Root->GetRoot().FindChild("Send");
    }

    TOperationNode* GetAckNode() const {
        return Root->GetRoot().FindChild("Ack");
    }

    bool Apply(TStatsUpdate update) {
        return Collector && Collector->Apply(std::move(update));
    }

    std::shared_ptr<NColumnShardStats::TReport> Report() {
        if (Frozen) {
            return Frozen;
        }
        auto report = Collector->MakeReport();
        if (report) {
            TReadReportFinalizer(*report).Finalize();
        }
        return report;
    }

    std::shared_ptr<NColumnShardStats::TReport> Finish(NColumnShardStats::EExecutionOutcome outcome) {
        Frozen = std::move(*Collector).Finish(outcome);
        Collector.reset();
        if (Frozen) {
            TReadReportFinalizer(*Frozen).Finalize();
        }
        return Frozen;
    }

    void Deliver() {
        for (auto& update : Updates) {
            if (Collector) {
                UNIT_ASSERT(Apply(std::move(update)));
            }
        }
        Updates.clear();
    }
};

const NColumnShardStats::TOperation& Send(const NColumnShardStats::TReport& report) {
    UNIT_ASSERT_VALUES_EQUAL(report.OperationsSize(), 1);
    UNIT_ASSERT(report.GetOperations(0).HasOutput());
    UNIT_ASSERT_VALUES_EQUAL(report.GetOperations(0).SubOperationsSize(), 1);
    return report.GetOperations(0).GetSubOperations(0);
}

void SendRows(TOperationScope& send, ui64 rows) {
    send.Observe([&](auto& observation) {
        observation.Metric(&NColumnShardStats::TSendDetails::MutableBatches).SetValue(1);
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetValue(rows);
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetValue(rows);
    });
}

void NoRows(TOperationScope& scope) {
    scope.Observe([](auto& observation) {
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    });
}

struct TStatisticsSourceStub {
    TLocalStats* Local = nullptr;
    mutable ui32 MetadataLookups = 0;

    TLocalStats* GetLocalStats() const {
        return Local;
    }

    const TStatisticsSourceStub& GetExecutionContext() const {
        return *this;
    }

    const TStatisticsSourceStub* GetProgramStats() const {
        return nullptr;
    }

    TOperationNode* GetCurrentNode() const {
        return nullptr;
    }

    TCallHandle GetCurrentStatisticsCall() const {
        return {};
    }

    void SetActiveStatistics(const TCallHandle&) const {
    }

    const TStatisticsSourceStub* GetContext() const {
        ++MetadataLookups;
        return this;
    }

    const TStatisticsSourceStub* GetCommonContext() const {
        return this;
    }

    const TStatisticsSourceStub* GetResolver() const {
        return this;
    }

    TString GetColumnName(ui32 id, bool) const {
        UNIT_ASSERT(id == 10 || id == 20);
        return id == 10 ? "a" : "b";
    }
};

struct TStatisticsCursorStub {
    std::shared_ptr<TOperationScope> Metastep;
    mutable std::shared_ptr<TOperationScope> Step;

    const TStatisticsCursorStub& GetStatistics() const {
        return *this;
    }

    const std::shared_ptr<TOperationScope>& GetMetastepStatistics() const {
        return Metastep;
    }

    const std::shared_ptr<TOperationScope>& GetStepStatistics() const {
        return Step;
    }

    void SetStepStatistics(const std::shared_ptr<TOperationScope>& step) const {
        Step = step;
    }
};

struct TStatisticsFetcherStub {
    ui32 EntityId;
    ui32* Descriptions;

    ui32 GetEntityId() const {
        ++*Descriptions;
        return EntityId;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardOutputSnapshot) {
    Y_UNIT_TEST(ReaderOrderUsesSemanticPhasesAndActualIndexOrder) {
        NColumnShardStats::TReport report;
        report.SetReaderKind(NColumnShardStats::TRIVIAL);
        auto* reader = report.AddOperations();
        *reader = MakeReader();
        auto* aggregation = reader->AddSubOperations();
        aggregation->SetKey("0.Aggregation");
        aggregation->MutableAggregation();
        auto* unknown = reader->AddSubOperations();
        *unknown = MakeOutput();
        unknown->SetKey("0.Unknown");
        unknown->SetName("Output source cut");
        auto* filter = reader->AddSubOperations();
        filter->SetKey("9.Filter");
        filter->MutableFilter();
        const auto addIndex = [&](ui64 id, ui32 order, const TString& key) {
            auto* operation = reader->AddSubOperations();
            operation->SetKey(key);
            operation->MutableIndexFilter()->MutableIndex()->SetIndexId(id);
            auto* check = operation->AddSubOperations();
            check->SetKey("IndexCheck");
            check->MutableIndexCheck()->SetApplicationOrder(order);
        };
        addIndex(10, 1, "1.IndexFilter:10");
        addIndex(2, 0, "1.IndexFilter:2");
        auto* pk = reader->AddSubOperations();
        pk->SetKey("9.PKFilter");
        pk->MutablePKFilter();
        reader->AddSubOperations()->MutableProjection();
        reader->AddSubOperations()->MutableFetchColumns();
        reader->AddSubOperations()->MutableBuildResult();

        TReadReportFinalizer(report).Finalize();
        const auto& operations = report.GetOperations(0).GetSubOperations();
        UNIT_ASSERT_VALUES_EQUAL(operations.size(), 9);
        UNIT_ASSERT(operations.Get(0).HasPKFilter());
        UNIT_ASSERT(operations.Get(1).HasOutput());
        UNIT_ASSERT_VALUES_EQUAL(operations.Get(1).GetName(), "Output source cut");
        UNIT_ASSERT_VALUES_EQUAL(operations.Get(1).GetKey(), "0.Unknown");
        UNIT_ASSERT_VALUES_EQUAL(operations.Get(2).GetIndexFilter().GetIndex().GetIndexId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(operations.Get(2).GetIndexFilter().GetApplicationOrder(), 0);
        UNIT_ASSERT_VALUES_EQUAL(operations.Get(3).GetIndexFilter().GetIndex().GetIndexId(), 10);
        UNIT_ASSERT_VALUES_EQUAL(operations.Get(3).GetIndexFilter().GetApplicationOrder(), 1);
        UNIT_ASSERT(operations.Get(4).HasFilter());
        UNIT_ASSERT(operations.Get(5).HasFetchColumns());
        UNIT_ASSERT(operations.Get(6).HasAggregation());
        UNIT_ASSERT_VALUES_EQUAL(operations.Get(6).GetKey(), "0.Aggregation");
        UNIT_ASSERT(operations.Get(7).HasProjection());
        UNIT_ASSERT(operations.Get(8).HasBuildResult());
        UNIT_ASSERT(report.GetReaderKind() == NColumnShardStats::TRIVIAL);
    }

    Y_UNIT_TEST(PreparationRollsUpDisjointPhasesAndKeepsOpenPhaseUnfinished) {
        TScanStats collector{ Identity() };
        std::vector<TStatsUpdate> updates;
        auto stats = TLocalStats::Create(Identity(), MakePreparation("/Root/table"), [&](TStatsUpdate&& update) {
            updates.push_back(std::move(update));
        });
        UNIT_ASSERT(stats);
        auto* prepare = stats->GetRoot().AddChild(MakePrepareRead());
        UNIT_ASSERT(prepare);
        {
            auto phase = prepare->Measure();
            auto* parse = prepare->AddChild(MakeParseProgram());
            UNIT_ASSERT(parse);
            {
                auto call = parse->Measure();
                Sleep(TDuration::MilliSeconds(1));
            }
            auto* metadata = prepare->AddChild(MakePrepareReadMetadata());
            UNIT_ASSERT(metadata);
            auto call = metadata->Measure();
            call.Observe([](auto& observation) {
                observation.Metric(&NColumnShardStats::TPrepareReadMetadataDetails::MutableSelectedPortions).SetValue(3);
                observation.Metric(&NColumnShardStats::TPrepareReadMetadataDetails::MutableSelectedSources).SetValue(4);
            });
        }
        Sleep(TDuration::MilliSeconds(1));
        auto* init = stats->GetRoot().AddChild(MakeReaderInit());
        UNIT_ASSERT(init);
        auto initCall = init->Measure();
        stats->PublishTo([&](TStatsUpdate&& update) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        });
        auto pending = collector.MakeReport();
        UNIT_ASSERT(pending);
        TReadReportFinalizer(*pending).Finalize();
        UNIT_ASSERT_VALUES_EQUAL(pending->OperationsSize(), 1);
        const auto& pendingPreparation = pending->GetOperations(0);
        UNIT_ASSERT(!pendingPreparation.HasCallsCount());
        UNIT_ASSERT(pendingPreparation.HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(pendingPreparation.GetUnfinished().GetCallsCount(), 1);
        const auto& pendingPhases = pendingPreparation.GetSubOperations();
        const auto pendingExpected = pendingPhases.Get(0).GetStatistics().GetActiveTimeUs().GetValue() +
                                     pendingPhases.Get(1).GetUnfinished().GetStatistics().GetActiveTimeUs().GetValue();
        UNIT_ASSERT_VALUES_EQUAL(pendingPreparation.GetUnfinished().GetStatistics().GetActiveTimeUs().GetValue(), pendingExpected);
        UNIT_ASSERT_VALUES_EQUAL(pendingPreparation.GetUnfinished().GetStatistics().GetActiveTimeUs().GetCoverage().GetMeasured(), 1);
        UNIT_ASSERT_VALUES_EQUAL(pendingPreparation.GetUnfinished().GetStatistics().GetActiveTimeUs().GetCoverage().GetApplicable(), 1);
        initCall.Finish();
        stats->Close();
        for (auto& update : updates) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        }
        const auto report = collector.MakeReport();
        TReadReportFinalizer(*report).Finalize();
        UNIT_ASSERT(report);
        const auto& preparation = report->GetOperations(0);
        UNIT_ASSERT(!preparation.HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetCallsCount(), 1);
        const auto& phases = preparation.GetSubOperations();
        const auto expected =
            phases.Get(0).GetStatistics().GetActiveTimeUs().GetValue() + phases.Get(1).GetStatistics().GetActiveTimeUs().GetValue();
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetStatistics().GetActiveTimeUs().GetValue(), expected);
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetStatistics().GetActiveTimeUs().GetCoverage().GetMeasured(), 1);
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetStatistics().GetActiveTimeUs().GetCoverage().GetApplicable(), 1);
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetStatistics().GetWaitTimeUs().GetValue(), 0);
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetStatistics().GetWaitTimeUs().GetCoverage().GetMeasured(), 1);
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetStatistics().GetWaitTimeUs().GetCoverage().GetApplicable(), 1);
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetPreparation().GetSelectedPortions().GetValue(), 3);
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetPreparation().GetSelectedSources().GetValue(), 4);
        UNIT_ASSERT(phases.Get(0).GetSubOperations(0).GetStatistics().GetActiveTimeUs().GetValue() > 0);
    }

    Y_UNIT_TEST(PreparationMissingPhaseTimeKeepsKnownChildMeasurements) {
        TScanStats collector{ Identity() };
        std::vector<TStatsUpdate> updates;
        auto stats = TLocalStats::Create(Identity(), MakePreparation("/Root/table"), [&](TStatsUpdate&& update) {
            updates.push_back(std::move(update));
        });
        UNIT_ASSERT(stats);
        auto* prepare = stats->GetRoot().AddChild(MakePrepareRead());
        UNIT_ASSERT(prepare);
        prepare->Measure().Finish();
        prepare->Measure().FinishWithoutTime();
        auto* init = stats->GetRoot().AddChild(MakeReaderInit());
        UNIT_ASSERT(init);
        init->Measure().Finish();
        stats->Close();
        for (auto& update : updates) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        }
        const auto report = collector.MakeReport();
        TReadReportFinalizer(*report).Finalize();
        UNIT_ASSERT(report);
        const auto& preparation = report->GetOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetCallsCount(), 1);
        for (const auto* metric : { &preparation.GetStatistics().GetActiveTimeUs(), &preparation.GetStatistics().GetWaitTimeUs() }) {
            UNIT_ASSERT(!metric->HasValue());
            UNIT_ASSERT_VALUES_EQUAL(metric->GetCoverage().GetMeasured(), 0);
            UNIT_ASSERT_VALUES_EQUAL(metric->GetCoverage().GetApplicable(), 1);
            UNIT_ASSERT(metric->GetCoverage().GetMissingReason() == NColumnShardStats::UNAVAILABLE);
        }
        const auto& prepareRead = preparation.GetSubOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(prepareRead.GetCallsCount(), 2);
        const auto& childTime = prepareRead.GetStatistics().GetActiveTimeUs();
        UNIT_ASSERT(childTime.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(childTime.GetCoverage().GetMeasured(), 1);
        UNIT_ASSERT_VALUES_EQUAL(childTime.GetCoverage().GetApplicable(), 2);
        UNIT_ASSERT(childTime.GetCoverage().GetMissingReason() == NColumnShardStats::UNAVAILABLE);
    }

    Y_UNIT_TEST(PreparationKeepsNestedSelectionMeasurements) {
        TScanStats collector{ Identity() };
        std::vector<TStatsUpdate> updates;
        auto stats = TLocalStats::Create(Identity(), MakePreparation("/Root/table"), [&](TStatsUpdate&& update) {
            updates.push_back(std::move(update));
        });
        UNIT_ASSERT(stats);
        auto* prepare = stats->AddChild(stats->GetRoot(), MakePrepareRead());
        UNIT_ASSERT(prepare);
        auto* metadata = stats->AddChild(*prepare, MakePrepareReadMetadata());
        UNIT_ASSERT(metadata);
        auto* selection = stats->AddChild(*metadata, MakeSelectPortions());
        UNIT_ASSERT(selection);
        for (ui64 portions : { 1, 2 }) {
            auto call = stats->Measure(selection);
            call.Observe([&](auto& observation) {
                observation.Metric(&NColumnShardStats::TSelectPortionsDetails::MutableSelectedPortions).SetValue(portions);
                observation.Metric(&NColumnShardStats::TSelectPortionsDetails::MutableSelectedRows).SetValue(portions * 10);
                observation.Metric(&NColumnShardStats::TSelectPortionsDetails::MutableSelectedBlobBytes).SetValue(portions * 100);
            });
        }
        stats->Close();
        for (auto& update : updates) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        }
        const auto report = collector.MakeReport();
        TReadReportFinalizer(*report).Finalize();
        UNIT_ASSERT(report);
        UNIT_ASSERT_VALUES_EQUAL(report->OperationsSize(), 1);
        const auto& preparation = report->GetOperations(0);
        UNIT_ASSERT(preparation.HasPreparation());
        UNIT_ASSERT_VALUES_EQUAL(preparation.GetPreparation().GetTablePath(), "/Root/table");
        const auto& prepareRead = preparation.GetSubOperations(0);
        UNIT_ASSERT(prepareRead.HasPrepareRead());
        const auto& readMetadata = prepareRead.GetSubOperations(0);
        UNIT_ASSERT(readMetadata.HasPrepareReadMetadata());
        const auto& select = readMetadata.GetSubOperations(0);
        UNIT_ASSERT(select.HasSelectPortions());
        UNIT_ASSERT_VALUES_EQUAL(select.GetCallsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(select.GetSelectPortions().GetSelectedPortions().GetValue(), 3);
        UNIT_ASSERT_VALUES_EQUAL(select.GetSelectPortions().GetSelectedRows().GetValue(), 30);
        UNIT_ASSERT_VALUES_EQUAL(select.GetSelectPortions().GetSelectedBlobBytes().GetValue(), 300);
        UNIT_ASSERT_VALUES_EQUAL(select.GetSelectPortions().GetSelectedBlobBytes().GetCoverage().GetMeasured(), 2);
    }

    Y_UNIT_TEST(MemoryReservationHasOnlyAllocationMetrics) {
        TScanStats collector{ Identity() };
        auto local = TLocalStats::Create(Identity(), MakeReader());
        TStatisticsSourceStub source{ local.get() };
        TStatisticsCursorStub cursor;
        auto* filter = local->AddChild(local->GetRoot(), MakeMetastepStatisticsDefinition(NColumnShardStats::TOperation::kFilter));
        cursor.Metastep = std::make_shared<TOperationScope>(local->Measure(filter));

        auto scoped = TMemoryReservationObserver::ForSource(source, cursor, ui32(2));
        UNIT_ASSERT(scoped.GetCall());
        UNIT_ASSERT(filter->FindChild("Memory:2") == scoped.GetCall().GetNode());
        scoped.OnRequested(0);
        scoped.OnAllocated(0);

        for (const auto bytes : { ui64(200), ui64(300) }) {
            auto shared = TMemoryReservationObserver::ForSource(source, cursor, ui32(2), true);
            UNIT_ASSERT(shared.GetCall());
            UNIT_ASSERT(local->GetRoot().FindChild("Memory:2") == shared.GetCall().GetNode());
            shared.OnRequested(bytes);
            shared.OnAllocated(bytes - 50);
        }
        cursor.Metastep->Finish();
        local->PublishTo([&](TStatsUpdate&& update) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        });
        const auto report = collector.MakeReport();
        const NColumnShardStats::TOperation* memory = nullptr;
        for (const auto& operation : report->GetOperations(0).GetSubOperations()) {
            if (operation.HasReserveMemory()) {
                UNIT_ASSERT(!memory);
                memory = &operation;
            }
        }
        UNIT_ASSERT(memory);
        UNIT_ASSERT_VALUES_EQUAL(memory->GetCallsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(memory->GetReserveMemory().GetRequestedBytes().GetValue(), 500);
        UNIT_ASSERT_VALUES_EQUAL(memory->GetReserveMemory().GetQuotaBytes().GetValue(), 400);
    }

    Y_UNIT_TEST(MemoryReservationFailureKeepsRequestedBytesAndZeroQuota) {
        TScanStats collector{ Identity() };
        auto local = TLocalStats::Create(Identity(), MakeReader());
        TStatisticsSourceStub source{ local.get() };
        TStatisticsCursorStub cursor;
        auto observer = TMemoryReservationObserver::ForSource(source, cursor, ui32(3));
        observer.OnRequested(100);
        observer.OnAllocationImpossible();
        local->PublishTo([&](TStatsUpdate&& update) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        });
        const auto report = collector.MakeReport();
        const auto& operation = report->GetOperations(0).GetSubOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetReserveMemory().GetRequestedBytes().GetValue(), 100);
        UNIT_ASSERT(operation.GetReserveMemory().GetQuotaBytes().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(operation.GetReserveMemory().GetQuotaBytes().GetValue(), 0);

        TStatisticsSourceStub disabled{ nullptr };
        auto absent = TMemoryReservationObserver::ForSource(disabled, cursor, ui32(3));
        absent.OnRequested(100);
        absent.OnAllocated(50);
        UNIT_ASSERT(!absent.GetCall());
    }

    Y_UNIT_TEST(CursorObserverKeepsOneReaderCallAcrossSuspendedTurns) {
        TScanStats collector{ Identity() };
        auto local = TLocalStats::Create(Identity(), MakeReader());
        TStatisticsSourceStub source{ local.get() };
        TCursorObserver observer;
        observer.OnReaderStart(source);
        observer.OnSuspend();
        observer.OnReaderStart(source);
        observer.OnScriptComplete();
        observer.OnNext(false);
        local->PublishTo([&](TStatsUpdate&& update) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        });
        UNIT_ASSERT_VALUES_EQUAL(collector.MakeReport()->GetOperations(0).GetCallsCount(), 1);

        TStatisticsSourceStub disabled{ nullptr };
        TCursorObserver absent;
        absent.OnReaderStart(disabled);
        absent.OnScriptComplete();
        UNIT_ASSERT(!absent.GetReaderStatistics());
    }

    Y_UNIT_TEST(SourceRowsObserverDistinguishesZeroFromUnknownFilterResult) {
        struct TSource {
            bool AllowAll = true;
            std::optional<ui64> Rows;

            bool HasStageData() const {
                return true;
            }

            const TSource& GetStageData() const {
                return *this;
            }

            const TSource& GetTable() const {
                return *this;
            }

            const TSource& GetFilter() const {
                return *this;
            }

            bool GetFilterUsage() const {
                return false;
            }

            bool IsTotalAllowFilter() const {
                return AllowAll;
            }

            std::optional<ui64> GetFilteredCountIfKnown() const {
                return std::nullopt;
            }

            std::optional<ui64> GetRecordsCountActualOptional() const {
                return Rows;
            }
        };

        TScanStats collector{ Identity() };
        auto local = TLocalStats::Create(Identity(), MakeReader());
        const auto measure = [&](TStringBuf key, const TSource& source) {
            auto definition = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kFilter, key);
            definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
            auto scope = local->Measure(local->AddChild(local->GetRoot(), definition));
            TSourceRowsObserver(source).OnInputRows(scope);
            scope.Finish();
        };
        measure("zero", TSource{ true, ui64(0) });
        measure("unknown", TSource{ false, ui64(100) });
        local->PublishTo([&](TStatsUpdate&& update) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        });
        const auto report = collector.MakeReport();
        for (const auto& operation : report->GetOperations(0).GetSubOperations()) {
            const auto& rows = operation.GetStatistics().GetInputRows();
            if (operation.GetKey() == "zero") {
                UNIT_ASSERT(rows.HasValue());
                UNIT_ASSERT_VALUES_EQUAL(rows.GetValue(), 0);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(operation.GetKey(), "unknown");
                UNIT_ASSERT(!rows.HasValue());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).GetSubOperations().size(), 2);
    }

    Y_UNIT_TEST(ColumnCacheReportsHitAndUnknownProviderOriginSeparately) {
        TEnvironment env;
        auto observer = MakeColumnCacheObserver(env.Root->Fork(MakeReader()));
        UNIT_ASSERT(observer);
        env.Deliver();
        UNIT_ASSERT_VALUES_EQUAL(env.Report()->OperationsSize(), 0);
        auto array = NArrow::NAccessor::TTrivialArray::BuildArrayFromScalar(std::make_shared<arrow::UInt64Scalar>(42));
        auto column = std::make_shared<NArrow::NAccessor::TTrivialArray>(array);
        const auto bytes = column->GetRawSizeVerified();
        const TPortionAddress portion(TInternalPathId::FromRawValue(1), 1);
        observer->OnStarted();
        observer->OnCacheHit(NGeneralCache::TGlobalColumnAddress(NActors::TActorId(1, 42), portion, 1), column);
        observer->OnRetrieved(NGeneralCache::TGlobalColumnAddress(NActors::TActorId(1, 42), portion, 2), column);
        observer->OnFinished();
        env.Deliver();
        const auto report = env.Report();
        UNIT_ASSERT_VALUES_EQUAL(report->DataSourcesSize(), 2);
        bool cached = false;
        bool retrieved = false;
        for (const auto& source : report->GetDataSources()) {
            UNIT_ASSERT(source.HasKind());
            cached |= source.GetKind() == NColumnShardStats::COLUMN_CACHE;
            retrieved |= source.GetKind() == NColumnShardStats::SOURCE_UNKNOWN;
            UNIT_ASSERT_VALUES_EQUAL(source.GetStorageId(), NGeneralCache::TColumnDataCachePolicy::GetCacheName());
            UNIT_ASSERT_VALUES_EQUAL(source.GetReads(), 1);
            UNIT_ASSERT(source.GetReadBytes().HasValue());
            UNIT_ASSERT_VALUES_EQUAL(source.GetReadBytes().GetValue(), bytes);
            UNIT_ASSERT(source.GetReadBytes().GetBoundary() == NColumnShardStats::LOGICAL_DELIVERY);
            UNIT_ASSERT(source.GetReadBytes().GetRepresentation() == NColumnShardStats::ARROW_LOGICAL);
        }
        UNIT_ASSERT(cached && retrieved);
    }

    Y_UNIT_TEST(ColumnCacheUnknownSizeRemainsUnmeasured) {
        TEnvironment env;
        auto observer = MakeColumnCacheObserver(env.Root->Fork(MakeReader()));
        auto array = NArrow::NAccessor::TTrivialArray::BuildArrayFromScalar(std::make_shared<arrow::UInt64Scalar>(42));
        auto column = std::make_shared<TArrayWithoutRawSize>(array);
        observer->OnStarted();
        observer->OnRetrieved(
            NGeneralCache::TGlobalColumnAddress(NActors::TActorId(1, 42), TPortionAddress(TInternalPathId::FromRawValue(1), 1), 1), column);
        observer->OnFinished();
        env.Deliver();
        const auto report = env.Report();
        UNIT_ASSERT_VALUES_EQUAL(report->DataSourcesSize(), 1);
        const auto& source = report->GetDataSources(0);
        UNIT_ASSERT(source.HasKind());
        UNIT_ASSERT(source.GetKind() == NColumnShardStats::SOURCE_UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(source.GetReads(), 1);
        UNIT_ASSERT(!source.GetReadBytes().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(source.GetVolumeCoverage().GetMeasured(), 0);
        UNIT_ASSERT_VALUES_EQUAL(source.GetVolumeCoverage().GetApplicable(), 1);
    }

    Y_UNIT_TEST(OutputCoordinationKeepsReadyCountsAndIntervalLimitHitsSeparate) {
        TEnvironment env;
        auto* readySources = env.Root->AddChild(env.Root->GetRoot(), MakeReadySources());
        auto* readyIntervals = env.Root->AddChild(env.Root->GetRoot(), MakeReadyIntervals());
        for (ui32 call = 0; call < 2; ++call) {
            auto output = env.Root->Measure(&env.Root->GetRoot());
            NoRows(output);
            auto stats = env.Root->Measure(readySources);
            stats.Observe([](auto& observation) {
                observation.Metric(&NColumnShardStats::TReadyDetails::MutableSources).SetValue(1);
            });
        }
        {
            auto output = env.Root->Measure(&env.Root->GetRoot());
            NoRows(output);
            auto stats = env.Root->Measure(readyIntervals);
            stats.Observe([](auto& observation) {
                observation.Metric(&NColumnShardStats::TReadyDetails::MutableIntervals).SetValue(1);
            });
        }
        env.Deliver();
        const auto unrelated = env.Report();
        const auto& unmeasuredHits = unrelated->GetOperations(0).GetOutput().GetIntervalLimitHits();
        UNIT_ASSERT(!unmeasuredHits.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(unmeasuredHits.GetCoverage().GetApplicable(), 0);
        for (ui64 hits : { 0u, 1u, 2u }) {
            auto output = env.Root->Measure(&env.Root->GetRoot());
            NoRows(output);
            output.Observe([&](auto& observation) {
                observation.Metric(&NColumnShardStats::TOutputDetails::MutableIntervalLimitHits).SetApplicability(NColumnShardStats::APPLICABLE);
                observation.Metric(&NColumnShardStats::TOutputDetails::MutableIntervalLimitHits).SetValue(hits);
            });
        }
        env.Deliver();
        const auto report = env.Report();
        UNIT_ASSERT_VALUES_EQUAL(report->OperationsSize(), 1);
        const auto& output = report->GetOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(output.SubOperationsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(output.GetCallsCount(), 6);
        UNIT_ASSERT(!output.GetStatistics().GetInputRows().HasValue());
        UNIT_ASSERT(!output.GetStatistics().GetOutputRows().HasValue());
        const auto& hits = output.GetOutput().GetIntervalLimitHits();
        UNIT_ASSERT_VALUES_EQUAL(hits.GetValue(), 3);
        UNIT_ASSERT_VALUES_EQUAL(hits.GetCoverage().GetMeasured(), 3);
        UNIT_ASSERT_VALUES_EQUAL(hits.GetCoverage().GetApplicable(), 3);
        ui32 sources = 0;
        ui32 intervals = 0;
        for (const auto& operation : output.GetSubOperations()) {
            UNIT_ASSERT(!operation.GetStatistics().GetInputRows().HasValue());
            UNIT_ASSERT(!operation.GetStatistics().GetOutputRows().HasValue());
            UNIT_ASSERT(operation.GetStatistics().GetInputRows().GetApplicability() == NColumnShardStats::NOT_APPLICABLE);
            UNIT_ASSERT(operation.GetStatistics().GetOutputRows().GetApplicability() == NColumnShardStats::NOT_APPLICABLE);
            if (operation.GetReady().GetSources().HasValue()) {
                ++sources;
                UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(operation.GetReady().GetSources().GetValue(), 2);
                UNIT_ASSERT(!operation.GetReady().GetIntervals().HasValue());
            } else {
                ++intervals;
                UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(operation.GetReady().GetIntervals().GetValue(), 1);
                UNIT_ASSERT(!operation.GetReady().GetSources().HasValue());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(sources, 1);
        UNIT_ASSERT_VALUES_EQUAL(intervals, 1);
    }

    Y_UNIT_TEST(ActorOutputFlushDoesNotCollectOrWaitForWorkerState) {
        TEnvironment env;
        auto worker = env.Root->Fork(MakeReader());
        auto workerCall = worker->Measure(&worker->GetRoot());
        auto output = env.Root->Measure(&env.Root->GetRoot());
        auto send = env.Root->Measure(env.GetSendNode());
        SendRows(send, 7);
        send.Finish();
        output.Finish();
        bool applied = false;
        env.Root->PublishTo([&](TStatsUpdate&& update) {
            applied = env.Apply(std::move(update));
        });
        UNIT_ASSERT(applied);
        const auto snapshot = env.Finish(NColumnShardStats::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->OperationsSize(), 1);
        UNIT_ASSERT(snapshot->GetOperations(0).HasOutput());
        UNIT_ASSERT_VALUES_EQUAL(snapshot->GetOperations(0).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Send(*snapshot).GetStatistics().GetOutputRows().GetValue(), 7);
        UNIT_ASSERT(workerCall);
        workerCall.Finish();
        worker->Close();
        for (auto& update : env.Updates) {
            UNIT_ASSERT(!env.Apply(std::move(update)));
        }
        UNIT_ASSERT(env.Report() == snapshot);
    }

    Y_UNIT_TEST(OutputRowCoverageExcludesCoordinationCalls) {
        for (bool separateOwner : { false, true }) {
            TEnvironment env;
            auto coordinator = separateOwner ? env.Root->Fork() : nullptr;
            {
                auto output = env.Root->Measure(&env.Root->GetRoot());
                output.Observe([](auto& observation) {
                    auto* rows = &observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows);
                    rows->SetApplicability(NColumnShardStats::APPLICABLE);
                    rows->SetValue(9);
                });
            }
            {
                auto& local = coordinator ? *coordinator : *env.Root;
                auto output = local.Measure(&local.GetRoot());
                NoRows(output);
            }
            coordinator.reset();
            env.Deliver();
            const auto report = env.Report();
            UNIT_ASSERT_VALUES_EQUAL(report->OperationsSize(), 1);
            const auto& output = report->GetOperations(0);
            UNIT_ASSERT_VALUES_EQUAL(output.GetCallsCount(), 2);
            const auto& rows = output.GetStatistics().GetOutputRows();
            UNIT_ASSERT_VALUES_EQUAL(rows.GetValue(), 9);
            UNIT_ASSERT_VALUES_EQUAL(rows.GetCoverage().GetMeasured(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rows.GetCoverage().GetApplicable(), 1);
        }
    }

    Y_UNIT_TEST(OutputChildrenKeepSendAndAckMetricsSeparate) {
        TEnvironment env;
        UNIT_ASSERT(env.Root->GetRoot().GetDefinition().HasOutput());
        auto send = env.Root->Measure(env.GetSendNode());
        SendRows(send, 7);
        send.Finish();
        auto ack = env.Root->Measure(env.GetAckNode());
        ack.Observe([](auto& observation) {
            observation.Metric(&NColumnShardStats::TAckDetails::MutableGrantedBytes).SetValue(4096);
            observation.Metric(&NColumnShardStats::TAckDetails::MutableGrantedBatches).SetValue(1);
        });
        ack.Finish();
        env.Deliver();
        const auto report = env.Report();
        UNIT_ASSERT_VALUES_EQUAL(report->OperationsSize(), 1);
        const auto& output = report->GetOperations(0);
        UNIT_ASSERT(output.HasOutput());
        UNIT_ASSERT_VALUES_EQUAL(output.SubOperationsSize(), 2);
        bool hasSend = false;
        bool hasAck = false;
        for (const auto& operation : output.GetSubOperations()) {
            UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 1);
            UNIT_ASSERT(!operation.HasUnfinished());
            if (operation.HasSend()) {
                hasSend = true;
                UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetValue(), 7);
                UNIT_ASSERT(operation.GetStatistics().GetOutputRows().GetApplicability() == NColumnShardStats::APPLICABLE);
            } else if (operation.HasAck()) {
                hasAck = true;
                UNIT_ASSERT_VALUES_EQUAL(operation.GetAck().GetGrantedBytes().GetValue(), 4096);
                UNIT_ASSERT(!operation.GetStatistics().GetInputRows().HasValue());
                UNIT_ASSERT(!operation.GetStatistics().GetOutputRows().HasValue());
                UNIT_ASSERT(operation.GetStatistics().GetInputRows().GetApplicability() == NColumnShardStats::NOT_APPLICABLE);
                UNIT_ASSERT(operation.GetStatistics().GetOutputRows().GetApplicability() == NColumnShardStats::NOT_APPLICABLE);
            }
        }
        UNIT_ASSERT(hasSend && hasAck);
    }

    Y_UNIT_TEST(LastSendEstimateIsAddedToTotalTimeWithoutChangingCoverage) {
        TEnvironment env;
        for (ui32 i = 0; i < 2; ++i) {
            auto send = env.Root->Measure(env.GetSendNode());
            SendRows(send, 10);
            send.Finish();
        }
        env.Deliver();
        const auto prior = env.Report();
        const auto measured = Send(*prior).GetStatistics().GetActiveTimeUs().GetValue();
        auto last = env.Root->Measure(env.GetSendNode());
        SendRows(last, 0);
        last.FinishWithoutTime();
        env.Deliver();
        const auto report = env.Finish(NColumnShardStats::SUCCESS);
        const auto& send = Send(*report);
        UNIT_ASSERT_VALUES_EQUAL(send.GetCallsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(send.GetSend().GetBatches().GetValue(), 3);
        UNIT_ASSERT_VALUES_EQUAL(send.GetStatistics().GetOutputRows().GetValue(), 20);
        UNIT_ASSERT_VALUES_EQUAL(send.GetStatistics().GetActiveTimeUs().GetValue(), measured + measured / 2);
        UNIT_ASSERT_VALUES_EQUAL(send.GetStatistics().GetActiveTimeUs().GetCoverage().GetMeasured(), 2);
        UNIT_ASSERT_VALUES_EQUAL(send.GetStatistics().GetActiveTimeUs().GetCoverage().GetApplicable(), 3);
        UNIT_ASSERT(!send.HasUnfinished());
    }

    Y_UNIT_TEST(LastSendMeanIsAddedOnlyForOneUnmeasuredCall) {
        for (const ui64 calls : { 2, 3, 4 }) {
            NColumnShardStats::TReport report;
            auto* output = report.AddOperations();
            *output = MakeOutput();
            auto* send = output->AddSubOperations();
            *send = MakeSend();
            send->SetCallsCount(calls);
            auto* active = send->MutableStatistics()->MutableActiveTimeUs();
            active->SetApplicability(NColumnShardStats::APPLICABLE);
            active->SetValue(81);
            active->MutableCoverage()->SetMeasured(2);
            active->MutableCoverage()->SetApplicable(calls);

            TReadReportFinalizer(report).Finalize();

            UNIT_ASSERT_VALUES_EQUAL(active->GetValue(), calls == 3 ? 121 : 81);
            UNIT_ASSERT_VALUES_EQUAL(active->GetCoverage().GetMeasured(), 2);
            UNIT_ASSERT_VALUES_EQUAL(active->GetCoverage().GetApplicable(), calls);
            UNIT_ASSERT(!output->GetStatistics().GetActiveTimeUs().HasValue());
        }
    }

    Y_UNIT_TEST(OnlySendHasNoInventedMean) {
        TEnvironment env;
        auto last = env.Root->Measure(env.GetSendNode());
        SendRows(last, 0);
        last.FinishWithoutTime();
        env.Deliver();
        const auto report = env.Finish(NColumnShardStats::SUCCESS);
        const auto& send = Send(*report);
        UNIT_ASSERT_VALUES_EQUAL(send.GetCallsCount(), 1);
        UNIT_ASSERT(!send.GetStatistics().GetActiveTimeUs().HasValue());
        UNIT_ASSERT(!send.HasUnfinished());
    }

    Y_UNIT_TEST(PendingReadReportsOnlyAlreadyObservedPayload) {
        TEnvironment env;
        auto io = MakeReadObserver(env.Root->Fork(MakeReader()));
        io->OnStart(IStoragesManager::DefaultStorageId, 2);
        io->OnData(IStoragesManager::DefaultStorageId, 100, false);
        env.Deliver();
        const auto snapshot = env.Finish(NColumnShardStats::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->DataSourcesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->GetDataSources(0).GetReads(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->GetDataSources(0).GetReadBytes().GetValue(), 100);
        const auto& reader = snapshot->GetOperations(0);
        const auto& read = reader.GetSubOperations(0);
        UNIT_ASSERT(read.HasStorageRead());
        UNIT_ASSERT(!read.HasCallsCount());
        UNIT_ASSERT_VALUES_EQUAL(read.GetUnfinished().GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(read.GetUnfinished().GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 100);
        const auto frozen = snapshot->SerializeAsString();
        io->OnData(IStoragesManager::DefaultStorageId, 20, true);
        io.reset();
        for (auto& update : env.Updates) {
            UNIT_ASSERT(!env.Apply(std::move(update)));
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Report()->SerializeAsString(), frozen);
    }

    Y_UNIT_TEST(StorageObserversMeasureTheirOwnLeafBelowAStorageReadAncestor) {
        for (const bool metadata : { false, true }) {
            TEnvironment env;
            auto local = env.Root->Fork(MakeReader());
            auto definition = MakeStorageRead();
            if (metadata) {
                definition.SetKey("StorageRead.MetadataCache");
            }
            auto* ancestor = local->AddChild(local->GetRoot(), definition);
            UNIT_ASSERT(ancestor);
            if (metadata) {
                auto observer = MakeMetadataCacheObserver(local->Fork(), ancestor);
                UNIT_ASSERT(observer);
                const NGeneralCache::TGlobalPortionAddress address(
                    NActors::TActorId(1, 42), TPortionAddress(TInternalPathId::FromRawValue(1), 1));
                observer->OnStarted();
                observer->OnCacheHit(address, {});
                observer->OnFinished();
            } else {
                auto observer = MakeReadObserver(local->Fork(), ancestor);
                UNIT_ASSERT(observer);
                observer->OnStart("tier-cold", 1);
                observer->OnStarted();
                observer->OnData("tier-cold", 20, false);
            }
            env.Deliver();
            const auto report = env.Report();
            const auto& parent = report->GetOperations(0).GetSubOperations(0);
            UNIT_ASSERT(!parent.HasCallsCount());
            UNIT_ASSERT_VALUES_EQUAL(parent.SubOperationsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parent.GetSubOperations(0).GetKey(), definition.GetKey());
            UNIT_ASSERT_VALUES_EQUAL(parent.GetSubOperations(0).GetCallsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(report->DataSourcesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(report->GetDataSources(0).GetReads(), 1);
        }
    }

    Y_UNIT_TEST(CompletedIoIsInMainCohort) {
        TEnvironment env;
        auto io = MakeReadObserver(env.Root->Fork(MakeReader()));
        io->OnStart("tier-cold", 1);
        io->OnData("tier-cold", 20, false);
        io.reset();
        env.Deliver();
        const auto snapshot = env.Finish(NColumnShardStats::SUCCESS);
        const auto& read = snapshot->GetOperations(0).GetSubOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(read.GetCallsCount(), 1);
        UNIT_ASSERT(!read.HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(read.GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 20);
    }

    Y_UNIT_TEST(ProviderFetchingSeparatesPayloadsAndDoesNotReuseCursorScope) {
        auto local = TLocalStats::Create(Identity(), MakeReader());
        TStatisticsSourceStub source{ local.get() };
        TStatisticsCursorStub cursor;
        auto* pk = local->AddChild(local->GetRoot(), MakeMetastepStatisticsDefinition(NColumnShardStats::TOperation::kPKFilter));
        cursor.Metastep = std::make_shared<TOperationScope>(local->Measure(pk));
        ui32 descriptions = 0;
        TStatisticsFetcherStub columnA{ 10, &descriptions }, columnB{ 20, &descriptions };
        TStatisticsFetcherStub indexA{ 100, &descriptions }, indexB{ 200, &descriptions };
        const TFetchingScopeFactory factory(source, cursor);
        const auto columns = factory.StartProviderColumns(std::vector{ &columnB, &columnA });
        const auto indexes = factory.StartProviderIndexes(std::vector{ &indexB, &indexA });
        UNIT_ASSERT(columns && *columns);
        UNIT_ASSERT(indexes && *indexes);
        UNIT_ASSERT(columns != indexes);
        UNIT_ASSERT(!cursor.Step);
        UNIT_ASSERT_VALUES_EQUAL(descriptions, 4);
        const auto& columnDetails = columns->GetNode()->GetDefinition().GetFetchingColumns();
        UNIT_ASSERT_VALUES_EQUAL(columnDetails.ColumnsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(columnDetails.GetColumns(0), "a");
        UNIT_ASSERT_VALUES_EQUAL(columnDetails.GetColumns(1), "b");
        const auto& indexDetails = indexes->GetNode()->GetDefinition().GetFetchingIndexes();
        UNIT_ASSERT_VALUES_EQUAL(indexDetails.IndexIdsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(indexDetails.GetIndexIds(0), 100);
        UNIT_ASSERT_VALUES_EQUAL(indexDetails.GetIndexIds(1), 200);
        UNIT_ASSERT(pk->FindChildIf([](const auto& operation) {
            return operation.HasFetchingColumns();
        }) == columns->GetNode());
        UNIT_ASSERT(local->GetRoot().FindChildIf([](const auto& operation) {
            return operation.HasFetchingIndexes();
        }) == indexes->GetNode());
        columns->Finish();
        indexes->Finish();
        cursor.Metastep->Finish();
    }

    Y_UNIT_TEST(DisabledProviderFetchingDoesNotDescribePayloads) {
        TStatisticsSourceStub source;
        TStatisticsCursorStub cursor;
        ui32 descriptions = 0;
        TStatisticsFetcherStub fetcher{ 10, &descriptions };
        const std::vector fetchers{ &fetcher };
        const TFetchingScopeFactory factory(source, cursor);
        UNIT_ASSERT(!factory.StartProviderColumns(fetchers));
        UNIT_ASSERT(!factory.StartProviderIndexes(fetchers));
        UNIT_ASSERT_VALUES_EQUAL(descriptions, 0);
        UNIT_ASSERT_VALUES_EQUAL(source.MetadataLookups, 0);
        UNIT_ASSERT(!cursor.Step);
    }

    Y_UNIT_TEST(ColumnAndIndexFetchingKeepRequestedBytesSeparate) {
        TEnvironment env;
        auto local = env.Root->Fork(MakeReader());
        for (const bool indexes : { false, true }) {
            auto definition =
                MakeStepStatisticsDefinition(indexes ? NColumnShardStats::TOperation::kFetchingIndexes
                                                     : NColumnShardStats::TOperation::kFetchingColumns, indexes ? "Indexes" : "Columns");
            auto* requested = indexes ? definition.MutableFetchingIndexes()->MutableRequestedBytes()
                                      : definition.MutableFetchingColumns()->MutableRequestedBytes();
            requested->SetApplicability(NColumnShardStats::APPLICABLE);
            auto* node = local->AddChild(local->GetRoot(), definition);
            auto fetching = local->Measure(node);
            NoRows(fetching);
            auto io = MakeReadObserver(local->Fork(), node, fetching.GetHandle());
            io->OnRequested("tier-cold", 10);
            io->OnRequested("tier-cold", indexes ? 60 : 20);
            io->OnStart("tier-cold", 1);
            io->OnStarted();
            io->OnStarted();
            io->OnData("tier-cold", indexes ? 60 : 25, false);
            io.reset();
            fetching.Finish();
        }
        local->Publish();
        env.Deliver();
        const auto report = env.Finish(NColumnShardStats::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(report->OperationsSize(), 1);
        const auto& reader = report->GetOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(reader.SubOperationsSize(), 2);
        for (const auto& fetching : reader.GetSubOperations()) {
            UNIT_ASSERT(fetching.HasFetchingColumns() || fetching.HasFetchingIndexes());
            UNIT_ASSERT_VALUES_EQUAL(fetching.GetCallsCount(), 1);
            const ui64 expected = fetching.HasFetchingIndexes() ? 70 : 30;
            const auto& requested = fetching.HasFetchingIndexes() ? fetching.GetFetchingIndexes().GetRequestedBytes()
                                                                  : fetching.GetFetchingColumns().GetRequestedBytes();
            UNIT_ASSERT_VALUES_EQUAL(requested.GetValue(), expected);
            UNIT_ASSERT_VALUES_EQUAL(fetching.SubOperationsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fetching.GetSubOperations(0).GetStorageRead().GetRequestedBytes().GetValue(), expected);
        }
        UNIT_ASSERT_VALUES_EQUAL(report->DataSourcesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(report->GetDataSources(0).GetReads(), 2);
        UNIT_ASSERT_VALUES_EQUAL(report->GetDataSources(0).GetReadBytes().GetValue(), 85);
    }

    Y_UNIT_TEST(PreprovidedReadCompletesWithMemoryAttribution) {
        TEnvironment env;
        auto io = MakeReadObserver(env.Root->Fork(MakeReader()));
        io->OnRequested("tier-cold", 20);
        io->OnPreprovided("tier-cold", 20);
        io->OnStarted();
        env.Deliver();
        const auto snapshot = env.Finish(NColumnShardStats::SUCCESS);
        const auto& read = snapshot->GetOperations(0).GetSubOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(read.GetCallsCount(), 1);
        UNIT_ASSERT(!read.HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(read.GetStorageRead().GetRequestedBytes().GetValue(), 20);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->DataSourcesSize(), 1);
        const auto& source = snapshot->GetDataSources(0);
        UNIT_ASSERT(source.GetKind() == NColumnShardStats::MEMORY);
        UNIT_ASSERT_VALUES_EQUAL(source.GetStorageId(), "tier-cold");
        UNIT_ASSERT_VALUES_EQUAL(source.GetReadBytes().GetValue(), 20);
        UNIT_ASSERT(source.GetReadBytes().GetBoundary() == NColumnShardStats::LOGICAL_DELIVERY);
        UNIT_ASSERT(source.GetReadBytes().GetRepresentation() == NColumnShardStats::ENCODED_PAYLOAD);
    }

    Y_UNIT_TEST(MixedReadKeepsRequestedBytesOutOfDeliveredVolume) {
        TEnvironment env;
        auto io = MakeReadObserver(env.Root->Fork(MakeReader()));
        io->OnRequested("tier-cold", 120);
        io->OnPreprovided("tier-cold", 20);
        io->OnStart("tier-cold", 1);
        io->OnStarted();
        env.Deliver();
        const auto partial = env.Report();
        const auto& unfinished = partial->GetOperations(0).GetSubOperations(0);
        UNIT_ASSERT(!unfinished.HasCallsCount());
        UNIT_ASSERT_VALUES_EQUAL(unfinished.GetUnfinished().GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(partial->GetDataSources(0).GetReadBytes().GetValue(), 20);

        io->OnData("tier-cold", 100, false);
        env.Deliver();
        const auto snapshot = env.Finish(NColumnShardStats::SUCCESS);
        const auto& read = snapshot->GetOperations(0).GetSubOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(read.GetCallsCount(), 1);
        UNIT_ASSERT(!read.HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(read.GetStorageRead().GetRequestedBytes().GetValue(), 120);
        ui64 delivered = 0;
        for (const auto& source : snapshot->GetDataSources()) {
            delivered += source.GetReadBytes().GetValue();
        }
        UNIT_ASSERT_VALUES_EQUAL(delivered, 120);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->DataSourcesSize(), 2);
    }

    Y_UNIT_TEST(MetadataCacheReportsOriginWithoutInventingPayloadSize) {
        TEnvironment env;
        ui32 attributed = 0;
        auto observer = MakeMetadataCacheObserver(env.Root->Fork(MakeReader()), nullptr, [&](const auto&, auto bytes) {
            UNIT_ASSERT(!bytes);
            ++attributed;
            return true;
        });
        UNIT_ASSERT(observer);
        const NGeneralCache::TGlobalPortionAddress address(NActors::TActorId(1, 42), TPortionAddress(TInternalPathId::FromRawValue(1), 1));
        observer->OnStarted();
        observer->OnCacheHit(address, {});
        observer->OnRetrieved(address, {});
        observer->OnFinished();
        env.Deliver();
        const auto snapshot = env.Report();
        UNIT_ASSERT_VALUES_EQUAL(attributed, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->DataSourcesSize(), 2);
        bool cache = false;
        bool provider = false;
        for (const auto& source : snapshot->GetDataSources()) {
            cache |= source.GetKind() == NColumnShardStats::METADATA_CACHE;
            provider |= source.GetKind() == NColumnShardStats::SOURCE_UNKNOWN;
            UNIT_ASSERT_VALUES_EQUAL(source.GetStorageId(), NGeneralCache::TPortionsMetadataCachePolicy::GetCacheName());
            UNIT_ASSERT_VALUES_EQUAL(source.GetReads(), 1);
            UNIT_ASSERT(!source.GetReadBytes().HasValue());
            UNIT_ASSERT(source.GetReadBytes().GetRepresentation() == NColumnShardStats::METADATA_FOOTPRINT);
            UNIT_ASSERT_VALUES_EQUAL(source.GetVolumeCoverage().GetMeasured(), 0);
            UNIT_ASSERT_VALUES_EQUAL(source.GetVolumeCoverage().GetApplicable(), 1);
        }
        UNIT_ASSERT(cache && provider);
        UNIT_ASSERT(!snapshot->GetOperations(0).HasCallsCount());
        UNIT_ASSERT_VALUES_EQUAL(snapshot->GetOperations(0).GetSubOperations(0).GetCallsCount(), 1);
    }

    Y_UNIT_TEST(IoAttributionFollowsTheLiveConsumerAndKeepsCanonicalBytesOnce) {
        TEnvironment env;
        auto local = env.Root->Fork(MakeReader());
        auto consumer = local->Measure(&local->GetRoot());
        auto io = MakeReadObserver(local->Fork(), &local->GetRoot(), {}, [call = consumer.GetHandle()](const auto& source, auto bytes) {
            return call.AttributeRead(source, bytes);
        });
        UNIT_ASSERT(io);
        io->OnPreprovided("memory", 20);
        io->OnStarted();
        env.Deliver();
        {
            const auto snapshot = env.Report();
            const auto& reader = snapshot->GetOperations(0);
            UNIT_ASSERT_VALUES_EQUAL(snapshot->GetDataSources(0).GetReads(), 1);
            UNIT_ASSERT_VALUES_EQUAL(snapshot->GetDataSources(0).GetReadBytes().GetValue(), 20);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetStatistics().DataSourcesSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetUnfinished().GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 20);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetSubOperations(0).GetCallsCount(), 1);
        }
        consumer.Finish();
        env.Deliver();
        const auto snapshot = env.Report();
        UNIT_ASSERT_VALUES_EQUAL(snapshot->GetDataSources(0).GetReadBytes().GetValue(), 20);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->GetOperations(0).GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 20);
        UNIT_ASSERT(!snapshot->GetOperations(0).HasUnfinished());
    }

    Y_UNIT_TEST(LateIoDoesNotWriteToAHandoffConsumer) {
        TEnvironment env;
        auto local = env.Root->Fork(MakeReader());
        auto consumer = local->Measure(&local->GetRoot());
        bool leaseHeld = false;
        bool attributed = false;
        auto io = MakeReadObserver(local->Fork(), &local->GetRoot(), {}, [&](const auto& source, auto bytes) {
            if (!leaseHeld) {
                return false;
            }
            attributed = true;
            consumer.AttributeRead(source, bytes);
            return true;
        });
        UNIT_ASSERT(io);
        io->OnPreprovided("memory", 20);
        io->OnStarted();
        consumer.Finish();
        env.Deliver();
        const auto snapshot = env.Report();
        UNIT_ASSERT(!attributed);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->GetDataSources(0).GetReadBytes().GetValue(), 20);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->GetOperations(0).GetStatistics().DataSourcesSize(), 0);
        const auto& read = snapshot->GetOperations(0).GetSubOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(read.GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 20);
    }

    Y_UNIT_TEST(CacheAndTierRepliesKeepTheirStorageIdentity) {
        TEnvironment env;
        auto io = MakeReadObserver(env.Root->Fork(MakeReader()));
        io->OnStart("tier-cold", 2);
        io->OnStarted();
        io->OnData("tier-cold", 100, false);
        io->OnData("tier-cold", 20, true);
        env.Deliver();
        const auto report = env.Finish(NColumnShardStats::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(report->DataSourcesSize(), 2);
        for (const auto& source : report->GetDataSources()) {
            UNIT_ASSERT_VALUES_EQUAL(source.GetStorageId(), "tier-cold");
            UNIT_ASSERT_VALUES_EQUAL(source.GetReads(), 1);
            UNIT_ASSERT(source.GetReadBytes().GetBoundary() == NColumnShardStats::LOGICAL_DELIVERY);
            if (source.GetKind() == NColumnShardStats::BLOB_CACHE) {
                UNIT_ASSERT_VALUES_EQUAL(source.GetReadBytes().GetValue(), 20);
            } else {
                UNIT_ASSERT(source.GetKind() == NColumnShardStats::EXTERNAL_STORAGE);
                UNIT_ASSERT_VALUES_EQUAL(source.GetReadBytes().GetValue(), 100);
            }
        }
    }
}

}   // namespace NKikimr::NOlap::NStats::NScan
