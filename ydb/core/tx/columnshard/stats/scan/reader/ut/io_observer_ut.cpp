#include <ydb/core/tx/columnshard/stats/scan/collector.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/index_observer_factory.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/merge_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/read_delivery_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <arrow/api.h>
#include <library/cpp/testing/unittest/registar.h>

#include <vector>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

NColumnShardStats::TScanIdentity Identity() {
    NColumnShardStats::TScanIdentity identity;
    identity.SetTabletId(42);
    identity.SetScanAttemptId("io-observers");
    return identity;
}

struct TEnvironment {
    TScanStats Collector{ Identity() };
    std::unique_ptr<TLocalStats> Local = TLocalStats::Create(Identity(), MakeReader());

    auto Snapshot() {
        Local->PublishTo([&](TStatsUpdate&& update) {
            UNIT_ASSERT(Collector.Apply(std::move(update)));
        });
        return Collector.MakeReport();
    }
};

const NColumnShardStats::TDataSource& Source(const NColumnShardStats::TReport& report, NColumnShardStats::ESourceKind kind) {
    const NColumnShardStats::TDataSource* result = nullptr;
    for (const auto& source : report.GetDataSources()) {
        if (source.GetKind() == kind) {
            result = &source;
            break;
        }
    }
    UNIT_ASSERT(result);
    return *result;
}

const NColumnShardStats::TOperation& Child(const NColumnShardStats::TOperation& parent, NColumnShardStats::TOperation::DetailsCase kind) {
    const NColumnShardStats::TOperation* result = nullptr;
    for (const auto& child : parent.GetSubOperations()) {
        if (child.details_case() == kind) {
            result = &child;
            break;
        }
    }
    UNIT_ASSERT(result);
    return *result;
}

std::shared_ptr<arrow::Array> Array() {
    arrow::Int64Builder builder;
    UNIT_ASSERT(builder.AppendValues(std::vector<i64>{ 1, 2, 3 }).ok());
    std::shared_ptr<arrow::Array> result;
    UNIT_ASSERT(builder.Finish(&result).ok());
    return result;
}

class TIndexSourceStub {
    YDB_READONLY(TLocalStats*, LocalStats, nullptr);
    YDB_READONLY(TIndexRegistry*, IndexRegistry, nullptr);

public:
    TIndexSourceStub(TLocalStats* localStats, TIndexRegistry* indexRegistry)
        : LocalStats(localStats)
        , IndexRegistry(indexRegistry)
    {
    }

    const TIndexSourceStub* GetContext() const {
        return this;
    }

    const TIndexSourceStub* GetCommonContext() const {
        return this;
    }

    const TIndexSourceStub* GetResolver() const {
        return this;
    }

    TString GetColumnName(ui32, bool) const {
        return "column";
    }

    ui64 GetRawPathId() const {
        return 42;
    }

    ui64 GetRecordsCount() const {
        return 7;
    }

    TCallHandle GetCurrentStatisticsCall() const {
        return {};
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(ReaderIOObservers) {
    Y_UNIT_TEST(DeliveryKeepsUnknownDistinctFromZeroAndDoesNotRetainArrays) {
        TEnvironment env;
        auto parent = env.Local->Measure(&env.Local->GetRoot());
        auto index = TReadDeliveryObserver::ForInlineIndex(env.Local.get(), parent.GetHandle(), 7);
        index.OnData(0);
        index.OnData(100);
        auto unknown = TReadDeliveryObserver::ForSystemView(env.Local.get(), parent.GetHandle(), "/Root/view");
        unknown.OnData(std::shared_ptr<arrow::Array>());

        const auto first = env.Snapshot();
        const auto& unknownSource = Source(*first, NColumnShardStats::SYSTEM_VIEW);
        UNIT_ASSERT(!unknownSource.GetReadBytes().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(unknownSource.GetVolumeCoverage().GetMeasured(), 0);
        UNIT_ASSERT_VALUES_EQUAL(unknownSource.GetVolumeCoverage().GetApplicable(), 1);
        const auto& zero = Source(*first, NColumnShardStats::INLINE_INDEX);
        UNIT_ASSERT(zero.GetReadBytes().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(zero.GetReadBytes().GetValue(), 0);
        UNIT_ASSERT_VALUES_EQUAL(zero.GetReads(), 1);

        auto view = TReadDeliveryObserver::ForSystemView(env.Local.get(), parent.GetHandle(), "/Root/view");
        auto array = Array();
        std::weak_ptr<arrow::Array> weak = array;
        view.OnData(array);
        array.reset();
        UNIT_ASSERT(weak.expired());
        parent.Finish();
        const auto final = env.Snapshot();
        const auto& data = Source(*final, NColumnShardStats::SYSTEM_VIEW);
        UNIT_ASSERT_VALUES_EQUAL(data.GetReads(), 2);
        UNIT_ASSERT_VALUES_EQUAL(data.GetVolumeCoverage().GetMeasured(), 1);
        UNIT_ASSERT_VALUES_EQUAL(data.GetVolumeCoverage().GetApplicable(), 2);
        UNIT_ASSERT(data.GetReadBytes().GetValue() > 0);
        UNIT_ASSERT_VALUES_EQUAL(Source(*final, NColumnShardStats::INLINE_INDEX).GetReads(), 1);
        UNIT_ASSERT_VALUES_EQUAL(final->GetDataSources().size(), 2);
    }

    Y_UNIT_TEST(MissingIndexReportsAvailabilityAndRowsWithoutInventingData) {
        TEnvironment env;
        TIndexRegistry registry(*env.Local);
        TIndexSourceStub source(env.Local.get(), &registry);
        TIndexObserverFactory observer(source);
        NArrow::NAccessor::TAccessorsCollection resources(7);
        const NArrow::NSSA::TIndexCheckOperation operation(NArrow::NSSA::TIndexCheckOperation::EOperation::Equals, true);
        auto check = observer.StartCheck(nullptr, 1, operation, nullptr, resources);
        UNIT_ASSERT(check.GetInputRows() && *check.GetInputRows() == 7);
        check.OnOutputRows(7);
        check.OnFinished();
        const auto report = env.Snapshot();
        const auto& result = Child(report->GetOperations(0), NColumnShardStats::TOperation::kIndexCheck);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatistics().GetInputRows().GetValue(), 7);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatistics().GetOutputRows().GetValue(), 7);
        UNIT_ASSERT_VALUES_EQUAL(result.GetIndexCheck().GetIndexAvailable().GetFalseCount(), 1);
        UNIT_ASSERT(result.GetIndexCheck().GetDataAvailable().GetApplicability() == NColumnShardStats::NOT_APPLICABLE);
        UNIT_ASSERT(result.GetIndexCheck().GetMatched().GetApplicability() == NColumnShardStats::NOT_APPLICABLE);

        TIndexSourceStub disabled(nullptr, nullptr);
        TIndexObserverFactory disabledObserver(disabled);
        auto absent = disabledObserver.StartCheck(nullptr, 1, operation, nullptr, resources);
        UNIT_ASSERT(!absent.GetInputRows());
        absent.OnOutputRows(7);
        absent.OnFinished();
    }

    Y_UNIT_TEST(MergeObserverKeepsReservationAndRowCohorts) {
        TEnvironment env;
        TMergeObserver observer(env.Local.get(), 128);
        observer.OnMemoryAllocated(128);
        observer.OnPKResumed();
        observer.OnMergeStarted(2, 10);
        observer.OnMergeFinished(4);
        const auto report = env.Snapshot();
        const auto& pk = Child(report->GetOperations(0), NColumnShardStats::TOperation::kPKFilter);
        const auto& merge = Child(pk, NColumnShardStats::TOperation::kMerge);
        const auto& memory = Child(pk, NColumnShardStats::TOperation::kReserveMemory);
        UNIT_ASSERT_VALUES_EQUAL(merge.GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(merge.GetStatistics().GetInputRows().GetValue(), 10);
        UNIT_ASSERT_VALUES_EQUAL(merge.GetStatistics().GetOutputRows().GetValue(), 4);
        UNIT_ASSERT_VALUES_EQUAL(merge.GetMerge().GetSources().GetValue(), 2);
        UNIT_ASSERT_VALUES_EQUAL(memory.GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(memory.GetReserveMemory().GetRequestedBytes().GetValue(), 128);
        UNIT_ASSERT_VALUES_EQUAL(memory.GetReserveMemory().GetQuotaBytes().GetValue(), 128);
    }
}

}   // namespace NKikimr::NOlap::NStats::NScan
