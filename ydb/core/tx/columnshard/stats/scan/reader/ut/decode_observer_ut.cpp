#include <ydb/core/formats/arrow/accessor/composite_serial/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/save_load/saver.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/decode_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/read_report_finalizer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

const NColumnShardStats::TOperation* FindDecode(const NColumnShardStats::TOperation& operation) {
    if (operation.HasDecode()) {
        return &operation;
    }
    for (const auto& child : operation.GetSubOperations()) {
        if (const auto* found = FindDecode(child)) {
            return found;
        }
    }
    return nullptr;
}

const NColumnShardStats::TOperation* FindDecode(const NColumnShardStats::TReport& report) {
    for (const auto& operation : report.GetOperations()) {
        if (const auto* found = FindDecode(operation)) {
            return found;
        }
    }
    return nullptr;
}

struct TDecodeEnvironment {
    NColumnShardStats::TScanIdentity Identity;
    std::vector<TStatsUpdate> Updates;
    std::unique_ptr<TLocalStats> Local;
    std::unique_ptr<TDecodeObserver> Observer;

    explicit TDecodeEnvironment(const TString& attempt) {
        Identity.SetTabletId(42);
        Identity.SetScanAttemptId(attempt);
        Local = TLocalStats::Create(Identity, MakeOutput(), [this](TStatsUpdate&& update) {
            Updates.emplace_back(std::move(update));
        });
        UNIT_ASSERT(Local);
        Observer = std::make_unique<TDecodeObserver>(*Local);
        Observer->SetParent(&Local->GetRoot());
    }

    std::shared_ptr<const NColumnShardStats::TReport> Snapshot() const {
        TScanStats collector(Identity);
        for (const auto& update : Updates) {
            UNIT_ASSERT(collector.Apply(update));
        }
        auto report = std::move(collector).Finish(NColumnShardStats::SUCCESS);
        if (report) {
            TReadReportFinalizer(*report).Finalize();
        }
        return report;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TCSDecodeObserverTest) {
    Y_UNIT_TEST(UnknownInputRowsDoNotBecomeAMeasuredZero) {
        TDecodeEnvironment env("dictionary-rows");
        const auto serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
        NArrow::NAccessor::TColumnLoader loader(serializer,
            NArrow::NAccessor::TConstructorContainer(std::make_shared<NArrow::NAccessor::NPlain::TConstructor>()),
            arrow::field("dictionary", arrow::utf8()), nullptr, 1);
        auto dictionary = env.Observer->OnDecodeBegin(loader, 64, std::nullopt);
        UNIT_ASSERT(dictionary);
        dictionary->Finish(7);
        {
            const auto report = env.Snapshot();
            const auto* operation = FindDecode(*report);
            UNIT_ASSERT(operation);
            const auto& input = operation->GetStatistics().GetInputRows();
            UNIT_ASSERT(!input.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(input.GetCoverage().GetMeasured(), 0);
            UNIT_ASSERT_VALUES_EQUAL(input.GetCoverage().GetApplicable(), 1);
            UNIT_ASSERT_VALUES_EQUAL(operation->GetStatistics().GetOutputRows().GetValue(), 7);
        }
        auto empty = env.Observer->OnDecodeBegin(loader, 0, 0);
        UNIT_ASSERT(empty);
        empty->Finish(0);
        const auto report = env.Snapshot();
        const auto* operation = FindDecode(*report);
        UNIT_ASSERT(operation);
        const auto& input = operation->GetStatistics().GetInputRows();
        UNIT_ASSERT(input.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(input.GetValue(), 0);
        UNIT_ASSERT_VALUES_EQUAL(input.GetCoverage().GetMeasured(), 1);
        UNIT_ASSERT_VALUES_EQUAL(input.GetCoverage().GetApplicable(), 2);
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatistics().GetOutputRows().GetValue(), 7);
    }

    Y_UNIT_TEST(NestedDecodeFinishesBeforeTheNextSiblingStarts) {
        TDecodeEnvironment env("nested-scopes");
        const auto serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
        const auto constructor = NArrow::NAccessor::TConstructorContainer(std::make_shared<NArrow::NAccessor::NPlain::TConstructor>());
        NArrow::NAccessor::TColumnLoader outerLoader(serializer, constructor, arrow::field("outer", arrow::int64()), nullptr, 1);
        NArrow::NAccessor::TColumnLoader innerLoader(serializer, constructor, arrow::field("inner", arrow::int64()), nullptr, 2);
        auto outer = env.Observer->OnDecodeBegin(outerLoader, 100, 4);
        UNIT_ASSERT(outer);
        auto inner = env.Observer->OnDecodeBegin(innerLoader, 20, 4);
        UNIT_ASSERT(inner);
        inner->Finish(std::nullopt);
        outer->Finish(std::nullopt);
        auto sibling = env.Observer->OnDecodeBegin(innerLoader, 20, 4);
        UNIT_ASSERT(sibling);
        sibling->Finish(std::nullopt);
        const auto report = env.Snapshot();
        const auto& root = report->GetOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(root.SubOperationsSize(), 2);
        const NColumnShardStats::TOperation* parent = nullptr;
        const NColumnShardStats::TOperation* next = nullptr;
        for (const auto& operation : root.GetSubOperations()) {
            if (operation.GetDecode().GetColumns(0) == "outer") {
                parent = &operation;
            } else {
                next = &operation;
            }
        }
        UNIT_ASSERT(parent && next);
        UNIT_ASSERT_VALUES_EQUAL(parent->GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parent->SubOperationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parent->GetSubOperations(0).GetDecode().GetColumns(0), "inner");
        UNIT_ASSERT_VALUES_EQUAL(parent->GetSubOperations(0).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(next->GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(report->DataSourcesSize(), 0);
    }

    Y_UNIT_TEST(NestedFieldsWithTheSameColumnIdKeepTheirOwnNamesAndMeasurements) {
        arrow::Int64Builder input;
        NArrow::TStatusValidator::Validate(input.AppendValues(std::vector<i64>{ 10, 20, 30, 40 }));
        const auto values = NArrow::TStatusValidator::GetValid(input.Finish());
        const auto serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
        TDecodeEnvironment env("nested-fields");
        ui64 expectedBytes = 0;
        for (const auto* name : { "record_idx", "value" }) {
            const auto field = std::make_shared<arrow::Field>(name, arrow::int64());
            const auto payload = NArrow::NAccessor::TColumnSaver(serializer).Apply(values, field);
            expectedBytes += payload.size();
            const auto loader = std::make_shared<NArrow::NAccessor::TColumnLoader>(serializer,
                NArrow::NAccessor::TConstructorContainer(std::make_shared<NArrow::NAccessor::NPlain::TConstructor>()), field, nullptr, 0);
            const auto lazy = std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(4, loader, payload);
            auto decode = env.Observer->OnDecodeBegin(*loader, payload.size(), 4);
            UNIT_ASSERT(decode);
            const auto decoded = lazy->GetArraySlow(0, lazy).GetArray()->GetChunkedArray();
            decode->Finish(decoded->length());
            UNIT_ASSERT(decoded->chunk(0)->Equals(*values));
        }
        const auto report = env.Snapshot();
        const auto& root = report->GetOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(root.SubOperationsSize(), 2);
        bool recordIndex = false;
        bool value = false;
        ui64 encodedBytes = 0;
        for (const auto& operation : root.GetSubOperations()) {
            UNIT_ASSERT(operation.HasDecode());
            UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(operation.GetDecode().ColumnsSize(), 1);
            recordIndex |= operation.GetDecode().GetColumns(0) == "record_idx";
            value |= operation.GetDecode().GetColumns(0) == "value";
            UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetInputRows().GetValue(), 4);
            UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetValue(), 4);
            encodedBytes += operation.GetStatistics().GetInputBytes().GetValue();
        }
        UNIT_ASSERT(recordIndex && value);
        UNIT_ASSERT_VALUES_EQUAL(encodedBytes, expectedBytes);
    }

    Y_UNIT_TEST(DecodeCallbacksKeepReportsAndValuesIndependent) {
        arrow::Int64Builder input;
        NArrow::TStatusValidator::Validate(input.AppendValues(std::vector<i64>{ 10, 20, 30, 40 }));
        const auto values = NArrow::TStatusValidator::GetValid(input.Finish());
        const auto field = std::make_shared<arrow::Field>("value", arrow::int64());
        const auto serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
        const auto payload = NArrow::NAccessor::TColumnSaver(serializer).Apply(values, field);
        const auto loader = std::make_shared<NArrow::NAccessor::TColumnLoader>(serializer,
            NArrow::NAccessor::TConstructorContainer(std::make_shared<NArrow::NAccessor::NPlain::TConstructor>()), field, nullptr, 1);
        const auto sharedLazy = std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(4, loader, payload);

        const auto baseline = sharedLazy->GetArraySlow(0, sharedLazy).GetArray()->GetChunkedArray();
        TDecodeEnvironment first("first");
        TDecodeEnvironment second("second");
        auto firstCall = first.Observer->OnDecodeBegin(*loader, payload.size(), 4);
        UNIT_ASSERT(firstCall);
        const auto firstValues = sharedLazy->GetArraySlow(0, sharedLazy).GetArray()->GetChunkedArray();
        firstCall->Finish(firstValues->length());
        auto secondCall = second.Observer->OnDecodeBegin(*loader, payload.size(), 4);
        UNIT_ASSERT(secondCall);
        const auto secondValues = sharedLazy->GetArraySlow(0, sharedLazy).GetArray()->GetChunkedArray();
        secondCall->Finish(secondValues->length());
        UNIT_ASSERT(baseline->Equals(*firstValues));
        UNIT_ASSERT(baseline->Equals(*secondValues));
        const auto firstReport = first.Snapshot();
        const auto secondReport = second.Snapshot();
        const auto* firstDecode = FindDecode(*firstReport);
        const auto* secondDecode = FindDecode(*secondReport);
        UNIT_ASSERT(firstDecode && secondDecode);
        for (const auto* operation : { firstDecode, secondDecode }) {
            UNIT_ASSERT_VALUES_EQUAL(operation->GetCallsCount(), 1);
            UNIT_ASSERT(!operation->HasUnfinished());
            UNIT_ASSERT_VALUES_EQUAL(operation->GetDecode().GetColumns(0), "value");
            UNIT_ASSERT(operation->GetDecode().HasCodec());
            UNIT_ASSERT(!operation->GetDecode().GetCodec().empty());
            const auto& stats = operation->GetStatistics();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetInputRows().GetValue(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetInputBytes().GetValue(), payload.size());
            UNIT_ASSERT_VALUES_EQUAL(stats.GetOutputRows().GetValue(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetInputRows().GetCoverage().GetMeasured(), 1);
            UNIT_ASSERT(!stats.GetOutputBytes().HasValue());
        }
        UNIT_ASSERT_VALUES_EQUAL(firstDecode->GetDecode().GetCodec(), secondDecode->GetDecode().GetCodec());
        UNIT_ASSERT_VALUES_EQUAL(firstReport->GetIdentity().GetScanAttemptId(), "first");
        UNIT_ASSERT_VALUES_EQUAL(secondReport->GetIdentity().GetScanAttemptId(), "second");
        sharedLazy->GetArraySlow(0, sharedLazy);
        UNIT_ASSERT_VALUES_EQUAL(FindDecode(*first.Snapshot())->GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(FindDecode(*second.Snapshot())->GetCallsCount(), 1);
    }
}

}   // namespace NKikimr::NOlap::NStats::NScan
