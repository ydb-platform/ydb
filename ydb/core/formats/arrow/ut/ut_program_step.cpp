#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/program/aggr_keys.h>
#include <ydb/core/formats/arrow/program/ascii_contains/ascii_contains.h>
#include <ydb/core/formats/arrow/program/assign_const.h>
#include <ydb/core/formats/arrow/program/assign_internal.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/program/custom_registry.h>
#include <ydb/core/formats/arrow/program/execution.h>
#include <ydb/core/formats/arrow/program/filter.h>
#include <ydb/core/formats/arrow/program/functions.h>
#include <ydb/core/formats/arrow/program/graph_execute.h>
#include <ydb/core/formats/arrow/program/graph_optimization.h>
#include <ydb/core/formats/arrow/program/original.h>
#include <ydb/core/formats/arrow/program/projection.h>
#include <ydb/core/formats/arrow/program/short_circuit_logic.h>
#include <ydb/core/formats/arrow/program/visitor.h>

#include <ydb/library/formats/arrow/helpers/array.h>
#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/library/arrow_kernels/ut_common.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

#include <algorithm>
#include <array>
#include <initializer_list>
#include <memory>
#include <vector>

using namespace NKikimr::NArrow;
using NKikimr::NKernels::BoolVecToArray;
using NKikimr::NKernels::NumVecToArray;
using EOperation = NKikimr::NKernels::EOperation;
using EAggregate = NKikimr::NArrow::NSSA::NAggregation::EAggregate;
using namespace NKikimr::NArrow::NSSA;
using namespace NKikimr::NArrow::NSSA::NGraph;

enum class ETest {
    DEFAULT,
    EMPTY,
    ONE_VALUE
};

size_t FilterTest(const std::vector<std::shared_ptr<arrow::Array>>& args, const EOperation op1, const EOperation op2) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{ std::make_shared<arrow::Field>("x", args.at(0)->type()),
        std::make_shared<arrow::Field>("y", args.at(1)->type()), std::make_shared<arrow::Field>("z", args.at(2)->type()) });
    TSchemaColumnResolver resolver(schema);
    NOptimization::TGraph::TBuilder builder(resolver);
    builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({1, 2}), TColumnChainInfo(4), std::make_shared<TSimpleFunction>(op1), std::make_shared<TSimpleKernelLogic>()).DetachResult());
    builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({4, 3}), TColumnChainInfo(5), std::make_shared<TSimpleFunction>(op2), std::make_shared<TSimpleKernelLogic>()).DetachResult());
    builder.Add(std::make_shared<TFilterOperation>(TColumnChainInfo(5)));
    builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 4, 5 })));
    auto chain = builder.Finish().DetachResult();
    auto sds = std::make_shared<TSimpleDataSource>();

    ui32 idx = 1;
    for (auto&& i : args) {
        sds->AddBlob(idx, "", i);
        ++idx;
    }

    sds->ReturnResources(chain->Apply(*sds, sds->ExtractResources()).DetachResult());
    YDB_LOG_VERIFY(sds->GetResources().GetColumnsCount() == 2,
        {"count", sds->GetResources().GetColumnsCount()});
    return sds->GetResources().GetRecordsCountActualVerified();
}

size_t FilterTestUnary(std::vector<std::shared_ptr<arrow::Array>> args, const EOperation op1, const EOperation op2) {
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{ std::make_shared<arrow::Field>("x", args.at(0)->type()), std::make_shared<arrow::Field>("z", args.at(1)->type()) });
    TSchemaColumnResolver resolver(schema);

    auto sds = std::make_shared<TSimpleDataSource>();
    ui32 idx = 1;
    for (auto&& i : args) {
        sds->AddBlob(idx, "", i);
        ++idx;
    }

    NOptimization::TGraph::TBuilder builder(resolver);
    builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({1}), TColumnChainInfo(4), std::make_shared<TSimpleFunction>(op1), std::make_shared<TSimpleKernelLogic>()).DetachResult());
    builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({2, 4}), TColumnChainInfo(5), std::make_shared<TSimpleFunction>(op2), std::make_shared<TSimpleKernelLogic>()).DetachResult());
    builder.Add(std::make_shared<TFilterOperation>(TColumnChainInfo(5)));
    builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 4, 5 })));
    auto chain = builder.Finish().DetachResult();
    sds->ReturnResources(chain->Apply(*sds, sds->ExtractResources()).DetachResult());
    UNIT_ASSERT_VALUES_EQUAL(sds->GetResources().GetColumnsCount(), 2);
    return sds->GetResources().GetRecordsCountActualVerified();
}

std::vector<bool> LikeTest(const std::vector<std::string>& data, EOperation op, const std::string& pattern,
    std::shared_ptr<arrow::DataType> type = arrow::utf8(), bool ignoreCase = false) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{ std::make_shared<arrow::Field>("x", type) });
    std::shared_ptr<arrow::RecordBatch> batch;
    if (type->id() == arrow::utf8()->id()) {
        arrow::StringBuilder sb;
        sb.AppendValues(data).ok();
        batch = arrow::RecordBatch::Make(schema, data.size(), { *sb.Finish() });
    } else if (type->id() == arrow::binary()->id()) {
        arrow::BinaryBuilder sb;
        sb.AppendValues(data).ok();
        batch = arrow::RecordBatch::Make(schema, data.size(), { *sb.Finish() });
    }
    UNIT_ASSERT(batch->ValidateFull().ok());

    TSchemaColumnResolver resolver(schema);

    NOptimization::TGraph::TBuilder builder(resolver);
    builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({1}), TColumnChainInfo(2),
        std::make_shared<TSimpleFunction>(op, std::make_shared<arrow::compute::MatchSubstringOptions>(pattern, ignoreCase)), std::make_shared<TSimpleKernelLogic>()).DetachResult());
    builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 2 })));
    auto chain = builder.Finish().DetachResult();

    auto sds = std::make_shared<TSimpleDataSource>();
    ui32 idx = 1;
    for (auto&& i : batch->columns()) {
        sds->AddBlob(idx, "", i);
        ++idx;
    }

    sds->ReturnResources(chain->Apply(*sds, sds->ExtractResources()).DetachResult());
    UNIT_ASSERT_VALUES_EQUAL(sds->GetResources().GetColumnsCount(), 1);
    auto arr = sds->GetResources().GetAccessorVerified(2)->GetChunkedArray();
    YDB_LOG_VERIFY(arr->type()->id() == arrow::boolean()->id());
    std::vector<bool> vec;
    for (auto&& i : arr->chunks()) {
        auto& resColumn = static_cast<const arrow::BooleanArray&>(*i);
        for (int i = 0; i < resColumn.length(); ++i) {
            UNIT_ASSERT(!resColumn.IsNull(i));
            vec.push_back(resColumn.Value(i));
        }
    }
    return vec;
}

struct TSumData {
    static std::shared_ptr<arrow::RecordBatch> Data(ETest test, std::shared_ptr<arrow::Schema>& schema, bool nullable) {
        std::optional<double> null;
        if (nullable) {
            null = 0;
        }

        if (test == ETest::DEFAULT) {
            return arrow::RecordBatch::Make(schema, 4,
                std::vector{ NumVecToArray(arrow::int16(), { -1, 0, 0, -1 }, null), NumVecToArray(arrow::uint32(), { 1, 0, 0, 1 }, null) });
        } else if (test == ETest::EMPTY) {
            return arrow::RecordBatch::Make(schema, 0, std::vector{ NumVecToArray(arrow::int16(), {}), NumVecToArray(arrow::uint32(), {}) });
        } else if (test == ETest::ONE_VALUE) {
            return arrow::RecordBatch::Make(
                schema, 1, std::vector{ NumVecToArray(arrow::int16(), { 1 }), NumVecToArray(arrow::uint32(), { 0 }, null) });
        }
        return {};
    }

    static void CheckResult(ETest test, const TAccessorsCollection& batch, ui32 numKeys, bool nullable) {
        if (test == ETest::EMPTY) {
            UNIT_ASSERT(!batch.HasData());
            return;
        } else {
            YDB_LOG_VERIFY(batch.GetColumnsCount() == numKeys + 2);
        }

        auto aggXOriginal = batch.GetArrayVerified(3);
        auto aggYOriginal = batch.GetArrayVerified(4);
        auto colXOriginal = batch.GetArrayVerified(1);
        auto colYOriginal = (numKeys == 2) ? batch.GetArrayVerified(2) : nullptr;

        UNIT_ASSERT_EQUAL(aggXOriginal->type_id(), arrow::Type::INT64);
        UNIT_ASSERT_EQUAL(aggYOriginal->type_id(), arrow::Type::UINT64);
        UNIT_ASSERT_EQUAL(colXOriginal->type_id(), arrow::Type::INT16);
        if (numKeys == 2) {
            UNIT_ASSERT_EQUAL(colYOriginal->type_id(), arrow::Type::UINT32);
        }

        if (test == ETest::EMPTY) {
            UNIT_ASSERT_VALUES_EQUAL(batch.GetRecordsCountActualVerified(), 0);
            return;
        }

        auto& aggX = static_cast<arrow::Int64Array&>(*aggXOriginal);
        auto& aggY = static_cast<arrow::UInt64Array&>(*aggYOriginal);
        auto& colX = static_cast<arrow::Int16Array&>(*colXOriginal);

        if (test == ETest::ONE_VALUE) {
            UNIT_ASSERT_VALUES_EQUAL(batch.GetRecordsCountActualVerified(), 1);

            UNIT_ASSERT_VALUES_EQUAL(aggX.Value(0), 1);
            if (nullable) {
                UNIT_ASSERT(aggY.IsNull(0));
            } else {
                UNIT_ASSERT(!aggY.IsNull(0));
                UNIT_ASSERT_VALUES_EQUAL(aggY.Value(0), 0);
            }
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL(batch.GetRecordsCountActualVerified(), 2);

        for (ui32 row = 0; row < 2; ++row) {
            if (colX.IsNull(row)) {
                UNIT_ASSERT(aggX.IsNull(row));
                UNIT_ASSERT(aggY.IsNull(row));
            } else {
                UNIT_ASSERT(!aggX.IsNull(row));
                UNIT_ASSERT(!aggY.IsNull(row));
                if (colX.Value(row) == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(aggX.Value(row), 0);
                    UNIT_ASSERT_VALUES_EQUAL(aggY.Value(row), 0);
                } else if (colX.Value(row) == -1) {
                    UNIT_ASSERT_VALUES_EQUAL(aggX.Value(row), -2);
                    UNIT_ASSERT_VALUES_EQUAL(aggY.Value(row), 2);
                } else {
                    UNIT_ASSERT(false);
                }
            }
        }
    }
};

struct TMinMaxSomeData {
    static std::shared_ptr<arrow::RecordBatch> Data(ETest /*test*/, std::shared_ptr<arrow::Schema>& schema, bool nullable) {
        std::optional<double> null;
        if (nullable) {
            null = 0;
        }

        return arrow::RecordBatch::Make(
            schema, 1, std::vector{ NumVecToArray(arrow::int16(), { 1 }), NumVecToArray(arrow::uint32(), { 0 }, null) });
    }

    static void CheckResult(ETest /*test*/, const TAccessorsCollection& batch, ui32 numKeys, bool nullable) {
        UNIT_ASSERT_VALUES_EQUAL(numKeys, 1);
        auto aggXOriginal = batch.GetArrayVerified(3);
        auto aggYOriginal = batch.GetArrayVerified(4);
        auto colXOriginal = batch.GetArrayVerified(1);

        UNIT_ASSERT_VALUES_EQUAL(batch.GetColumnsCount(), numKeys + 2);
        UNIT_ASSERT_EQUAL(aggXOriginal->type_id(), arrow::Type::INT16);
        UNIT_ASSERT_EQUAL(aggYOriginal->type_id(), arrow::Type::UINT32);
        UNIT_ASSERT_EQUAL(colXOriginal->type_id(), arrow::Type::INT16);

        auto& aggX = static_cast<arrow::Int16Array&>(*aggXOriginal);
        auto& aggY = static_cast<arrow::UInt32Array&>(*aggYOriginal);
        auto& colX = static_cast<arrow::Int16Array&>(*colXOriginal);

        UNIT_ASSERT_VALUES_EQUAL(batch.GetRecordsCountActualVerified(), 1);

        UNIT_ASSERT_VALUES_EQUAL(colX.Value(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(aggX.Value(0), 1);
        if (nullable) {
            UNIT_ASSERT(aggY.IsNull(0));
        } else {
            UNIT_ASSERT(!aggY.IsNull(0));
            UNIT_ASSERT_VALUES_EQUAL(aggY.Value(0), 0);
        }
        return;
    }
};

void GroupByXY(bool nullable, ui32 numKeys, ETest test = ETest::DEFAULT, EAggregate aggFunc = EAggregate::Sum) {
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{ std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", arrow::uint32()) });

    std::shared_ptr<arrow::RecordBatch> batch;
    switch (aggFunc) {
        case EAggregate::Sum:
            batch = TSumData::Data(test, schema, nullable);
            break;
        case EAggregate::Min:
        case EAggregate::Max:
        case EAggregate::Some:
            batch = TMinMaxSomeData::Data(test, schema, nullable);
            break;
        default:
            break;
    }
    UNIT_ASSERT(batch);
    auto status = batch->ValidateFull();
    if (!status.ok()) {
        Cerr << status.ToString() << "\n";
    }
    UNIT_ASSERT(status.ok());

    TSchemaColumnResolver resolver(schema);

    NOptimization::TGraph::TBuilder builder(resolver);
    NAggregation::TGroupedAggregationOperation::TBuilder aggrBuilder;
    aggrBuilder.AddGroupBy(TColumnChainInfo(1), TColumnChainInfo(3), aggFunc);
    aggrBuilder.AddGroupBy(TColumnChainInfo(2), TColumnChainInfo(4), aggFunc);
    aggrBuilder.AddKey(TColumnChainInfo(1));
    if (numKeys == 2) {
        aggrBuilder.AddKey(TColumnChainInfo(2));
    }
    builder.Add(aggrBuilder.Finish().DetachResult());
    if (numKeys == 2) {
        builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 1, 2, 3, 4 })));
    } else {
        builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 1, 3, 4 })));
    }
    auto chain = builder.Finish().DetachResult();

    auto sds = std::make_shared<TSimpleDataSource>();
    ui32 idx = 1;
    for (auto&& i : batch->columns()) {
        sds->AddBlob(idx, "", i);
        ++idx;
    }

    sds->ReturnResources(chain->Apply(*sds, sds->ExtractResources()).DetachResult());

    switch (aggFunc) {
        case EAggregate::Sum:
            TSumData::CheckResult(test, sds->GetResources(), numKeys, nullable);
            break;
        case EAggregate::Min:
        case EAggregate::Max:
        case EAggregate::Some:
            TMinMaxSomeData::CheckResult(test, sds->GetResources(), numKeys, nullable);
            break;
        default:
            break;
    }
}

std::shared_ptr<NExecution::TCompiledGraph> BuildMergeFilterSimpleGraph(const bool reserveIndexMemory) {
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{ std::make_shared<arrow::Field>("int", arrow::int64()), std::make_shared<arrow::Field>("string", arrow::utf8()) });
    TSchemaColumnResolver resolver(schema);
    NOptimization::TGraph::TBuilder builder(resolver);
    if (reserveIndexMemory) {
        builder.EnableIndexMemoryReserve();
    }
    builder.Add(std::make_shared<TConstantOperation>(std::make_shared<arrow::Int64Scalar>(56), 3));
    builder.Add(std::make_shared<TConstantOperation>(std::make_shared<arrow::Int64Scalar>(0), 4));

    builder.Add(std::make_shared<TConstantOperation>(std::make_shared<arrow::StringScalar>("abc"), 10));
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({2, 10}), TColumnChainInfo(10001),
            std::make_shared<TSimpleFunction>(EOperation::Add), std::make_shared<TGetJsonPath>()).DetachResult();
        builder.Add(proc);
    }
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({10001}), TColumnChainInfo(1001), std::make_shared<TSimpleFunction>(EOperation::MatchSubstring),
            std::make_shared<NKikimr::NArrow::NSSA::TLogicMatchString>(TIndexCheckOperation::EOperation::Contains, true, false)).DetachResult();
        builder.Add(proc);
    }
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({1001, 4}), TColumnChainInfo(1101), std::make_shared<TSimpleFunction>(EOperation::Add),
            std::make_shared<TSimpleKernelLogic>((ui32)NYql::TKernelRequestBuilder::EBinaryOp::Coalesce)).DetachResult();
        builder.Add(proc);
    }
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({2}), TColumnChainInfo(1002), std::make_shared<TSimpleFunction>(EOperation::StartsWith), std::make_shared<NKikimr::NArrow::NSSA::TLogicMatchString>(
            NKikimr::NArrow::NSSA::TIndexCheckOperation::EOperation::StartsWith, true, false)).DetachResult();
        builder.Add(proc);
    }
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({1002, 4}), TColumnChainInfo(1102), std::make_shared<TSimpleFunction>(EOperation::Add),
            std::make_shared<TSimpleKernelLogic>((ui32)NYql::TKernelRequestBuilder::EBinaryOp::Coalesce)).DetachResult();
        builder.Add(proc);
    }
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({1, 3}), TColumnChainInfo(1003), std::make_shared<TSimpleFunction>(EOperation::Equal),
            std::make_shared<TCompareKernel>(false, TIndexCheckOperation::EOperation::Equals)).DetachResult();
        builder.Add(proc);
    }
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({1003, 4}), TColumnChainInfo(1103), std::make_shared<TSimpleFunction>(EOperation::Add),
            std::make_shared<TSimpleKernelLogic>((ui32)NYql::TKernelRequestBuilder::EBinaryOp::Coalesce)).DetachResult();
        builder.Add(proc);
    }

    builder.Add(std::make_shared<TFilterOperation>(1101));
    builder.Add(std::make_shared<TFilterOperation>(1102));
    builder.Add(std::make_shared<TFilterOperation>(1103));
    builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 1, 2 })));
    return builder.Finish().DetachResult();
}

std::shared_ptr<NExecution::TCompiledGraph> BuildSingleIndexFilterGraph(const bool reserveIndexMemory) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{ std::make_shared<arrow::Field>("int", arrow::int64()) });
    TSchemaColumnResolver resolver(schema);
    NOptimization::TGraph::TBuilder builder(resolver);
    if (reserveIndexMemory) {
        builder.EnableIndexMemoryReserve();
    }
    builder.Add(std::make_shared<TConstantOperation>(std::make_shared<arrow::Int64Scalar>(56), 3));
    builder.Add(std::make_shared<TConstantOperation>(std::make_shared<arrow::Int64Scalar>(0), 4));
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({ 1, 3 }), TColumnChainInfo(1003),
            std::make_shared<TSimpleFunction>(EOperation::Equal),
            std::make_shared<TCompareKernel>(false, TIndexCheckOperation::EOperation::Equals))
                        .DetachResult();
        builder.Add(proc);
    }
    {
        auto proc = TCalculationOperation::Build(TColumnChainInfo::BuildVector({ 1003, 4 }), TColumnChainInfo(1103),
            std::make_shared<TSimpleFunction>(EOperation::Add),
            std::make_shared<TSimpleKernelLogic>((ui32)NYql::TKernelRequestBuilder::EBinaryOp::Coalesce))
                        .DetachResult();
        builder.Add(proc);
    }
    builder.Add(std::make_shared<TFilterOperation>(1103));
    builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 1 })));
    return builder.Finish().DetachResult();
}

ui32 CountIndexReservesWiredToIndexFetch(const std::shared_ptr<NExecution::TCompiledGraph>& chain) {
    ui32 indexReserves = 0;
    for (const auto& [_, node] : chain->GetNodes()) {
        if (node->GetProcessor()->GetProcessorType() != EProcessorType::ReserveMemory) {
            continue;
        }
        if (!node->GetProcessor()->DebugJson()["extra_data"].Has("indexes")) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL(node->GetOutputEdges().size(), 1);
        const auto* fetch = node->GetOutputEdges().front();
        UNIT_ASSERT(fetch->GetProcessor()->GetProcessorType() == EProcessorType::FetchOriginalData);
        UNIT_ASSERT(fetch->GetProcessor()->DebugJson()["extra_data"].Has("indexes"));
        const auto& inputs = fetch->GetInputEdges();
        UNIT_ASSERT_VALUES_EQUAL(std::count(inputs.begin(), inputs.end(), node.get()), 1);
        UNIT_ASSERT(node->GetProcessor()->GetInput().empty());
        UNIT_ASSERT(node->GetProcessor()->GetOutput().empty());
        UNIT_ASSERT(!fetch->GetProcessor()->HasInput(0));
        ++indexReserves;
    }
    return indexReserves;
}

namespace {

class TExecutionDependencyDataSource: public TSimpleDataSource {
private:
    THashSet<ui32> ReservedColumns;
    THashSet<ui32> FetchedColumns;

    NKikimr::TConclusion<TExecutionResult> DoStartReserveMemory(const TProcessorContext& /*context*/,
        const THashMap<ui32, TDataAddress>& columns, const THashMap<ui32, TFetchIndexContext>& indexes,
        const THashMap<ui32, TFetchHeaderContext>& headers, const std::shared_ptr<IMemoryCalculationPolicy>& /*policy*/) override {
        UNIT_ASSERT(indexes.empty());
        UNIT_ASSERT(headers.empty());
        for (const auto& [id, _] : columns) {
            UNIT_ASSERT_C(ReservedColumns.emplace(id).second, "column reserved twice: " << id);
            Events.emplace_back(TStringBuilder() << "reserve:" << id);
        }
        return TExecutionResult::Done();
    }

    NKikimr::TConclusion<std::shared_ptr<IFetchLogic>> DoStartFetchData(const TProcessorContext& /*context*/, const TDataAddress& addr) override {
        const ui32 id = addr.GetColumnId();
        UNIT_ASSERT_C(ReservedColumns.contains(id), "fetch before reserve: " << id);
        UNIT_ASSERT_C(FetchedColumns.emplace(id).second, "column fetched twice: " << id);
        Events.emplace_back(TStringBuilder() << "fetch:" << id);
        return std::shared_ptr<IFetchLogic>();
    }

public:
    std::vector<TString> Events;
};

class TDataInputsCheckingVisitor: public NExecution::TExecutionVisitor {
public:
    using TExecutionVisitor::TExecutionVisitor;
    ui32 Notifications = 0;
    THashMap<ui32, std::vector<std::vector<TResourceId>>> ReadyInputsByNode;

    NKikimr::TConclusionStatus DoOnComeback(const NExecution::TCompiledGraph::TNode& node,
        const std::vector<TColumnChainInfo>& readyInputs) override {
        UNIT_ASSERT(!readyInputs.empty());
        for (const auto& input : readyInputs) {
            UNIT_ASSERT_C(input.GetColumnId() != 0, "execution dependency reported as a data column");
        }
        ++Notifications;
        ReadyInputsByNode[node.GetIdentifier()].emplace_back(TColumnChainInfo::ExtractColumnIds(readyInputs));
        return TExecutionVisitor::DoOnComeback(node, readyInputs);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(ProgramStep) {
    Y_UNIT_TEST(DependencyKindsAndResourcesAreRemovedIndependently) {
        NOptimization::TGraphNode producer(1, std::make_shared<TFetchOriginalDataOperation>(std::vector<ui32>{ 1, 2 }));
        NOptimization::TGraphNode consumer(2, std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 1, 2 })));
        const auto checkDependencies = [&](std::initializer_list<TDependency> expected) {
            UNIT_ASSERT_VALUES_EQUAL(producer.GetOutputDependencies().size(), expected.size());
            UNIT_ASSERT_VALUES_EQUAL(consumer.GetInputDependencies().size(), expected.size());
            for (const auto& [address, node] : producer.GetOutputDependencies()) {
                UNIT_ASSERT_VALUES_EQUAL(address.GetNodeId(), consumer.GetIdentifier());
                UNIT_ASSERT(node == &consumer);
                UNIT_ASSERT(std::find(expected.begin(), expected.end(), address.GetDependency()) != expected.end());
            }
            for (const auto& [address, node] : consumer.GetInputDependencies()) {
                UNIT_ASSERT_VALUES_EQUAL(address.GetNodeId(), producer.GetIdentifier());
                UNIT_ASSERT(node == &producer);
                UNIT_ASSERT(std::find(expected.begin(), expected.end(), address.GetDependency()) != expected.end());
            }
        };

        producer.AddDependencyTo(consumer, TDependency::Data(1));
        producer.AddDependencyTo(consumer, TDependency::Data(2));
        producer.AddDependencyTo(consumer, TDependency::Execution());
        checkDependencies({ TDependency::Data(1), TDependency::Data(2), TDependency::Execution() });
        UNIT_ASSERT_VALUES_EQUAL(producer.GetOutputDataDependenciesCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(std::ranges::distance(consumer.GetInputDataDependencies()), 2);

        producer.RemoveDependencyTo(consumer, TDependency::Data(1));
        checkDependencies({ TDependency::Data(2), TDependency::Execution() });
        UNIT_ASSERT_VALUES_EQUAL(producer.GetOutputDataDependencies().begin()->first.GetDependency().GetResourceId(), 2);

        producer.RemoveDependencyTo(consumer, TDependency::Data(2));
        checkDependencies({ TDependency::Execution() });
        UNIT_ASSERT(producer.GetOutputDataDependencies().empty());
        UNIT_ASSERT(consumer.GetInputDataDependencies().empty());
        UNIT_ASSERT(!producer.IsDisconnected());
        UNIT_ASSERT(!consumer.IsDisconnected());

        producer.AddDependencyTo(consumer, TDependency::Data(2));
        producer.RemoveDependencyTo(consumer, TDependency::Execution());
        checkDependencies({ TDependency::Data(2) });
        producer.RemoveDependencyTo(consumer, producer.GetOutputDependencies().begin()->first.GetDependency());
        checkDependencies({});
        UNIT_ASSERT(producer.IsDisconnected());
        UNIT_ASSERT(consumer.IsDisconnected());
    }

    Y_UNIT_TEST(ExecutionDependenciesOrderReserveAndFetch) {
        auto schema = arrow::schema({ arrow::field("predicate", arrow::boolean()), arrow::field("value", arrow::int64()) });
        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        builder.Add(std::make_shared<TFilterOperation>(TColumnChainInfo(1)));
        // Column 1 is shared by the filter and projection: it must be reserved and fetched only once.
        builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 1, 2 })));
        auto graph = builder.Finish().DetachResult();

        ui32 reserves = 0;
        for (const auto& [_, node] : graph->GetNodes()) {
            if (node->GetProcessor()->GetProcessorType() == EProcessorType::ReserveMemory) {
                ++reserves;
                UNIT_ASSERT(node->GetProcessor()->GetOutput().empty());
            }
            for (const auto& input : node->GetProcessor()->GetInput()) {
                UNIT_ASSERT(input.GetColumnId() != 0);
            }
            for (const auto& output : node->GetProcessor()->GetOutput()) {
                UNIT_ASSERT(output.GetColumnId() != 0);
            }
            for (const auto* input : node->GetInputEdges()) {
                UNIT_ASSERT(input->GetSequentialIdx() < node->GetSequentialIdx());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(reserves, 2);

        TExecutionDependencyDataSource source;
        source.AddBlob(1, "", BoolVecToArray({ true, false, true }));
        source.AddBlob(2, "", NumVecToArray(arrow::int64(), { 10, 20, 30 }));
        auto visitor = std::make_shared<TDataInputsCheckingVisitor>(TProcessorContext(source, source.ExtractResources(), std::nullopt, false));
        auto iterator = graph->BuildIterator(visitor);
        while (iterator->IsValid()) {
            auto executed = visitor->Execute();
            UNIT_ASSERT_C(executed.IsSuccess(), executed.GetErrorMessage());
            UNIT_ASSERT(!executed->IsPending());
            auto next = iterator->Next();
            UNIT_ASSERT_C(next.IsSuccess(), next.GetErrorMessage());
        }

        UNIT_ASSERT(visitor->Notifications > 0);
        for (const auto& [id, node] : graph->GetNodes()) {
            if (node->GetProcessor()->GetProcessorType() == EProcessorType::FetchOriginalData) {
                UNIT_ASSERT(!visitor->ReadyInputsByNode.contains(id));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", source.Events), "reserve:1,fetch:1,reserve:2,fetch:2");
        const auto& result = visitor->MutableContext().GetResources();
        UNIT_ASSERT_VALUES_EQUAL(result.GetRecordsCountActualVerified(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result.GetColumnsCount(), 2);
        UNIT_ASSERT(result.GetArrayVerified(2)->Equals(NumVecToArray(arrow::int64(), { 10, 30 })));
    }

    Y_UNIT_TEST(MultipleInputsFromOneNodeNotifyOnlyConsumedResources) {
        TSchemaColumnResolver resolver(arrow::schema({ arrow::field("key", arrow::int64()) }));
        NOptimization::TGraph::TBuilder builder(resolver);
        NAggregation::TGroupedAggregationOperation::TBuilder aggregationBuilder;
        aggregationBuilder.AddKey(TColumnChainInfo(1));
        UNIT_ASSERT(aggregationBuilder.AddGroupBy(TColumnChainInfo(1), TColumnChainInfo(2), EAggregate::Sum).IsSuccess());
        UNIT_ASSERT(aggregationBuilder.AddGroupBy(TColumnChainInfo(1), TColumnChainInfo(3), EAggregate::Count).IsSuccess());
        UNIT_ASSERT(aggregationBuilder.AddGroupBy(TColumnChainInfo(1), TColumnChainInfo(4), EAggregate::Min).IsSuccess());
        const auto aggregation = aggregationBuilder.Finish().DetachResult();
        builder.Add(aggregation);
        builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 2, 3 })));
        const auto graph = builder.Finish().DetachResult();

        const auto& projection = graph->GetResultRoot();
        UNIT_ASSERT_VALUES_EQUAL(projection->GetInputEdges().size(), 1);
        const auto* input = projection->GetInputEdges().front();
        UNIT_ASSERT(input->GetProcessor() == aggregation);
        UNIT_ASSERT_VALUES_EQUAL(input->GetOutputEdges().size(), 1);
        UNIT_ASSERT(input->GetOutputEdges().front() == projection.get());

        TSimpleDataSource source;
        source.AddBlob(1, "", NumVecToArray(arrow::int64(), { 1, 1, 2 }));
        auto visitor = std::make_shared<TDataInputsCheckingVisitor>(TProcessorContext(source, source.ExtractResources(), std::nullopt, false));
        const auto iterator = graph->BuildIterator(visitor);
        while (iterator->IsValid()) {
            const auto executed = visitor->Execute();
            UNIT_ASSERT_C(executed.IsSuccess(), executed.GetErrorMessage());
            UNIT_ASSERT(!executed->IsPending());
            const auto next = iterator->Next();
            UNIT_ASSERT_C(next.IsSuccess(), next.GetErrorMessage());
        }

        UNIT_ASSERT_VALUES_EQUAL(visitor->GetExecutedIds().size(), graph->GetNodes().size());
        const auto& notifications = visitor->ReadyInputsByNode.at(projection->GetIdentifier());
        UNIT_ASSERT_VALUES_EQUAL(notifications.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", notifications.front()), "2,3");
        const auto& result = visitor->MutableContext().GetResources();
        UNIT_ASSERT_VALUES_EQUAL(result.GetRecordsCountActualVerified(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result.GetColumnsCount(), 2);
        UNIT_ASSERT(result.GetArrayVerified(2)->Equals(NumVecToArray(arrow::int64(), { 2, 2 })));
    }

    Y_UNIT_TEST(Round0) {
        for (auto eop : { EOperation::Round, EOperation::RoundBankers, EOperation::RoundToExp2 }) {
            auto x = NumVecToArray(arrow::float64(), { 32.3, 12.5, 34.7 });
            auto z = arrow::compute::CallFunction(TSimpleFunction::GetFunctionName(eop), { x }, GetCustomExecContext());
            UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Round1) {
        for (auto eop : { EOperation::Ceil, EOperation::Floor, EOperation::Trunc }) {
            auto x = NumVecToArray(arrow::float64(), { 32.3, 12.5, 34.7 });
            auto z = arrow::compute::CallFunction(TSimpleFunction::GetFunctionName(eop), { x });
            UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Filter) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::uint32(), { 10, 34, 8 });
        auto z = NumVecToArray(arrow::int64(), { 33, 70, 12 });
        UNIT_ASSERT(FilterTest({ x, y, z }, EOperation::Add, EOperation::Less) == 2);
    }

    Y_UNIT_TEST(Add) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("add", { x, y });
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Add, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Substract) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("subtract", { x, y });
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Subtract, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Multiply) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("multiply", { x, y });
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Multiply, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Divide) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("divide", { x, y });
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Divide, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Gcd) {
        auto x = NumVecToArray(arrow::int32(), { 64, 16, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("gcd", { x, y }, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Gcd, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Lcm) {
        auto x = NumVecToArray(arrow::int32(), { 64, 16, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("lcm", { x, y }, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Lcm, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Mod) {
        auto x = NumVecToArray(arrow::int32(), { 64, 16, 8 });
        auto y = NumVecToArray(arrow::int32(), { 3, 5, 2 });
        auto z = arrow::compute::CallFunction("mod", { x, y }, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Modulo, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ModOrZero) {
        auto x = NumVecToArray(arrow::int32(), { 64, 16, 8 });
        auto y = NumVecToArray(arrow::int32(), { 3, 5, 0 });
        auto z = arrow::compute::CallFunction("modOrZero", { x, y }, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::ModuloOrZero, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Abs) {
        auto x = NumVecToArray(arrow::int32(), { -64, -16, 8 });
        auto z = arrow::compute::CallFunction("abs", { x });
        UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, EOperation::Abs, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Negate) {
        auto x = NumVecToArray(arrow::int32(), { -64, -16, 8 });
        auto z = arrow::compute::CallFunction("negate", { x });
        UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, EOperation::Negate, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Compares) {
        for (auto eop : { EOperation::Equal, EOperation::Less, EOperation::Greater, EOperation::GreaterEqual, EOperation::LessEqual,
                 EOperation::NotEqual }) {
            auto x = NumVecToArray(arrow::int32(), { 64, 5, 1 });
            auto y = NumVecToArray(arrow::int32(), { 64, 1, 5 });
            auto z = arrow::compute::CallFunction(TSimpleFunction::GetFunctionName(eop), { x, y });
            UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Logic0) {
        for (auto eop : { EOperation::And, EOperation::Or, EOperation::Xor }) {
            auto x = BoolVecToArray({ true, false, false });
            auto y = BoolVecToArray({ true, true, false });
            auto z = arrow::compute::CallFunction(TSimpleFunction::GetFunctionName(eop), { x, y });
            UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Logic1) {
        auto x = BoolVecToArray({ true, false, false });
        auto z = arrow::compute::CallFunction("invert", { x });
        UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, EOperation::Invert, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(StartsWith) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "aa", "abaaba", "baa", "" }, EOperation::StartsWith, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], false);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(EndsWith) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "aa", "abaaba", "baa", "" }, EOperation::EndsWith, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(MatchSubstring) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "aa", "abaaba", "baa", "" }, EOperation::MatchSubstring, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], true);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(StartsWithIgnoreCase) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "Aa", "abAaba", "baA", "" }, EOperation::StartsWith, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], false);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(EndsWithIgnoreCase) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "Aa", "abAaba", "baA", "" }, EOperation::EndsWith, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(MatchSubstringIgnoreCase) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "Aa", "abAaba", "baA", "" }, EOperation::MatchSubstring, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], true);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(AsciiContainsIgnoreCaseMemchrDirect) {
        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("", ""));
        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("abc", ""));
        UNIT_ASSERT(!AsciiContainsIgnoreCaseMemchr("", "a"));

        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("fdsa", "DS"));
        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("fdsa", "FdSa"));
        UNIT_ASSERT(!AsciiContainsIgnoreCaseMemchr("fdsa", "AS"));
        UNIT_ASSERT(!AsciiContainsIgnoreCaseMemchr("fdsa", "xyz"));

        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("AbC", "b"));
        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("AbC", "B"));
        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("a12b", "12"));
        UNIT_ASSERT(!AsciiContainsIgnoreCaseMemchr("a12b", "13"));

        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("xxxAAAyyy", "aaa"));
        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("ababcabcd", "AbCd"));

        UNIT_ASSERT(AsciiContainsIgnoreCaseMemchr("`Привет, мир!`", "Привет"));
        UNIT_ASSERT(!AsciiContainsIgnoreCaseMemchr("`Привет, мир!`", "привет"));
    }

    Y_UNIT_TEST(ScalarTest) {
        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("x", arrow::int64()), std::make_shared<arrow::Field>("filter", arrow::boolean()) });
        auto batch = arrow::RecordBatch::Make(
            schema, 4, std::vector{ NumVecToArray(arrow::int64(), { 64, 5, 1, 43 }), BoolVecToArray({ true, false, false, true }) });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        builder.Add(std::make_shared<TConstantOperation>(std::make_shared<arrow::Int64Scalar>(56), 3));
        builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({1, 3}), TColumnChainInfo(4), std::make_shared<TSimpleFunction>(EOperation::Add), std::make_shared<TSimpleKernelLogic>()).DetachResult());
        builder.Add(std::make_shared<TFilterOperation>(TColumnChainInfo(2)));
        builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 2, 4 })));
        auto chain = builder.Finish().DetachResult();

        auto sds = std::make_shared<TSimpleDataSource>();
        ui32 idx = 1;
        for (auto&& i : batch->columns()) {
            sds->AddBlob(idx, "", i);
            ++idx;
        }
        sds->ReturnResources(chain->Apply(*sds, sds->ExtractResources()).DetachResult());

        YDB_LOG_VERIFY(sds->GetResources().GetColumnsCount() == 2);
        YDB_LOG_VERIFY(sds->GetResources().GetRecordsCountActualVerified() == 2);
    }

    Y_UNIT_TEST(TestValueFromNull) {
        arrow::UInt32Builder sb;
        sb.AppendNulls(10).ok();
        auto arr = std::dynamic_pointer_cast<arrow::UInt32Array>(*sb.Finish());
        YDB_LOG_VERIFY(arr->Value(0) == 0,
            {"val", arr->Value(0)});
    }

    Y_UNIT_TEST(MergeFilterSimple) {
        std::vector<std::string> data = { "aa", "aaa", "aaaa", "bbbbb" };
        arrow::StringBuilder sb;
        sb.AppendValues(data).ok();

        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("int", arrow::int64()), std::make_shared<arrow::Field>("string", arrow::utf8()) });
        auto batch = arrow::RecordBatch::Make(schema, 4, std::vector{ NumVecToArray(arrow::int64(), { 64, 5, 1, 43 }), *sb.Finish() });
        UNIT_ASSERT(batch->ValidateFull().ok());

        auto chain = BuildMergeFilterSimpleGraph(false);
        Cerr << chain->DebugDOT() << Endl;
        YDB_LOG_VERIFY(chain->DebugStats() == "[TOTAL:Const:2;Calculation:4;Projection:1;Filter:1;FetchOriginalData:2;AssembleOriginalData:3;CheckIndexData:1;ShortCircuitLogic:1;ReserveMemory:1;];SUB:[AssembleOriginalData:1;];",
            {"debug", chain->DebugStats()});
    }

    Y_UNIT_TEST(MergeFilterSimpleIndexMemory) {
        auto chain = BuildMergeFilterSimpleGraph(true);
        YDB_LOG_VERIFY(chain->DebugStats() == "[TOTAL:Const:2;Calculation:4;Projection:1;Filter:1;FetchOriginalData:2;AssembleOriginalData:3;CheckIndexData:1;ShortCircuitLogic:1;ReserveMemory:2;];SUB:[AssembleOriginalData:1;];",
            {"debug", chain->DebugStats()});
        UNIT_ASSERT_VALUES_EQUAL(CountIndexReservesWiredToIndexFetch(chain), 1);
    }

    Y_UNIT_TEST(SingleIndexFetchMemory) {
        auto withoutReserve = BuildSingleIndexFilterGraph(false);
        UNIT_ASSERT_VALUES_EQUAL(CountIndexReservesWiredToIndexFetch(withoutReserve), 0);
        auto chain = BuildSingleIndexFilterGraph(true);
        UNIT_ASSERT_VALUES_EQUAL(CountIndexReservesWiredToIndexFetch(chain), 1);
    }

    Y_UNIT_TEST(Projection) {
        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("x", arrow::int64()), std::make_shared<arrow::Field>("y", arrow::boolean()) });
        auto batch = arrow::RecordBatch::Make(
            schema, 4, std::vector{ NumVecToArray(arrow::int64(), { 64, 5, 1, 43 }), BoolVecToArray({ true, false, false, true }) });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 1 })));
        auto chain = builder.Finish().DetachResult();

        auto sds = std::make_shared<TSimpleDataSource>();
        ui32 idx = 1;
        for (auto&& i : batch->columns()) {
            sds->AddBlob(idx, "", i);
            ++idx;
        }
        sds->ReturnResources(chain->Apply(*sds, sds->ExtractResources()).DetachResult());

        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources().GetColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources().GetRecordsCountActualVerified(), 4);
    }

    Y_UNIT_TEST(MinMax) {
        auto tsType = arrow::timestamp(arrow::TimeUnit::MICRO);

        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", tsType) });
        auto batch = arrow::RecordBatch::Make(
            schema, 4, std::vector{ NumVecToArray(arrow::int16(), { 1, 0, -1, 2 }), NumVecToArray(tsType, { 1, 4, 2, 3 }) });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        NAggregation::TGroupedAggregationOperation::TBuilder aggrBuilder;
        builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({1}), TColumnChainInfo(3), std::make_shared<NAggregation::TAggregateFunction>(EAggregate::Min), std::make_shared<TSimpleKernelLogic>()).DetachResult());
        builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({2}), TColumnChainInfo(4), std::make_shared<NAggregation::TAggregateFunction>(EAggregate::Max), std::make_shared<TSimpleKernelLogic>()).DetachResult());
        builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 3, 4 })));
        auto chain = builder.Finish().DetachResult();
        auto sds = std::make_shared<TSimpleDataSource>();
        ui32 idx = 1;
        for (auto&& i : batch->columns()) {
            sds->AddBlob(idx, "", i);
            ++idx;
        }
        sds->ReturnResources(chain->Apply(*sds, sds->ExtractResources()).DetachResult());
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources().GetColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources().GetRecordsCountActualVerified(), 1);
        UNIT_ASSERT_EQUAL(sds->GetResources().GetConstantScalarVerified(3)->type->id(), arrow::Type::INT16);
        UNIT_ASSERT_EQUAL(sds->GetResources().GetConstantScalarVerified(4)->type->id(), arrow::Type::TIMESTAMP);

        UNIT_ASSERT_EQUAL(static_pointer_cast<arrow::Int16Scalar>(sds->GetResources().GetConstantScalarVerified(3))->value, -1);
        UNIT_ASSERT_EQUAL(static_pointer_cast<arrow::TimestampScalar>(sds->GetResources().GetConstantScalarVerified(4))->value, 4);
    }

    Y_UNIT_TEST(Sum) {
        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", arrow::uint32()) });
        auto batch = arrow::RecordBatch::Make(
            schema, 4, std::vector{ NumVecToArray(arrow::int16(), { -1, 0, 1, 2 }), NumVecToArray(arrow::uint32(), { 1, 2, 3, 4 }) });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({1}), TColumnChainInfo(3), std::make_shared<NAggregation::TAggregateFunction>(EAggregate::Sum), std::make_shared<TSimpleKernelLogic>()).DetachResult());
        builder.Add(TCalculationOperation::Build(TColumnChainInfo::BuildVector({2}), TColumnChainInfo(4), std::make_shared<NAggregation::TAggregateFunction>(EAggregate::Sum), std::make_shared<TSimpleKernelLogic>()).DetachResult());
        builder.Add(std::make_shared<TProjectionOperation>(TColumnChainInfo::BuildVector({ 3, 4 })));
        auto chain = builder.Finish().DetachResult();

        auto sds = std::make_shared<TSimpleDataSource>();
        ui32 idx = 1;
        for (auto&& i : batch->columns()) {
            sds->AddBlob(idx, "", i);
            ++idx;
        }

        sds->ReturnResources(chain->Apply(*sds, sds->ExtractResources()).DetachResult());
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources().GetColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources().GetRecordsCountActualVerified(), 1);
        UNIT_ASSERT_EQUAL(sds->GetResources().GetConstantScalarVerified(3)->type->id(), arrow::Type::INT64);
        UNIT_ASSERT_EQUAL(sds->GetResources().GetConstantScalarVerified(4)->type->id(), arrow::Type::UINT64);

        UNIT_ASSERT_EQUAL(static_pointer_cast<arrow::Int64Scalar>(sds->GetResources().GetConstantScalarVerified(3))->value, 2);
        UNIT_ASSERT_EQUAL(static_pointer_cast<arrow::UInt64Scalar>(sds->GetResources().GetConstantScalarVerified(4))->value, 10);
    }

    Y_UNIT_TEST(SumGroupBy) {
        GroupByXY(true, 1);
        GroupByXY(true, 2);

        GroupByXY(true, 1, ETest::EMPTY);
        GroupByXY(true, 2, ETest::EMPTY);

        GroupByXY(true, 1, ETest::ONE_VALUE);
    }

    Y_UNIT_TEST(SumGroupByNotNull) {
        GroupByXY(false, 1);
        GroupByXY(false, 2);

        GroupByXY(false, 1, ETest::EMPTY);
        GroupByXY(false, 2, ETest::EMPTY);

        GroupByXY(false, 1, ETest::ONE_VALUE);
    }

    Y_UNIT_TEST(MinMaxSomeGroupBy) {
        GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Min);
        GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Max);
        GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Some);
    }

    Y_UNIT_TEST(MinMaxSomeGroupByNotNull) {
        GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Min);
        GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Max);
        GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Some);
    }
}
