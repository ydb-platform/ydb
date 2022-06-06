#include "columnshard_ut_common.h"

#include "columnshard__stats_scan.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_resolver.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTxUT {

using namespace NColumnShard;
using namespace Tests;

void TTester::Setup(TTestActorRuntime& runtime) {
    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_INFO);
    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);

    ui32 domainId = 0;
    ui32 planResolution = 500;

    TAppPrepare app;

    auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
                      "dc-1", domainId, FAKE_SCHEMESHARD_TABLET_ID,
                      domainId, domainId, TVector<ui32>{domainId},
                      domainId, TVector<ui32>{domainId},
                      planResolution,
                      TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(domainId, 1)},
                      TVector<ui64>{},
                      TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(domainId, 1)});

    TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
    ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
    runtime.SetTxAllocatorTabletIds(ids);

    app.AddDomain(domain.Release());
    SetupTabletServices(runtime, &app);
}

bool ProposeSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, NOlap::TSnapshot snap) {
    auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
        NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, snap.TxId, txBody);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), snap.TxId);
    UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
    return (res.GetStatus() == NKikimrTxColumnShard::PREPARED);
}

void PlanSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, NOlap::TSnapshot snap) {
    auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(snap.PlanStep, 0, TTestTxConfig::TxTablet0);
    auto tx = plan->Record.AddTransactions();
    tx->SetTxId(snap.TxId);
    ActorIdToProto(sender, tx->MutableAckTo());

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());
    UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(sender));
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), snap.TxId);
    UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
    UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::SUCCESS);
}

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, ui64 metaShard, ui64 writeId, ui64 tableId,
               const TString& data, std::shared_ptr<arrow::Schema> schema) {
    const TString dedupId = ToString(writeId);
    auto write = std::make_unique<TEvColumnShard::TEvWrite>(sender, metaShard, writeId, tableId, dedupId, data);
    if (schema) {
        write->SetArrowSchema(NArrow::SerializeSchema(*schema));
    }
    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, write.release());
    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvWriteResult>(handle);
    UNIT_ASSERT(event);

    auto& resWrite = Proto(event);
    UNIT_ASSERT_EQUAL(resWrite.GetOrigin(), TTestTxConfig::TxTablet0);
    UNIT_ASSERT_EQUAL(resWrite.GetTxInitiator(), metaShard);
    return (resWrite.GetStatus() == NKikimrTxColumnShard::EResultStatus::SUCCESS);
}

void ScanIndexStats(TTestBasicRuntime& runtime, TActorId& sender, const TVector<ui64>& pathIds,
                  NOlap::TSnapshot snap, ui64 scanId) {
    auto scan = std::make_unique<TEvColumnShard::TEvScan>();
    auto& record = scan->Record;

    record.SetTxId(snap.PlanStep);
    record.SetScanId(scanId);
    // record.SetLocalPathId(0);
    record.SetTablePath(NOlap::TIndexInfo::STORE_INDEX_STATS_TABLE);

    // Schema: pathId, kind, rows, bytes, rawBytes. PK: {pathId, kind}
    //record.SetSchemaVersion(0);
    auto ydbSchema = PrimaryIndexStatsSchema;
    for (const auto& col : ydbSchema.Columns) {
        record.AddColumnTags(col.second.Id);
        record.AddColumnTypes(col.second.PType);
    }

    for (ui64 pathId : pathIds) {
        TVector<TCell> pk{TCell::Make<ui64>(pathId)};
        TSerializedTableRange range(TConstArrayRef<TCell>(pk), true, TConstArrayRef<TCell>(pk), true);
        auto newRange = record.MutableRanges()->Add();
        range.Serialize(*newRange);
    }

    record.MutableSnapshot()->SetStep(snap.PlanStep);
    record.MutableSnapshot()->SetTxId(snap.TxId);
    record.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::ARROW);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, scan.release());
}

void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 metaShard, ui64 txId, const TVector<ui64>& writeIds) {
    NKikimrTxColumnShard::ETransactionKind txKind = NKikimrTxColumnShard::ETransactionKind::TX_KIND_COMMIT;
    TString txBody = TTestSchema::CommitTxBody(metaShard, writeIds);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
                new TEvColumnShard::TEvProposeTransaction(txKind, sender, txId, txBody));
    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(handle);
    UNIT_ASSERT(event);

    auto& res = Proto(event);
    UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
    UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
    UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::EResultStatus::PREPARED);
}

void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, const TSet<ui64>& txIds) {
    auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(planStep, 0, TTestTxConfig::TxTablet0);
    for (ui64 txId : txIds) {
        auto tx = plan->Record.AddTransactions();
        tx->SetTxId(txId);
        ActorIdToProto(sender, tx->MutableAckTo());
    }

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());
    TAutoPtr<IEventHandle> handle;

    for (ui32 i = 0; i < txIds.size(); ++i) {
        auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(handle);
        UNIT_ASSERT(event);

        auto& res = Proto(event);
        UNIT_ASSERT(txIds.count(res.GetTxId()));
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::EResultStatus::SUCCESS);
    }
}

TVector<TCell> MakeTestCells(const TVector<TTypeId>& types, ui32 value, TVector<TString>& mem) {
    TVector<TCell> cells;
    cells.reserve(types.size());

    for (auto& type : types) {
        // test only: 64-bit integer or string
        if (type == NTypeIds::Utf8 ||
            type == NTypeIds::String ||
            type == NTypeIds::String4k ||
            type == NTypeIds::String2m) {
            mem.push_back(ToString(value));
            const TString& str = mem.back();
            cells.push_back(TCell(str.data(), str.size()));
        } else if (type == NTypeIds::JsonDocument || type == NTypeIds::Json) {
            mem.push_back("{}");
            const TString& str = mem.back();
            cells.push_back(TCell(str.data(), str.size()));
        } else if (type == NTypeIds::Yson) {
            mem.push_back("{ \"a\" = [ { \"b\" = 1; } ]; }");
            const TString& str = mem.back();
            cells.push_back(TCell(str.data(), str.size()));
        } else if (type == NTypeIds::Timestamp ||
                    type == NTypeIds::Uint64 ||
                    type == NTypeIds::Int64) {
            cells.push_back(TCell::Make<ui64>(value));
        } else if (type == NTypeIds::Int32) {
            cells.push_back(TCell::Make<i32>(value));
        } else {
            UNIT_ASSERT(false);
        }
    }

    return cells;
}

TString MakeTestBlob(std::pair<ui64, ui64> range, const TVector<std::pair<TString, TTypeId>>& columns,
                     const THashSet<TString>& nullColumns) {
    TString err;
    NArrow::TArrowBatchBuilder batchBuilder(arrow::Compression::LZ4_FRAME);
    batchBuilder.Start(columns, 0, 0, err);

    TVector<ui32> nullPositions;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (nullColumns.count(columns[i].first)) {
            nullPositions.push_back(i);
        }
    }

    TVector<TString> mem;
    TVector<TTypeId> types = TTestSchema::ExtractTypes(columns);
    // insert, not ordered
    for (size_t i = range.first; i < range.second; i += 2) {
        TVector<TCell> cells = MakeTestCells(types, i, mem);
        for (auto& pos : nullPositions) {
            cells[pos] = TCell();
        }
        NKikimr::TDbTupleRef unused;
        batchBuilder.AddRow(unused, NKikimr::TDbTupleRef(types.data(), cells.data(), types.size()));
    }
    for (size_t i = range.first + 1; i < range.second; i += 2) {
        TVector<TCell> cells = MakeTestCells(types, i, mem);
        for (auto& pos : nullPositions) {
            cells[pos] = TCell();
        }
        NKikimr::TDbTupleRef unused;
        batchBuilder.AddRow(unused, NKikimr::TDbTupleRef(types.data(), cells.data(), types.size()));
    }

    auto batch = batchBuilder.FlushBatch(true);
    UNIT_ASSERT(batch);
    auto status = batch->ValidateFull();
    UNIT_ASSERT(status.ok());

    TString blob = batchBuilder.Finish();
    UNIT_ASSERT(!blob.empty());
    return blob;
}

TSerializedTableRange MakeTestRange(std::pair<ui64, ui64> range, bool inclusiveFrom, bool inclusiveTo,
                                    const TVector<std::pair<TString, TTypeId>>& columns) {
    TVector<TString> mem;
    TVector<TTypeId> types = TTestSchema::ExtractTypes(columns);
    TVector<TCell> cellsFrom = MakeTestCells(types, range.first, mem);
    TVector<TCell> cellsTo = MakeTestCells(types, range.second, mem);

    return TSerializedTableRange(TConstArrayRef<TCell>(cellsFrom), inclusiveFrom,
                                 TConstArrayRef<TCell>(cellsTo), inclusiveTo);
}

}
