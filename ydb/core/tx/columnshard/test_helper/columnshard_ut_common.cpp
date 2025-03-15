#include "columnshard_ut_common.h"
#include "shard_reader.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/portions/portions.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/data_events/common/modification_type.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTxUT {

using namespace NColumnShard;
using namespace Tests;

void TTester::Setup(TTestActorRuntime& runtime) {
    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_INFO);
    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::S3_WRAPPER, NLog::PRI_DEBUG);

    ui32 domainId = 0;
    ui32 planResolution = 500;

    TAppPrepare app;

    auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
                      "dc-1", domainId, FAKE_SCHEMESHARD_TABLET_ID,
                      planResolution,
                      std::vector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
                      std::vector<ui64>{},
                      std::vector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)});

    TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
    ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
    runtime.SetTxAllocatorTabletIds(ids);

    app.AddDomain(domain.Release());
    SetupTabletServices(runtime, &app);

    runtime.UpdateCurrentTime(TInstant::Now());
}

void RefreshTiering(TTestBasicRuntime& runtime, const TActorId& sender) {
    auto event = std::make_unique<TEvPrivate::TEvTieringModified>();

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
}

bool ProposeSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, NOlap::TSnapshot snap) {
    auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
        NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, snap.GetTxId(), txBody, 0, 0);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), snap.GetTxId());
    UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
    return (res.GetStatus() == NKikimrTxColumnShard::PREPARED);
}

void PlanSchemaTx(TTestBasicRuntime& runtime, const TActorId& sender, NOlap::TSnapshot snap) {
    auto evSubscribe = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(snap.GetTxId());
    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evSubscribe.release());

    auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(snap.GetPlanStep(), 0, TTestTxConfig::TxTablet0);
    auto tx = plan->Record.AddTransactions();
    tx->SetTxId(snap.GetTxId());
    ActorIdToProto(sender, tx->MutableAckTo());

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());
    UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(sender));
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
    UNIT_ASSERT_EQUAL(ev->Get()->Record.GetTxId(), snap.GetTxId());
}

void PlanWriteTx(TTestBasicRuntime& runtime, const TActorId& sender, NOlap::TSnapshot snap, bool waitResult) {
    auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(snap.GetPlanStep(), 0, TTestTxConfig::TxTablet0);
    auto tx = plan->Record.AddTransactions();
    tx->SetTxId(snap.GetTxId());
    ActorIdToProto(sender, tx->MutableAckTo());

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());
    UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(sender));
    if (waitResult) {
        auto ev = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), snap.GetTxId());
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }
}

ui32 WaitWriteResult(TTestBasicRuntime& runtime, ui64 shardId, std::vector<ui64>* writeIds) {
    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(handle);
    UNIT_ASSERT(event);

    auto& resWrite = event->Record;
    UNIT_ASSERT_EQUAL(resWrite.GetOrigin(), shardId);
    if (writeIds && resWrite.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
        writeIds->push_back(resWrite.GetTxId());
    }
    return resWrite.GetStatus();
}

bool WriteDataImpl(TTestBasicRuntime& runtime, TActorId& sender, const ui64 shardId, const ui64 tableId, const ui64 writeId,
                    const TString& data, const std::shared_ptr<arrow::Schema>& schema, std::vector<ui64>* writeIds, const NEvWrite::EModificationType mType, const ui64 lockId) {
    const TString dedupId = ToString(writeId);

    auto write = std::make_unique<NEvents::TDataEvents::TEvWrite>(writeId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    write->SetLockId(lockId, 1);
    auto& operation = write->AddOperation(TEnumOperator<NEvWrite::EModificationType>::SerializeToWriteProto(mType), TTableId(0, tableId, 1), {},
        0, NKikimrDataEvents::FORMAT_ARROW);
    *operation.MutablePayloadSchema() = NArrow::SerializeSchema(*schema);
    NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite> writer(*write);
    auto dataCopy = data;
    writer.AddDataToPayload(std::move(dataCopy));
    ForwardToTablet(runtime, shardId, sender, write.release());

    if (writeIds) {
        return WaitWriteResult(runtime, shardId, writeIds) == NKikimrTxColumnShard::EResultStatus::SUCCESS;
    }
    return true;
}

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, const ui64 shardId, const ui64 writeId, const ui64 tableId, const TString& data,
    const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, std::vector<ui64>* writeIds, const NEvWrite::EModificationType mType,
    const ui64 lockId) {
    return WriteDataImpl(runtime, sender, shardId, tableId, writeId, data, NArrow::MakeArrowSchema(ydbSchema), writeIds, mType, lockId);
}

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, const ui64 writeId, const ui64 tableId, const TString& data,
    const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, bool waitResult, std::vector<ui64>* writeIds,
    const NEvWrite::EModificationType mType, const ui64 lockId) {
    if (writeIds) {
        return WriteDataImpl(
            runtime, sender, TTestTxConfig::TxTablet0, tableId, writeId, data, NArrow::MakeArrowSchema(ydbSchema), writeIds, mType, lockId);
    }
    std::vector<ui64> ids;
    return WriteDataImpl(runtime, sender, TTestTxConfig::TxTablet0, tableId, writeId, data, NArrow::MakeArrowSchema(ydbSchema),
        waitResult ? &ids : nullptr, mType, lockId);
}

std::optional<ui64> WriteData(TTestBasicRuntime& runtime, TActorId& sender, const NLongTxService::TLongTxId& longTxId,
                              ui64 tableId, const ui64 writePartId, const TString& data,
                              const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, const NEvWrite::EModificationType mType)
{
    auto write = std::make_unique<TEvColumnShard::TEvWrite>(sender, longTxId, tableId, "0", data, writePartId, mType);
    write->SetArrowSchema(NArrow::SerializeSchema(*NArrow::MakeArrowSchema(ydbSchema)));

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, write.release());
    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvWriteResult>(handle);
    UNIT_ASSERT(event);

    auto& resWrite = Proto(event);
    UNIT_ASSERT_EQUAL(resWrite.GetOrigin(), TTestTxConfig::TxTablet0);
    UNIT_ASSERT_EQUAL(resWrite.GetTxInitiator(), 0);
    if (resWrite.GetStatus() == NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        return resWrite.GetWriteId();
    }
    return {};
}

void ScanIndexStats(TTestBasicRuntime& runtime, TActorId& sender, const std::vector<ui64>& pathIds,
                  NOlap::TSnapshot snap, ui64 scanId) {
    auto scan = std::make_unique<TEvDataShard::TEvKqpScan>();
    auto& record = scan->Record;

    record.SetTxId(snap.GetPlanStep());
    record.SetScanId(scanId);
    // record.SetLocalPathId(0);
    record.SetTablePath(TString("/") + NSysView::SysPathName + "/" + NSysView::StorePrimaryIndexPortionStatsName);

    // Schema: pathId, kind, rows, bytes, rawBytes. PK: {pathId, kind}
    //record.SetSchemaVersion(0);
    auto ydbSchema = NOlap::NReader::NSysView::NPortions::TStatsIterator::StatsSchema;
    for (const auto& col : ydbSchema.Columns) {
        record.AddColumnTags(col.second.Id);
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(col.second.PType, col.second.PTypeMod);
        record.AddColumnTypes(columnType.TypeId);
        if (columnType.TypeInfo) {
            *record.AddColumnTypeInfos() = *columnType.TypeInfo;
        } else {
            *record.AddColumnTypeInfos() = NKikimrProto::TTypeInfo();
        }
    }

    for (ui64 pathId : pathIds) {
        std::vector<TCell> pk{TCell::Make<ui64>(pathId)};
        TSerializedTableRange range(TConstArrayRef<TCell>(pk), true, TConstArrayRef<TCell>(pk), true);
        auto newRange = record.MutableRanges()->Add();
        range.Serialize(*newRange);
    }

    record.MutableSnapshot()->SetStep(snap.GetPlanStep());
    record.MutableSnapshot()->SetTxId(snap.GetTxId());
    record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, scan.release());
}

template<class Checker>
void ProposeCommitCheck(TTestBasicRuntime& runtime, TActorId& sender, ui64 shardId, ui64 txId, const std::vector<ui64>& /* writeIds */, const ui64 lockId, Checker&& checker) {
    auto write = std::make_unique<NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
    auto* lock = write->Record.MutableLocks()->AddLocks();
    lock->SetLockId(lockId);
    write->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);

    ForwardToTablet(runtime, shardId, sender, write.release());
    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(handle);
    UNIT_ASSERT(event);

    auto& res = event->Record;
    checker(res);
}

void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 shardId, ui64 txId, const std::vector<ui64>& writeIds, const ui64 lockId) {
    ProposeCommitCheck(runtime, sender, shardId, txId, writeIds, lockId, [&](auto& res) {
        AFL_VERIFY(res.GetTxId() == txId)("tx_id", txId)("res", res.GetTxId());
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    });
}

void ProposeCommitFail(TTestBasicRuntime& runtime, TActorId& sender, ui64 shardId, ui64 txId, const std::vector<ui64>& writeIds, const ui64 lockId) {
    ProposeCommitCheck(runtime, sender, shardId, txId, writeIds, lockId, [&](auto& res) {
        UNIT_ASSERT_UNEQUAL(res.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    });
}

void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 txId, const std::vector<ui64>& writeIds, const ui64 lockId) {
    ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, txId, writeIds, lockId);
}


void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 txId, const std::vector<ui64>& writeIds) {
    ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, txId, writeIds);
}

void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, const TSet<ui64>& txIds) {
    PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep, txIds);
}

void Wakeup(TTestBasicRuntime& runtime, const TActorId& sender, const ui64 shardId) {
    auto wakeup = std::make_unique<TEvPrivate::TEvPeriodicWakeup>(true);
    ForwardToTablet(runtime, shardId, sender, wakeup.release());
}

void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 shardId, ui64 planStep, const TSet<ui64>& txIds) {
    auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(planStep, 0, shardId);
    for (ui64 txId : txIds) {
        auto tx = plan->Record.AddTransactions();
        tx->SetTxId(txId);
        ActorIdToProto(sender, tx->MutableAckTo());
    }

    ForwardToTablet(runtime, shardId, sender, plan.release());
    TAutoPtr<IEventHandle> handle;

    for (ui32 i = 0; i < txIds.size(); ++i) {
        auto event = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(handle);
        UNIT_ASSERT(event);

        auto& res = event->Record;
        UNIT_ASSERT(txIds.contains(res.GetTxId()));
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }
    Wakeup(runtime, sender, shardId);
}

TCell MakeTestCell(const TTypeInfo& typeInfo, ui32 value, std::vector<TString>& mem) {
    auto type = typeInfo.GetTypeId();

    if (type == NTypeIds::Utf8 ||
        type == NTypeIds::String ||
        type == NTypeIds::String4k ||
        type == NTypeIds::String2m) {
        mem.push_back(ToString(value));
        const TString& str = mem.back();
        return TCell(str.data(), str.size());
    } else if (type == NTypeIds::JsonDocument || type == NTypeIds::Json) {
        mem.push_back("{}");
        const TString& str = mem.back();
        return TCell(str.data(), str.size());
    } else if (type == NTypeIds::Yson) {
        mem.push_back("{ \"a\" = [ { \"b\" = 1; } ]; }");
        const TString& str = mem.back();
        return TCell(str.data(), str.size());
    } else if (type == NTypeIds::Timestamp || type == NTypeIds::Interval ||
                type == NTypeIds::Timestamp64 || type == NTypeIds::Interval64 ||
                type == NTypeIds::Uint64 || type == NTypeIds::Int64) {
        return TCell::Make<ui64>(value);
    } else if (type == NTypeIds::Uint32 || type == NTypeIds::Int32 || type == NTypeIds::Datetime) {
        return TCell::Make<ui32>(value);
    } else if (type == NTypeIds::Uint16 || type == NTypeIds::Int16 || type == NTypeIds::Date) {
        return TCell::Make<ui16>(value);
    } else if (type == NTypeIds::Uint8 || type == NTypeIds::Int8 || type == NTypeIds::Byte ||
                type == NTypeIds::Bool) {
        return TCell::Make<ui8>(value);
    } else if (type == NTypeIds::Float) {
        return TCell::Make<float>(value);
    } else if (type == NTypeIds::Double) {
        return TCell::Make<double>(value);
    }

    UNIT_ASSERT(false);
    return {};
}

std::vector<TCell> MakeTestCells(const std::vector<TTypeInfo>& types, ui32 value, std::vector<TString>& mem) {
    std::vector<TCell> cells;
    cells.reserve(types.size());

    for (const auto& typeInfo : types) {
        cells.push_back(MakeTestCell(typeInfo, value, mem));
    }

    return cells;
}


TString MakeTestBlob(std::pair<ui64, ui64> range, const std::vector<NArrow::NTest::TTestColumn>& columns,
                     const TTestBlobOptions& options, const std::set<std::string>& notNullColumns) {
    NArrow::TArrowBatchBuilder batchBuilder(arrow::Compression::LZ4_FRAME, notNullColumns);
    const auto startStatus = batchBuilder.Start(NArrow::NTest::TTestColumn::ConvertToPairs(columns));
    UNIT_ASSERT_C(startStatus.ok(), startStatus.ToString());
    std::vector<ui32> nullPositions;
    std::vector<ui32> samePositions;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (options.NullColumns.contains(columns[i].GetName())) {
            nullPositions.push_back(i);
        } else if (options.SameValueColumns.contains(columns[i].GetName())) {
            samePositions.push_back(i);
        }
    }

    std::vector<TString> mem;
    std::vector<TTypeInfo> types = TTestSchema::ExtractTypes(columns);
    // insert, not ordered
    for (size_t i = range.first; i < range.second; i += 2) {
        std::vector<TCell> cells = MakeTestCells(types, i, mem);
        for (auto& pos : nullPositions) {
            cells[pos] = TCell();
        }
        for (auto& pos : samePositions) {
            cells[pos] = MakeTestCell(types[pos], options.SameValue, mem);
        }
        NKikimr::TDbTupleRef unused;
        batchBuilder.AddRow(unused, NKikimr::TDbTupleRef(types.data(), cells.data(), types.size()));
    }
    for (size_t i = range.first + 1; i < range.second; i += 2) {
        std::vector<TCell> cells = MakeTestCells(types, i, mem);
        for (auto& pos : nullPositions) {
            cells[pos] = TCell();
        }
        for (auto& pos : samePositions) {
            cells[pos] = MakeTestCell(types[pos], options.SameValue, mem);
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
                                    const std::vector<NArrow::NTest::TTestColumn>& columns) {
    std::vector<TString> mem;
    std::vector<TTypeInfo> types = TTestSchema::ExtractTypes(columns);
    std::vector<TCell> cellsFrom = MakeTestCells(types, range.first, mem);
    std::vector<TCell> cellsTo = MakeTestCells(types, range.second, mem);

    return TSerializedTableRange(TConstArrayRef<TCell>(cellsFrom), inclusiveFrom,
                                 TConstArrayRef<TCell>(cellsTo), inclusiveTo);
}

THashMap<TString, NColumnShard::NTiers::TTierConfig> TTestSchema::BuildSnapshot(const TTableSpecials& specials) {
    if (specials.Tiers.empty()) {
        return {};
    }
    THashMap<TString, NColumnShard::NTiers::TTierConfig> tiers;
    for (auto&& tier : specials.Tiers) {
        {
            NKikimrSchemeOp::TCompressionOptions compressionProto;
            if (tier.Codec) {
                compressionProto.SetCodec(tier.GetCodecId());
            }
            if (tier.CompressionLevel) {
                compressionProto.SetLevel(*tier.CompressionLevel);
            }
            NColumnShard::NTiers::TTierConfig tConfig(tier.S3, compressionProto);
            tiers.emplace(tier.Name, tConfig);
        }
    }
    return tiers;
}

void TTestSchema::InitSchema(const std::vector<NArrow::NTest::TTestColumn>& columns, const std::vector<NArrow::NTest::TTestColumn>& pk,
    const TTableSpecials& specials, NKikimrSchemeOp::TColumnTableSchema* schema) {

    for (ui32 i = 0; i < columns.size(); ++i) {
        *schema->MutableColumns()->Add() = columns[i].CreateColumn(i + 1);
        if (!specials.NeedTestStatistics(pk)) {
            continue;
        }
        if (NOlap::NIndexes::NMax::TIndexMeta::IsAvailableType(columns[i].GetType())) {
            *schema->AddIndexes() = NOlap::NIndexes::TIndexMetaContainer(
                std::make_shared<NOlap::NIndexes::NMax::TIndexMeta>(1000 + i, "MAX::INDEX::" + columns[i].GetName(), "__LOCAL_METADATA", i + 1))
                    .SerializeToProto();
        }
    }

    Y_ABORT_UNLESS(pk.size() > 0);
    for (auto& column : ExtractNames(pk)) {
        schema->AddKeyColumnNames(column);
    }

    if (specials.HasCodec()) {
        schema->MutableDefaultCompression()->SetCodec(specials.GetCodecId());
    }
    if (specials.CompressionLevel) {
        schema->MutableDefaultCompression()->SetLevel(*specials.CompressionLevel);
    }
}

}

namespace NKikimr::NColumnShard {
    NOlap::TIndexInfo BuildTableInfo(const std::vector<NArrow::NTest::TTestColumn>& ydbSchema,
                         const std::vector<NArrow::NTest::TTestColumn>& key) {
        THashMap<ui32, NTable::TColumn> columns;
        THashMap<TString, ui32> columnIdByName;
        for (ui32 i = 0; i < ydbSchema.size(); ++i) {
            ui32 id = i + 1;
            auto& name = ydbSchema[i].GetName();
            auto& type = ydbSchema[i].GetType();

            columns[id] = NTable::TColumn(name, id, type, "");
            AFL_VERIFY(columnIdByName.emplace(name, id).second);
        }

        std::vector<ui32> pkIds;
        ui32 idx = 0;
        for (const auto& c : key) {
            auto it = columnIdByName.FindPtr(c.GetName());
            AFL_VERIFY(it);
            AFL_VERIFY(*it < columns.size());
            columns[*it].KeyOrder = idx++;
            pkIds.push_back(*it);
        }
        return NOlap::TIndexInfo::BuildDefault(NOlap::TTestStoragesManager::GetInstance(), columns, pkIds);
    }

    void SetupSchema(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, const NOlap::TSnapshot& snapshot, bool succeed) {

        auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
        while (controller && !controller->IsActiveTablet(TTestTxConfig::TxTablet0)) {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }

        using namespace NTxUT;
        bool ok = ProposeSchemaTx(runtime, sender, txBody, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(ok, succeed);
        if (succeed) {
            PlanSchemaTx(runtime, sender, snapshot);
        }
    }

    void SetupSchema(TTestBasicRuntime& runtime, TActorId& sender, ui64 pathId,
                 const TestTableDescription& table, TString codec) {
        using namespace NTxUT;
        NOlap::TSnapshot snapshot(10, 10);
        TString txBody;
        auto specials = TTestSchema::TTableSpecials().WithCodec(codec);
        if (table.InStore) {
            txBody = TTestSchema::CreateTableTxBody(pathId, table.Schema, table.Pk, specials);
        } else {
            txBody = TTestSchema::CreateStandaloneTableTxBody(pathId, table.Schema, table.Pk, specials);
        }
        SetupSchema(runtime, sender, txBody, snapshot, true);
    }


    void PrepareTablet(TTestBasicRuntime& runtime, const ui64 tableId, const std::vector<NArrow::NTest::TTestColumn>& schema, const ui32 keySize) {
        using namespace NTxUT;
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        runtime.DispatchEvents(options);

        TestTableDescription tableDescription;
        tableDescription.Schema = schema;
        tableDescription.Pk = {};
        for (ui64 i = 0; i < keySize; ++i) {
            Y_ABORT_UNLESS(i < schema.size());
            tableDescription.Pk.push_back(schema[i]);
        }
        TActorId sender = runtime.AllocateEdgeActor();
        SetupSchema(runtime, sender, tableId, tableDescription);
    }

    void PrepareTablet(TTestBasicRuntime& runtime, const TString& schemaTxBody, bool succeed) {
        using namespace NTxUT;
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        runtime.DispatchEvents(options);

        TActorId sender = runtime.AllocateEdgeActor();
        SetupSchema(runtime, sender, schemaTxBody, NOlap::TSnapshot(1000, 100), succeed);
    }

     std::shared_ptr<arrow::RecordBatch> ReadAllAsBatch(TTestBasicRuntime& runtime, const ui64 tableId, const NOlap::TSnapshot& snapshot, const std::vector<NArrow::NTest::TTestColumn>& schema) {
         std::vector<ui32> fields;
         ui32 idx = 1;
         for (auto&& f : schema) {
             Y_UNUSED(f);
             fields.emplace_back(idx++);
         }
 
         NTxUT::TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, snapshot);
         reader.SetReplyColumnIds(fields);
         auto rb = reader.ReadAll();
         UNIT_ASSERT(reader.IsCorrectlyFinished());
         return rb ? rb : NArrow::MakeEmptyBatch(NArrow::MakeArrowSchema(schema));
     }
}
