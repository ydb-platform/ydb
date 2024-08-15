#include "columnshard_ut_common.h"
#include "shard_reader.h"

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/portions/portions.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/tiering/snapshot.h>
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

void ProvideTieringSnapshot(TTestBasicRuntime& runtime, const TActorId& sender, NMetadata::NFetcher::ISnapshot::TPtr snapshot) {
    auto event = std::make_unique<NMetadata::NProvider::TEvRefreshSubscriberData>(snapshot);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
}


NKikimrTxColumnShard::EResultStatus TColumnShardTestSetup::ProposeTxWaitProposeResult(const NKikimrTxColumnShard::ETransactionKind kind, const TString& txBody, const ui64 txId) {
    auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, Sender, txId, txBody);
    ForwardToTablet(event.release());
    auto ev = Runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(Sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
    UNIT_ASSERT_EQUAL(res.GetTxKind(), kind);
    return res.GetStatus();
}

void TColumnShardTestSetup::PlanTxWaitAck(const ui64 txId, const ui64 planStep) {
    auto evPlan = std::make_unique<TEvTxProcessing::TEvPlanStep>(planStep, 0, TTestTxConfig::TxTablet0);
    auto tx = evPlan->Record.AddTransactions();
    tx->SetTxId(txId);
    ActorIdToProto(Sender, tx->MutableAckTo());
    ForwardToTablet(evPlan.release());
    UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(Sender));
}

void TColumnShardTestSetup::PlanTxWaitTxResult(const ui64 txId, const ui64 planStep, const NKikimrTxColumnShard::ETransactionKind expectedKind) {
    PlanTxWaitAck(txId, planStep);
    auto ev = Runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(Sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
    UNIT_ASSERT_EQUAL(res.GetTxKind(), expectedKind);
    UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::SUCCESS);
}

void TColumnShardTestSetup::ProposeTxPlanTxWaitTxResult(const NKikimrTxColumnShard::ETransactionKind kind, const TString& txBody, const ui64 txId, const ui64 planStep) {
    UNIT_ASSERT_EQUAL(
        ProposeTxWaitProposeResult(kind, txBody, txId),
        NKikimrTxColumnShard::PREPARED
    );
    PlanTxWaitTxResult(txId, planStep, kind);
}

void PlanWriteTx(TTestBasicRuntime& runtime, TActorId& sender, NOlap::TSnapshot snap, bool waitResult) {
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
    auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvWriteResult>(handle);
    UNIT_ASSERT(event);

    auto& resWrite = Proto(event);
    UNIT_ASSERT_EQUAL(resWrite.GetOrigin(), shardId);
    UNIT_ASSERT_EQUAL(resWrite.GetTxInitiator(), 0);
    if (writeIds && resWrite.GetStatus() == NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        writeIds->push_back(resWrite.GetWriteId());
    }
    return resWrite.GetStatus();
}

bool WriteDataImpl(TTestBasicRuntime& runtime, TActorId& sender, const ui64 shardId, const ui64 tableId,
                    const NLongTxService::TLongTxId& longTxId, const ui64 writeId,
                    const TString& data, const std::shared_ptr<arrow::Schema>& schema, std::vector<ui64>* writeIds, const NEvWrite::EModificationType mType) {
    const TString dedupId = ToString(writeId);

    auto write = std::make_unique<TEvColumnShard::TEvWrite>(sender, longTxId, tableId, dedupId, data, writeId, mType);
    Y_ABORT_UNLESS(schema);
    write->SetArrowSchema(NArrow::SerializeSchema(*schema));
    ForwardToTablet(runtime, shardId, sender, write.release());

    if (writeIds) {
        return WaitWriteResult(runtime, shardId, writeIds) == NKikimrTxColumnShard::EResultStatus::SUCCESS;
    }
    return true;
}

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, const ui64 shardId, const ui64 writeId, const ui64 tableId, const TString& data,
    const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, std::vector<ui64>* writeIds, const NEvWrite::EModificationType mType) {
    NLongTxService::TLongTxId longTxId;
    UNIT_ASSERT(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1"));
    return WriteDataImpl(
        runtime, sender, shardId, tableId, longTxId, writeId, data, NArrow::MakeArrowSchema(ydbSchema), writeIds, mType);
}

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, const ui64 writeId, const ui64 tableId, const TString& data,
    const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, bool waitResult, std::vector<ui64>* writeIds,
    const NEvWrite::EModificationType mType) {
    NLongTxService::TLongTxId longTxId;
    UNIT_ASSERT(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1"));
    if (writeIds) {
        return WriteDataImpl(runtime, sender, TTestTxConfig::TxTablet0, tableId, longTxId, writeId, data,
            NArrow::MakeArrowSchema(ydbSchema), writeIds, mType);
    }
    std::vector<ui64> ids;
    return WriteDataImpl(runtime, sender, TTestTxConfig::TxTablet0, tableId, longTxId, writeId, data,
        NArrow::MakeArrowSchema(ydbSchema), waitResult ? &ids : nullptr, mType);
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
    auto scan = std::make_unique<TEvColumnShard::TEvScan>();
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

void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 shardId, ui64 txId, const std::vector<ui64>& writeIds) {
    NKikimrTxColumnShard::ETransactionKind txKind = NKikimrTxColumnShard::ETransactionKind::TX_KIND_COMMIT;
    TString txBody = TTestSchema::CommitTxBody(writeIds);

    ForwardToTablet(runtime, shardId, sender,
                new TEvColumnShard::TEvProposeTransaction(txKind, sender, txId, txBody));
    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(handle);
    UNIT_ASSERT(event);

    auto& res = Proto(event);
    UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
    UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
    UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::EResultStatus::PREPARED);
}

void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 txId, const std::vector<ui64>& writeIds) {
    ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, txId, writeIds);
}

void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, const TSet<ui64>& txIds) {
    PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep, txIds);
}

void Wakeup(TTestBasicRuntime& runtime, TActorId& sender, const ui64 shardId) {
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
        auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(handle);
        UNIT_ASSERT(event);

        auto& res = Proto(event);
        UNIT_ASSERT(txIds.contains(res.GetTxId()));
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::EResultStatus::SUCCESS);
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

NMetadata::NFetcher::ISnapshot::TPtr TTestSchema::BuildSnapshot(const TTableSpecials& specials) {
    std::unique_ptr<NColumnShard::NTiers::TConfigsSnapshot> cs(new NColumnShard::NTiers::TConfigsSnapshot(Now()));
    if (specials.Tiers.empty()) {
        return cs;
    }
    NColumnShard::NTiers::TTieringRule tRule;
    tRule.SetTieringRuleId("Tiering1");
    for (auto&& tier : specials.Tiers) {
        if (!tRule.GetDefaultColumn()) {
            tRule.SetDefaultColumn(tier.TtlColumn);
        }
        UNIT_ASSERT(tRule.GetDefaultColumn() == tier.TtlColumn);
        {
            NKikimrSchemeOp::TStorageTierConfig cProto;
            cProto.SetName(tier.Name);
            *cProto.MutableObjectStorage() = tier.S3;
            if (tier.Codec) {
                cProto.MutableCompression()->SetCodec(tier.GetCodecId());
            }
            if (tier.CompressionLevel) {
                cProto.MutableCompression()->SetLevel(*tier.CompressionLevel);
            }
            NColumnShard::NTiers::TTierConfig tConfig(tier.Name, cProto);
            cs->MutableTierConfigs().emplace(tConfig.GetTierName(), tConfig);
        }
        tRule.AddInterval(tier.Name, TDuration::Seconds((*tier.EvictAfter).Seconds()));
    }
    cs->MutableTableTierings().emplace(tRule.GetTieringRuleId(), tRule);
    return cs;
}

void TTestSchema::InitSchema(const std::vector<NArrow::NTest::TTestColumn>& columns, const std::vector<NArrow::NTest::TTestColumn>& pk,
    const TTableSpecials& specials, NKikimrSchemeOp::TColumnTableSchema* schema) {
    schema->SetEngine(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);

    for (ui32 i = 0; i < columns.size(); ++i) {
        *schema->MutableColumns()->Add() = columns[i].CreateColumn(i + 1);
        if (!specials.NeedTestStatistics()) {
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

TColumnShardTestSetup::TColumnShardTestSetup() {
    TTester::Setup(Runtime); //fixme
    Runtime.SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);
    Runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

    Sender = Runtime.AllocateEdgeActor();
    CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    Runtime.DispatchEvents(options);
    
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    while (!controller->IsActiveTablet(TTestTxConfig::TxTablet0)) {
        Runtime.SimulateSleep(TDuration::Seconds(1));
    }
}

void TColumnShardTestSetup::CreateTable(const ui64 pathId, const TestTableDescription& table, TString codec) {
    TString txBody;
    auto specials = TTestSchema::TTableSpecials().WithCodec(codec);
    if (table.InStore) {
        txBody = TTestSchema::CreateTableTxBody(pathId, table.Schema, table.Pk, specials);
    } else {
        txBody = TTestSchema::CreateStandaloneTableTxBody(pathId, table.Schema, table.Pk);
    }
    const ui64 txId = 10;
    UNIT_ASSERT_EQUAL(ProposeTxWaitProposeResult(NKikimrTxColumnShard::TX_KIND_SCHEMA, txBody, txId), NKikimrTxColumnShard::PREPARED);
    const ui64 planStep = 10;
    PlanTxWaitTxResult(txId, planStep, NKikimrTxColumnShard::TX_KIND_SCHEMA);
}

std::shared_ptr<arrow::RecordBatch> TColumnShardTestSetup::ReadAllAsBatch(const ui64 tableId, const NOlap::TSnapshot& snapshot, const std::vector<NArrow::NTest::TTestColumn>& schema) {
    return ::NKikimr::NColumnShard::ReadAllAsBatch(Runtime, tableId, snapshot, schema);
}

void TColumnShardTestSetup::ScanIndexStats(const std::vector<ui64>& pathIds, NOlap::TSnapshot snap, ui64 scanId) {
    ::NKikimr::NTxUT::ScanIndexStats(Runtime, Sender, pathIds, snap, scanId);
}

bool TColumnShardTestSetup::TriggerTTL(NOlap::TSnapshot snap, const std::vector<ui64>& pathIds, ui64 tsSeconds, const TString& ttlColumnName) {
    TString txBody = TTestSchema::TtlTxBody(pathIds, ttlColumnName, tsSeconds);
    auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
        NKikimrTxColumnShard::TX_KIND_TTL, Sender, snap.GetTxId(), txBody);

    ForwardToTablet(event.release());
    auto ev = Runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(Sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), snap.GetTxId());
    UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_TTL);
    return (res.GetStatus() == NKikimrTxColumnShard::SUCCESS);
}

} //namespace NKikimr::NTxUT

namespace NKikimr::NColumnShard {
    NOlap::TIndexInfo BuildTableInfo(const std::vector<NArrow::NTest::TTestColumn>& ydbSchema,
                         const std::vector<NArrow::NTest::TTestColumn>& key) {
        NOlap::TIndexInfo indexInfo = NOlap::TIndexInfo::BuildDefault();

        for (ui32 i = 0; i < ydbSchema.size(); ++i) {
            ui32 id = i + 1;
            auto& name = ydbSchema[i].GetName();
            auto& type = ydbSchema[i].GetType();

            indexInfo.Columns[id] = NTable::TColumn(name, id, type, "");
            indexInfo.ColumnNames[name] = id;
        }

        for (const auto& c : key) {
            indexInfo.KeyColumns.push_back(indexInfo.ColumnNames[c.GetName()]);
        }

        auto storage = std::make_shared<NOlap::TTestStoragesManager>();
        storage->Initialize(TInstant::Now().Seconds());
        indexInfo.SetAllKeys(NOlap::TTestStoragesManager::GetInstance());
        return indexInfo;
    }




    std::shared_ptr<arrow::RecordBatch> ReadAllAsBatch(TTestBasicRuntime& runtime, const ui64 tableId, const NOlap::TSnapshot& snapshot, const std::vector<NArrow::NTest::TTestColumn>& schema) {
        std::vector<TString> fields;
        for (auto&& f : schema) {
            fields.emplace_back(f.GetName());
        }

        NTxUT::TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, snapshot);
        reader.SetReplyColumns(fields);
        auto rb = reader.ReadAll();
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        return rb ? rb : NArrow::MakeEmptyBatch(NArrow::MakeArrowSchema(schema));
    }
}
