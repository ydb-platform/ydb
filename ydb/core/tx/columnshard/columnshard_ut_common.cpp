#include "columnshard_ut_common.h"

#include "columnshard__stats_scan.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/scheme/scheme_types_proto.h>
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
                      domainId, domainId, std::vector<ui32>{domainId},
                      domainId, std::vector<ui32>{domainId},
                      planResolution,
                      std::vector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(domainId, 1)},
                      std::vector<ui64>{},
                      std::vector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(domainId, 1)});

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

bool ProposeSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, NOlap::TSnapshot snap) {
    auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
        NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, snap.GetTxId(), txBody);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), snap.GetTxId());
    UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
    return (res.GetStatus() == NKikimrTxColumnShard::PREPARED);
}

void PlanSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, NOlap::TSnapshot snap) {
    auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(snap.GetPlanStep(), 0, TTestTxConfig::TxTablet0);
    auto tx = plan->Record.AddTransactions();
    tx->SetTxId(snap.GetTxId());
    ActorIdToProto(sender, tx->MutableAckTo());

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());
    UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(sender));
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), snap.GetTxId());
    UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
    UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::SUCCESS);
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
                    const TString& data, const std::shared_ptr<arrow::Schema>& schema, std::vector<ui64>* writeIds) {
    const TString dedupId = ToString(writeId);

    auto write = std::make_unique<TEvColumnShard::TEvWrite>(sender, longTxId, tableId, dedupId, data, writeId);
    Y_ABORT_UNLESS(schema);
    write->SetArrowSchema(NArrow::SerializeSchema(*schema));
    ForwardToTablet(runtime, shardId, sender, write.release());

    if (writeIds) {
        return WaitWriteResult(runtime, shardId, writeIds) == NKikimrTxColumnShard::EResultStatus::SUCCESS;
    }
    return true;
}

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, const ui64 shardId, const ui64 writeId, const ui64 tableId, const TString& data,
                              const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema, std::vector<ui64>* writeIds) {
    NLongTxService::TLongTxId longTxId;
    UNIT_ASSERT(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1"));
    return WriteDataImpl(runtime, sender, shardId, tableId, longTxId, writeId, data, NArrow::MakeArrowSchema(ydbSchema), writeIds);

}

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, const ui64 writeId, const ui64 tableId, const TString& data,
                              const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema, bool waitResult, std::vector<ui64>* writeIds) {
    NLongTxService::TLongTxId longTxId;
    UNIT_ASSERT(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1"));
    if (writeIds) {
        return WriteDataImpl(runtime, sender, TTestTxConfig::TxTablet0, tableId, longTxId, writeId, data, NArrow::MakeArrowSchema(ydbSchema), writeIds);
    }
    std::vector<ui64> ids;
    return WriteDataImpl(runtime, sender, TTestTxConfig::TxTablet0, tableId, longTxId, writeId, data, NArrow::MakeArrowSchema(ydbSchema), waitResult ? &ids : nullptr);
}

std::optional<ui64> WriteData(TTestBasicRuntime& runtime, TActorId& sender, const NLongTxService::TLongTxId& longTxId,
                              ui64 tableId, const ui64 writePartId, const TString& data,
                              const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema)
{
    auto write = std::make_unique<TEvColumnShard::TEvWrite>(sender, longTxId, tableId, "0", data, writePartId);
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
    record.SetTablePath(NOlap::TIndexInfo::STORE_INDEX_STATS_TABLE);

    // Schema: pathId, kind, rows, bytes, rawBytes. PK: {pathId, kind}
    //record.SetSchemaVersion(0);
    auto ydbSchema = PrimaryIndexStatsSchema;
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
    TString txBody = TTestSchema::CommitTxBody(0, writeIds);

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


TString MakeTestBlob(std::pair<ui64, ui64> range, const std::vector<std::pair<TString, TTypeInfo>>& columns,
                     const TTestBlobOptions& options, const std::set<std::string>& notNullColumns) {
    TString err;
    NArrow::TArrowBatchBuilder batchBuilder(arrow::Compression::LZ4_FRAME, notNullColumns);
    batchBuilder.Start(columns, 0, 0, err);

    std::vector<ui32> nullPositions;
    std::vector<ui32> samePositions;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (options.NullColumns.contains(columns[i].first)) {
            nullPositions.push_back(i);
        } else if (options.SameValueColumns.contains(columns[i].first)) {
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
                                    const std::vector<std::pair<TString, TTypeInfo>>& columns) {
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
                cProto.MutableCompression()->SetCompressionCodec(tier.GetCodecId());
            }
            if (tier.CompressionLevel) {
                cProto.MutableCompression()->SetCompressionLevel(*tier.CompressionLevel);
            }
            NColumnShard::NTiers::TTierConfig tConfig(tier.Name, cProto);
            cs->MutableTierConfigs().emplace(tConfig.GetTierName(), tConfig);
        }
        tRule.AddInterval(tier.Name, TDuration::Seconds((*tier.EvictAfter).Seconds()));
    }
    cs->MutableTableTierings().emplace(tRule.GetTieringRuleId(), tRule);
    return cs;
}

}

namespace NKikimr::NColumnShard {
    NOlap::TIndexInfo BuildTableInfo(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema,
                         const std::vector<std::pair<TString, NScheme::TTypeInfo>>& key) {
        NOlap::TIndexInfo indexInfo = NOlap::TIndexInfo::BuildDefault();

        for (ui32 i = 0; i < ydbSchema.size(); ++i) {
            ui32 id = i + 1;
            auto& name = ydbSchema[i].first;
            auto& type = ydbSchema[i].second;

            indexInfo.Columns[id] = NTable::TColumn(name, id, type, "");
            indexInfo.ColumnNames[name] = id;
        }

        for (const auto& [keyName, keyType] : key) {
            indexInfo.KeyColumns.push_back(indexInfo.ColumnNames[keyName]);
        }

        indexInfo.SetAllKeys();
        return indexInfo;
    }

    void SetupSchema(TTestBasicRuntime& runtime, TActorId& sender, ui64 pathId,
                 const TestTableDescription& table, TString codec) {
        using namespace NTxUT;
        NOlap::TSnapshot snap(10, 10);
        TString txBody;
        auto specials = TTestSchema::TTableSpecials().WithCodec(codec);
        if (table.InStore) {
            txBody = TTestSchema::CreateTableTxBody(pathId, table.Schema, table.Pk, specials);
        } else {
            txBody = TTestSchema::CreateStandaloneTableTxBody(pathId, table.Schema, table.Pk, specials);
        }
        bool ok = ProposeSchemaTx(runtime, sender, txBody, snap);
        UNIT_ASSERT(ok);

        PlanSchemaTx(runtime, sender, snap);
    }

    void PrepareTablet(TTestBasicRuntime& runtime, const ui64 tableId, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& schema, const ui32 keySize) {
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

    std::shared_ptr<arrow::RecordBatch> ReadAllAsBatch(TTestBasicRuntime& runtime, const ui64 tableId, const NOlap::TSnapshot& snapshot, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& schema) {
        using namespace NTxUT;
        TActorId sender = runtime.AllocateEdgeActor();

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
                new TEvColumnShard::TEvRead(sender, TTestTxConfig::TxTablet1, snapshot.GetPlanStep(), snapshot.GetTxId(), tableId));

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        while(true) {
            TAutoPtr<IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle);
            UNIT_ASSERT(event);
            auto b = event->GetArrowBatch();
            if (b) {
                batches.push_back(b);
            }
            if (!event->HasMore()) {
                break;
            }
        }
        auto res = NArrow::CombineBatches(batches);
        return res ? res : NArrow::MakeEmptyBatch(NArrow::MakeArrowSchema(schema));
    }
}
