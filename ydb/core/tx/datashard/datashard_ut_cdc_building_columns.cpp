#include "change_record.h"
#include "change_record_cdc_serializer.h"
#include "datashard_impl.h"

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr {

using namespace NDataShard;
using namespace Tests;

namespace {

NJson::TJsonValue ParseJson(const TString& text) {
    NJson::TJsonValue result;
    UNIT_ASSERT_C(NJson::ReadJsonTree(text, &result), text);
    return result;
}

void AssertJsonEqual(const NJson::TJsonValue& actual, const NJson::TJsonValue& expected) {
    UNIT_ASSERT_C(actual == expected,
        "Actual: " << NJson::WriteJson(actual) << "; expected: " << NJson::WriteJson(expected));
}

NJson::TJsonValue SerializeChange(const TChangeRecord& record, NKikimrSchemeOp::ECdcStreamMode mode) {
    THolder<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer({
        .StreamFormat = NKikimrSchemeOp::ECdcStreamFormatJson,
        .StreamMode = mode,
    }));
    NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
    serializer->Serialize(cmd, record);

    NKikimrPQClient::TDataChunk data;
    if (record.GetKind() == TChangeRecord::EKind::CdcSchemaChange) {
        UNIT_ASSERT(cmd.HasSchemaChange());
        UNIT_ASSERT_VALUES_EQUAL(cmd.GetSchemaChange().GetStep(), record.GetStep());
        UNIT_ASSERT_VALUES_EQUAL(cmd.GetSchemaChange().GetTxId(), record.GetTxId());
        UNIT_ASSERT(data.ParseFromString(cmd.GetSchemaChange().GetData()));
    } else {
        UNIT_ASSERT(data.ParseFromString(cmd.GetData()));
    }
    return ParseJson(data.GetData());
}

NKikimrSchemeOp::TTableDescription BuildingColumnsSchema() {
    NKikimrSchemeOp::TTableDescription schema;
    schema.SetName("Table");
    schema.SetPath("/Root/Table");
    schema.SetTableSchemaVersion(17);
    const auto addColumn = [&](ui32 id, const TString& name, ui32 typeId, ui32 family, bool building = false) {
        auto* column = schema.AddColumns();
        column->SetId(id);
        column->SetName(name);
        column->SetTypeId(typeId);
        column->SetFamily(family);
        column->SetIsBuildInProgress(building);
    };
    // The primary-key order deliberately differs from the column-id order.
    addColumn(20, "tenant", NScheme::NTypeIds::Utf8, 0);
    addColumn(2, "key", NScheme::NTypeIds::Uint32, 0);
    addColumn(4, "amount", NScheme::NTypeIds::Int64, 0);
    addColumn(6, "pending_flag", NScheme::NTypeIds::Bool, 9, true);
    addColumn(8, "message", NScheme::NTypeIds::Utf8, 7);
    addColumn(10, "pending_score", NScheme::NTypeIds::Double, 0, true);
    addColumn(12, "enabled", NScheme::NTypeIds::Bool, 0);
    schema.AddKeyColumnIds(20);
    schema.AddKeyColumnIds(2);

    auto* hot = schema.MutablePartitionConfig()->AddColumnFamilies();
    hot->SetId(7);
    hot->SetName("hot");
    hot->SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
    hot->SetColumnCacheMode(NKikimrSchemeOp::ColumnCacheModeTryKeepInMemory);
    hot->MutableStorageConfig()->MutableData()->SetPreferredPoolKind("ssd");
    hot->MutableStorageConfig()->MutableData()->SetAllowOtherKinds(false);

    // This family must still be described while its only column is hidden.
    auto* archive = schema.MutablePartitionConfig()->AddColumnFamilies();
    archive->SetId(9);
    archive->SetName("archive");
    archive->SetColumnCodec(NKikimrSchemeOp::ColumnCodecZSTD);
    archive->SetColumnCodecLevel(7);
    archive->MutableStorageConfig()->MutableData()->SetPreferredPoolKind("hdd");
    archive->MutableStorageConfig()->MutableData()->SetAllowOtherKinds(false);
    return schema;
}

NJson::TJsonValue ExpectedSchemaChange() {
    return ParseJson(R"({
        "tableChanges": [{"table": {
            "schemaVersion": 17,
            "primaryKeyColumnNames": ["tenant", "key"],
            "columns": {
                "tenant": {"type": "Utf8", "family": "default"},
                "key": {"type": "Uint32", "family": "default"},
                "amount": {"type": "Int64", "family": "default"},
                "message": {"type": "Utf8", "family": "hot"},
                "enabled": {"type": "Bool", "family": "default"}
            },
            "columnFamilies": {
                "default": {"compression": "off", "cacheMode": "regular"},
                "hot": {"compression": "lz4", "cacheMode": "in_memory", "data": {"media": "ssd"}},
                "archive": {"compression": "zstd", "compressionLevel": 7, "cacheMode": "regular", "data": {"media": "hdd"}}
            },
            "indexes": {}
        }}],
        "ts": [100, 200]
    })");
}

NJson::TJsonValue SerializeSchemaChange(TUserTable::TCPtr schema) {
    const auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcSchemaChange)
        .WithOrder(1)
        .WithStep(100)
        .WithTxId(200)
        .WithSchemaVersion(schema->GetTableSchemaVersion())
        .WithSchema(schema)
        .Build();
    return SerializeChange(*record, NKikimrSchemeOp::ECdcStreamModeUpdate);
}

void CheckInitialScan(NKikimrSchemeOp::ECdcStreamMode mode, const TVector<TString>& expected) {
    TPortManager portManager;
    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.SetEnabled(true);
    pqConfig.SetEnableProtoSourceIdInfo(true);
    pqConfig.SetTopicsAreFirstClassCitizen(true);
    pqConfig.SetCheckACL(false);
    pqConfig.SetRequireCredentialsInNewProtocol(false);
    pqConfig.MutableQuotingConfig()->SetEnableQuoting(false);
    TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, pqConfig)
        .SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetEnableChangefeedInitialScan(true));
    auto& runtime = *server->GetRuntime();
    const auto sender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);
    CreateShardedTable(server, sender, "/Root", "Table", TShardedTableOptions().Columns({
        {"tenant", "Utf8", true, false},
        {"key", "Uint32", true, false},
        {"pending_flag", "Bool", false, false},
        {"amount", "Int64", false, false},
        {"message", "Utf8", false, false},
        {"pending_score", "Double", false, false},
        {"enabled", "Bool", false, false},
    }));
    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/Table` (tenant, key, pending_flag, amount, message, pending_score, enabled) VALUES
            ("acme", 1, true, -10, "first", 1.5, true),
            ("acme", 2, false, 20, "second", 2.5, false);
    )");

    TBlockEvents<TEvDataShard::TEvCdcStreamScanRequest> blockScan(runtime);
    WaitTxNotification(server, sender, AsyncAlterAddStream(server, "/Root", "Table", TShardedTableOptions::TCdcStream{
        .Name = "Stream",
        .Mode = mode,
        .Format = NKikimrSchemeOp::ECdcStreamFormatJson,
        .InitialState = NKikimrSchemeOp::ECdcStreamStateScan,
    }));
    runtime.WaitFor("blocked initial scan", [&] { return !blockScan.empty(); });
    auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(blockScan.front()->GetRecipientRewrite()));
    UNIT_ASSERT(shard);
    UNIT_ASSERT_VALUES_EQUAL(shard->GetUserTables().size(), 1);

    // SchemeShard's snapshot locks serialize live DEFAULT builds and stream
    // creation. Inject their DataShard schema state at the scan boundary instead.
    // Real threads are disabled and no scan has started reading the schema yet.
    const auto table = MakeIntrusive<TUserTable>(*shard->GetUserTables().begin()->second);
    NKikimrSchemeOp::TTableDescription schema;
    table->GetSchema(schema);
    ui32 buildingColumns = 0;
    for (auto& column : *schema.MutableColumns()) {
        if (column.GetName() == "pending_flag" || column.GetName() == "pending_score") {
            UNIT_ASSERT(!table->Columns.at(column.GetId()).IsKey);
            column.SetIsBuildInProgress(true);
            table->Columns.at(column.GetId()).IsBuildInProgress = true;
            ++buildingColumns;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(buildingColumns, 2);
    table->SetSchema(schema);
    shard->SetUserTable(TPathId::FromProto(blockScan.front()->Get()->Record.GetTablePathId()), table);

    bool scanDone = false;
    auto observeDone = runtime.AddObserver<TEvDataShard::TEvCdcStreamScanResponse>([&](auto& ev) {
        if (ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStats().GetRowsProcessed(), expected.size());
            scanDone = true;
        }
    });
    TMap<ui64, NJson::TJsonValue> records;
    auto observeRecords = runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvRecords>([&](auto& ev) {
        for (const auto& record : ev->Get()->Records) {
            if (record->GetKind() != TChangeRecord::EKind::CdcDataChange) {
                continue;
            }
            UNIT_ASSERT(record->GetSource() == TChangeRecord::ESource::InitialScan);
            const auto* change = dynamic_cast<const TChangeRecord*>(record.Get());
            UNIT_ASSERT(change);
            const TChangeRecord copy = *change;
            records.emplace(copy.GetOrder(), SerializeChange(copy, mode));
        }
    });
    blockScan.Unblock().Stop();
    runtime.WaitFor("initial scan records and completion", [&] {
        return scanDone && records.size() >= expected.size();
    });
    UNIT_ASSERT_VALUES_EQUAL(records.size(), expected.size());
    size_t index = 0;
    for (const auto& [order, body] : records) {
        Y_UNUSED(order);
        AssertJsonEqual(body, ParseJson(expected.at(index++)));
    }
}

} // namespace

Y_UNIT_TEST_SUITE(CdcBuildingColumns) {
    Y_UNIT_TEST_TWIN(InitialScanFiltersBuildingColumns, Images) {
        if constexpr (Images) {
            CheckInitialScan(NKikimrSchemeOp::ECdcStreamModeNewAndOldImages, {
                R"({"key":["acme",1],"update":{},"newImage":{"amount":-10,"message":"first","enabled":true}})",
                R"({"key":["acme",2],"update":{},"newImage":{"amount":20,"message":"second","enabled":false}})",
            });
        } else {
            CheckInitialScan(NKikimrSchemeOp::ECdcStreamModeUpdate, {
                R"({"key":["acme",1],"update":{"amount":-10,"message":"first","enabled":true}})",
                R"({"key":["acme",2],"update":{"amount":20,"message":"second","enabled":false}})",
            });
        }
    }

    Y_UNIT_TEST(SchemaChangeFiltersBuildingColumns) {
        const auto schema = MakeIntrusive<TUserTable>(1u, BuildingColumnsSchema(), 0u);
        AssertJsonEqual(SerializeSchemaChange(schema), ExpectedSchemaChange());
    }

    Y_UNIT_TEST(SchemaChangeAfterBuildCompletion) {
        const auto schema = MakeIntrusive<TUserTable>(1u, BuildingColumnsSchema(), 0u);
        UNIT_ASSERT(schema->FinishColumnBuilding());
        schema->SetTableSchemaVersion(18);

        auto expected = ExpectedSchemaChange();
        auto& table = expected["tableChanges"][0]["table"];
        table["schemaVersion"] = 18;
        table["columns"]["pending_flag"] = ParseJson(R"({"type":"Bool","family":"archive"})");
        table["columns"]["pending_score"] = ParseJson(R"({"type":"Double","family":"default"})");
        AssertJsonEqual(SerializeSchemaChange(schema), expected);
    }

    Y_UNIT_TEST(SchemaChangeAfterBuildCancellation) {
        const auto schema = MakeIntrusive<TUserTable>(1u, BuildingColumnsSchema(), 0u);
        NKikimrSchemeOp::TTableDescription alter;
        alter.SetName("Table");
        alter.SetTableSchemaVersion(18);
        alter.AddDropColumns()->SetId(6);
        alter.AddDropColumns()->SetId(10);
        const auto cancelled = MakeIntrusive<TUserTable>(*schema, alter);
        UNIT_ASSERT(!cancelled->FinishColumnBuilding());

        auto expected = ExpectedSchemaChange();
        expected["tableChanges"][0]["table"]["schemaVersion"] = 18;
        AssertJsonEqual(SerializeSchemaChange(cancelled), expected);
    }
}

} // namespace NKikimr
