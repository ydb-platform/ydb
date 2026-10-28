#include "json_change_record.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/aclib/user_context.h>

#include <util/string/builder.h>

namespace NKikimr::NReplication::NService {

using TFamily = NKikimrReplication::TSchemaChange::TFamily;

namespace {

auto MakeIndexSchemaChangeRecord(TStringBuf indexes = {}) {
    TStringBuilder body;
    body << R"json({
        "ts": [10, 20],
        "tableChanges": [{
            "table": {
                "schemaVersion": 2,
                "columns": {
                    "key": {"type": "Uint64"},
                    "value": {"type": "Utf8"}
                },
                "primaryKeyColumnNames": ["key"]
    )json";
    if (indexes) {
        body << ", \"indexes\": " << indexes;
    }

    body << "}}]}";

    return TChangeRecordBuilder()
        .WithBody(body)
        .Build();
}

} // namespace

Y_UNIT_TEST_SUITE(JsonChangeRecord) {
    Y_UNIT_TEST(DataChange) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":100500}})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcDataChange);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 0);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 0);
    }

    Y_UNIT_TEST(DataChangeVersion) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":100500}, "ts":[10,20]})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
    }

    Y_UNIT_TEST(Heartbeat) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"resolved":[10,20]})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
    }

    Y_UNIT_TEST(SchemaChange) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 2,
                        "columns": {"key": {"type": "Uint64"}},
                        "primaryKeyColumnNames": ["key"]
                    }
                }],
                "ts": [10, 20]
            })json")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcSchemaChange);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);

        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT_C(record->TryGetSchemaChange(schema, error), error);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetSourceSchemaVersion(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetVersion().GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetVersion().GetTxId(), 20);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetPrimaryKeyColumnNames(0), "key");
        UNIT_ASSERT_VALUES_EQUAL(schema.ColumnsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(0).GetName(), "key");
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(0).GetType(), "Uint64");
    }

    Y_UNIT_TEST(SchemaChangeIndexMetadata) {
        auto parse = [](const TString& indexes) {
            auto record = MakeIndexSchemaChangeRecord(indexes);
            NKikimrReplication::TSchemaChange schema;
            TString error;
            UNIT_ASSERT_C(record->TryGetSchemaChange(schema, error), error);
            return schema;
        };

        UNIT_ASSERT(!parse("").HasIndexes());
        const auto empty = parse("{}");
        UNIT_ASSERT(empty.HasIndexes());
        UNIT_ASSERT_VALUES_EQUAL(empty.GetIndexes().ItemsSize(), 0);
        NKikimrReplication::TSchemaChange restored;
        UNIT_ASSERT(restored.ParseFromString(empty.SerializeAsString()));
        UNIT_ASSERT(restored.HasIndexes());

        const auto first = parse(R"json({
            "z": {"type": "GlobalSync", "indexColumns": ["value"], "dataColumns": []},
            "a": {"type": "GlobalAsync", "indexColumns": ["key"], "dataColumns": ["value"]}
        })json");
        const auto second = parse(R"json({
            "a": {"dataColumns": ["value"], "indexColumns": ["key"], "type": "GlobalAsync"},
            "z": {"dataColumns": [], "indexColumns": ["value"], "type": "GlobalSync"}
        })json");
        UNIT_ASSERT_VALUES_EQUAL(first.SerializeAsString(), second.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(first.GetIndexes().GetItems(0).GetName(), "a");
        UNIT_ASSERT_VALUES_EQUAL(first.GetIndexes().GetItems(1).GetType(), "GlobalSync");
    }

    Y_UNIT_TEST(SchemaChangeRejectsInvalidIndexes) {
        for (const auto* indexes : {
            "null", "[]", R"({"":{}})",
            R"({"idx":{"type":"GlobalSync","indexColumns":[],"dataColumns":[]}})",
            R"({"idx":{"type":"GlobalSync","indexColumns":["missing"],"dataColumns":[]}})",
            R"({"idx":{"type":"GlobalSync","indexColumns":["key","key"],"dataColumns":[]}})",
            R"({"idx":{"type":"GlobalSync","indexColumns":["key"],"dataColumns":["key"]}})",
            R"({"idx":{"type":"GlobalSync","indexColumns":["key"]}})",
            R"({"idx":{},"idx":{"type":"GlobalSync","indexColumns":["key"],"dataColumns":[]}})",
        }) {
            auto record = MakeIndexSchemaChangeRecord(indexes);
            NKikimrReplication::TSchemaChange schema;
            TString error;
            UNIT_ASSERT_C(!record->TryGetSchemaChange(schema, error), indexes);
            UNIT_ASSERT(!error.empty());
        }
    }

    Y_UNIT_TEST(MalformedSchemaChange) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 2,
                        "columns": {"key": {"type": "Uint64"}},
                        "primaryKeyColumnNames": ["missing"]
                    }
                }],
                "ts": [10, 20]
            })json")
            .Build();
        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT(!record->TryGetSchemaChange(schema, error));
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(SchemaChangeRejectsInvalidColumnDescriptions) {
        for (const auto* columns : {
            R"json({"key": "Uint64"})json",
            R"json({"key": 42})json",
            R"json({"key": null})json",
            R"json({"key": []})json",
            R"json({"key": {}})json",
            R"json({"key": {"type": ""}})json",
            R"json({"key": {"type": 42}})json",
            R"json({"": {"type": "Uint64"}, "key": {"type": "Uint64"}})json",
        }) {
            const TString body = TStringBuilder() << R"json({
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 2,
                        "columns": )json" << columns << R"json(,
                        "primaryKeyColumnNames": ["key"]
                    }
                }],
                "ts": [10, 20]
            })json";
            auto record = TChangeRecordBuilder()
                .WithBody(body)
                .Build();

            NKikimrReplication::TSchemaChange schema;
            TString error;
            UNIT_ASSERT_C(!record->TryGetSchemaChange(schema, error), columns);
            UNIT_ASSERT_C(!error.empty(), columns);
        }
    }

    Y_UNIT_TEST(SchemaChangeRequiresNonZeroSchemaVersion) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 0,
                        "columns": {"key": {"type": "Uint64"}},
                        "primaryKeyColumnNames": ["key"]
                    }
                }],
                "ts": [10, 20]
            })json")
            .Build();

        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT(!record->TryGetSchemaChange(schema, error));
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(SchemaChangeRequiresTimestamp) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 2,
                        "columns": {"key": {"type": "Uint64"}},
                        "primaryKeyColumnNames": ["key"]
                    }
                }]
            })json")
            .Build();

        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT(!record->TryGetSchemaChange(schema, error));
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(InvalidJsonIsFallible) {
        auto record = TChangeRecordBuilder()
            .WithBody("{")
            .Build();

        TString error;
        UNIT_ASSERT(!record->IsValidJson(error));
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(SchemaChangeCompositeKeyAndParameterizedType) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "ts": [11, 21],
                "tableChanges": [{
                    "table": {
                        "primaryKeyColumnNames": ["tenant", "id"],
                        "columns": {
                            "amount": {"type": "Decimal(35,10)"},
                            "id": {"type": "Uint64"},
                            "tenant": {"type": "Utf8"}
                        },
                        "schemaVersion": 3
                    }
                }]
            })json")
            .Build();

        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT_C(record->TryGetSchemaChange(schema, error), error);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetVersion().GetStep(), 11);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetVersion().GetTxId(), 21);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetPrimaryKeyColumnNames(0), "tenant");
        UNIT_ASSERT_VALUES_EQUAL(schema.GetPrimaryKeyColumnNames(1), "id");
        UNIT_ASSERT_VALUES_EQUAL(schema.ColumnsSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(0).GetName(), "amount");
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(0).GetType(), "Decimal(35,10)");
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(1).GetName(), "id");
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(1).GetType(), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(2).GetName(), "tenant");
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(2).GetType(), "Utf8");
    }

    Y_UNIT_TEST(SchemaChangeColumnsAreCanonical) {
        auto firstRecord = TChangeRecordBuilder()
            .WithBody(R"json({
                "ts": [10, 20],
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 2,
                        "columns": {
                            "value": {"type": "Utf8"},
                            "key": {"type": "Uint64"},
                            "extra": {"type": "Bool"}
                        },
                        "primaryKeyColumnNames": ["key"]
                    }
                }]
            })json")
            .Build();
        auto secondRecord = TChangeRecordBuilder()
            .WithBody(R"json({
                "ts": [10, 20],
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 2,
                        "columns": {
                            "extra": {"type": "Bool"},
                            "key": {"type": "Uint64"},
                            "value": {"type": "Utf8"}
                        },
                        "primaryKeyColumnNames": ["key"]
                    }
                }]
            })json")
            .Build();

        NKikimrReplication::TSchemaChange firstSchema;
        NKikimrReplication::TSchemaChange secondSchema;
        TString error;
        UNIT_ASSERT_C(firstRecord->TryGetSchemaChange(firstSchema, error), error);
        UNIT_ASSERT_C(secondRecord->TryGetSchemaChange(secondSchema, error), error);

        UNIT_ASSERT_VALUES_EQUAL(firstSchema.GetColumns(0).GetName(), "extra");
        UNIT_ASSERT_VALUES_EQUAL(firstSchema.GetColumns(1).GetName(), "key");
        UNIT_ASSERT_VALUES_EQUAL(firstSchema.GetColumns(2).GetName(), "value");
        UNIT_ASSERT_VALUES_EQUAL(firstSchema.SerializeAsString(), secondSchema.SerializeAsString());
    }

    Y_UNIT_TEST(SchemaChangeColumnFamilies) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "ts": [10, 20],
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 3,
                        "primaryKeyColumnNames": ["key"],
                        "columns": {
                            "value": {"type": "Utf8", "family": "archive"},
                            "key": {"type": "Uint64", "family": "default"}
                        },
                        "columnFamilies": {
                            "default": {"compression": "off", "cacheMode": "regular"},
                            "archive": {
                                "data": {"media": "ssd"},
                                "compression": "lz4",
                                "cacheMode": "in_memory"
                            }
                        }
                    }
                }]
            })json")
            .Build();
        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT_C(record->TryGetSchemaChange(schema, error), error);
        UNIT_ASSERT_VALUES_EQUAL(schema.FamiliesSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetFamilies(0).GetName(), "archive");
        UNIT_ASSERT_VALUES_EQUAL(schema.GetFamilies(0).GetMedia(), "ssd");
        UNIT_ASSERT(schema.GetFamilies(0).GetCompression() == TFamily::COMPRESSION_LZ4);
        UNIT_ASSERT(schema.GetFamilies(0).GetCacheMode() == TFamily::CACHE_MODE_IN_MEMORY);
        UNIT_ASSERT(schema.GetFamilies(1).GetCompression() == TFamily::COMPRESSION_OFF);
        UNIT_ASSERT(schema.GetFamilies(1).GetCacheMode() == TFamily::CACHE_MODE_REGULAR);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(1).GetFamily(), "archive");
    }

    Y_UNIT_TEST(SchemaChangeRejectsDuplicateFamilyNames) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "ts": [10, 20],
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 3,
                        "primaryKeyColumnNames": ["key"],
                        "columns": {"key": {"type": "Uint64", "family": "archive"}},
                        "columnFamilies": {
                            "default": {"compression": "off", "cacheMode": "regular"},
                            "archive": {"compression": "off", "cacheMode": "regular"},
                            "archive": {"compression": "lz4", "cacheMode": "regular"}
                        }
                    }
                }]
            })json")
            .Build();

        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT(!record->TryGetSchemaChange(schema, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "duplicate column family: archive");
    }

    Y_UNIT_TEST(SchemaChangeRejectsDuplicateAfterColumnFamiliesFamily) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "ts": [10, 20],
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 3,
                        "primaryKeyColumnNames": ["key"],
                        "columns": {"key": {"type": "Uint64", "family": "archive"}},
                        "columnFamilies": {
                            "default": {"compression": "off", "cacheMode": "regular"},
                            "columnFamilies": {"compression": "off", "cacheMode": "regular"},
                            "archive": {"compression": "off", "cacheMode": "regular"},
                            "archive": {"compression": "lz4", "cacheMode": "regular"}
                        }
                    }
                }]
            })json")
            .Build();

        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT(!record->TryGetSchemaChange(schema, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "duplicate column family: archive");
    }

    Y_UNIT_TEST(SchemaChangeRejectsDuplicateMedia) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"json({
                "ts": [10, 20],
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 3,
                        "primaryKeyColumnNames": ["key"],
                        "columns": {"key": {"type": "Uint64", "family": "archive"}},
                        "columnFamilies": {
                            "default": {"compression": "off", "cacheMode": "regular"},
                            "archive": {
                                "data": {"media": "ssd", "media": "rot"},
                                "compression": "lz4", "cacheMode": "regular"
                            }
                        }
                    }
                }]
            })json")
            .Build();

        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT(!record->TryGetSchemaChange(schema, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "duplicate column family setting: media");
    }

    Y_UNIT_TEST(SchemaChangeRejectsIncompleteFamilyMetadata) {
        for (const auto* fragment : {
            R"json("columnFamilies": {
                "default": {"compression": "off", "cacheMode": "regular"}
            })json",
            R"json("columnFamilies": {
                "archive": {"compression": "off", "cacheMode": "regular"}
            })json",
            R"json("columnFamilies": {
                "default": {"compression": "zstd", "cacheMode": "regular"}
            })json",
            R"json("columnFamilies": {
                "default": {"compression": "off", "cacheMode": "unknown"}
            })json",
            R"json("columnFamilies": {
                "default": {
                    "compression": "off", "cacheMode": "regular", "data": {"media": ""}
                }
            })json",
            R"json("columnFamilies": {
                "default": {
                    "compression": "off", "cacheMode": "regular", "unexpected": 1
                }
            })json",
        }) {
            const TString body = TStringBuilder() << R"json({
                "ts": [10, 20],
                "tableChanges": [{
                    "table": {
                        "schemaVersion": 3,
                        "primaryKeyColumnNames": ["key"],
                        "columns": {"key": {"type": "Uint64", "family": "archive"}},
                        )json" << fragment << R"json(
                    }
                }]
            })json";
            auto record = TChangeRecordBuilder()
                .WithBody(body)
                .Build();
            NKikimrReplication::TSchemaChange schema;
            TString error;
            UNIT_ASSERT_C(!record->TryGetSchemaChange(schema, error), body);
            UNIT_ASSERT_C(!error.empty(), body);
        }
    }

    TLightweightSchema::TCPtr MakeSchema() {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        schema->ValueColumns.emplace("alpha", TLightweightSchema::TColumn{
            7, NScheme::TTypeInfo(NScheme::NTypeIds::String)});
        schema->ValueColumns.emplace("zeta", TLightweightSchema::TColumn{
            3, NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)});
        return schema;
    }

    TString KeyBytes() {
        const ui64 key = 42;
        const auto cell = TCell::Make(key);
        return TSerializedCellVec::Serialize(TConstArrayRef<TCell>(&cell, 1));
    }

    TString ValueBytes() {
        const TString alpha = "alpha-value";
        const TString zeta = "zeta-value";
        const TCell cells[] = {
            TCell(zeta.data(), zeta.size()),
            TCell(alpha.data(), alpha.size()),
        };
        return TSerializedCellVec::Serialize(cells);
    }

    // Wire tags are name-sorted indexes: zeta is 1, alpha is 0.
    NKikimrChangeExchange::TChangeRecord MakeBinaryUpsert() {
        NKikimrChangeExchange::TChangeRecord proto;
        proto.SetStep(10);
        proto.SetTxId(20);
        auto& change = *proto.MutableCdcDataChange();
        change.MutableKey()->AddTags(0);
        change.MutableKey()->SetData(KeyBytes());
        auto& upsert = *change.MutableUpsert();
        upsert.AddTags(1);
        upsert.AddTags(0);
        upsert.SetData(ValueBytes());
        return proto;
    }

    Y_UNIT_TEST(BinaryUpsertMapsIndexesToDestinationTags) {
        const auto proto = MakeBinaryUpsert();
        auto record = TChangeRecordBuilder()
            .WithOrder(7)
            .WithSchema(MakeSchema())
            .WithBody(proto.SerializeAsString())
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcDataChange);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);

        TString error;
        UNIT_ASSERT_C(record->CheckData(error), error);

        const auto key = record->GetKey();
        UNIT_ASSERT_VALUES_EQUAL(key.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(key[0].AsValue<ui64>(), 42);

        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange apply;
        TMemoryPool pool(256);
        record->Serialize(apply, pool);
        UNIT_ASSERT_VALUES_EQUAL(apply.GetSourceOffset(), 7);
        UNIT_ASSERT_VALUES_EQUAL(apply.GetKey(), KeyBytes());
        UNIT_ASSERT(apply.HasUpsert());
        UNIT_ASSERT_VALUES_EQUAL(apply.GetUpsert().TagsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(apply.GetUpsert().GetTags(0), 3);
        UNIT_ASSERT_VALUES_EQUAL(apply.GetUpsert().GetTags(1), 7);
        UNIT_ASSERT_VALUES_EQUAL(apply.GetUpsert().GetData(), ValueBytes());
    }

    Y_UNIT_TEST(BinaryResetAndErase) {
        auto schema = MakeSchema();
        {
            auto proto = MakeBinaryUpsert();
            proto.MutableCdcDataChange()->MutableReset()->CopyFrom(proto.GetCdcDataChange().GetUpsert());
            proto.MutableCdcDataChange()->ClearUpsert();
            auto record = TChangeRecordBuilder()
                .WithSchema(schema)
                .WithBody(proto.SerializeAsString())
                .Build();
            TString error;
            UNIT_ASSERT_C(record->CheckData(error), error);
            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange apply;
            TMemoryPool pool(256);
            record->Serialize(apply, pool);
            UNIT_ASSERT(apply.HasReset());
            UNIT_ASSERT_VALUES_EQUAL(apply.GetReset().GetTags(0), 3);
            UNIT_ASSERT_VALUES_EQUAL(apply.GetReset().GetTags(1), 7);
            UNIT_ASSERT_VALUES_EQUAL(apply.GetReset().GetData(), ValueBytes());
        }
        {
            NKikimrChangeExchange::TChangeRecord proto;
            proto.MutableCdcDataChange()->MutableKey()->AddTags(0);
            proto.MutableCdcDataChange()->MutableKey()->SetData(KeyBytes());
            proto.MutableCdcDataChange()->MutableErase();
            auto record = TChangeRecordBuilder()
                .WithSchema(schema)
                .WithBody(proto.SerializeAsString())
                .Build();
            TString error;
            UNIT_ASSERT_C(record->CheckData(error), error);
            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange apply;
            TMemoryPool pool(256);
            record->Serialize(apply, pool);
            UNIT_ASSERT(apply.HasErase());
            UNIT_ASSERT_VALUES_EQUAL(apply.GetKey(), KeyBytes());
        }
    }

    Y_UNIT_TEST(BinaryHeartbeat) {
        NKikimrChangeExchange::TChangeRecord proto;
        proto.SetStep(10);
        proto.SetTxId(20);
        proto.MutableCdcHeartbeat();
        auto record = TChangeRecordBuilder()
            .WithBody(proto.SerializeAsString())
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
        TString error;
        UNIT_ASSERT(record->CheckData(error));
    }

    Y_UNIT_TEST(BinaryRejectsImagesAndMalformedRecords) {
        {
            auto proto = MakeBinaryUpsert();
            proto.MutableCdcDataChange()->MutableOldImage()->CopyFrom(proto.GetCdcDataChange().GetUpsert());
            auto record = TChangeRecordBuilder()
                .WithSchema(MakeSchema())
                .WithBody(proto.SerializeAsString())
                .Build();
            TString error;
            UNIT_ASSERT(!record->CheckData(error));
            UNIT_ASSERT_STRING_CONTAINS(error, "image columns are not applied");
        }
        {
            auto record = TChangeRecordBuilder()
                .WithBody(TString("\x07", 1))
                .Build();
            TString error;
            UNIT_ASSERT(!record->IsValidJson(error));
            UNIT_ASSERT_STRING_CONTAINS(error, "neither JSON nor a change-exchange protobuf");
        }
        {
            NKikimrChangeExchange::TChangeRecord proto;
            proto.MutableAsyncIndex();
            auto record = TChangeRecordBuilder()
                .WithBody(proto.SerializeAsString())
                .Build();
            TString error;
            UNIT_ASSERT(!record->IsValidJson(error));
            UNIT_ASSERT_STRING_CONTAINS(error, "unexpected protobuf CDC record kind");
        }
        {
            auto proto = MakeBinaryUpsert();
            auto record = TChangeRecordBuilder()
                .WithBody(proto.SerializeAsString())
                .Build();
            TString error;
            UNIT_ASSERT(!record->CheckData(error));
            UNIT_ASSERT_STRING_CONTAINS(error, "has no schema");
        }
        {
            auto proto = MakeBinaryUpsert();
            proto.MutableCdcDataChange()->MutableUpsert()->SetTags(0, 5);
            auto record = TChangeRecordBuilder()
                .WithSchema(MakeSchema())
                .WithBody(proto.SerializeAsString())
                .Build();
            TString error;
            UNIT_ASSERT(!record->CheckData(error));
            UNIT_ASSERT_STRING_CONTAINS(error, "value index is out of range");
        }
        {
            auto proto = MakeBinaryUpsert();
            const ui64 extra = 1;
            const TCell cells[] = {TCell::Make(ui64(42)), TCell::Make(extra)};
            proto.MutableCdcDataChange()->MutableKey()->SetData(TSerializedCellVec::Serialize(cells));
            auto record = TChangeRecordBuilder()
                .WithSchema(MakeSchema())
                .WithBody(proto.SerializeAsString())
                .Build();
            TString error;
            UNIT_ASSERT(!record->CheckData(error));
            UNIT_ASSERT_STRING_CONTAINS(error, "key cell count does not match");
        }
        {
            auto proto = MakeBinaryUpsert();
            proto.MutableCdcDataChange()->MutableUpsert()->ClearTags();
            auto record = TChangeRecordBuilder()
                .WithSchema(MakeSchema())
                .WithBody(proto.SerializeAsString())
                .Build();
            TString error;
            UNIT_ASSERT(!record->CheckData(error));
            UNIT_ASSERT_STRING_CONTAINS(error, "value cell count does not match tags");
        }
    }
}

}
