#include "json_change_record.h"

#include <ydb/core/change_exchange/binary_cdc.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>

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

    TLightweightSchema::TCPtr MakeBinarySchema() {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        schema->ValueColumns.emplace("b", TLightweightSchema::TColumn{
            static_cast<NTable::TTag>(10),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
        });
        schema->ValueColumns.emplace("a", TLightweightSchema::TColumn{
            static_cast<NTable::TTag>(11),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });
        return schema;
    }

    TString MakeBinaryData(NChangeExchange::EBinaryCdcOp op, bool nullA, ui64 step, ui64 txId) {
        const ui64 key = 7;
        const ui64 valueB = 42;
        const TString valueA = "hi";

        NChangeExchange::TBinaryCdcHeader header;
        header.Version = NChangeExchange::BinaryCdcVersion;
        header.Kind = NChangeExchange::EBinaryCdcKind::Data;
        header.Op = op;
        header.KeyCount = 1;
        header.ValueCount = op == NChangeExchange::EBinaryCdcOp::Erase ? 0 : 2;
        if (step || txId) {
            header.Flags = static_cast<ui8>(NChangeExchange::EBinaryCdcFlag::HasTs);
            header.Step = step;
            header.TxId = txId;
        }

        NChangeExchange::TBinaryCdcWriter writer;
        writer.Start(header);
        writer.AddKey(TCell::Make(key), NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        if (op != NChangeExchange::EBinaryCdcOp::Erase) {
            // Non-key columns are indexed in name order: "a" is 0, "b" is 1.
            if (nullA) {
                writer.AddValue(0, TCell(), NScheme::TTypeInfo(NScheme::NTypeIds::Utf8));
            } else {
                writer.AddValue(0, TCell(valueA.data(), valueA.size()), NScheme::TTypeInfo(NScheme::NTypeIds::Utf8));
            }
            writer.AddValue(1, TCell::Make(valueB), NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        }
        return writer.Finish();
    }

    Y_UNIT_TEST(BinaryData) {
        auto record = TChangeRecordBuilder()
            .WithBody(MakeBinaryData(NChangeExchange::EBinaryCdcOp::Upsert, false, 10, 20))
            .WithSchema(MakeBinarySchema())
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcDataChange);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);

        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
        TMemoryPool pool(256);
        record->Serialize(change, pool);
        UNIT_ASSERT(change.HasUpsert());
        UNIT_ASSERT_VALUES_EQUAL(change.GetUpsert().TagsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(change.GetUpsert().GetTags(0), 11);
        UNIT_ASSERT_VALUES_EQUAL(change.GetUpsert().GetTags(1), 10);

        TSerializedCellVec key;
        UNIT_ASSERT(TSerializedCellVec::TryParse(TString(change.GetKey()), key));
        ui64 keyValue = 0;
        TString error;
        UNIT_ASSERT(key.GetCells()[0].ToValue(keyValue, error));
        UNIT_ASSERT_VALUES_EQUAL(keyValue, 7);

        TSerializedCellVec values;
        UNIT_ASSERT(TSerializedCellVec::TryParse(TString(change.GetUpsert().GetData()), values));
        UNIT_ASSERT_VALUES_EQUAL(TString(values.GetCells()[0].AsBuf()), "hi");
        ui64 valueB = 0;
        UNIT_ASSERT(values.GetCells()[1].ToValue(valueB, error));
        UNIT_ASSERT_VALUES_EQUAL(valueB, 42);

        const auto keyCells = record->GetKey();
        UNIT_ASSERT_VALUES_EQUAL(keyCells.size(), 1);
        UNIT_ASSERT(keyCells[0].ToValue(keyValue, error));
        UNIT_ASSERT_VALUES_EQUAL(keyValue, 7);
    }

    Y_UNIT_TEST(BinaryNullAndErase) {
        auto schema = MakeBinarySchema();
        auto nullRecord = TChangeRecordBuilder()
            .WithBody(MakeBinaryData(NChangeExchange::EBinaryCdcOp::Upsert, true, 0, 0))
            .WithSchema(schema)
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(nullRecord->GetStep(), 0);

        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
        TMemoryPool pool(256);
        nullRecord->Serialize(change, pool);
        TSerializedCellVec values;
        UNIT_ASSERT(TSerializedCellVec::TryParse(TString(change.GetUpsert().GetData()), values));
        UNIT_ASSERT(values.GetCells()[0].IsNull());

        auto erase = TChangeRecordBuilder()
            .WithBody(MakeBinaryData(NChangeExchange::EBinaryCdcOp::Erase, false, 3, 4))
            .WithSchema(schema)
            .Build();
        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange erased;
        erase->Serialize(erased, pool);
        UNIT_ASSERT(erased.HasErase());
        UNIT_ASSERT_VALUES_EQUAL(erase->GetStep(), 3);
        UNIT_ASSERT_VALUES_EQUAL(erase->GetTxId(), 4);

        auto reset = TChangeRecordBuilder()
            .WithBody(MakeBinaryData(NChangeExchange::EBinaryCdcOp::Reset, false, 1, 2))
            .WithSchema(schema)
            .Build();
        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange resetChange;
        reset->Serialize(resetChange, pool);
        UNIT_ASSERT(resetChange.HasReset());
        UNIT_ASSERT_VALUES_EQUAL(resetChange.GetReset().GetTags(0), 11);
    }

    Y_UNIT_TEST(BinaryNullKey) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        schema->KeyColumns.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8));
        schema->ValueColumns.emplace("v", TLightweightSchema::TColumn{
            static_cast<NTable::TTag>(4),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
        });

        const ui64 key = 7;
        const TString text = "ab";
        const ui32 value = 5;
        auto write = [&](bool nullFirst, bool nullSecond) {
            NChangeExchange::TBinaryCdcHeader header;
            header.Version = NChangeExchange::BinaryCdcVersion;
            header.Kind = NChangeExchange::EBinaryCdcKind::Data;
            header.Op = NChangeExchange::EBinaryCdcOp::Upsert;
            header.KeyCount = 2;
            header.ValueCount = 1;
            if (nullFirst || nullSecond) {
                header.Flags = static_cast<ui8>(NChangeExchange::EBinaryCdcFlag::NullKeys);
            }
            NChangeExchange::TBinaryCdcWriter writer;
            writer.Start(header);
            writer.AddKey(nullFirst ? TCell() : TCell::Make(key), schema->KeyColumns[0]);
            writer.AddKey(nullSecond ? TCell() : TCell(text.data(), text.size()), schema->KeyColumns[1]);
            writer.AddValue(0, TCell::Make(value), NScheme::TTypeInfo(NScheme::NTypeIds::Uint32));
            return writer.Finish();
        };

        auto check = [&](const TString& body, bool nullFirst, bool nullSecond) {
            auto record = TChangeRecordBuilder()
                .WithBody(body)
                .WithSchema(schema)
                .Build();
            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
            TMemoryPool pool(256);
            record->Serialize(change, pool);
            TSerializedCellVec keys;
            UNIT_ASSERT(TSerializedCellVec::TryParse(TString(change.GetKey()), keys));
            UNIT_ASSERT_VALUES_EQUAL(keys.GetCells()[0].IsNull(), nullFirst);
            UNIT_ASSERT_VALUES_EQUAL(keys.GetCells()[1].IsNull(), nullSecond);
            if (!nullFirst) {
                ui64 parsed = 0;
                TString error;
                UNIT_ASSERT(keys.GetCells()[0].ToValue(parsed, error));
                UNIT_ASSERT_VALUES_EQUAL(parsed, key);
            }
            if (!nullSecond) {
                UNIT_ASSERT_VALUES_EQUAL(TString(keys.GetCells()[1].AsBuf()), text);
            }
            TSerializedCellVec values;
            UNIT_ASSERT(TSerializedCellVec::TryParse(TString(change.GetUpsert().GetData()), values));
            ui32 parsedValue = 0;
            TString error;
            UNIT_ASSERT(values.GetCells()[0].ToValue(parsedValue, error));
            UNIT_ASSERT_VALUES_EQUAL(parsedValue, value);
        };

        const TString plain = write(false, false);
        NChangeExchange::TBinaryCdcReader plainReader;
        TString error;
        UNIT_ASSERT_C(plainReader.Parse(plain, error), error);
        UNIT_ASSERT(!plainReader.Header().Has(NChangeExchange::EBinaryCdcFlag::NullKeys));
        check(plain, false, false);
        check(write(true, false), true, false);
        check(write(false, true), false, true);
        check(write(true, true), true, true);

        TString bad = write(true, false);
        // The null marker of the first key is the first byte after the 8-byte header.
        UNIT_ASSERT_VALUES_EQUAL(static_cast<unsigned char>(bad[8]), 1);
        bad[8] = 2;
        NChangeExchange::TBinaryCdcReader reader;
        UNIT_ASSERT_C(reader.Parse(bad, error), error);
        TVector<TCell> cells;
        UNIT_ASSERT(!reader.ReadKeys(schema->KeyColumns, cells, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "key null marker");

        TString heartbeat(8, '\0');
        heartbeat[0] = 1;
        heartbeat[1] = static_cast<char>(NChangeExchange::EBinaryCdcKind::Heartbeat);
        heartbeat[3] = static_cast<char>(NChangeExchange::EBinaryCdcFlag::NullKeys);
        UNIT_ASSERT(!reader.Parse(heartbeat, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "null key flag requires key cells");
    }

    Y_UNIT_TEST(BinaryHeartbeat) {
        NChangeExchange::TBinaryCdcHeader header;
        header.Version = NChangeExchange::BinaryCdcVersion;
        header.Kind = NChangeExchange::EBinaryCdcKind::Heartbeat;
        header.Flags = static_cast<ui8>(NChangeExchange::EBinaryCdcFlag::HasTs);
        header.Step = 10;
        header.TxId = 20;
        NChangeExchange::TBinaryCdcWriter writer;
        writer.Start(header);

        auto record = TChangeRecordBuilder()
            .WithBody(writer.Finish())
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
    }

    Y_UNIT_TEST(BinarySchemaUnwrap) {
        const TString json = R"json({
            "tableChanges": [{
                "table": {
                    "schemaVersion": 2,
                    "columns": {"key": {"type": "Uint64"}},
                    "primaryKeyColumnNames": ["key"]
                }
            }],
            "ts": [10, 20]
        })json";
        NChangeExchange::TBinaryCdcHeader header;
        header.Version = NChangeExchange::BinaryCdcVersion;
        header.Kind = NChangeExchange::EBinaryCdcKind::Schema;
        NChangeExchange::TBinaryCdcWriter writer;
        writer.Start(header);
        writer.SetSchemaJson(json);

        auto record = TChangeRecordBuilder()
            .WithBody(writer.Finish())
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcSchemaChange);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);

        NKikimrReplication::TSchemaChange schema;
        TString error;
        UNIT_ASSERT_C(record->TryGetSchemaChange(schema, error), error);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetSourceSchemaVersion(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(0).GetName(), "key");
    }

    Y_UNIT_TEST(BinaryRejectsUnknownVersion) {
        TString body(8, '\0');
        body[0] = 2;
        body[1] = 1;
        auto record = TChangeRecordBuilder()
            .WithBody(std::move(body))
            .Build();
        TString error;
        UNIT_ASSERT(!record->IsValidJson(error));
        UNIT_ASSERT_STRING_CONTAINS(error, "unsupported binary CDC version");
    }
}

}
