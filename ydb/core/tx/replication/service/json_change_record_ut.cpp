#include "json_change_record.h"

#include <ydb/core/io_formats/json/json.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/tx/datashard/change_record_cdc_serializer.h>
#include <ydb/library/aclib/user_context.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/builder.h>

#include <cmath>

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

void CheckFloatingPointRoundTrip(bool reset) {
    constexpr ui32 keyTag = 1;
    constexpr ui32 floatTag = 2;
    constexpr ui32 doubleTag = 3;
    const NScheme::TTypeInfo keyType(NScheme::NTypeIds::Uint32);
    const NScheme::TTypeInfo floatType(NScheme::NTypeIds::Float);
    const NScheme::TTypeInfo doubleType(NScheme::NTypeIds::Double);

    auto sourceSchema = MakeIntrusive<NDataShard::TUserTable>();
    sourceSchema->KeyColumnIds = {keyTag};
    sourceSchema->KeyColumnTypes = {keyType};
    sourceSchema->Columns.emplace(keyTag, NDataShard::TUserTable::TUserColumn(keyType, "", "key", true));
    sourceSchema->Columns.emplace(floatTag, NDataShard::TUserTable::TUserColumn(floatType, "", "float_value"));
    sourceSchema->Columns.emplace(doubleTag, NDataShard::TUserTable::TUserColumn(doubleType, "", "double_value"));

    auto destinationSchema = MakeIntrusive<TLightweightSchema>();
    destinationSchema->KeyColumns = {keyType};
    destinationSchema->ValueColumns = {
        {"float_value", {floatTag, floatType}},
        {"double_value", {doubleTag, doubleType}},
    };

    THolder<NDataShard::IChangeRecordSerializer> serializer(NDataShard::CreateChangeRecordSerializer({
        .StreamFormat = NKikimrSchemeOp::ECdcStreamFormatJson,
        .StreamMode = NKikimrSchemeOp::ECdcStreamModeUpdate,
    }));

    for (const double expected : {-0.0, 0.0, -1.25, 1.25}) {
        NKikimrChangeExchange::TDataChange body;
        body.MutableKey()->AddTags(keyTag);
        body.MutableKey()->SetData(TSerializedCellVec::Serialize(TVector<TCell>{TCell::Make(ui32(1))}));
        auto* values = reset ? body.MutableReset() : body.MutableUpsert();
        values->AddTags(floatTag);
        values->AddTags(doubleTag);
        values->SetData(TSerializedCellVec::Serialize(TVector<TCell>{
            TCell::Make(static_cast<float>(expected)), TCell::Make(expected),
        }));

        auto source = NDataShard::TChangeRecordBuilder(NDataShard::TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithSchema(sourceSchema)
            .WithBody(body.SerializeAsString())
            .Build();
        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite command;
        serializer->Serialize(command, *source);
        NKikimrPQClient::TDataChunk chunk;
        UNIT_ASSERT(chunk.ParseFromString(command.GetData()));

        auto destination = TChangeRecordBuilder()
            .WithOrder(1)
            .WithSchema(destinationSchema)
            .WithBody(chunk.GetData())
            .Build();
        TString error;
        UNIT_ASSERT_C(destination->IsValidJson(error), error);
        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
        TMemoryPool pool(256);
        destination->Serialize(change, pool);

        const auto& updates = reset ? change.GetReset() : change.GetUpsert();
        TSerializedCellVec cells;
        UNIT_ASSERT(TSerializedCellVec::TryParse(updates.GetData(), cells));
        UNIT_ASSERT_VALUES_EQUAL(updates.TagsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(cells.GetCells().size(), 2);
        for (ui32 i = 0; i < updates.TagsSize(); ++i) {
            const auto tag = updates.GetTags(i);
            UNIT_ASSERT(tag == floatTag || tag == doubleTag);
            const auto& cell = cells.GetCells()[i];
            const double actual = tag == floatTag ? cell.AsValue<float>() : cell.AsValue<double>();
            UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, chunk.GetData());
            UNIT_ASSERT_VALUES_EQUAL_C(std::signbit(actual), std::signbit(expected), chunk.GetData());
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(JsonChangeRecord) {
    Y_UNIT_TEST(FloatingPointRoundTripUpdate) {
        CheckFloatingPointRoundTrip(false);
    }

    Y_UNIT_TEST(FloatingPointRoundTripReset) {
        CheckFloatingPointRoundTrip(true);
    }

    Y_UNIT_TEST_QUAD(YsonRoundTrip, Reset, VirtualTimestamps) {
        constexpr ui32 KeyTag = 1;
        constexpr ui32 YsonTag = 2;
        constexpr ui32 JsonTag = 3;
        const NScheme::TTypeInfo keyType(NScheme::NTypeIds::Uint32);
        const NScheme::TTypeInfo ysonType(NScheme::NTypeIds::Yson);
        const NScheme::TTypeInfo jsonType(NScheme::NTypeIds::Json);
        auto sourceSchema = MakeIntrusive<NDataShard::TUserTable>();
        sourceSchema->KeyColumnIds = {KeyTag};
        sourceSchema->KeyColumnTypes = {keyType};
        sourceSchema->Columns.emplace(KeyTag, NDataShard::TUserTable::TUserColumn(keyType, "", "key", true));
        sourceSchema->Columns.emplace(YsonTag, NDataShard::TUserTable::TUserColumn(ysonType, "", "value"));
        sourceSchema->Columns.emplace(JsonTag, NDataShard::TUserTable::TUserColumn(jsonType, "", "json"));
        auto destinationSchema = MakeIntrusive<TLightweightSchema>();
        destinationSchema->KeyColumns = {keyType};
        destinationSchema->ValueColumns = {{"value", {YsonTag, ysonType}}, {"json", {JsonTag, jsonType}}};
        THolder<NDataShard::IChangeRecordSerializer> serializer(NDataShard::CreateChangeRecordSerializer({
            .StreamFormat = NKikimrSchemeOp::ECdcStreamFormatJson,
            .StreamMode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .VirtualTimestamps = VirtualTimestamps,
        }));

        // Include binary YSON: uint64 marker followed by varint 1.
        TVector<TString> values = {
            R"(<a="first";>42)", "1u", "1.0", R"({plain=1u;meta=<x="nested";>2.0;})",
            "%nan", "%inf", "%-inf", "#", R"("1u")", R"("nan")", "%true", "[1u;1.0;#;]",
            R"({yson={update={value="MQ==";};};})", TString("\x06\x01", 2), "",
        };
        values.push_back(R"({"ключ"=["строка";"😀";"\x00";];})");
        const size_t validProjectionCount = values.size();
        for (const auto* bytes : {
            R"("\xff")", R"("\xc0\xaf")", R"("\xe2\x82")", R"("\xed\xa0\x80")",
            R"({nested=[{value="\xff";};];})", R"({nested=[{"\xff"=1;};];})",
            R"({"\xed\xa0\x80"=1;})", R"("\xf4\x90\x80\x80")",
        }) {
            values.push_back(bytes);
        }
        values.push_back(TString("\"\xff\"", 3));
        TString allBytes("\x01\x80\x04", 3); // Binary string marker and length 256.
        for (ui32 byte = 0; byte < 256; ++byte) {
            allBytes.push_back(static_cast<char>(byte));
        }
        values.push_back(allBytes);
        const TString jsonValue = R"("1u")";
        for (size_t index = 0; index < values.size(); ++index) {
            const auto& expected = values[index];
            NKikimrChangeExchange::TDataChange body;
            body.MutableKey()->AddTags(KeyTag);
            body.MutableKey()->SetData(TSerializedCellVec::Serialize(TVector<TCell>{TCell::Make(ui32(1))}));
            auto* value = Reset ? body.MutableReset() : body.MutableUpsert();
            value->AddTags(YsonTag);
            value->AddTags(JsonTag);
            value->SetData(TSerializedCellVec::Serialize(TVector<TCell>{
                expected.empty() ? TCell() : TCell(expected.data(), expected.size()),
                TCell(jsonValue.data(), jsonValue.size()),
            }));
            auto source = NDataShard::TChangeRecordBuilder(NDataShard::TChangeRecord::EKind::CdcDataChange)
                .WithOrder(1).WithStep(10).WithTxId(20).WithSchema(sourceSchema)
                .WithBody(body.SerializeAsString()).Build();
            NKikimrClient::TPersQueuePartitionRequest::TCmdWrite command;
            serializer->Serialize(command, *source);
            NKikimrPQClient::TDataChunk chunk;
            UNIT_ASSERT(chunk.ParseFromString(command.GetData()));
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(chunk.GetData(), &json));
            const TString section = Reset ? "reset" : "update";

            // Valid UTF-8 projections remain compatible; arbitrary bytes use metadata.
            NJson::TJsonValue legacy;
            if (!expected.empty()) {
                UNIT_ASSERT(NJson2Yson::DeserializeYsonAsJsonValue(expected, &legacy));
                UNIT_ASSERT_VALUES_EQUAL(Base64StrictDecode(json["yson"][section]["value"].GetString()), expected);
                UNIT_ASSERT(!json["yson"][section].Has("json"));
            } else {
                legacy.SetType(NJson::JSON_NULL);
                UNIT_ASSERT(!json.Has("yson"));
            }
            TStringStream legacyJson;
            NJson::WriteJson(&legacyJson, &legacy, NFormats::DefaultJsonWriterConfig());
            NJson::TJsonValue legacyProjection;
            const bool validProjection = index < validProjectionCount;
            UNIT_ASSERT_VALUES_EQUAL(NJson::ReadJsonTree(legacyJson.Str(), &legacyProjection), validProjection);
            if (validProjection) {
                UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(json[section]["value"]), NJson::WriteJson(legacyProjection));
            } else {
                UNIT_ASSERT(json[section]["value"].IsNull());
            }
            UNIT_ASSERT_VALUES_EQUAL(json[section]["json"].GetString(), "1u");
            auto destination = TChangeRecordBuilder().WithSchema(destinationSchema).WithBody(chunk.GetData()).Build();
            TString error;
            UNIT_ASSERT_C(destination->IsValidJson(error), error);
            UNIT_ASSERT_VALUES_EQUAL(destination->GetStep(), VirtualTimestamps ? 10 : 0);
            UNIT_ASSERT_VALUES_EQUAL(destination->GetTxId(), VirtualTimestamps ? 20 : 0);
            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
            TMemoryPool pool(256);
            destination->Serialize(change, pool);
            const auto& updates = Reset ? change.GetReset() : change.GetUpsert();
            TSerializedCellVec cells;
            UNIT_ASSERT(TSerializedCellVec::TryParse(updates.GetData(), cells));
            UNIT_ASSERT_VALUES_EQUAL(updates.TagsSize(), 2);
            for (ui32 i = 0; i < updates.TagsSize(); ++i) {
                const auto& cell = cells.GetCells()[i];
                if (updates.GetTags(i) == YsonTag) {
                    UNIT_ASSERT_VALUES_EQUAL(cell.IsNull(), expected.empty());
                    if (!expected.empty()) {
                        UNIT_ASSERT_VALUES_EQUAL_C(cell.AsBuf(), expected, chunk.GetData());
                    }
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(cell.AsBuf(), jsonValue);
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(YsonImageMetadata, InvalidUtf8) {
        const NScheme::TTypeInfo keyType(NScheme::NTypeIds::Uint32);
        const NScheme::TTypeInfo ysonType(NScheme::NTypeIds::Yson);
        auto schema = MakeIntrusive<NDataShard::TUserTable>();
        schema->KeyColumnIds = {1};
        schema->KeyColumnTypes = {keyType};
        schema->Columns.emplace(1, NDataShard::TUserTable::TUserColumn(keyType, "", "key", true));
        schema->Columns.emplace(2, NDataShard::TUserTable::TUserColumn(ysonType, "", "value"));
        schema->Columns.emplace(3, NDataShard::TUserTable::TUserColumn(ysonType, "", "null_value"));
        NKikimrChangeExchange::TDataChange body;
        body.MutableKey()->AddTags(1);
        body.MutableKey()->SetData(TSerializedCellVec::Serialize(TVector<TCell>{TCell::Make(ui32(1))}));
        body.MutableUpsert();
        const TString before = InvalidUtf8 ? R"({nested=[{"\xff"=1;};];})" : R"(<a="old";>1u)";
        const TString after = InvalidUtf8 ? R"({nested=["\xed\xa0\x80";];})" : "#";
        for (const auto& [image, bytes] : {
            std::pair{body.MutableOldImage(), before}, std::pair{body.MutableNewImage(), after},
        }) {
            image->AddTags(2);
            image->AddTags(3);
            image->SetData(TSerializedCellVec::Serialize(TVector<TCell>{TCell(bytes.data(), bytes.size()), TCell()}));
        }
        auto source = NDataShard::TChangeRecordBuilder(NDataShard::TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1).WithSchema(schema).WithBody(body.SerializeAsString()).Build();
        THolder<NDataShard::IChangeRecordSerializer> serializer(NDataShard::CreateChangeRecordSerializer({
            .StreamFormat = NKikimrSchemeOp::ECdcStreamFormatJson,
            .StreamMode = NKikimrSchemeOp::ECdcStreamModeNewAndOldImages,
        }));
        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite command;
        serializer->Serialize(command, *source);
        NKikimrPQClient::TDataChunk chunk;
        UNIT_ASSERT(chunk.ParseFromString(command.GetData()));
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(chunk.GetData(), &json));
        if (InvalidUtf8) {
            UNIT_ASSERT(json["oldImage"]["value"].IsNull());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(json["oldImage"]["value"].GetUIntegerRobust(), 1);
        }
        UNIT_ASSERT(json["newImage"]["value"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(Base64StrictDecode(json["yson"]["oldImage"]["value"].GetString()), before);
        UNIT_ASSERT_VALUES_EQUAL(Base64StrictDecode(json["yson"]["newImage"]["value"].GetString()), after);
        UNIT_ASSERT(!json["yson"]["oldImage"].Has("null_value"));
        UNIT_ASSERT(!json["yson"]["newImage"].Has("null_value"));
        UNIT_ASSERT(json["oldImage"]["null_value"].IsNull());
        UNIT_ASSERT(json["newImage"]["null_value"].IsNull());
        UNIT_ASSERT(json["update"].GetMap().empty());
        auto destination = TChangeRecordBuilder().WithBody(chunk.GetData()).Build();
        TString error;
        UNIT_ASSERT_C(destination->IsValidJson(error), error);
    }

    Y_UNIT_TEST(LegacyYsonRecords) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
        schema->ValueColumns = {{"value", {2, NScheme::TTypeInfo(NScheme::NTypeIds::Yson)}}};
        for (const auto* value : {"1", "1.5", R"("1u")", R"({"yson":{"update":{"value":"MQ=="}}})", "null"}) {
            auto record = TChangeRecordBuilder().WithSchema(schema)
                .WithBody(TStringBuilder() << R"({"key":[1],"update":{"value":)" << value << "}}")
                .Build();
            TString error;
            UNIT_ASSERT_C(record->IsValidJson(error), error);
            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
            TMemoryPool pool(256);
            record->Serialize(change, pool);
            TSerializedCellVec cells;
            UNIT_ASSERT(TSerializedCellVec::TryParse(change.GetUpsert().GetData(), cells));
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(value, &json));
            if (json.IsNull()) {
                UNIT_ASSERT(cells.GetCells()[0].IsNull());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(cells.GetCells()[0].AsBuf(), NJson2Yson::SerializeJsonValueAsYson(json));
            }
        }
    }

    Y_UNIT_TEST(MalformedYsonMetadata) {
        for (const auto* metadata : {
            "null", "[]", R"({"update":[]})", R"({"erase":{"value":"MQ=="}})",
            R"({"update":{"missing":"MQ=="}})", R"({"update":{"value":1}})",
            R"({"update":{"value":"!"}})", R"({"update":{"value":""}})",
            R"({"update":{"value":"ew=="}})",
        }) {
            auto record = TChangeRecordBuilder().WithBody(TStringBuilder()
                << R"({"key":[1],"update":{"value":1},"yson":)" << metadata << "}").Build();
            TString error;
            UNIT_ASSERT_C(!record->IsValidJson(error), metadata);
            UNIT_ASSERT_STRING_CONTAINS(error, "invalid YSON metadata");
        }
    }

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
}

}
