#include "json_change_record.h"

#include <ydb/core/io_formats/json/json.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
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
#include <limits>

namespace NKikimr::NReplication::NService {

namespace {

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

    Y_UNIT_TEST_QUAD(CdcJsonSerialization, Reset, Debezium) {
        const NScheme::TTypeInfo keyType(NScheme::NTypeIds::Uint32);
        auto schema = MakeIntrusive<NDataShard::TUserTable>();
        schema->KeyColumnIds = {1};
        schema->KeyColumnTypes = {keyType};
        schema->Columns.emplace(1, NDataShard::TUserTable::TUserColumn(keyType, "", "key", true));
        schema->Columns.emplace(2, NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Float), "", "float_value"));
        schema->Columns.emplace(3, NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Double), "", "double_value"));
        schema->Columns.emplace(4, NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Json), "", "json"));
        schema->Columns.emplace(5, NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "text"));
        THolder<NDataShard::IChangeRecordSerializer> serializer(NDataShard::CreateChangeRecordSerializer({
            .StreamFormat = Debezium ? NKikimrSchemeOp::ECdcStreamFormatDebeziumJson : NKikimrSchemeOp::ECdcStreamFormatJson,
            .StreamMode = Debezium ? NKikimrSchemeOp::ECdcStreamModeNewAndOldImages : NKikimrSchemeOp::ECdcStreamModeUpdate,
        }));
        const TString nested = R"({"array":[-0.0,{"negative":-0.0},0,0.0,-1,1.2345678901234567,true,null,18446744073709551615,-9223372036854775808],"escaped\"key":"-0\n\"\\"})";
        const TString text = "-0\n\"\\строка";
        const auto writeScalar = [](const NJson::TJsonValue& value) {
            TStringStream out;
            NJson::WriteJson(&out, &value, NFormats::DefaultJsonWriterConfig());
            return out.Str();
        };

        for (const double value : {
            -0.0, 0.0, -1.25, 1.25, 1.2345678901234567, std::nextafter(1.0, 2.0),
            std::numeric_limits<double>::min(), std::numeric_limits<double>::max(),
            std::numeric_limits<double>::denorm_min(),
            static_cast<double>(std::numeric_limits<float>::min()),
            static_cast<double>(std::numeric_limits<float>::max()),
            static_cast<double>(std::numeric_limits<float>::denorm_min()),
            std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
            std::numeric_limits<double>::quiet_NaN(),
        }) {
            float floatValue;
            if (value > std::numeric_limits<float>::max()) {
                floatValue = std::numeric_limits<float>::infinity();
            } else if (value < -std::numeric_limits<float>::max()) {
                floatValue = -std::numeric_limits<float>::infinity();
            } else {
                floatValue = static_cast<float>(value);
            }

            NKikimrChangeExchange::TDataChange body;
            body.MutableKey()->AddTags(1);
            body.MutableKey()->SetData(TSerializedCellVec::Serialize(TVector<TCell>{TCell::Make(ui32(1))}));
            auto* values = Reset ? body.MutableReset() : body.MutableUpsert();
            for (const ui32 tag : {2, 3, 4, 5}) {
                values->AddTags(tag);
            }
            values->SetData(TSerializedCellVec::Serialize(TVector<TCell>{
                TCell::Make(floatValue), TCell::Make(value),
                TCell(nested.data(), nested.size()), TCell(text.data(), text.size()),
            }));
            if (Debezium) {
                *body.MutableOldImage() = *values;
                *body.MutableNewImage() = *values;
            }
            auto source = NDataShard::TChangeRecordBuilder(NDataShard::TChangeRecord::EKind::CdcDataChange)
                .WithOrder(1).WithSchema(schema).WithBody(body.SerializeAsString()).Build();
            NKikimrClient::TPersQueuePartitionRequest::TCmdWrite command;
            serializer->Serialize(command, *source);
            NKikimrPQClient::TDataChunk chunk;
            UNIT_ASSERT(chunk.ParseFromString(command.GetData()));

            // Other scalars retain the default precision, escaping and NaN/Inf policy.
            for (const auto& [name, number] : {
                std::pair{"float_value", static_cast<double>(floatValue)}, std::pair{"double_value", value},
            }) {
                const TString expected = number == 0.0 && std::signbit(number)
                    ? "-0.0" : writeScalar(NJson::TJsonValue(number));
                const TString field = TStringBuilder() << '"' << name << "\":" << expected;
                UNIT_ASSERT_C(chunk.GetData().Contains(field), chunk.GetData());
            }
            const TString textField = TStringBuilder() << "\"text\":" << writeScalar(NJson::TJsonValue(text));
            UNIT_ASSERT_C(chunk.GetData().Contains(textField), chunk.GetData());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(chunk.GetData(), &json));
            const auto& update = Debezium ? json["payload"]["after"] : json[Reset ? "reset" : "update"];
            if (Debezium) {
                UNIT_ASSERT_VALUES_EQUAL(chunk.MessageMetaSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(chunk.GetMessageMeta(0).key(), "__key");
                NJson::TJsonValue key;
                UNIT_ASSERT(NJson::ReadJsonTree(chunk.GetMessageMeta(0).value(), &key));
                UNIT_ASSERT_VALUES_EQUAL(key["payload"]["key"].GetUInteger(), 1);
                UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(json["payload"]["before"]), NJson::WriteJson(update));
            }
            UNIT_ASSERT_VALUES_EQUAL(update["text"].GetString(), text);
            const auto& array = update["json"]["array"];
            for (const auto* zero : {&array[0], &array[1]["negative"]}) {
                UNIT_ASSERT_VALUES_EQUAL(zero->GetType(), NJson::JSON_DOUBLE);
                UNIT_ASSERT_VALUES_EQUAL(zero->GetDouble(), 0.0);
                UNIT_ASSERT(std::signbit(zero->GetDouble()));
            }
            NJson::TJsonValue expectedNested;
            UNIT_ASSERT(NJson::ReadJsonTree(nested, &expectedNested));
            UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(update["json"]), NJson::WriteJson(expectedNested));
        }
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
        UNIT_ASSERT(destination);
    }

    Y_UNIT_TEST(LegacyYsonRecords) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
        schema->ValueColumns = {{"value", {2, NScheme::TTypeInfo(NScheme::NTypeIds::Yson)}}};
        for (const auto* value : {"1", "1.5", R"("1u")", R"({"yson":{"update":{"value":"MQ=="}}})", "null"}) {
            auto record = TChangeRecordBuilder().WithSchema(schema)
                .WithBody(TStringBuilder() << R"({"key":[1],"update":{"value":)" << value << "}}")
                .Build();
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
            const TString body = TStringBuilder()
                << R"({"key":[1],"update":{"value":1},"yson":)" << metadata << "}";
            NJson::TJsonValue json;
            TString error;
            UNIT_ASSERT_C(!TChangeRecordBuilder::ParseJsonBody(body, json, error), metadata);
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

    Y_UNIT_TEST(DataChangeDoubleKeyPrecision) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.emplace_back(NScheme::NTypeIds::Double);

        const std::pair<TStringBuf, double> cases[] = {
            {"93.829127966695111", 93.829127966695111},
            {"-93.829127966695111", -93.829127966695111},
            {"9.3829127966695111e1", 93.829127966695111},
            {"2.2250738585072014e-308", 2.2250738585072014e-308},
            {"4.9406564584124654e-324", 4.9406564584124654e-324},
            {"1.7976931348623157e308", 1.7976931348623157e308},
        };
        for (const auto& [text, expected] : cases) {
            auto record = TChangeRecordBuilder()
                .WithBody(TStringBuilder() << R"({"key":[)" << text << R"(], "erase":{}})")
                .WithSchema(schema)
                .Build();

            TMemoryPool pool(256);
            const auto key = record->GetKey(pool);
            UNIT_ASSERT_VALUES_EQUAL(key.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL_C(key[0].AsValue<double>(), expected, text);

            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
            record->Serialize(change, pool);
            const TSerializedCellVec serializedKey(change.GetKey());
            UNIT_ASSERT_VALUES_EQUAL(serializedKey.GetCells().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL_C(serializedKey.GetCells()[0].AsValue<double>(), expected, text);
        }
    }

    Y_UNIT_TEST(DataChangeDoubleValuePrecision) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.emplace_back(NScheme::NTypeIds::Uint64);
        schema->ValueColumns["value"] = {2, NScheme::TTypeInfo(NScheme::NTypeIds::Double)};

        for (const auto* operation : {"update", "reset"}) {
            auto record = TChangeRecordBuilder()
                .WithBody(TStringBuilder() << R"({"key":[1], ")" << operation
                    << R"(":{"value":93.829127966695111}})")
                .WithSchema(schema)
                .Build();

            TMemoryPool pool(256);
            NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
            record->Serialize(change, pool);
            const auto& updates = change.HasUpsert() ? change.GetUpsert() : change.GetReset();
            UNIT_ASSERT_VALUES_EQUAL(updates.TagsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(updates.GetTags(0), 2);
            const TSerializedCellVec values(updates.GetData());
            UNIT_ASSERT_VALUES_EQUAL(values.GetCells().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL_C(values.GetCells()[0].AsValue<double>(), 93.829127966695111, operation);
        }
    }

    Y_UNIT_TEST(DataChangeIntegerTypes) {
        auto schema = MakeIntrusive<TLightweightSchema>();
        schema->KeyColumns.emplace_back(NScheme::NTypeIds::Uint64);
        schema->ValueColumns["value"] = {2, NScheme::TTypeInfo(NScheme::NTypeIds::Yson)};

        auto record = TChangeRecordBuilder()
            .WithBody(R"({"key":[1], "update":{"value":[
                1, 4294967295, 4294967296, 9223372036854775807,
                9223372036854775808, 18446744073709551615,
                -2147483648, -2147483649, -9223372036854775808,
                18446744073709551616, -9223372036854775809
            ]}})")
            .WithSchema(schema)
            .Build();

        NJson::TJsonValue expected(NJson::JSON_ARRAY);
        for (const i64 value : {i64(1), i64(4294967295), i64(4294967296), Max<i64>()}) {
            expected.AppendValue(value);
        }
        expected.AppendValue(ui64(9223372036854775808ull));
        expected.AppendValue(Max<ui64>());
        for (const i64 value : {i64(-2147483648), i64(-2147483649), Min<i64>()}) {
            expected.AppendValue(value);
        }
        expected.AppendValue(18446744073709551616.0);
        expected.AppendValue(-9223372036854775809.0);

        TMemoryPool pool(256);
        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange change;
        record->Serialize(change, pool);
        const TSerializedCellVec values(change.GetUpsert().GetData());
        UNIT_ASSERT_VALUES_EQUAL(values.GetCells().size(), 1);
        // YSON distinguishes signed integers, unsigned integers and doubles.
        UNIT_ASSERT_VALUES_EQUAL(values.GetCells()[0].AsBuf(), NJson2Yson::SerializeJsonValueAsYson(expected));
    }

    Y_UNIT_TEST(InvalidJson) {
        for (const auto* body : {
            "", "{", R"({"key":[1],})", R"({'key':[1]})", R"({key:[1]})",
            R"({"key":[01]})", R"({"key":[NaN]})", R"({"key":[1e999]})",
            R"({"key":[1]} {"key":[2]})", R"({/*comment*/"key":[1]})",
            R"({"key":["\uD800"]})", "{\"key\":[\"\xC0\xAF\"]}",
        }) {
            NJson::TJsonValue json;
            TString error;
            UNIT_ASSERT_C(!TChangeRecordBuilder::ParseJsonBody(body, json, error), body);
            UNIT_ASSERT_VALUES_EQUAL(error, "cannot parse JSON");
        }
    }

    Y_UNIT_TEST(Heartbeat) {
        auto record = TChangeRecordBuilder()
            .WithBody(R"({"resolved":[10,20]})")
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(record->GetKind(), TChangeRecord::EKind::CdcHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(record->GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(record->GetTxId(), 20);
    }
}

}
