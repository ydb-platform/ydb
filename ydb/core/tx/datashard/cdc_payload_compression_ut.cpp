#include "cdc_payload_compression.h"
#include "change_record.h"
#include "change_record_cdc_serializer.h"

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/library/aclib/user_context.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/stream/zlib.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/hex.h>

namespace NKikimr::NDataShard {
namespace {

// Independent input generator: no product codec or policy logic is used.
TString Noise(size_t size) {
    ui64 state = 0x6a09e667f3bcc909ULL;
    TString result(size, '\0');
    for (auto& ch : result) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        ch = static_cast<char>(state & 255);
    }
    return result;
}

TString DecodeSdkGzip(const TString& data) {
    NYdb::NTopic::TGzipCodec gzip;
    // TGzipCodec overrides are private; dispatch through the public interface.
    const NYdb::NTopic::ICodec& codec = gzip;
    return TString(codec.Decompress(std::string(data.data(), data.size())));
}

TString ReferenceGzip(const TString& data) {
    TStringStream output;
    TZLibCompress compressor(&output, ZLib::GZip, 1);
    compressor.Write(data.data(), data.size());
    compressor.Finish();
    return output.Str();
}

NKikimrConfig::TDataShardConfig EnabledConfig() {
    NKikimrConfig::TDataShardConfig config;
    config.SetCdcPayloadCompression(NKikimrConfig::TDataShardConfig::CDC_PAYLOAD_COMPRESSION_GZIP);
    return config;
}

TUserTable::TPtr SerializerSchema() {
    TUserTable::TPtr schema = new TUserTable;
    schema->KeyColumnIds = {1};
    schema->KeyColumnTypes = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
    schema->Columns.emplace(1, TUserTable::TUserColumn(schema->KeyColumnTypes[0], "", "key", true));
    schema->Columns.emplace(2, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "text"));
    schema->Columns.emplace(3, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "binary"));
    schema->Columns.emplace(4, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "", "number"));
    schema->Columns.emplace(5, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Bool), "", "nullable"));
    schema->Columns.emplace(6, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Int64), "", "signed_number"));
    schema->Columns.emplace(7, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Bool), "", "boolean"));
    schema->Columns.emplace(8, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Json), "", "json"));
    NKikimrSchemeOp::TTableDescription description;
    schema->SetSchema(description);
    return schema;
}

NWilson::TTraceId CompressionTraceId() {
    const TString hex = "6A4ABE11C2D44B6576C8476461CC167F0000000000000000FFFF0000";
    NWilson::TTraceId::TSerializedTraceId serialized{};
    HexDecode(hex.c_str(), hex.size(), &serialized);
    return NWilson::TTraceId(serialized);
}

TIntrusivePtr<TChangeRecord> DataRecord(const TUserTable::TPtr& schema, const TString& text) {
    const ui32 key = 17;
    const ui64 number = 18446744073709551615ULL;
    const TString binary("\0\xff" "A", 3);
    const i64 signedNumber = -9223372036854775807LL - 1;
    const bool boolean = true;
    const TString json = R"({"nested":{"value":"Ж\n"},"array":[null,true,1]})";
    NKikimrChangeExchange::TDataChange body;
    body.MutableKey()->AddTags(1);
    body.MutableKey()->SetData(TSerializedCellVec::Serialize({TCell::Make(key)}));
    auto* update = body.MutableUpsert();
    for (ui32 tag : {2u, 3u, 4u, 5u, 6u, 7u, 8u}) {
        update->AddTags(tag);
    }
    update->SetData(TSerializedCellVec::Serialize({TCell(text.data(), text.size()),
        TCell(binary.data(), binary.size()), TCell::Make(number), TCell(),
        TCell::Make(signedNumber), TCell::Make(boolean), TCell(json.data(), json.size())}));
    return TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
        .WithOrder(29).WithStep(123456).WithTxId(456).WithPathId({1, 2})
        .WithSchemaVersion(9).WithSchema(schema).WithBody(body.SerializeAsString())
        .WithUserCtx(NACLib::TUserContextBuilder().WithUserSID("compression-user")
            .WithUserTraceId(CompressionTraceId()).Build()).Build();
}

NKikimrClient::TPersQueuePartitionRequest::TCmdWrite Serialize(
    const TChangeRecord& record, ECdcPayloadCompression compression)
{
    TChangeRecordSerializerOpts opts;
    opts.StreamFormat = NKikimrSchemeOp::ECdcStreamFormatJson;
    opts.StreamMode = NKikimrSchemeOp::ECdcStreamModeUpdate;
    opts.VirtualTimestamps = true;
    opts.UserSIDs = true;
    opts.TraceIds = true;
    opts.PayloadCompression = compression;
    THolder<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(opts));
    NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
    cmd.SetSourceId("source-must-survive");
    cmd.SetIgnoreQuotaDeadline(true);
    serializer->Serialize(cmd, record);
    return cmd;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(CdcPayloadCompression) {
    Y_UNIT_TEST(ConfigurationDefaultsRoundTrip) {
        NKikimrConfig::TDataShardConfig config;
        UNIT_ASSERT(!config.HasCdcPayloadCompression());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i32>(config.GetCdcPayloadCompression()),
            static_cast<i32>(NKikimrConfig::TDataShardConfig::CDC_PAYLOAD_COMPRESSION_DISABLED));
        NKikimrConfig::TDataShardConfig copy;
        UNIT_ASSERT(copy.ParseFromString(config.SerializeAsString()));
        UNIT_ASSERT(!copy.HasCdcPayloadCompression());
        UNIT_ASSERT(SelectCdcPayloadCompression(copy, R"({"supported_payload_codecs":["gzip"]})", {}) == ECdcPayloadCompression::Raw);
        config = EnabledConfig();
        UNIT_ASSERT(copy.ParseFromString(config.SerializeAsString()));
        UNIT_ASSERT(copy.HasCdcPayloadCompression());
        UNIT_ASSERT(SelectCdcPayloadCompression(copy, R"({"supported_payload_codecs":["gzip"]})", {}) == ECdcPayloadCompression::Gzip);
        UNIT_ASSERT(TChangeRecordSerializerOpts{}.PayloadCompression == ECdcPayloadCompression::Raw);
    }

    Y_UNIT_TEST(CapabilityDeclarationFailsClosed) {
        const auto config = EnabledConfig();
        const NKikimrPQ::TPQTabletConfig topic;
        const struct { TStringBuf Attributes; ECdcPayloadCompression Want; } cases[] = {
            {"", ECdcPayloadCompression::Raw},
            {"{", ECdcPayloadCompression::Raw},
            {"null", ECdcPayloadCompression::Raw},
            {"[]", ECdcPayloadCompression::Raw},
            {"42", ECdcPayloadCompression::Raw},
            {R"("gzip")", ECdcPayloadCompression::Raw},
            {"{}", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":"gzip"})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":null})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":{ "gzip":true }})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":[]})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":["GZIP"]})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":[" gzip"]})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":["zstd"]})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":[42,"gzip"]})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":["gzip",42]})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":["gzip",null]})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":["gzip",["gzip"]]})", ECdcPayloadCompression::Raw},
            {R"({"supported_payload_codecs":["gzip"]})", ECdcPayloadCompression::Gzip},
            {R"({"path":"/Root/dst","id":"1","supported_payload_codecs":["future","gzip"]})", ECdcPayloadCompression::Gzip},
        };
        for (const auto& test : cases) {
            UNIT_ASSERT_C(SelectCdcPayloadCompression(config, test.Attributes, topic) == test.Want, test.Attributes);
        }
    }

    Y_UNIT_TEST(ConfigurationAndTopicCodecDomains) {
        const TString declaration = R"({"supported_payload_codecs":["gzip"]})";
        NKikimrPQ::TPQTabletConfig topic;
        NKikimrConfig::TDataShardConfig config;
        UNIT_ASSERT(SelectCdcPayloadCompression(config, declaration, topic) == ECdcPayloadCompression::Raw);
        config.SetCdcPayloadCompression(NKikimrConfig::TDataShardConfig::CDC_PAYLOAD_COMPRESSION_DISABLED);
        UNIT_ASSERT(SelectCdcPayloadCompression(config, declaration, topic) == ECdcPayloadCompression::Raw);
        // Unknown proto2 enum value (field 34, wire type varint) must stay disabled
        // after serialization, rather than accidentally enabling a nonzero mode.
        UNIT_ASSERT(config.ParseFromString(TString("\x90\x02\x63", 3)));
        UNIT_ASSERT(SelectCdcPayloadCompression(config, declaration, topic) == ECdcPayloadCompression::Raw);
        NKikimrConfig::TDataShardConfig roundTrip;
        UNIT_ASSERT(roundTrip.ParseFromString(config.SerializeAsString()));
        UNIT_ASSERT(SelectCdcPayloadCompression(roundTrip, declaration, topic) == ECdcPayloadCompression::Raw);
        config = EnabledConfig();
        UNIT_ASSERT(SelectCdcPayloadCompression(config, declaration, topic) == ECdcPayloadCompression::Gzip);
        topic.MutableCodecs();
        UNIT_ASSERT(SelectCdcPayloadCompression(config, declaration, topic) == ECdcPayloadCompression::Gzip);
        for (const auto& ids : TVector<TVector<i64>>{{0}, {2}, {-1}, {10000}, {0, 2}, {1}, {0, 1, 2}}) {
            topic.MutableCodecs()->ClearIds();
            for (const auto id : ids) {
                topic.MutableCodecs()->AddIds(id);
            }
            // Explicit literal wire-ID expectations: public GZIP=2 is NOT stored GZIP=1.
            const auto expected = ids == TVector<i64>{1} || ids == TVector<i64>{0, 1, 2}
                ? ECdcPayloadCompression::Gzip : ECdcPayloadCompression::Raw;
            UNIT_ASSERT_C(SelectCdcPayloadCompression(config, declaration, topic) == expected, topic.ShortDebugString());
        }
    }

    Y_UNIT_TEST(ThresholdAndIncompressibleFallback) {
        for (const auto& payload : TVector<TString>{TString(), TString(1023, 'A'), Noise(1024), Noise(600 * 1024)}) {
            const TString before = payload;
            UNIT_ASSERT(!CompressCdcPayload(payload, ECdcPayloadCompression::Gzip));
            UNIT_ASSERT_VALUES_EQUAL(payload, before);
        }
        for (size_t size : {size_t(1024), size_t(16 * 1024), size_t(600 * 1024)}) {
            const TString payload(size, 'A');
            UNIT_ASSERT(!CompressCdcPayload(payload, ECdcPayloadCompression::Raw));
            const auto encoded = CompressCdcPayload(payload, ECdcPayloadCompression::Gzip);
            UNIT_ASSERT(encoded);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>((*encoded)[0]), 0x1f);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>((*encoded)[1]), 0x8b);
            UNIT_ASSERT_VALUES_EQUAL(DecodeSdkGzip(*encoded), payload);
            UNIT_ASSERT(encoded->size() < payload.size() / 4);
        }
    }

    Y_UNIT_TEST(OneEighthSavingsBoundary) {
        // Hand-calibrated deterministic 4096-byte inputs. At zlib level 1,
        // encoded lengths are 3585, 3584, 3583: savings 511, 512, 513.
        // Independent encoder preconditions expose toolchain drift explicitly.
        const auto noise = Noise(4096);
        for (size_t tail : {size_t(591), size_t(592), size_t(593)}) {
            const auto payload = noise.substr(0, 4096 - tail) + TString(tail, 'A');
            UNIT_ASSERT_VALUES_EQUAL(ReferenceGzip(payload).size(), 4176 - tail);
            const auto encoded = CompressCdcPayload(payload, ECdcPayloadCompression::Gzip);
            if (tail == 591) {
                UNIT_ASSERT(!encoded);
            } else {
                UNIT_ASSERT(encoded);
                UNIT_ASSERT_VALUES_EQUAL(DecodeSdkGzip(*encoded), payload);
            }
        }
    }

    Y_UNIT_TEST(SerializerPreservesTypesAndWireMetadata) {
        TString text;
        for (size_t i = 0; i < 8192; ++i) {
            text += "Ж";
        }
        const auto record = DataRecord(SerializerSchema(), text);
        auto raw = Serialize(*record, ECdcPayloadCompression::Raw);
        auto gzip = Serialize(*record, ECdcPayloadCompression::Gzip);
        NKikimrPQClient::TDataChunk rawChunk;
        NKikimrPQClient::TDataChunk gzipChunk;
        UNIT_ASSERT(rawChunk.ParseFromString(raw.GetData()));
        UNIT_ASSERT(gzipChunk.ParseFromString(gzip.GetData()));
        UNIT_ASSERT_VALUES_EQUAL(rawChunk.GetCodec(), 0);
        UNIT_ASSERT_VALUES_EQUAL(gzipChunk.GetCodec(), 1);
        UNIT_ASSERT(!raw.HasUncompressedSize());
        UNIT_ASSERT(gzip.HasUncompressedSize());
        UNIT_ASSERT_VALUES_EQUAL(gzip.GetUncompressedSize(), rawChunk.GetData().size());
        UNIT_ASSERT_VALUES_EQUAL(DecodeSdkGzip(gzipChunk.GetData()), rawChunk.GetData());
        UNIT_ASSERT(gzip.GetData().size() < raw.GetData().size() / 4);
        Cerr << "CDC compression serializer fixture: raw_payload_bytes=" << rawChunk.GetData().size()
            << " gzip_payload_bytes=" << gzipChunk.GetData().size()
            << " raw_envelope_bytes=" << raw.GetData().size()
            << " gzip_envelope_bytes=" << gzip.GetData().size() << Endl;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(rawChunk.GetData(), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["key"][0].GetUInteger(), 17);
        UNIT_ASSERT_VALUES_EQUAL(json["update"]["text"].GetString(), text);
        UNIT_ASSERT_VALUES_EQUAL(json["update"]["binary"].GetString(), "AP9B");
        UNIT_ASSERT_VALUES_EQUAL(json["update"]["number"].GetUInteger(), 18446744073709551615ULL);
        UNIT_ASSERT(json["update"]["nullable"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(json["update"]["signed_number"].GetInteger(), -9223372036854775807LL - 1);
        UNIT_ASSERT(json["update"]["boolean"].IsBoolean());
        UNIT_ASSERT(json["update"]["boolean"].GetBoolean());
        UNIT_ASSERT(json["update"]["json"].IsMap());
        UNIT_ASSERT_VALUES_EQUAL(json["update"]["json"]["nested"]["value"].GetString(), "Ж\n");
        const auto& array = json["update"]["json"]["array"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(array.size(), 3);
        UNIT_ASSERT(array[0].IsNull());
        UNIT_ASSERT(array[1].IsBoolean());
        UNIT_ASSERT(array[1].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(array[2].GetUInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["user"].GetString(), "compression-user");
        UNIT_ASSERT_VALUES_EQUAL(json["traceId"].GetString(), "6A4ABE11C2D44B6576C8476461CC167F");
        UNIT_ASSERT_VALUES_EQUAL(json["ts"][0].GetUInteger(), 123456);
        UNIT_ASSERT_VALUES_EQUAL(json["ts"][1].GetUInteger(), 456);
        UNIT_ASSERT_VALUES_EQUAL(raw.GetSeqNo(), 29);
        UNIT_ASSERT_VALUES_EQUAL(raw.GetCreateTimeMS(), 123456);
        // Compare every envelope/command field after removing only the intended changes.
        gzipChunk.SetData(rawChunk.GetData());
        gzipChunk.SetCodec(0);
        UNIT_ASSERT_VALUES_EQUAL(gzipChunk.SerializeAsString(), rawChunk.SerializeAsString());
        gzip.SetData(raw.GetData());
        gzip.ClearUncompressedSize();
        UNIT_ASSERT_VALUES_EQUAL(gzip.SerializeAsString(), raw.SerializeAsString());
    }

    Y_UNIT_TEST(ControlRecordsRemainRaw) {
        const auto schema = SerializerSchema();
        // Ensure the schema record itself crosses the compression threshold.
        for (ui32 tag = 100; tag < 230; ++tag) {
            schema->Columns.emplace(tag, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "",
                TStringBuilder() << "repetitive_schema_column_" << tag));
        }
        for (const auto kind : {TChangeRecord::EKind::CdcSchemaChange, TChangeRecord::EKind::CdcHeartbeat}) {
            const auto record = TChangeRecordBuilder(kind).WithOrder(31).WithStep(777).WithTxId(888)
                .WithSchemaVersion(9).WithSchema(schema).Build();
            const auto raw = Serialize(*record, ECdcPayloadCompression::Raw);
            const auto gzip = Serialize(*record, ECdcPayloadCompression::Gzip);
            UNIT_ASSERT_VALUES_EQUAL(gzip.SerializeAsString(), raw.SerializeAsString());
            UNIT_ASSERT(!gzip.HasData());
            UNIT_ASSERT(!gzip.HasUncompressedSize());
            NKikimrPQClient::TDataChunk chunk;
            if (kind == TChangeRecord::EKind::CdcSchemaChange) {
                UNIT_ASSERT(gzip.HasSchemaChange());
                UNIT_ASSERT(!gzip.HasHeartbeat());
                UNIT_ASSERT_VALUES_EQUAL(gzip.GetSchemaChange().GetStep(), 777);
                UNIT_ASSERT_VALUES_EQUAL(gzip.GetSchemaChange().GetTxId(), 888);
                UNIT_ASSERT(chunk.ParseFromString(gzip.GetSchemaChange().GetData()));
                UNIT_ASSERT(chunk.GetData().size() >= 1024);
            } else {
                UNIT_ASSERT(gzip.HasHeartbeat());
                UNIT_ASSERT(!gzip.HasSchemaChange());
                UNIT_ASSERT_VALUES_EQUAL(gzip.GetHeartbeat().GetStep(), 777);
                UNIT_ASSERT_VALUES_EQUAL(gzip.GetHeartbeat().GetTxId(), 888);
                UNIT_ASSERT(chunk.ParseFromString(gzip.GetHeartbeat().GetData()));
                UNIT_ASSERT_VALUES_EQUAL(chunk.GetData(), R"({"resolved":[777,888]})");
            }
            UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 0);
        }
    }
}

} // namespace NKikimr::NDataShard
