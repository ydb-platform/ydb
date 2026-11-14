#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/tx/datashard/change_record_cdc_serializer.h>

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/zstd/include/zstd.h>

namespace NKikimr::NDataShard {
namespace {

using TCmdWrite = NKikimrClient::TPersQueuePartitionRequest::TCmdWrite;

TUserTable::TPtr MakeTable() {
    auto table = MakeIntrusive<TUserTable>();
    table->Columns.emplace(10, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "", "key", true));
    table->Columns.emplace(7, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "alpha", false));
    table->Columns.emplace(3, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "zeta", false));
    table->KeyColumnIds = {10};
    table->KeyColumnTypes = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)};
    return table;
}

TString SerializeCells(TConstArrayRef<TCell> cells) {
    return TSerializedCellVec::Serialize(cells);
}

TString MakeKeyBytes() {
    const ui64 key = 42;
    const auto cell = TCell::Make(key);
    return SerializeCells(TConstArrayRef<TCell>(&cell, 1));
}

TString MakeValueBytes(size_t size) {
    const TString zeta(size, 'z');
    const TString alpha(size, 'a');
    const TCell cells[] = {
        TCell(zeta.data(), zeta.size()),
        TCell(alpha.data(), alpha.size()),
    };
    return SerializeCells(cells);
}

NKikimrChangeExchange::TDataChange MakeUpsert(const TString& valueBytes) {
    NKikimrChangeExchange::TDataChange change;
    auto* key = change.MutableKey();
    key->AddTags(10);
    key->SetData(MakeKeyBytes());
    auto* upsert = change.MutableUpsert();
    upsert->AddTags(3);
    upsert->AddTags(7);
    upsert->SetData(valueBytes);
    return change;
}

TIntrusivePtr<TChangeRecord> MakeRecord(TChangeRecord::EKind kind, TUserTable::TPtr table, const TString& body) {
    return TChangeRecordBuilder(kind)
        .WithOrder(1)
        .WithStep(10)
        .WithTxId(20)
        .WithSchemaVersion(3)
        .WithSchema(table)
        .WithBody(body)
        .Build();
}

TChangeRecordSerializerOpts JsonOpts(TUserTable::TCdcStream::ECompression compression, i32 level = 0) {
    TChangeRecordSerializerOpts opts;
    opts.StreamFormat = NKikimrSchemeOp::ECdcStreamFormatJson;
    opts.StreamMode = NKikimrSchemeOp::ECdcStreamModeUpdate;
    opts.Compression = compression;
    opts.CompressionLevel = level;
    return opts;
}

NKikimrPQClient::TDataChunk ParseChunk(const TString& bytes) {
    NKikimrPQClient::TDataChunk chunk;
    UNIT_ASSERT(chunk.ParseFromString(bytes));
    return chunk;
}

TString ReferenceCompress(TStringBuf payload, int level) {
    const size_t bound = ZSTD_compressBound(payload.size());
    UNIT_ASSERT(!ZSTD_isError(bound));
    TString compressed = TString::Uninitialized(bound);
    const size_t written = ZSTD_compress(
        compressed.begin(), compressed.size(), payload.data(), payload.size(), level);
    UNIT_ASSERT(!ZSTD_isError(written));
    compressed.resize(written);
    return compressed;
}

TString RawJson(const TChangeRecord& record) {
    std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(JsonOpts(
        NKikimrSchemeOp::ECdcStreamCompressionNone)));
    TCmdWrite cmd;
    serializer->Serialize(cmd, record);
    UNIT_ASSERT(!cmd.HasUncompressedSize());
    const auto chunk = ParseChunk(cmd.GetData());
    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 0);
    return chunk.GetData();
}

} // namespace

Y_UNIT_TEST_SUITE(CdcZstdCompression) {
    Y_UNIT_TEST(ConfigDefaultMatchesSkipThreshold) {
        NKikimrConfig::TDataShardConfig config;
        UNIT_ASSERT_VALUES_EQUAL(config.GetCdcZstdMinPayloadBytes(), 200);
        UNIT_ASSERT_VALUES_EQUAL(config.GetCdcZstdMinPayloadBytes(), DefaultCdcZstdMinPayloadBytes);
    }

    Y_UNIT_TEST(ShortPayloadStaysRaw) {
        const auto table = MakeTable();
        const auto record = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table, MakeUpsert(MakeValueBytes(1)).SerializeAsString());
        const auto raw = RawJson(*record);
        UNIT_ASSERT_LT(raw.size(), DefaultCdcZstdMinPayloadBytes);

        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(JsonOpts(
            NKikimrSchemeOp::ECdcStreamCompressionZstd)));
        TCmdWrite cmd;
        serializer->Serialize(cmd, *record);
        UNIT_ASSERT(!cmd.HasUncompressedSize());
        const auto chunk = ParseChunk(cmd.GetData());
        UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 0);
        UNIT_ASSERT_VALUES_EQUAL(chunk.GetData(), raw);
    }

    Y_UNIT_TEST(LargePayloadReusesContextAtFastLevel) {
        const auto table = MakeTable();
        const auto record = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table, MakeUpsert(MakeValueBytes(2048)).SerializeAsString());
        const auto raw = RawJson(*record);
        UNIT_ASSERT_GE(raw.size(), DefaultCdcZstdMinPayloadBytes);
        const auto expected = ReferenceCompress(raw, DefaultCdcZstdCompressionLevel);
        UNIT_ASSERT_LT(expected.size(), raw.size());

        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(JsonOpts(
            NKikimrSchemeOp::ECdcStreamCompressionZstd)));
        for (int i = 0; i < 2; ++i) {
            TCmdWrite cmd;
            serializer->Serialize(cmd, *record);
            UNIT_ASSERT_VALUES_EQUAL(cmd.GetUncompressedSize(), static_cast<i32>(raw.size()));
            const auto chunk = ParseChunk(cmd.GetData());
            UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));
            // A fresh ZSTD_compress frame matches a reused context. A context that
            // kept streaming state would produce different bytes on the second row.
            UNIT_ASSERT_VALUES_EQUAL(chunk.GetData(), expected);
        }

        const auto heartbeat = MakeRecord(TChangeRecord::EKind::CdcHeartbeat, table, {});
        TCmdWrite heartbeatCmd;
        serializer->Serialize(heartbeatCmd, *heartbeat);
        UNIT_ASSERT(!heartbeatCmd.HasUncompressedSize());
        UNIT_ASSERT(heartbeatCmd.HasHeartbeat());
        const auto heartbeatChunk = ParseChunk(heartbeatCmd.GetHeartbeat().GetData());
        UNIT_ASSERT_VALUES_EQUAL(heartbeatChunk.GetCodec(), 0);
    }

    Y_UNIT_TEST(ExplicitLevelOverridesDefault) {
        const auto table = MakeTable();
        const auto record = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table, MakeUpsert(MakeValueBytes(2048)).SerializeAsString());
        const auto raw = RawJson(*record);
        const auto fast = ReferenceCompress(raw, DefaultCdcZstdCompressionLevel);
        const auto level3 = ReferenceCompress(raw, 3);
        UNIT_ASSERT_VALUES_UNEQUAL(fast, level3);

        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(JsonOpts(
            NKikimrSchemeOp::ECdcStreamCompressionZstd, 3)));
        TCmdWrite cmd;
        serializer->Serialize(cmd, *record);
        UNIT_ASSERT_VALUES_EQUAL(cmd.GetUncompressedSize(), static_cast<i32>(raw.size()));
        const auto chunk = ParseChunk(cmd.GetData());
        UNIT_ASSERT_VALUES_EQUAL(chunk.GetData(), level3);
    }
}

}
