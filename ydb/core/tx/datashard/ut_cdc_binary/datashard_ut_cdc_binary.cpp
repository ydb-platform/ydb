#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/tx/datashard/change_record_cdc_serializer.h>

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/aclib/user_context.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/zstd/include/zstd.h>

#include <util/generic/ptr.h>

namespace NKikimr::NDataShard {
namespace {

using TCmdWrite = NKikimrClient::TPersQueuePartitionRequest::TCmdWrite;

TUserTable::TPtr MakeTable() {
    auto table = MakeIntrusive<TUserTable>();
    table->Columns.emplace(10, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "", "key", true));
    table->Columns.emplace(7, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "alpha", false));
    table->Columns.emplace(3, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "zeta", false));
    table->KeyColumnIds = {10};
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

// Cell order is zeta, then alpha. Name-sorted indexes are the other way around.
TString MakeValueBytes() {
    const TString zeta(32, 'z');
    const TString alpha(32, 'a');
    const TCell cells[] = {
        TCell(zeta.data(), zeta.size()),
        TCell(alpha.data(), alpha.size()),
    };
    return SerializeCells(cells);
}

TString MakeLargeValueBytes() {
    const TString zeta(2048, 'z');
    const TString alpha(2048, 'a');
    const TCell cells[] = {
        TCell(zeta.data(), zeta.size()),
        TCell(alpha.data(), alpha.size()),
    };
    return SerializeCells(cells);
}

NKikimrChangeExchange::TDataChange MakeUpsert(const TString& valueBytes, ui32 firstTag, ui32 secondTag) {
    NKikimrChangeExchange::TDataChange change;
    auto* key = change.MutableKey();
    key->AddTags(10);
    key->SetData(MakeKeyBytes());
    auto* upsert = change.MutableUpsert();
    upsert->AddTags(firstTag);
    upsert->AddTags(secondTag);
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

TChangeRecordSerializerOpts BinaryOpts(TUserTable::TCdcStream::ECompression compression, bool virtualTimestamps = false) {
    TChangeRecordSerializerOpts opts;
    opts.StreamFormat = NKikimrSchemeOp::ECdcStreamFormatBinary;
    opts.StreamMode = NKikimrSchemeOp::ECdcStreamModeUpdate;
    opts.VirtualTimestamps = virtualTimestamps;
    opts.Compression = compression;
    return opts;
}

NKikimrPQClient::TDataChunk ParseChunk(const TString& bytes) {
    NKikimrPQClient::TDataChunk chunk;
    UNIT_ASSERT(chunk.ParseFromString(bytes));
    return chunk;
}

TString MaybeDecompress(const NKikimrPQClient::TDataChunk& chunk, const TCmdWrite& cmd) {
    if (chunk.GetCodec() == 0) {
        UNIT_ASSERT(!cmd.HasUncompressedSize());
        return chunk.GetData();
    }
    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));
    UNIT_ASSERT(cmd.HasUncompressedSize());
    UNIT_ASSERT(cmd.GetUncompressedSize() > 0);
    TString raw = TString::Uninitialized(cmd.GetUncompressedSize());
    const size_t written = ZSTD_decompress(raw.begin(), raw.size(), chunk.GetData().data(), chunk.GetData().size());
    UNIT_ASSERT(!ZSTD_isError(written));
    UNIT_ASSERT_VALUES_EQUAL(written, static_cast<size_t>(cmd.GetUncompressedSize()));
    return raw;
}

void ExpectRewrittenUpsert(const NKikimrChangeExchange::TDataChange& change, const TString& valueBytes, bool withTimestamps) {
    UNIT_ASSERT_VALUES_EQUAL(change.GetKey().TagsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(change.GetKey().GetTags(0), 0);
    UNIT_ASSERT_VALUES_EQUAL(change.GetKey().GetData(), MakeKeyBytes());
    UNIT_ASSERT(change.HasUpsert());
    UNIT_ASSERT_VALUES_EQUAL(change.GetUpsert().TagsSize(), 2);
    // Source order was zeta (tag 3) then alpha (tag 7). Indexes follow column names.
    UNIT_ASSERT_VALUES_EQUAL(change.GetUpsert().GetTags(0), 1);
    UNIT_ASSERT_VALUES_EQUAL(change.GetUpsert().GetTags(1), 0);
    UNIT_ASSERT_VALUES_EQUAL(change.GetUpsert().GetData(), valueBytes);
    if (withTimestamps) {
        UNIT_ASSERT_VALUES_EQUAL(change.GetKey().GetData(), MakeKeyBytes());
    }
}

} // namespace

Y_UNIT_TEST_SUITE(CdcBinaryFormat) {
    Y_UNIT_TEST(RewritesTagsAndKeepsCellBytes) {
        const auto valueBytes = MakeValueBytes();
        const auto table = MakeTable();
        const auto record = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table, MakeUpsert(valueBytes, 3, 7).SerializeAsString());

        UNIT_ASSERT_VALUES_EQUAL(record->GetRawKeyPartitionKey(), MD5::Calc(TStringBuf(MakeKeyBytes())));

        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(BinaryOpts(
            NKikimrSchemeOp::ECdcStreamCompressionUnspecified)));
        TCmdWrite cmd;
        serializer->Serialize(cmd, *record);
        UNIT_ASSERT_STRING_CONTAINS(serializer->DebugString(*record), "binary CDC");

        const auto chunk = ParseChunk(cmd.GetData());
        UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 0);
        NKikimrChangeExchange::TChangeRecord proto;
        UNIT_ASSERT(proto.ParseFromString(chunk.GetData()));
        UNIT_ASSERT(!proto.HasStep());
        UNIT_ASSERT(!proto.HasTxId());
        UNIT_ASSERT(!proto.HasPathOwnerId());
        UNIT_ASSERT(!proto.HasLocalPathId());
        UNIT_ASSERT(proto.HasCdcDataChange());
        ExpectRewrittenUpsert(proto.GetCdcDataChange(), valueBytes, false);
    }

    Y_UNIT_TEST(VirtualTimestampsAreOptional) {
        const auto valueBytes = MakeValueBytes();
        const auto record = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, MakeTable(), MakeUpsert(valueBytes, 3, 7).SerializeAsString());
        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(BinaryOpts(
            NKikimrSchemeOp::ECdcStreamCompressionNone, true)));
        TCmdWrite cmd;
        serializer->Serialize(cmd, *record);
        const auto chunk = ParseChunk(cmd.GetData());
        NKikimrChangeExchange::TChangeRecord proto;
        UNIT_ASSERT(proto.ParseFromString(chunk.GetData()));
        UNIT_ASSERT_VALUES_EQUAL(proto.GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetTxId(), 20);
        ExpectRewrittenUpsert(proto.GetCdcDataChange(), valueBytes, true);
    }

    Y_UNIT_TEST(SchemaStaysJsonAndHeartbeatStaysRaw) {
        auto table = MakeTable();
        table->Columns.emplace(11, TUserTable::TUserColumn(
                                       NScheme::TTypeInfo(NScheme::NTypeIds::String), "", TString(800, 'n'), false));
        const auto schema = MakeRecord(TChangeRecord::EKind::CdcSchemaChange, table, {});
        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(BinaryOpts(
            NKikimrSchemeOp::ECdcStreamCompressionZstd)));

        TCmdWrite schemaCmd;
        serializer->Serialize(schemaCmd, *schema);
        UNIT_ASSERT(schemaCmd.HasSchemaChange());
        const auto schemaChunk = ParseChunk(schemaCmd.GetSchemaChange().GetData());
        const auto schemaJson = MaybeDecompress(schemaChunk, schemaCmd);
        UNIT_ASSERT(schemaJson.StartsWith("{"));
        UNIT_ASSERT_STRING_CONTAINS(schemaJson, "\"key\"");
        UNIT_ASSERT_STRING_CONTAINS(schemaJson, TString(800, 'n'));
        UNIT_ASSERT_VALUES_EQUAL(schemaChunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));

        const auto heartbeat = MakeRecord(TChangeRecord::EKind::CdcHeartbeat, table, {});
        TCmdWrite heartbeatCmd;
        serializer->Serialize(heartbeatCmd, *heartbeat);
        UNIT_ASSERT(heartbeatCmd.HasHeartbeat());
        UNIT_ASSERT(!heartbeatCmd.HasUncompressedSize());
        const auto heartbeatChunk = ParseChunk(heartbeatCmd.GetHeartbeat().GetData());
        UNIT_ASSERT_VALUES_EQUAL(heartbeatChunk.GetCodec(), 0);
        NKikimrChangeExchange::TChangeRecord proto;
        UNIT_ASSERT(proto.ParseFromString(heartbeatChunk.GetData()));
        UNIT_ASSERT(proto.HasCdcHeartbeat());
        UNIT_ASSERT_VALUES_EQUAL(proto.GetStep(), 10);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetTxId(), 20);
    }

    Y_UNIT_TEST(ZstdCompressesLargeRowsOfEitherFormat) {
        const auto valueBytes = MakeLargeValueBytes();
        const auto table = MakeTable();
        const auto record = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table, MakeUpsert(valueBytes, 3, 7).SerializeAsString());

        std::unique_ptr<IChangeRecordSerializer> binary(CreateChangeRecordSerializer(BinaryOpts(
            NKikimrSchemeOp::ECdcStreamCompressionZstd)));
        TCmdWrite binaryCmd;
        binary->Serialize(binaryCmd, *record);
        const auto binaryChunk = ParseChunk(binaryCmd.GetData());
        const auto binaryRaw = MaybeDecompress(binaryChunk, binaryCmd);
        UNIT_ASSERT(binaryRaw.size() >= CdcZstdMinPayloadBytes);
        NKikimrChangeExchange::TChangeRecord proto;
        UNIT_ASSERT(proto.ParseFromString(binaryRaw));
        ExpectRewrittenUpsert(proto.GetCdcDataChange(), valueBytes, false);

        const auto small = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table, MakeUpsert(MakeValueBytes(), 3, 7).SerializeAsString());
        TCmdWrite smallCmd;
        binary->Serialize(smallCmd, *small);
        const auto smallChunk = ParseChunk(smallCmd.GetData());
        UNIT_ASSERT_VALUES_EQUAL(smallChunk.GetCodec(), 0);
        UNIT_ASSERT(smallChunk.GetData().size() < CdcZstdMinPayloadBytes);

        TChangeRecordSerializerOpts jsonOpts;
        jsonOpts.StreamFormat = NKikimrSchemeOp::ECdcStreamFormatJson;
        jsonOpts.StreamMode = NKikimrSchemeOp::ECdcStreamModeUpdate;
        jsonOpts.Compression = NKikimrSchemeOp::ECdcStreamCompressionZstd;
        std::unique_ptr<IChangeRecordSerializer> json(CreateChangeRecordSerializer(jsonOpts));
        TCmdWrite jsonCmd;
        json->Serialize(jsonCmd, *record);
        const auto jsonChunk = ParseChunk(jsonCmd.GetData());
        const auto jsonRaw = MaybeDecompress(jsonChunk, jsonCmd);
        UNIT_ASSERT(jsonRaw.StartsWith("{"));
        UNIT_ASSERT_VALUES_EQUAL(jsonChunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));

        jsonOpts.Compression = NKikimrSchemeOp::ECdcStreamCompressionUnspecified;
        std::unique_ptr<IChangeRecordSerializer> plain(CreateChangeRecordSerializer(jsonOpts));
        TCmdWrite plainCmd;
        plain->Serialize(plainCmd, *record);
        const auto plainChunk = ParseChunk(plainCmd.GetData());
        UNIT_ASSERT_VALUES_EQUAL(plainChunk.GetCodec(), 0);
        UNIT_ASSERT(plainChunk.GetData().StartsWith("{"));
    }

    Y_UNIT_TEST(RejectsKeyAndTagMismatch) {
        const auto table = MakeTable();
        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(BinaryOpts(
            NKikimrSchemeOp::ECdcStreamCompressionUnspecified)));

        {
            NKikimrChangeExchange::TDataChange change = MakeUpsert(MakeValueBytes(), 3, 7);
            change.MutableKey()->SetTags(0, 99);
            const auto record = MakeRecord(TChangeRecord::EKind::CdcDataChange, table, change.SerializeAsString());
            TCmdWrite cmd;
            UNIT_ASSERT_EXCEPTION_CONTAINS(serializer->Serialize(cmd, *record), yexception, "key tag order");
        }
        {
            const auto record = MakeRecord(
                TChangeRecord::EKind::CdcDataChange, table, MakeUpsert(MakeValueBytes(), 3, 99).SerializeAsString());
            TCmdWrite cmd;
            UNIT_ASSERT_EXCEPTION_CONTAINS(serializer->Serialize(cmd, *record), yexception, "unknown column tag");
        }
    }
} // Y_UNIT_TEST_SUITE(CdcBinaryFormat)

} // namespace NKikimr::NDataShard
