#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/tx/datashard/change_record_cdc_serializer.h>

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/aclib/user_context.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/streams/zstd/zstd.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/zstd/include/zstd.h>

#include <util/generic/ptr.h>
#include <util/stream/str.h>

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

TString MakeShortValueBytes() {
    const TString zeta("z");
    const TString alpha("a");
    const TCell cells[] = {
        TCell(zeta.data(), zeta.size()),
        TCell(alpha.data(), alpha.size()),
    };
    return SerializeCells(cells);
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

TString MaybeDecompress(const NKikimrPQClient::TDataChunk& chunk) {
    if (chunk.GetCodec() == 0) {
        return chunk.GetData();
    }
    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));
    TStringInput input(chunk.GetData());
    TZstdDecompress decompressor(&input);
    return decompressor.ReadAll();
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
            NKikimrSchemeOp::ECdcStreamCompressionNone)));
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
        const auto schemaJson = MaybeDecompress(schemaChunk);
        UNIT_ASSERT(schemaJson.StartsWith("{"));
        UNIT_ASSERT_STRING_CONTAINS(schemaJson, "\"key\"");
        UNIT_ASSERT_STRING_CONTAINS(schemaJson, TString(800, 'n'));
        UNIT_ASSERT_VALUES_EQUAL(schemaChunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));

        const auto heartbeat = MakeRecord(TChangeRecord::EKind::CdcHeartbeat, table, {});
        TCmdWrite heartbeatCmd;
        serializer->Serialize(heartbeatCmd, *heartbeat);
        UNIT_ASSERT(heartbeatCmd.HasHeartbeat());
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
        UNIT_ASSERT_VALUES_EQUAL(binaryChunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));
        const auto binaryRaw = MaybeDecompress(binaryChunk);
        NKikimrChangeExchange::TChangeRecord proto;
        UNIT_ASSERT(proto.ParseFromString(binaryRaw));
        ExpectRewrittenUpsert(proto.GetCdcDataChange(), valueBytes, false);

        const auto smallValue = MakeValueBytes();
        const auto small = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table, MakeUpsert(smallValue, 3, 7).SerializeAsString());
        TCmdWrite smallPlainCmd;
        std::unique_ptr<IChangeRecordSerializer> smallPlain(CreateChangeRecordSerializer(BinaryOpts(
            NKikimrSchemeOp::ECdcStreamCompressionNone)));
        smallPlain->Serialize(smallPlainCmd, *small);
        const auto smallPlain = ParseChunk(smallPlainCmd.GetData()).GetData();
        TCmdWrite smallCmd;
        binary->Serialize(smallCmd, *small);
        const auto smallChunk = ParseChunk(smallCmd.GetData());
        if (smallPlain.size() < CdcZstdMinPayloadBytes) {
            UNIT_ASSERT_VALUES_EQUAL(smallChunk.GetCodec(), 0);
            UNIT_ASSERT_VALUES_EQUAL(smallChunk.GetData(), smallPlain);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(smallChunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));
        }
        NKikimrChangeExchange::TChangeRecord smallProto;
        UNIT_ASSERT(smallProto.ParseFromString(MaybeDecompress(smallChunk)));
        ExpectRewrittenUpsert(smallProto.GetCdcDataChange(), smallValue, false);

        TChangeRecordSerializerOpts jsonOpts;
        jsonOpts.StreamFormat = NKikimrSchemeOp::ECdcStreamFormatJson;
        jsonOpts.StreamMode = NKikimrSchemeOp::ECdcStreamModeUpdate;
        jsonOpts.Compression = NKikimrSchemeOp::ECdcStreamCompressionZstd;
        std::unique_ptr<IChangeRecordSerializer> json(CreateChangeRecordSerializer(jsonOpts));
        TCmdWrite jsonCmd;
        json->Serialize(jsonCmd, *record);
        const auto jsonChunk = ParseChunk(jsonCmd.GetData());
        const auto jsonRaw = MaybeDecompress(jsonChunk);
        UNIT_ASSERT(jsonRaw.StartsWith("{"));
        UNIT_ASSERT_VALUES_EQUAL(jsonChunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));

        jsonOpts.Compression = NKikimrSchemeOp::ECdcStreamCompressionNone;
        std::unique_ptr<IChangeRecordSerializer> plain(CreateChangeRecordSerializer(jsonOpts));
        TCmdWrite plainCmd;
        plain->Serialize(plainCmd, *record);
        const auto plainChunk = ParseChunk(plainCmd.GetData());
        UNIT_ASSERT_VALUES_EQUAL(plainChunk.GetCodec(), 0);
        UNIT_ASSERT(plainChunk.GetData().StartsWith("{"));
    }

    Y_UNIT_TEST(SkipsShortPayloadsAndReusesFastContext) {
        const auto table = MakeTable();
        const auto shortRecord = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table,
            MakeUpsert(MakeShortValueBytes(), 3, 7).SerializeAsString());
        const auto longRecord = MakeRecord(
            TChangeRecord::EKind::CdcDataChange, table,
            MakeUpsert(MakeLargeValueBytes(), 3, 7).SerializeAsString());

        auto rawOf = [&](const TChangeRecord& record) {
            std::unique_ptr<IChangeRecordSerializer> plain(CreateChangeRecordSerializer(BinaryOpts(
                NKikimrSchemeOp::ECdcStreamCompressionNone)));
            TCmdWrite cmd;
            plain->Serialize(cmd, record);
            return ParseChunk(cmd.GetData()).GetData();
        };
        const auto shortRaw = rawOf(*shortRecord);
        const auto longRaw = rawOf(*longRecord);
        UNIT_ASSERT_LT(shortRaw.size(), CdcZstdMinPayloadBytes);
        UNIT_ASSERT_GE(longRaw.size(), CdcZstdMinPayloadBytes);

        // One serializer for every record: the zstd context has to survive the first call.
        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(BinaryOpts(
            NKikimrSchemeOp::ECdcStreamCompressionZstd)));

        TCmdWrite firstShort;
        serializer->Serialize(firstShort, *shortRecord);
        const auto firstShortChunk = ParseChunk(firstShort.GetData());
        UNIT_ASSERT_VALUES_EQUAL(firstShortChunk.GetCodec(), 0);
        UNIT_ASSERT_VALUES_EQUAL(firstShortChunk.GetData(), shortRaw);

        TCmdWrite firstLong;
        serializer->Serialize(firstLong, *longRecord);
        const auto firstLongChunk = ParseChunk(firstLong.GetData());
        UNIT_ASSERT_VALUES_EQUAL(firstLongChunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));
        UNIT_ASSERT_VALUES_EQUAL(
            firstLongChunk.GetData(), ReferenceCompress(longRaw, DefaultCdcZstdCompressionLevel));
        UNIT_ASSERT_LT(firstLongChunk.GetData().size(), longRaw.size());

        TCmdWrite secondLong;
        serializer->Serialize(secondLong, *longRecord);
        UNIT_ASSERT_VALUES_EQUAL(ParseChunk(secondLong.GetData()).GetData(), firstLongChunk.GetData());

        TCmdWrite secondShort;
        serializer->Serialize(secondShort, *shortRecord);
        const auto secondShortChunk = ParseChunk(secondShort.GetData());
        UNIT_ASSERT_VALUES_EQUAL(secondShortChunk.GetCodec(), 0);
        UNIT_ASSERT_VALUES_EQUAL(secondShortChunk.GetData(), shortRaw);

        auto level3 = BinaryOpts(NKikimrSchemeOp::ECdcStreamCompressionZstd);
        level3.CompressionLevel = 3;
        std::unique_ptr<IChangeRecordSerializer> slower(CreateChangeRecordSerializer(level3));
        TCmdWrite level3Cmd;
        slower->Serialize(level3Cmd, *longRecord);
        const auto level3Chunk = ParseChunk(level3Cmd.GetData());
        UNIT_ASSERT_VALUES_EQUAL(level3Chunk.GetCodec(), static_cast<i64>(NPersQueueCommon::ZSTD));
        UNIT_ASSERT_VALUES_EQUAL(level3Chunk.GetData(), ReferenceCompress(longRaw, 3));
        UNIT_ASSERT(level3Chunk.GetData() != firstLongChunk.GetData());
    }

    Y_UNIT_TEST(RejectsKeyAndTagMismatch) {
        const auto table = MakeTable();
        std::unique_ptr<IChangeRecordSerializer> serializer(CreateChangeRecordSerializer(BinaryOpts(
            NKikimrSchemeOp::ECdcStreamCompressionNone)));

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
