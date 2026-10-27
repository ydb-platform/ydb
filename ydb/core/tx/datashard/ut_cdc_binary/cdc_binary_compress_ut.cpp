#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/tx/datashard/change_record_cdc_serializer.h>

#include <ydb/core/change_exchange/binary_cdc.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/aclib/user_context.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/streams/zstd/zstd.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

#include <memory>

namespace NKikimr::NDataShard {
namespace {

TString ZstdDecompress(TStringBuf src) {
    TString data(src);
    TStringInput in(data);
    TZstdDecompress decompress(&in);
    return decompress.ReadAll();
}

TUserTable::TPtr MakeTable() {
    auto table = MakeIntrusive<TUserTable>();
    table->KeyColumnIds = {1};
    table->KeyColumnTypes.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
    table->Columns.emplace(1, TUserTable::TUserColumn(
        NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), {}, "key", true));
    table->Columns.emplace(2, TUserTable::TUserColumn(
        NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), {}, "value", false));
    NKikimrSchemeOp::TTableDescription description;
    table->SetSchema(description);
    return table;
}

TString MakeUpsertBody(ui64 key, TStringBuf value) {
    NKikimrChangeExchange::TDataChange change;
    auto& keyCells = *change.MutableKey();
    keyCells.AddTags(1);
    keyCells.SetData(TSerializedCellVec::Serialize({TCell::Make(key)}));

    auto& upsert = *change.MutableUpsert();
    upsert.AddTags(2);
    const TCell cell(value.data(), value.size());
    upsert.SetData(TSerializedCellVec::Serialize({cell}));
    return change.SerializeAsString();
}

TString MakeEraseBody(ui64 key) {
    NKikimrChangeExchange::TDataChange change;
    auto& keyCells = *change.MutableKey();
    keyCells.AddTags(1);
    keyCells.SetData(TSerializedCellVec::Serialize({TCell::Make(key)}));
    change.MutableErase();
    return change.SerializeAsString();
}

struct TStoredPayload {
    i64 Codec = 0;
    TString Payload;
};

TStoredPayload ReadPayload(const NKikimrClient::TPersQueuePartitionRequest::TCmdWrite& cmd) {
    TString serialized;
    if (cmd.HasHeartbeat()) {
        serialized = cmd.GetHeartbeat().GetData();
    } else if (cmd.HasSchemaChange()) {
        serialized = cmd.GetSchemaChange().GetData();
    } else {
        serialized = cmd.GetData();
    }

    NKikimrPQClient::TDataChunk chunk;
    UNIT_ASSERT(chunk.ParseFromString(serialized));

    TStoredPayload stored;
    stored.Codec = chunk.GetCodec();
    if (stored.Codec == static_cast<i64>(NPersQueueCommon::ZSTD)) {
        stored.Payload = ZstdDecompress(chunk.GetData());
        UNIT_ASSERT(cmd.HasUncompressedSize());
        UNIT_ASSERT_VALUES_EQUAL(cmd.GetUncompressedSize(), static_cast<i32>(stored.Payload.size()));
        UNIT_ASSERT(chunk.GetData().size() < stored.Payload.size());
    } else {
        UNIT_ASSERT_VALUES_EQUAL(stored.Codec, static_cast<i64>(NPersQueueCommon::RAW));
        UNIT_ASSERT(!cmd.HasUncompressedSize());
        stored.Payload = chunk.GetData();
    }
    return stored;
}

std::unique_ptr<IChangeRecordSerializer> MakeSerializer(NKikimrSchemeOp::ECdcStreamFormat format,
        bool virtualTimestamps = true,
        NKikimrSchemeOp::ECdcStreamCompression compression = NKikimrSchemeOp::ECdcStreamCompressionUnspecified)
{
    TChangeRecordSerializerOpts opts;
    opts.StreamFormat = format;
    opts.VirtualTimestamps = virtualTimestamps;
    opts.Compression = compression;
    return std::unique_ptr<IChangeRecordSerializer>(CreateChangeRecordSerializer(opts));
}

} // namespace

Y_UNIT_TEST_SUITE(BinaryCdcCompression) {
    Y_UNIT_TEST(CompressesRepetitivePayload) {
        const TString value(1024, 'a');
        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithStep(10)
            .WithTxId(20)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, value))
            .Build();

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatBinary, true,
            NKikimrSchemeOp::ECdcStreamCompressionZstd)->Serialize(cmd, *record);

        const auto stored = ReadPayload(cmd);
        UNIT_ASSERT_VALUES_EQUAL(stored.Codec, static_cast<i64>(NPersQueueCommon::ZSTD));

        NChangeExchange::TBinaryCdcReader reader;
        TString error;
        UNIT_ASSERT_C(reader.Parse(stored.Payload, error), error);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(reader.Header().Kind), static_cast<int>(NChangeExchange::EBinaryCdcKind::Data));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(reader.Header().Op), static_cast<int>(NChangeExchange::EBinaryCdcOp::Upsert));
        UNIT_ASSERT(reader.Header().Has(NChangeExchange::EBinaryCdcFlag::HasTs));
        UNIT_ASSERT(!reader.Header().Has(NChangeExchange::EBinaryCdcFlag::NullKeys));
        UNIT_ASSERT_VALUES_EQUAL(reader.Header().Step, 10);
        UNIT_ASSERT_VALUES_EQUAL(reader.Header().TxId, 20);

        const TVector<NScheme::TTypeInfo> keyTypes{NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)};
        const TVector<NScheme::TTypeInfo> valueTypes{NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)};
        TVector<TCell> keys;
        UNIT_ASSERT_C(reader.ReadKeys(keyTypes, keys, error), error);
        ui64 key = 0;
        UNIT_ASSERT(keys[0].ToValue(key, error));
        UNIT_ASSERT_VALUES_EQUAL(key, 7);

        TVector<ui16> indexes;
        TVector<TCell> cells;
        UNIT_ASSERT_C(reader.ReadIndexed(1, valueTypes, indexes, cells, error), error);
        UNIT_ASSERT_VALUES_EQUAL(indexes[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(TString(cells[0].AsBuf()), value);
        UNIT_ASSERT(reader.Rest().empty());
    }

    Y_UNIT_TEST(LeavesIncompressibleAndTinyRecordsRaw) {
        const TString value = "q7mN2pLx9VcR4sT8wY1aZ6bH3kJ0eF5unQ8rG2tP6yM1cB9vL4xS7hW0jK5fA3dE";
        auto table = MakeTable();
        auto upsert = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithSchema(table)
            .WithBody(MakeUpsertBody(7, value))
            .Build();
        auto erase = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(2)
            .WithSchema(table)
            .WithBody(MakeEraseBody(7))
            .Build();
        auto heartbeat = TChangeRecordBuilder(TChangeRecord::EKind::CdcHeartbeat)
            .WithOrder(3)
            .WithStep(10)
            .WithTxId(20)
            .Build();

        // These payloads are under CdcZstdMinPayloadBytes, and a heartbeat is never compressed.
        auto serializer = MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatBinary, false,
            NKikimrSchemeOp::ECdcStreamCompressionZstd);
        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite upsertCmd;
        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite eraseCmd;
        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite heartbeatCmd;
        serializer->Serialize(upsertCmd, *upsert);
        serializer->Serialize(eraseCmd, *erase);
        serializer->Serialize(heartbeatCmd, *heartbeat);

        UNIT_ASSERT_VALUES_EQUAL(ReadPayload(upsertCmd).Codec, static_cast<i64>(NPersQueueCommon::RAW));
        UNIT_ASSERT_VALUES_EQUAL(ReadPayload(eraseCmd).Codec, static_cast<i64>(NPersQueueCommon::RAW));
        const auto beat = ReadPayload(heartbeatCmd);
        UNIT_ASSERT_VALUES_EQUAL(beat.Codec, static_cast<i64>(NPersQueueCommon::RAW));

        NChangeExchange::TBinaryCdcReader reader;
        TString error;
        UNIT_ASSERT_C(reader.Parse(beat.Payload, error), error);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(reader.Header().Kind), static_cast<int>(NChangeExchange::EBinaryCdcKind::Heartbeat));
        UNIT_ASSERT_VALUES_EQUAL(reader.Header().Step, 10);
        UNIT_ASSERT_VALUES_EQUAL(reader.Header().TxId, 20);
    }

    Y_UNIT_TEST(JsonFormatStaysRaw) {
        const TString value(1024, 'a');
        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithStep(10)
            .WithTxId(20)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, value))
            .Build();

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatJson)->Serialize(cmd, *record);
        const auto stored = ReadPayload(cmd);
        UNIT_ASSERT_VALUES_EQUAL(stored.Codec, static_cast<i64>(NPersQueueCommon::RAW));
        UNIT_ASSERT(stored.Payload.Contains("aaaa"));
    }

    Y_UNIT_TEST(RepetitivePayloadStaysRawWithoutCompression) {
        const TString value(1024, 'a');
        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, value))
            .Build();

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatBinary, false)->Serialize(cmd, *record);
        const auto stored = ReadPayload(cmd);
        UNIT_ASSERT_VALUES_EQUAL(stored.Codec, static_cast<i64>(NPersQueueCommon::RAW));
        UNIT_ASSERT(stored.Payload.size() > value.size());
    }

    Y_UNIT_TEST(JsonCompressesWhenZstdRequested) {
        const TString value(1024, 'a');
        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithStep(10)
            .WithTxId(20)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, value))
            .Build();

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatJson, true,
            NKikimrSchemeOp::ECdcStreamCompressionZstd)->Serialize(cmd, *record);
        const auto stored = ReadPayload(cmd);
        UNIT_ASSERT_VALUES_EQUAL(stored.Codec, static_cast<i64>(NPersQueueCommon::ZSTD));
        UNIT_ASSERT(stored.Payload.Contains("aaaa"));
        UNIT_ASSERT(stored.Payload.size() >= CdcZstdMinPayloadBytes);
    }

    Y_UNIT_TEST(JsonShortRecordStaysRawWhenZstdRequested) {
        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, "hi"))
            .Build();

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatJson, false,
            NKikimrSchemeOp::ECdcStreamCompressionZstd)->Serialize(cmd, *record);
        const auto stored = ReadPayload(cmd);
        UNIT_ASSERT_VALUES_EQUAL(stored.Codec, static_cast<i64>(NPersQueueCommon::RAW));
        UNIT_ASSERT(stored.Payload.size() < CdcZstdMinPayloadBytes);
        UNIT_ASSERT(stored.Payload.Contains("hi"));
    }

    Y_UNIT_TEST(SkipsZstdBelowMinPayload) {
        const TString value(128, 'a');
        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, value))
            .Build();

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatBinary, false,
            NKikimrSchemeOp::ECdcStreamCompressionZstd)->Serialize(cmd, *record);
        const auto stored = ReadPayload(cmd);
        UNIT_ASSERT(stored.Payload.size() < CdcZstdMinPayloadBytes);
        UNIT_ASSERT_VALUES_EQUAL(stored.Codec, static_cast<i64>(NPersQueueCommon::RAW));
        UNIT_ASSERT(stored.Payload.Contains(value));
    }

    Y_UNIT_TEST(LeavesLongIncompressibleRaw) {
        TString value(1024, '\0');
        ui32 state = 0xA5A5A5A5u;
        for (size_t i = 0; i < value.size(); ++i) {
            state = state * 1664525u + 1013904223u;
            value[i] = static_cast<char>(state >> 24);
        }
        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, value))
            .Build();

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatBinary, false,
            NKikimrSchemeOp::ECdcStreamCompressionZstd)->Serialize(cmd, *record);
        const auto stored = ReadPayload(cmd);
        UNIT_ASSERT(stored.Payload.size() >= CdcZstdMinPayloadBytes);
        UNIT_ASSERT_VALUES_EQUAL(stored.Codec, static_cast<i64>(NPersQueueCommon::RAW));
    }

    Y_UNIT_TEST(SerializerReusesColumnMapAcrossRows) {
        auto table = MakeTable();
        auto serializer = MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatBinary, false);
        auto write = [&](ui64 key, TStringBuf value) {
            auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
                .WithOrder(key)
                .WithSchema(table)
                .WithBody(MakeUpsertBody(key, value))
                .Build();
            NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
            serializer->Serialize(cmd, *record);
            return ReadPayload(cmd).Payload;
        };

        const auto first = write(1, "alpha");
        const auto second = write(2, "beta");
        UNIT_ASSERT(first.Contains("alpha"));
        UNIT_ASSERT(!first.Contains("beta"));
        UNIT_ASSERT(second.Contains("beta"));
        UNIT_ASSERT(!second.Contains("alpha"));
    }

    Y_UNIT_TEST(RawKeyPartitionHashCoversEmbeddedZeros) {
        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, "v"))
            .Build();
        const auto& key = record->GetDataChange().GetKey().GetData();
        UNIT_ASSERT(key.Contains('\0'));
        const auto raw = record->GetRawKeyPartitionKey();
        UNIT_ASSERT_VALUES_EQUAL(raw, MD5::Calc(TStringBuf(key.data(), key.size())));
        UNIT_ASSERT_VALUES_UNEQUAL(raw, MD5::Calc(TStringBuf(key.data())));
        UNIT_ASSERT_VALUES_UNEQUAL(raw, record->GetPartitionKey());

        auto again = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(2)
            .WithSchema(MakeTable())
            .WithBody(MakeUpsertBody(7, "other"))
            .Build();
        UNIT_ASSERT_VALUES_EQUAL(again->GetRawKeyPartitionKey(), raw);
    }

    Y_UNIT_TEST(WritesNullPrimaryKey) {
        auto table = MakeIntrusive<TUserTable>();
        table->KeyColumnIds = {1, 3};
        table->KeyColumnTypes.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        table->KeyColumnTypes.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8));
        table->Columns.emplace(1, TUserTable::TUserColumn(
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), {}, "k", true));
        table->Columns.emplace(3, TUserTable::TUserColumn(
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), {}, "s", true));
        table->Columns.emplace(2, TUserTable::TUserColumn(
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), {}, "value", false));
        NKikimrSchemeOp::TTableDescription description;
        table->SetSchema(description);

        const ui64 key = 7;
        const TString value = "x";
        NKikimrChangeExchange::TDataChange change;
        auto& keyCells = *change.MutableKey();
        keyCells.AddTags(1);
        keyCells.AddTags(3);
        keyCells.SetData(TSerializedCellVec::Serialize({TCell::Make(key), TCell()}));
        auto& upsert = *change.MutableUpsert();
        upsert.AddTags(2);
        upsert.SetData(TSerializedCellVec::Serialize({TCell(value.data(), value.size())}));

        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(1)
            .WithSchema(table)
            .WithBody(change.SerializeAsString())
            .Build();

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatBinary, false)->Serialize(cmd, *record);
        const auto stored = ReadPayload(cmd);

        NChangeExchange::TBinaryCdcReader reader;
        TString error;
        UNIT_ASSERT_C(reader.Parse(stored.Payload, error), error);
        UNIT_ASSERT(reader.Header().Has(NChangeExchange::EBinaryCdcFlag::NullKeys));
        UNIT_ASSERT(!reader.Header().Has(NChangeExchange::EBinaryCdcFlag::HasTs));

        const TVector<NScheme::TTypeInfo> keyTypes{
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        };
        TVector<TCell> keys;
        UNIT_ASSERT_C(reader.ReadKeys(keyTypes, keys, error), error);
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        ui64 keyValue = 0;
        UNIT_ASSERT(keys[0].ToValue(keyValue, error));
        UNIT_ASSERT_VALUES_EQUAL(keyValue, key);
        UNIT_ASSERT(keys[1].IsNull());

        const TVector<NScheme::TTypeInfo> valueTypes{NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)};
        TVector<ui16> indexes;
        TVector<TCell> cells;
        UNIT_ASSERT_C(reader.ReadIndexed(1, valueTypes, indexes, cells, error), error);
        UNIT_ASSERT_VALUES_EQUAL(TString(cells[0].AsBuf()), value);
        UNIT_ASSERT(reader.Rest().empty());
    }

    Y_UNIT_TEST(WideIndexStartsAfter127ValueColumns) {
        auto makeTable = [](ui32 valueColumns) {
            auto table = MakeIntrusive<TUserTable>();
            table->KeyColumnIds = {1};
            table->KeyColumnTypes.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
            table->Columns.emplace(1, TUserTable::TUserColumn(
                NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), {}, "key", true));
            for (ui32 i = 0; i < valueColumns; ++i) {
                TString name = "c000";
                name[1] = static_cast<char>('0' + i / 100);
                name[2] = static_cast<char>('0' + (i / 10) % 10);
                name[3] = static_cast<char>('0' + i % 10);
                table->Columns.emplace(i + 2, TUserTable::TUserColumn(
                    NScheme::TTypeInfo(NScheme::NTypeIds::Uint8), {}, name, false));
            }
            NKikimrSchemeOp::TTableDescription description;
            table->SetSchema(description);
            return table;
        };

        auto write = [](TUserTable::TPtr table) {
            NKikimrChangeExchange::TDataChange change;
            auto& keyCells = *change.MutableKey();
            keyCells.AddTags(1);
            keyCells.SetData(TSerializedCellVec::Serialize({TCell::Make(ui64(1))}));
            const ui8 value = 3;
            auto& upsert = *change.MutableUpsert();
            upsert.AddTags(2);
            upsert.SetData(TSerializedCellVec::Serialize({TCell::Make(value)}));
            auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
                .WithOrder(1)
                .WithSchema(table)
                .WithBody(change.SerializeAsString())
                .Build();
            NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
            MakeSerializer(NKikimrSchemeOp::ECdcStreamFormatBinary, false)->Serialize(cmd, *record);
            NChangeExchange::TBinaryCdcReader reader;
            TString error;
            const auto stored = ReadPayload(cmd);
            UNIT_ASSERT_C(reader.Parse(stored.Payload, error), error);
            return reader.Header().Has(NChangeExchange::EBinaryCdcFlag::WideIndex);
        };

        UNIT_ASSERT(!write(makeTable(127)));
        UNIT_ASSERT(write(makeTable(128)));
    }
}

} // namespace NKikimr::NDataShard
