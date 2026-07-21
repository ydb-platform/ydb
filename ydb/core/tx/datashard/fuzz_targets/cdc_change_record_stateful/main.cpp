#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/tx/datashard/change_record_body_serializer.h>
#include <ydb/core/tx/datashard/change_record_cdc_serializer.h>
#include <ydb/library/aclib/user_context.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <memory>

namespace {

using namespace NKikimr;
using namespace NKikimr::NDataShard;

constexpr size_t MaxRecords = 160;

TUserTable::TPtr MakeSchema(ui64 version) {
    auto schema = MakeIntrusive<TUserTable>();
    schema->LocalTid = 1;
    schema->Name = "cdc_fuzz_table";
    schema->Path = "/Root/cdc_fuzz_table";
    schema->Columns.emplace(1, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true));
    schema->Columns.emplace(2, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "", "value", false));
    schema->Columns.emplace(3, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "payload", false));
    schema->KeyColumnIds = {1};
    schema->KeyColumnTypes = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
    schema->Range = TSerializedTableRange();
    Y_UNUSED(version);
    return schema;
}

NKikimrChangeExchange::TDataChange MakeBody(FuzzedDataProvider& provider, TString& payloadStorage) {
    const ui32 key = provider.ConsumeIntegral<ui32>();
    const ui64 value = provider.ConsumeIntegral<ui64>();
    payloadStorage = provider.ConsumeRandomLengthString(48);

    TRawTypeValue keyRaw(&key, sizeof(key), NScheme::NTypeIds::Uint32);
    TVector<TRawTypeValue> keyValues = {keyRaw};
    TVector<NTable::TTag> keyTags = {1};

    TVector<NTable::TUpdateOp> updates;
    if (provider.ConsumeBool()) {
        updates.emplace_back(2, NTable::ECellOp::Set, TRawTypeValue(&value, sizeof(value), NScheme::NTypeIds::Uint64));
    }
    if (provider.ConsumeBool()) {
        updates.emplace_back(3, NTable::ECellOp::Set, TRawTypeValue(payloadStorage.data(), payloadStorage.size(), NScheme::NTypeIds::String));
    }
    if (updates.empty()) {
        updates.emplace_back(2, NTable::ECellOp::Set, TRawTypeValue(&value, sizeof(value), NScheme::NTypeIds::Uint64));
    }

    NKikimrChangeExchange::TDataChange body;
    const auto op = provider.ConsumeIntegralInRange<ui8>(0, 2);
    if (op == 0) {
        TChangeRecordBodySerializer::Serialize(body, NTable::ERowOp::Upsert, keyValues, keyTags, updates);
    } else if (op == 1) {
        TChangeRecordBodySerializer::Serialize(body, NTable::ERowOp::Reset, keyValues, keyTags, updates);
    } else {
        TChangeRecordBodySerializer::Serialize(body, NTable::ERowOp::Erase, keyValues, keyTags, {});
    }
    return body;
}

void CheckDataChangeBody(const TChangeRecord& record) {
    const auto key = record.GetKey();
    Y_ABORT_UNLESS(key.size() == 1);
    Y_ABORT_UNLESS(key[0].Size() == sizeof(ui32));

    NKikimrChangeExchange::TChangeRecord proto;
    record.Serialize(proto);
    Y_ABORT_UNLESS(proto.HasCdcDataChange());
    Y_ABORT_UNLESS(proto.GetOrder() == record.GetOrder());
    Y_ABORT_UNLESS(proto.GetGroup() == record.GetGroup());
    Y_ABORT_UNLESS(proto.GetStep() == record.GetStep());
    Y_ABORT_UNLESS(proto.GetTxId() == record.GetTxId());
    Y_ABORT_UNLESS(proto.GetPathOwnerId() == record.GetPathId().OwnerId);
    Y_ABORT_UNLESS(proto.GetLocalPathId() == record.GetPathId().LocalPathId);

    TString bytes;
    Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&bytes);
    NKikimrChangeExchange::TChangeRecord parsed;
    Y_ABORT_UNLESS(parsed.ParseFromString(bytes));
    Y_ABORT_UNLESS(parsed.GetCdcDataChange().SerializeAsString() == proto.GetCdcDataChange().SerializeAsString());
}

void CheckSerializedCommand(IChangeRecordSerializer& serializer, const TChangeRecord& record) {
    NKikimrClient::TPersQueuePartitionRequest_TCmdWrite cmd;
    serializer.Serialize(cmd, record);
    Y_ABORT_UNLESS(cmd.GetSeqNo() == record.GetSeqNo());
    Y_ABORT_UNLESS(cmd.HasData() || cmd.HasHeartbeat());
    if (cmd.HasData()) {
        NKikimrPQClient::TDataChunk chunk;
        Y_ABORT_UNLESS(chunk.ParseFromString(cmd.GetData()));
        Y_ABORT_UNLESS(chunk.GetCodec() == 0);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    auto schema = MakeSchema(1);

    TChangeRecordSerializerOpts protoOpts;
    protoOpts.StreamFormat = TUserTable::TCdcStream::EFormat::ECdcStreamFormatProto;
    protoOpts.StreamMode = TUserTable::TCdcStream::EMode::ECdcStreamModeNewAndOldImages;
    std::unique_ptr<IChangeRecordSerializer> protoSerializer(CreateChangeRecordSerializer(protoOpts));
    std::unique_ptr<IChangeRecordSerializer> debugSerializer(CreateChangeRecordDebugSerializer());

    ui64 lastOrder = 0;
    for (size_t i = 0; i < MaxRecords && provider.remaining_bytes(); ++i) {
        const bool heartbeat = provider.ConsumeIntegralInRange<ui8>(0, 7) == 0;
        const ui64 order = lastOrder + provider.ConsumeIntegralInRange<ui64>(0, 4);
        lastOrder = order;

        TIntrusivePtr<TChangeRecord> record;
        if (heartbeat) {
            record = TChangeRecordBuilder(TChangeRecord::EKind::CdcHeartbeat)
                .WithOrder(order)
                .WithGroup(provider.ConsumeIntegralInRange<ui64>(0, 128))
                .WithStep(provider.ConsumeIntegralInRange<ui64>(0, 128))
                .WithTxId(provider.ConsumeIntegralInRange<ui64>(1, 1024))
                .WithPathId(TPathId(1, provider.ConsumeIntegralInRange<ui64>(1, 8)))
                .WithTableId(TPathId(1, 1))
                .WithSchemaVersion(1)
                .WithSchema(schema)
                .Build();
            Y_ABORT_UNLESS(record->IsBroadcast());
            CheckSerializedCommand(*protoSerializer, *record);
            CheckSerializedCommand(*debugSerializer, *record);
            continue;
        }

        TString payloadStorage;
        auto body = MakeBody(provider, payloadStorage);
        TString bodyBytes;
        Y_PROTOBUF_SUPPRESS_NODISCARD body.SerializeToString(&bodyBytes);

        const ui64 schemaVersion = provider.ConsumeIntegralInRange<ui64>(1, 4);
        record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
            .WithOrder(order)
            .WithGroup(provider.ConsumeIntegralInRange<ui64>(0, 128))
            .WithStep(provider.ConsumeIntegralInRange<ui64>(0, 128))
            .WithTxId(provider.ConsumeIntegralInRange<ui64>(1, 1024))
            .WithLockId(provider.ConsumeIntegralInRange<ui64>(0, 64))
            .WithLockOffset(provider.ConsumeIntegralInRange<ui64>(0, 64))
            .WithPathId(TPathId(1, provider.ConsumeIntegralInRange<ui64>(1, 8)))
            .WithTableId(TPathId(1, 1))
            .WithSchemaVersion(schemaVersion)
            .WithSchema(schema)
            .WithBody(std::move(bodyBytes))
            .Build();

        CheckDataChangeBody(*record);
        const TString partitionKey = record->GetPartitionKey();
        Y_ABORT_UNLESS(!partitionKey.empty());
        Y_ABORT_UNLESS(record->GetPartitionKey() == partitionKey);
        CheckSerializedCommand(*protoSerializer, *record);
        CheckSerializedCommand(*debugSerializer, *record);
        Y_ABORT_UNLESS(record->GetSchemaVersion() == schemaVersion);
    }

    return 0;
}
