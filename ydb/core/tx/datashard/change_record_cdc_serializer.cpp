#include "change_record_cdc_serializer.h"
#include "change_record.h"
#include "export_common.h"

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/library/binary_json/read.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/str.h>
#include <util/string/printf.h>

namespace NKikimr::NDataShard {

class TBaseSerializer: public IChangeRecordSerializer {
    static NKikimrPQClient::TDataChunk MakeDataChunk() {
        NKikimrPQClient::TDataChunk data;
        data.SetCodec(0 /* CODEC_RAW */);
        return data;
    }

    void SerializeDataChange(TCmdWrite& cmd, const TChangeRecord& record) {
        auto data = MakeDataChunk();
        FillDataChunk(data, record);
        cmd.SetData(data.SerializeAsString());
    }

    void SerializeHeartbeat(TCmdWrite& cmd, const TChangeRecord& record) {
        auto data = MakeDataChunk();
        FillDataChunk(data, record);

        auto& heartbeat = *cmd.MutableHeartbeat();
        heartbeat.SetStep(record.GetStep());
        heartbeat.SetTxId(record.GetTxId());
        heartbeat.SetData(data.SerializeAsString());
    }

protected:
    virtual void FillDataChunk(NKikimrPQClient::TDataChunk& chunk, const TChangeRecord& record) = 0;

public:
    explicit TBaseSerializer(const TChangeRecordSerializerOpts& opts)
        : Opts(opts)
    {}

    void Serialize(TCmdWrite& cmd, const TChangeRecord& record) override {
        cmd.SetSeqNo(record.GetSeqNo());
        cmd.SetCreateTimeMS(record.GetApproximateCreationDateTime().MilliSeconds());
        switch (record.GetKind()) {
        case TChangeRecord::EKind::CdcDataChange:
            return SerializeDataChange(cmd, record);
        case TChangeRecord::EKind::CdcHeartbeat:
            return SerializeHeartbeat(cmd, record);
        case TChangeRecord::EKind::AsyncIndex:
            Y_ABORT("Unexpected");
        }
    }

protected:
    const TChangeRecordSerializerOpts Opts;

}; // TBaseSerializer

class TProtoSerializer: public TBaseSerializer {
protected:
    void FillDataChunk(NKikimrPQClient::TDataChunk& data, const TChangeRecord& record) override {
        NKikimrChangeExchange::TChangeRecord proto;
        record.Serialize(proto);
        data.SetData(proto.SerializeAsString());
    }

public:
    using TBaseSerializer::TBaseSerializer;

}; // TProtoSerializer

class TJsonSerializer: public TBaseSerializer {
    friend class TChangeRecord; // used in GetPartitionKey()

    static NJson::TJsonWriterConfig DefaultJsonConfig() {
        constexpr ui32 doubleNDigits = std::numeric_limits<double>::max_digits10;
        constexpr ui32 floatNDigits = std::numeric_limits<float>::max_digits10;
        constexpr EFloatToStringMode floatMode = EFloatToStringMode::PREC_NDIGITS;
        return NJson::TJsonWriterConfig {
            .DoubleNDigits = doubleNDigits,
            .FloatNDigits = floatNDigits,
            .FloatToStringMode = floatMode,
            .ValidateUtf8 = false,
            .WriteNanAsString = true,
        };
    }

protected:
    static auto ParseBody(const TString& protoBody) {
        NKikimrChangeExchange::TDataChange body;
        Y_ABORT_UNLESS(body.ParseFromArray(protoBody.data(), protoBody.size()));
        return body;
    }

    static NJson::TJsonValue StringToJson(TStringBuf in) {
        NJson::TJsonValue result;
        Y_ABORT_UNLESS(NJson::ReadJsonTree(in, &result));
        return result;
    }

    static NJson::TJsonValue YsonToJson(TStringBuf in) {
        NJson::TJsonValue result;
        Y_ABORT_UNLESS(NJson2Yson::DeserializeYsonAsJsonValue(in, &result));
        return result;
    }

    static NJson::TJsonValue ToJson(const TCell& cell, NScheme::TTypeInfo type) {
        if (cell.IsNull()) {
            return NJson::TJsonValue(NJson::JSON_NULL);
        }

        switch (type.GetTypeId()) {
        case NScheme::NTypeIds::Bool:
            return NJson::TJsonValue(cell.AsValue<bool>());
        case NScheme::NTypeIds::Int8:
            return NJson::TJsonValue(cell.AsValue<i8>());
        case NScheme::NTypeIds::Uint8:
            return NJson::TJsonValue(cell.AsValue<ui8>());
        case NScheme::NTypeIds::Int16:
            return NJson::TJsonValue(cell.AsValue<i16>());
        case NScheme::NTypeIds::Uint16:
            return NJson::TJsonValue(cell.AsValue<ui16>());
        case NScheme::NTypeIds::Int32:
            return NJson::TJsonValue(cell.AsValue<i32>());
        case NScheme::NTypeIds::Uint32:
            return NJson::TJsonValue(cell.AsValue<ui32>());
        case NScheme::NTypeIds::Int64:
            return NJson::TJsonValue(cell.AsValue<i64>());
        case NScheme::NTypeIds::Uint64:
            return NJson::TJsonValue(cell.AsValue<ui64>());
        case NScheme::NTypeIds::Float:
            return NJson::TJsonValue(cell.AsValue<float>());
        case NScheme::NTypeIds::Double:
            return NJson::TJsonValue(cell.AsValue<double>());
        case NScheme::NTypeIds::Date:
            return NJson::TJsonValue(TInstant::Days(cell.AsValue<ui16>()).ToString());
        case NScheme::NTypeIds::Datetime:
            return NJson::TJsonValue(TInstant::Seconds(cell.AsValue<ui32>()).ToString());
        case NScheme::NTypeIds::Timestamp:
            return NJson::TJsonValue(TInstant::MicroSeconds(cell.AsValue<ui64>()).ToString());
        case NScheme::NTypeIds::Interval:
            return NJson::TJsonValue(cell.AsValue<i64>());
        case NScheme::NTypeIds::Decimal:
            return NJson::TJsonValue(DecimalToString(cell.AsValue<std::pair<ui64, i64>>()));
        case NScheme::NTypeIds::DyNumber:
            return NJson::TJsonValue(DyNumberToString(cell.AsBuf()));
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
            return NJson::TJsonValue(Base64Encode(cell.AsBuf()));
        case NScheme::NTypeIds::Utf8:
            return NJson::TJsonValue(cell.AsBuf());
        case NScheme::NTypeIds::Json:
            return StringToJson(cell.AsBuf());
        case NScheme::NTypeIds::JsonDocument:
            return StringToJson(NBinaryJson::SerializeToJson(cell.AsBuf()));
        case NScheme::NTypeIds::Yson:
            return YsonToJson(cell.AsBuf());
        case NScheme::NTypeIds::Pg:
            // TODO: support pg types
            Y_ABORT("pg types are not supported");
        default:
            Y_ABORT("Unexpected type");
        }
    }

    static void SerializeJsonKey(TUserTable::TCPtr schema, NJson::TJsonValue& key,
        const NKikimrChangeExchange::TDataChange::TSerializedCells& in)
    {
        Y_ABORT_UNLESS(in.TagsSize() == schema->KeyColumnIds.size());
        for (size_t i = 0; i < schema->KeyColumnIds.size(); ++i) {
            Y_ABORT_UNLESS(in.GetTags(i) == schema->KeyColumnIds.at(i));
        }

        TSerializedCellVec cells;
        Y_ABORT_UNLESS(TSerializedCellVec::TryParse(in.GetData(), cells));

        Y_ABORT_UNLESS(cells.GetCells().size() == schema->KeyColumnTypes.size());
        for (size_t i = 0; i < schema->KeyColumnTypes.size(); ++i) {
            const auto type = schema->KeyColumnTypes.at(i);
            const auto& cell = cells.GetCells().at(i);
            key.AppendValue(ToJson(cell, type));
        }
    }

    static void SerializeJsonValue(TUserTable::TCPtr schema, NJson::TJsonValue& value,
        const NKikimrChangeExchange::TDataChange::TSerializedCells& in)
    {
        TSerializedCellVec cells;
        Y_ABORT_UNLESS(TSerializedCellVec::TryParse(in.GetData(), cells));
        Y_ABORT_UNLESS(in.TagsSize() == cells.GetCells().size());

        for (ui32 i = 0; i < in.TagsSize(); ++i) {
            const auto tag = in.GetTags(i);
            const auto& cell = cells.GetCells().at(i);

            auto it = schema->Columns.find(tag);
            Y_ABORT_UNLESS(it != schema->Columns.end());

            const auto& column = it->second;
            value.InsertValue(column.Name, ToJson(cell, column.Type));
        }
    }

    static void ExtendJson(NJson::TJsonValue& value, const NJson::TJsonValue& ext) {
        Y_ABORT_UNLESS(ext.GetType() == NJson::JSON_MAP);
        for (const auto& [k, v] : ext.GetMapSafe()) {
            value.InsertValue(k, v);
        }
    }

    static void SerializeVirtualTimestamp(NJson::TJsonValue& value, std::initializer_list<ui64> vt) {
        for (auto v : vt) {
            value.AppendValue(v);
        }
    }

    void FillDataChunk(NKikimrPQClient::TDataChunk& data, const TChangeRecord& record) override {
        NJson::TJsonValue json;
        SerializeToJson(json, record);

        TStringStream str;
        NJson::WriteJson(&str, &json, JsonConfig);
        data.SetData(str.Str());
    }

    virtual void SerializeToJson(NJson::TJsonValue& json, const TChangeRecord& record) = 0;

public:
    explicit TJsonSerializer(const TChangeRecordSerializerOpts& opts)
        : TBaseSerializer(opts)
        , JsonConfig(DefaultJsonConfig())
    {}

    void Serialize(TCmdWrite& cmd, const TChangeRecord& record) override {
        TBaseSerializer::Serialize(cmd, record);
        if (record.GetKind() == TChangeRecord::EKind::CdcDataChange) {
            cmd.SetPartitionKey(record.GetPartitionKey());
        }
    }

protected:
    const NJson::TJsonWriterConfig JsonConfig;

}; // TJsonSerializer

class TYdbJsonSerializer: public TJsonSerializer {
protected:
    void SerializeToJson(NJson::TJsonValue& json, const TChangeRecord& record) override {
        if (record.GetKind() == TChangeRecord::EKind::CdcHeartbeat) {
            return SerializeVirtualTimestamp(json["resolved"], {record.GetStep(), record.GetTxId()});
        }

        Y_ABORT_UNLESS(record.GetKind() == TChangeRecord::EKind::CdcDataChange);
        Y_ABORT_UNLESS(record.GetSchema());

        const auto body = ParseBody(record.GetBody());
        SerializeJsonKey(record.GetSchema(), json["key"], body.GetKey());

        if (body.HasOldImage()) {
            SerializeJsonValue(record.GetSchema(), json["oldImage"], body.GetOldImage());
        }

        if (body.HasNewImage()) {
            SerializeJsonValue(record.GetSchema(), json["newImage"], body.GetNewImage());
        }

        const auto hasAnyImage = body.HasOldImage() || body.HasNewImage();
        switch (body.GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert:
            json["update"].SetType(NJson::JSON_MAP);
            if (!hasAnyImage) {
                SerializeJsonValue(record.GetSchema(), json["update"], body.GetUpsert());
            }
            break;
        case NKikimrChangeExchange::TDataChange::kReset:
            json["reset"].SetType(NJson::JSON_MAP);
            if (!hasAnyImage) {
                SerializeJsonValue(record.GetSchema(), json["reset"], body.GetReset());
            }
            break;
        case NKikimrChangeExchange::TDataChange::kErase:
            json["erase"].SetType(NJson::JSON_MAP);
            break;
        default:
            Y_FAIL_S("Unexpected row operation: " << static_cast<int>(body.GetRowOperationCase()));
        }

        if (Opts.VirtualTimestamps) {
            SerializeVirtualTimestamp(json["ts"], {record.GetStep(), record.GetTxId()});
        }
    }

public:
    using TJsonSerializer::TJsonSerializer;

}; // TYdbJsonSerializer

class TDynamoDBStreamsJsonSerializer: public TJsonSerializer {
    static void ToAttributeValues(TUserTable::TCPtr schema, NJson::TJsonValue& value,
        const NKikimrChangeExchange::TDataChange::TSerializedCells& in)
    {
        TSerializedCellVec cells;
        Y_ABORT_UNLESS(TSerializedCellVec::TryParse(in.GetData(), cells));
        Y_ABORT_UNLESS(in.TagsSize() == cells.GetCells().size());

        for (ui32 i = 0; i < in.TagsSize(); ++i) {
            const auto tag = in.GetTags(i);
            const auto& cell = cells.GetCells().at(i);

            if (cell.IsNull()) {
                continue;
            }

            auto it = schema->Columns.find(tag);
            Y_ABORT_UNLESS(it != schema->Columns.end());

            const auto& column = it->second;
            const auto& name = column.Name;
            const auto type = column.Type.GetTypeId();

            if (name == "__Hash" || name == "__CreatedAt") {
                continue; // hidden column
            } else if (name.StartsWith("__Hash_")) {
                bool indexed = false;
                for (const auto& [_, index] : schema->Indexes) {
                    Y_ABORT_UNLESS(index.KeyColumnIds.size() >= 1);
                    if (index.KeyColumnIds.at(0) == tag) {
                        indexed = true;
                        break;
                    }
                }
                if (indexed) {
                    continue; // index hash column
                }
            } else if (name == "__RowData") {
                Y_DEBUG_ABORT_UNLESS(type == NScheme::NTypeIds::JsonDocument);
                const auto rowData = StringToJson(NBinaryJson::SerializeToJson(cell.AsBuf()));
                if (rowData.GetType() == NJson::JSON_MAP) {
                    auto map = rowData.GetMapSafe().find("M");
                    if (map != rowData.GetMapSafe().end()) {
                        if (map->second.GetType() == NJson::JSON_MAP) {
                            ExtendJson(value, map->second);
                        }
                    }
                }
            }

            if (type == NScheme::NTypeIds::Bool) {
                value.InsertValue(name, NJson::TJsonMap({{"BOOL", cell.AsValue<bool>()}}));
            } else if (type == NScheme::NTypeIds::DyNumber) {
                value.InsertValue(name, NJson::TJsonMap({{"N", DyNumberToString(cell.AsBuf())}}));
            } else if (type == NScheme::NTypeIds::String) {
                value.InsertValue(name, NJson::TJsonMap({{"B", Base64Encode(cell.AsBuf())}}));
            } else if (type == NScheme::NTypeIds::Utf8) {
                value.InsertValue(name, NJson::TJsonMap({{"S", cell.AsBuf()}}));
            }
        }
    }

protected:
    void SerializeToJson(NJson::TJsonValue& json, const TChangeRecord& record) override {
        Y_ABORT_UNLESS(record.GetKind() == TChangeRecord::EKind::CdcDataChange);
        Y_ABORT_UNLESS(record.GetSchema());

        json = NJson::TJsonMap({
            {"awsRegion", Opts.AwsRegion},
            {"dynamodb", NJson::TJsonMap({
                {"ApproximateCreationDateTime", record.GetApproximateCreationDateTime().MilliSeconds()},
                {"SequenceNumber", Sprintf("%0*" PRIi64, 21 /* min length */, record.GetSeqNo())},
            })},
            {"eventID", Sprintf("%" PRIu64 "-%" PRIi64, Opts.ShardId, record.GetSeqNo())},
            {"eventSource", "ydb:document-table"},
            {"eventVersion", "1.0"},
        });

        auto& dynamodb = json["dynamodb"];
        const auto body = ParseBody(record.GetBody());

        bool keysOnly = false;
        bool newAndOldImages = false;
        switch (Opts.StreamMode) {
        case TUserTable::TCdcStream::EMode::ECdcStreamModeNewImage:
            dynamodb["StreamViewType"] = "NEW_IMAGE";
            break;
        case TUserTable::TCdcStream::EMode::ECdcStreamModeOldImage:
            dynamodb["StreamViewType"] = "OLD_IMAGE";
            break;
        case TUserTable::TCdcStream::EMode::ECdcStreamModeNewAndOldImages:
            dynamodb["StreamViewType"] = "NEW_AND_OLD_IMAGES";
            newAndOldImages = true;
            break;
        default:
            dynamodb["StreamViewType"] = "KEYS_ONLY";
            keysOnly = true;
            break;
        }

        NJson::TJsonMap keys;
        ToAttributeValues(record.GetSchema(), keys, body.GetKey());
        dynamodb["Keys"] = keys;

        if (!keysOnly && body.HasOldImage()) {
            ToAttributeValues(record.GetSchema(), dynamodb["OldImage"], body.GetOldImage());
            ExtendJson(dynamodb["OldImage"], keys);
        }

        if (!keysOnly && body.HasNewImage()) {
            ToAttributeValues(record.GetSchema(), dynamodb["NewImage"], body.GetNewImage());
            ExtendJson(dynamodb["NewImage"], keys);
        }

        switch (body.GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert:
        case NKikimrChangeExchange::TDataChange::kReset:
            if (newAndOldImages) {
                json["eventName"] = body.HasOldImage() ? "MODIFY" : "INSERT";
            } else {
                json["eventName"] = "MODIFY";
            }
            break;
        case NKikimrChangeExchange::TDataChange::kErase:
            json["eventName"] = "REMOVE";
            break;
        default:
            Y_FAIL_S("Unexpected row operation: " << static_cast<int>(body.GetRowOperationCase()));
        }
    }

public:
    using TJsonSerializer::TJsonSerializer;

}; // TDynamoDBStreamsJsonSerializer

class TDebeziumJsonSerializer: public TJsonSerializer {
protected:
    void SerializeToJson(NJson::TJsonValue& json, const TChangeRecord& record) override {
        Y_ABORT_UNLESS(record.GetKind() == TChangeRecord::EKind::CdcDataChange);
        Y_ABORT_UNLESS(record.GetSchema());

        const auto body = ParseBody(record.GetBody());
        auto& keyJson = json["key"];
        auto& valueJson = json["value"];

        keyJson["payload"].SetType(NJson::JSON_MAP);
        SerializeJsonValue(record.GetSchema(), keyJson["payload"], body.GetKey());

        valueJson["payload"].SetType(NJson::JSON_MAP);

        if (body.HasOldImage()) {
            SerializeJsonValue(record.GetSchema(), valueJson["payload"]["before"], body.GetOldImage());
            ExtendJson(valueJson["payload"]["before"], keyJson["payload"]);
        }

        if (body.HasNewImage()) {
            SerializeJsonValue(record.GetSchema(), valueJson["payload"]["after"], body.GetNewImage());
            ExtendJson(valueJson["payload"]["after"], keyJson["payload"]);
        }

        if (record.GetSource() == TChangeRecord::ESource::InitialScan) {
            valueJson["payload"]["op"] = "r"; // r = read
        } else {
            switch (body.GetRowOperationCase()) {
            case NKikimrChangeExchange::TDataChange::kUpsert:
            case NKikimrChangeExchange::TDataChange::kReset:
                if (Opts.StreamMode == TUserTable::TCdcStream::EMode::ECdcStreamModeNewAndOldImages) {
                    valueJson["payload"]["op"] = body.HasOldImage() ? "u" : "c"; // c = create
                } else {
                    valueJson["payload"]["op"] = "u"; // u = update
                }
                break;
            case NKikimrChangeExchange::TDataChange::kErase:
                valueJson["payload"]["op"] = "d"; // d = delete
                break;
            default:
                Y_FAIL_S("Unexpected row operation: " << static_cast<int>(body.GetRowOperationCase()));
            }
        }

        valueJson["payload"]["source"] = NJson::TJsonMap({
            {"version", "1.0.0"},
            {"connector", "ydb"},
            {"ts_ms", record.GetApproximateCreationDateTime().MilliSeconds()},
            {"snapshot", record.GetSource() == TChangeRecord::ESource::InitialScan},
            {"step", record.GetStep()},
            {"txId", record.GetTxId()},
            // TODO: db & table
        });
    }

    void FillDataChunk(NKikimrPQClient::TDataChunk& data, const TChangeRecord& record) override {
        NJson::TJsonValue json;
        SerializeToJson(json, record);
        {
            TStringStream str;
            NJson::WriteJson(&str, &json["key"], JsonConfig);
            auto& messageMeta = *data.AddMessageMeta();
            messageMeta.set_key("__key");
            messageMeta.set_value(str.Str());
        }
        {
            TStringStream str;
            NJson::WriteJson(&str, &json["value"], JsonConfig);
            data.SetData(str.Str());
        }
    }

public:
    using TJsonSerializer::TJsonSerializer;

}; // TDebeziumJsonSerializer

IChangeRecordSerializer* CreateChangeRecordSerializer(const TChangeRecordSerializerOpts& opts) {
    switch (opts.StreamFormat) {
    case TUserTable::TCdcStream::EFormat::ECdcStreamFormatProto:
        return new TProtoSerializer(opts);
    case TUserTable::TCdcStream::EFormat::ECdcStreamFormatJson:
        return new TYdbJsonSerializer(opts);
    case TUserTable::TCdcStream::EFormat::ECdcStreamFormatDynamoDBStreamsJson:
        return new TDynamoDBStreamsJsonSerializer(opts);
    case TUserTable::TCdcStream::EFormat::ECdcStreamFormatDebeziumJson:
        return new TDebeziumJsonSerializer(opts);
    default:
        Y_ABORT("Unsupported format");
    }
}

TString TChangeRecord::GetPartitionKey() const {
    if (PartitionKey) {
        return *PartitionKey;
    }

    Y_ABORT_UNLESS(Kind == EKind::CdcDataChange);
    Y_ABORT_UNLESS(Schema);

    const auto body = TJsonSerializer::ParseBody(Body);

    NJson::TJsonValue key;
    TJsonSerializer::SerializeJsonKey(Schema, key, body.GetKey());

    PartitionKey.ConstructInPlace(MD5::Calc(WriteJson(key, false)));
    return *PartitionKey;
}

}
