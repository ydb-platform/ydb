#include "change_record.h"
#include "export_common.h"

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/util/yverify_stream.h>
#include <ydb/library/binary_json/read.h>

#include <util/stream/str.h>
#include <util/string/printf.h>

namespace NKikimr::NDataShard {

void TChangeRecord::SerializeToProto(NKikimrChangeExchange::TChangeRecord& record) const {
    record.SetOrder(Order);
    record.SetGroup(Group);
    record.SetStep(Step);
    record.SetTxId(TxId);
    record.SetPathOwnerId(PathId.OwnerId);
    record.SetLocalPathId(PathId.LocalPathId);

    switch (Kind) {
        case EKind::AsyncIndex: {
            Y_VERIFY(record.MutableAsyncIndex()->ParseFromArray(Body.data(), Body.size()));
            break;
        }
        case EKind::CdcDataChange: {
            Y_VERIFY(record.MutableCdcDataChange()->ParseFromArray(Body.data(), Body.size()));
            break;
        }
    }
}

static auto ParseBody(const TString& protoBody) {
    NKikimrChangeExchange::TDataChange body;
    Y_VERIFY(body.ParseFromArray(protoBody.data(), protoBody.size()));
    return body;
}

static NJson::TJsonValue StringToJson(TStringBuf in) {
    NJson::TJsonValue result;
    Y_VERIFY(NJson::ReadJsonTree(in, &result));
    return result;
}

static NJson::TJsonValue YsonToJson(TStringBuf in) {
    NJson::TJsonValue result;
    Y_VERIFY(NJson2Yson::DeserializeYsonAsJsonValue(in, &result));
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
        Y_FAIL("pg types are not supported");
    default:
        Y_FAIL("Unexpected type");
    }
}

static void SerializeJsonKey(TUserTable::TCPtr schema, NJson::TJsonValue& key,
    const NKikimrChangeExchange::TDataChange::TSerializedCells& in)
{
    Y_VERIFY(in.TagsSize() == schema->KeyColumnIds.size());
    for (size_t i = 0; i < schema->KeyColumnIds.size(); ++i) {
        Y_VERIFY(in.GetTags(i) == schema->KeyColumnIds.at(i));
    }

    TSerializedCellVec cells;
    Y_VERIFY(TSerializedCellVec::TryParse(in.GetData(), cells));

    Y_VERIFY(cells.GetCells().size() == schema->KeyColumnTypes.size());
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
    Y_VERIFY(TSerializedCellVec::TryParse(in.GetData(), cells));
    Y_VERIFY(in.TagsSize() == cells.GetCells().size());

    for (ui32 i = 0; i < in.TagsSize(); ++i) {
        const auto tag = in.GetTags(i);
        const auto& cell = cells.GetCells().at(i);

        auto it = schema->Columns.find(tag);
        Y_VERIFY(it != schema->Columns.end());

        const auto& column = it->second;
        value.InsertValue(column.Name, ToJson(cell, column.Type));
    }
}

void TChangeRecord::SerializeToYdbJson(NJson::TJsonValue& json, bool virtualTimestamps) const {
    Y_VERIFY(Kind == EKind::CdcDataChange);
    Y_VERIFY(Schema);

    const auto body = ParseBody(Body);
    SerializeJsonKey(Schema, json["key"], body.GetKey());

    if (body.HasOldImage()) {
        SerializeJsonValue(Schema, json["oldImage"], body.GetOldImage());
    }

    if (body.HasNewImage()) {
        SerializeJsonValue(Schema, json["newImage"], body.GetNewImage());
    }

    const auto hasAnyImage = body.HasOldImage() || body.HasNewImage();
    switch (body.GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert:
            json["update"].SetType(NJson::JSON_MAP);
            if (!hasAnyImage) {
                SerializeJsonValue(Schema, json["update"], body.GetUpsert());
            }
            break;
        case NKikimrChangeExchange::TDataChange::kReset:
            json["reset"].SetType(NJson::JSON_MAP);
            if (!hasAnyImage) {
                SerializeJsonValue(Schema, json["reset"], body.GetReset());
            }
            break;
        case NKikimrChangeExchange::TDataChange::kErase:
            json["erase"].SetType(NJson::JSON_MAP);
            break;
        default:
            Y_FAIL_S("Unexpected row operation: " << static_cast<int>(body.GetRowOperationCase()));
    }

    if (virtualTimestamps) {
        for (auto v : {Step, TxId}) {
            json["ts"].AppendValue(v);
        }
    }
}

static void ExtendJson(NJson::TJsonValue& value, const NJson::TJsonValue& ext) {
    Y_VERIFY(ext.GetType() == NJson::JSON_MAP);
    for (const auto& [k, v] : ext.GetMapSafe()) {
        value.InsertValue(k, v);
    }
}

static void ToAttributeValues(TUserTable::TCPtr schema, NJson::TJsonValue& value,
    const NKikimrChangeExchange::TDataChange::TSerializedCells& in)
{
    TSerializedCellVec cells;
    Y_VERIFY(TSerializedCellVec::TryParse(in.GetData(), cells));
    Y_VERIFY(in.TagsSize() == cells.GetCells().size());

    for (ui32 i = 0; i < in.TagsSize(); ++i) {
        const auto tag = in.GetTags(i);
        const auto& cell = cells.GetCells().at(i);

        if (cell.IsNull()) {
            continue;
        }

        auto it = schema->Columns.find(tag);
        Y_VERIFY(it != schema->Columns.end());

        const auto& column = it->second;
        const auto& name = column.Name;
        const auto type = column.Type.GetTypeId();

        if (name == "__Hash" || name == "__CreatedAt") {
            continue; // hidden column
        } else if (name.StartsWith("__Hash_")) {
            bool indexed = false;
            for (const auto& [_, index] : schema->Indexes) {
                Y_VERIFY(index.KeyColumnIds.size() >= 1);
                if (index.KeyColumnIds.at(0) == tag) {
                    indexed = true;
                    break;
                }
            }
            if (indexed) {
                continue; // index hash column
            }
        } else if (name == "__RowData") {
            Y_VERIFY_DEBUG(type == NScheme::NTypeIds::JsonDocument);
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

void TChangeRecord::SerializeToDynamoDBStreamsJson(NJson::TJsonValue& json, const TAwsJsonOptions& opts) const {
    Y_VERIFY(Kind == EKind::CdcDataChange);
    Y_VERIFY(Schema);

    json = NJson::TJsonMap({
        {"awsRegion", opts.AwsRegion},
        {"dynamodb", NJson::TJsonMap({
            {"ApproximateCreationDateTime", GetApproximateCreationDateTime().MilliSeconds()},
            {"SequenceNumber", Sprintf("%0*" PRIi64, 21 /* min length */, GetSeqNo())},
        })},
        {"eventID", Sprintf("%" PRIu64 "-%" PRIi64, opts.ShardId, GetSeqNo())},
        {"eventSource", "ydb:document-table"},
        {"eventVersion", "1.0"},
    });

    auto& dynamodb = json["dynamodb"];
    const auto body = ParseBody(Body);

    bool keysOnly = false;
    bool newAndOldImages = false;
    switch (opts.StreamMode) {
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
    ToAttributeValues(Schema, keys, body.GetKey());
    dynamodb["Keys"] = keys;

    if (!keysOnly && body.HasOldImage()) {
        ToAttributeValues(Schema, dynamodb["OldImage"], body.GetOldImage());
        ExtendJson(dynamodb["OldImage"], keys);
    }

    if (!keysOnly && body.HasNewImage()) {
        ToAttributeValues(Schema, dynamodb["NewImage"], body.GetNewImage());
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

TConstArrayRef<TCell> TChangeRecord::GetKey() const {
    if (Key) {
        return *Key;
    }

    switch (Kind) {
        case EKind::AsyncIndex:
        case EKind::CdcDataChange: {
            const auto parsed = ParseBody(Body);

            TSerializedCellVec key;
            Y_VERIFY(TSerializedCellVec::TryParse(parsed.GetKey().GetData(), key));

            Key.ConstructInPlace(key.GetCells());
            break;
        }
    }

    Y_VERIFY(Key);
    return *Key;
}

i64 TChangeRecord::GetSeqNo() const {
    Y_VERIFY(Order <= Max<i64>());
    return static_cast<i64>(Order);
}

TString TChangeRecord::GetPartitionKey() const {
    if (PartitionKey) {
        return *PartitionKey;
    }

    switch (Kind) {
        case EKind::CdcDataChange: {
            Y_VERIFY(Schema);
            const auto body = ParseBody(Body);

            NJson::TJsonValue key;
            SerializeJsonKey(Schema, key, body.GetKey());

            PartitionKey.ConstructInPlace(MD5::Calc(WriteJson(key, false)));
            break;
        }

        case EKind::AsyncIndex: {
            Y_FAIL("Not supported");
        }
    }

    Y_VERIFY(PartitionKey);
    return *PartitionKey;
}

TInstant TChangeRecord::GetApproximateCreationDateTime() const {
    return GetGroup()
        ? TInstant::FromValue(GetGroup())
        : TInstant::MilliSeconds(GetStep());
}

TString TChangeRecord::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TChangeRecord::Out(IOutputStream& out) const {
    out << "{"
        << " Order: " << Order
        << " Group: " << Group
        << " Step: " << Step
        << " TxId: " << TxId
        << " PathId: " << PathId
        << " Kind: " << Kind
        << " Body: " << Body.size() << "b"
        << " TableId: " << TableId
        << " SchemaVersion: " << SchemaVersion
        << " LockId: " << LockId
        << " LockOffset: " << LockOffset
    << " }";
}

TChangeRecordBuilder::TChangeRecordBuilder(EKind kind) {
    Record.Kind = kind;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithLockId(ui64 lockId) {
    Record.LockId = lockId;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithLockOffset(ui64 lockOffset) {
    Record.LockOffset = lockOffset;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithOrder(ui64 order) {
    Record.Order = order;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithGroup(ui64 group) {
    Record.Group = group;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithStep(ui64 step) {
    Record.Step = step;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithTxId(ui64 txId) {
    Record.TxId = txId;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithPathId(const TPathId& pathId) {
    Record.PathId = pathId;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithTableId(const TPathId& tableId) {
    Record.TableId = tableId;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithSchemaVersion(ui64 version) {
    Record.SchemaVersion = version;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithSchema(TUserTable::TCPtr schema) {
    Record.Schema = schema;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithBody(const TString& body) {
    Record.Body = body;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithBody(TString&& body) {
    Record.Body = std::move(body);
    return *this;
}

TChangeRecord&& TChangeRecordBuilder::Build() {
    return std::move(Record);
}

}
