#include "change_record_cdc_serializer.h"
#include "change_record.h"
#include "type_serialization.h"

#include <ydb/core/change_exchange/binary_cdc.h>
#include <ydb/core/io_formats/json/json.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/library/aclib/user_context.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/uuid/uuid.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <contrib/libs/zstd/include/zstd.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ylimits.h>
#include <util/stream/str.h>
#include <util/string/printf.h>

namespace NKikimr::NDataShard {

class TBaseSerializer: public IChangeRecordSerializer {
    static NKikimrPQClient::TDataChunk MakeDataChunk() {
        NKikimrPQClient::TDataChunk data;
        data.SetCodec(0 /* CODEC_RAW */);
        return data;
    }

    // Heartbeats stay raw. A data row below CdcZstdMinPayloadBytes stays raw.
    // Anything we do compress is written with one ZSTD_compress2 into a context
    // owned by this serializer. The raw bytes stay in place when zstd does not
    // make them shorter.
    //
    // Negative levels select the fast strategy and use the magnitude as the
    // search step. ZSTD_minCLevel() is -128 KiB, so a 1 KiB run is stored as
    // literals and the frame grows. Level -1 still visits every byte and shrinks
    // that run. Schema snapshots use the library default level.
    static constexpr int CdcZstdDataLevel = -1;
    void ApplyCompression(NKikimrPQClient::TDataChunk& data, const TChangeRecord& record) {
        if (Opts.Compression != NKikimrSchemeOp::ECdcStreamCompressionZstd) {
            return;
        }
        if (record.GetKind() == TChangeRecord::EKind::CdcHeartbeat) {
            return;
        }

        const auto& payload = data.GetData();
        const bool schema = record.GetKind() == TChangeRecord::EKind::CdcSchemaChange;
        if (!schema && payload.size() < CdcZstdMinPayloadBytes) {
            return;
        }
        if (payload.size() > static_cast<size_t>(Max<i32>())) {
            return;
        }

        if (!CCtx_) {
            CCtx_ = ZSTD_createCCtx();
            Y_ENSURE(CCtx_, "failed to allocate a zstd compression context");
        }
        const int level = schema ? ZSTD_defaultCLevel() : CdcZstdDataLevel;
        if (ZSTD_isError(ZSTD_CCtx_setParameter(CCtx_, ZSTD_c_compressionLevel, level))) {
            return;
        }
        const size_t bound = ZSTD_compressBound(payload.size());
        if (bound == 0) {
            return;
        }

        TString compressed = TString::Uninitialized(bound);
        const size_t written = ZSTD_compress2(
            CCtx_, compressed.begin(), compressed.size(), payload.data(), payload.size());
        if (ZSTD_isError(written) || written >= payload.size()) {
            return;
        }
        compressed.resize(written);
        data.SetCodec(NPersQueueCommon::ZSTD);
        UncompressedSize = static_cast<ui32>(payload.size());
        data.SetData(std::move(compressed));
    }

    void SerializeDataChange(TCmdWrite& cmd, const TChangeRecord& record) {
        auto data = MakeDataChunk();
        FillDataChunk(data, record);
        ApplyCompression(data, record);
        cmd.SetData(data.SerializeAsString());
    }

    void SerializeSchemaChange(TCmdWrite& cmd, const TChangeRecord& record) {
        auto data = MakeDataChunk();
        FillDataChunk(data, record);
        ApplyCompression(data, record);

        auto& schemaChange = *cmd.MutableSchemaChange();
        schemaChange.SetStep(record.GetStep());
        schemaChange.SetTxId(record.GetTxId());
        schemaChange.SetData(data.SerializeAsString());
    }

    void SerializeHeartbeat(TCmdWrite& cmd, const TChangeRecord& record) {
        auto data = MakeDataChunk();
        FillDataChunk(data, record);
        ApplyCompression(data, record);

        auto& heartbeat = *cmd.MutableHeartbeat();
        heartbeat.SetStep(record.GetStep());
        heartbeat.SetTxId(record.GetTxId());
        heartbeat.SetData(data.SerializeAsString());
    }

    TMaybe<ui32> UncompressedSize;

protected:
    virtual void FillDataChunk(NKikimrPQClient::TDataChunk& chunk, const TChangeRecord& record) = 0;

public:
    explicit TBaseSerializer(const TChangeRecordSerializerOpts& opts)
        : Opts(opts)
    {}

    ~TBaseSerializer() override {
        if (CCtx_) {
            ZSTD_freeCCtx(CCtx_);
        }
    }

    TBaseSerializer(const TBaseSerializer&) = delete;
    TBaseSerializer& operator=(const TBaseSerializer&) = delete;

    void Serialize(TCmdWrite& cmd, const TChangeRecord& record) override {
        UncompressedSize.Clear();
        cmd.SetSeqNo(record.GetSeqNo());
        cmd.SetCreateTimeMS(record.GetApproximateCreationDateTime().MilliSeconds());
        switch (record.GetKind()) {
        case TChangeRecord::EKind::CdcDataChange:
            SerializeDataChange(cmd, record);
            break;
        case TChangeRecord::EKind::CdcSchemaChange:
            SerializeSchemaChange(cmd, record);
            break;
        case TChangeRecord::EKind::CdcHeartbeat:
            SerializeHeartbeat(cmd, record);
            break;
        case TChangeRecord::EKind::AsyncIndex:
        case TChangeRecord::EKind::IncrementalRestore:
            Y_ENSURE(false, "Unexpected");
        }
        if (UncompressedSize.Defined()) {
            cmd.SetUncompressedSize(static_cast<i32>(*UncompressedSize));
        }
    }

protected:
    const TChangeRecordSerializerOpts Opts;
    ZSTD_CCtx* CCtx_ = nullptr;

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

    TString DebugString(const TChangeRecord&) override {
        return "TProtoSerializer::DebugString() is not implemented";
    }

}; // TProtoSerializer

class TJsonSerializer: public TBaseSerializer {
    friend class TChangeRecord; // used in GetPartitionKey()

protected:
    static NJson::TJsonValue StringToJson(TStringBuf in) {
        NJson::TJsonValue result;
        Y_ENSURE(NJson::ReadJsonTree(in, &result));
        return result;
    }

    static NJson::TJsonValue YsonToJson(TStringBuf in) {
        NJson::TJsonValue result;
        Y_ENSURE(NJson2Yson::DeserializeYsonAsJsonValue(in, &result));
        return result;
    }

    static NJson::TJsonValue UuidToJson(const TCell& cell) {
        TStringStream ss;
        ui16 dw[8];
        Y_ENSURE(cell.Size() == 16);
        cell.CopyDataInto((char*)dw);
        NUuid::UuidToString(dw, ss);
        return NJson::TJsonValue(ss.Str());
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
        case NScheme::NTypeIds::Date32:
            return NJson::TJsonValue(cell.AsValue<i32>());
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Interval64:
        case NScheme::NTypeIds::Timestamp64:
            return NJson::TJsonValue(cell.AsValue<i64>());            
        case NScheme::NTypeIds::Decimal:
            return NJson::TJsonValue(DecimalToString(cell.AsValue<std::pair<ui64, i64>>(), type));
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
            return NJson::TJsonValue(PgToString(cell.AsBuf(), type));
        case NScheme::NTypeIds::Uuid:
            return UuidToJson(cell);
        default:
            Y_ENSURE(false, "Unexpected type");
        }
    }

    static void SerializeJsonKey(TUserTable::TCPtr schema, NJson::TJsonValue& key,
        const NKikimrChangeExchange::TDataChange::TSerializedCells& in)
    {
        Y_ENSURE(in.TagsSize() == schema->KeyColumnIds.size());
        for (size_t i = 0; i < schema->KeyColumnIds.size(); ++i) {
            Y_ENSURE(in.GetTags(i) == schema->KeyColumnIds.at(i));
        }

        TSerializedCellVec cells;
        Y_ENSURE(TSerializedCellVec::TryParse(in.GetData(), cells));

        Y_ENSURE(cells.GetCells().size() == schema->KeyColumnTypes.size());
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
        Y_ENSURE(TSerializedCellVec::TryParse(in.GetData(), cells));
        Y_ENSURE(in.TagsSize() == cells.GetCells().size());

        for (ui32 i = 0; i < in.TagsSize(); ++i) {
            const auto tag = in.GetTags(i);
            const auto& cell = cells.GetCells().at(i);

            auto it = schema->Columns.find(tag);
            Y_ENSURE(it != schema->Columns.end());

            const auto& column = it->second;
            value.InsertValue(column.Name, ToJson(cell, column.Type));
        }
    }

    static void ExtendJson(NJson::TJsonValue& value, const NJson::TJsonValue& ext) {
        Y_ENSURE(ext.GetType() == NJson::JSON_MAP);
        for (const auto& [k, v] : ext.GetMapSafe()) {
            value.InsertValue(k, v);
        }
    }

    static void SerializeVirtualTimestamp(NJson::TJsonValue& value, std::initializer_list<ui64> vt) {
        for (auto v : vt) {
            value.AppendValue(v);
        }
    }

    TString JsonToString(const NJson::TJsonValue& json) const {
        TStringStream str;
        NJson::WriteJson(&str, &json, JsonConfig);
        return str.Str();
    }

    void FillDataChunk(NKikimrPQClient::TDataChunk& data, const TChangeRecord& record) override {
        NJson::TJsonValue json;
        SerializeToJson(json, record);
        data.SetData(JsonToString(json));
    }

    virtual void SerializeToJson(NJson::TJsonValue& json, const TChangeRecord& record) = 0;

public:
    explicit TJsonSerializer(const TChangeRecordSerializerOpts& opts)
        : TBaseSerializer(opts)
        , JsonConfig(NFormats::DefaultJsonWriterConfig())
    {}

    void Serialize(TCmdWrite& cmd, const TChangeRecord& record) override {
        TBaseSerializer::Serialize(cmd, record);
        if (record.GetKind() == TChangeRecord::EKind::CdcDataChange) {
            cmd.SetPartitionKey(record.GetPartitionKey());
        }
    }

    TString DebugString(const TChangeRecord& record) override {
        NJson::TJsonValue json;
        // Debug output may be generated before persistence. Parsing a copy keeps
        // the original serialized body available for the database write.
        TChangeRecord copy = record;
        SerializeToJson(json, copy);

        if (record.GetLockId()) {
            json["lock"]["id"] = record.GetLockId();
            json["lock"]["offset"] = record.GetLockOffset();
        } else {
            json["order"] = record.GetOrder();
        }

        return JsonToString(json);
    }

protected:
    const NJson::TJsonWriterConfig JsonConfig;

}; // TJsonSerializer

class TYdbJsonSerializer: public TJsonSerializer {
protected:
    void SerializeDataChange(NJson::TJsonValue& json, const TChangeRecord& record) {
        Y_ENSURE(record.GetSchema());
        const auto& body = record.GetDataChange();

        if (record.GetKind() == TChangeRecord::EKind::CdcDataChange) {
            SerializeJsonKey(record.GetSchema(), json["key"], body.GetKey());
        } else if (record.GetKind() == TChangeRecord::EKind::AsyncIndex) {
            Y_ENSURE(Opts.Debug);
            SerializeJsonValue(record.GetSchema(), json["key"], body.GetKey());
        }

        if (body.HasOldImage()) {
            SerializeJsonValue(record.GetSchema(), json["oldImage"], body.GetOldImage());
        }

        if (body.HasNewImage()) {
            SerializeJsonValue(record.GetSchema(), json["newImage"], body.GetNewImage());
        }

        auto userCtx = record.GetUserCtx();
        if (userCtx != nullptr) {
            if (Opts.UserSIDs && !userCtx->GetUserSID().empty() ) {
                json["user"] = userCtx->GetUserSID();
            }
            if (Opts.TraceIds && userCtx->GetUserTraceId()) {
                json["traceId"] = userCtx->GetUserTraceId().GetHexTraceId();
            }
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
            Y_ENSURE(false, "Unexpected row operation: " << static_cast<int>(body.GetRowOperationCase()));
        }

        if (Opts.VirtualTimestamps) {
            SerializeVirtualTimestamp(json["ts"], {record.GetStep(), record.GetTxId()});
        }
    }

    void SerializeSchemaChange(NJson::TJsonValue& json, const TChangeRecord& record) {
        auto& tableChange = json["tableChanges"].AppendValue({});
        auto& table = tableChange["table"];
        table["schemaVersion"] = record.GetSchemaVersion();

        Y_ENSURE(record.GetSchema());
        auto schema = record.GetSchema();

        NKikimrSchemeOp::TTableDescription fullSchema;
        schema->GetSchema(fullSchema);

        for (const auto tag : schema->KeyColumnIds) {
            auto it = schema->Columns.find(tag);
            Y_ENSURE(it != schema->Columns.end());
            table["primaryKeyColumnNames"].AppendValue(it->second.Name);
        }

        for (const auto& [tag, column] : schema->Columns) {
            table["columns"][column.Name]["type"] = NScheme::TypeName(column.Type, column.TypeMod);
            if (const auto family = schema->Families.find(column.Family); family != schema->Families.end()) {
                table["columns"][column.Name]["family"] = family->second.GetName();
            } else {
                Y_ENSURE(column.Family == 0, "Unknown column family: " << column.Family);
                table["columns"][column.Name]["family"] = "default";
            }
        }

        auto& families = table["columnFamilies"];
        families.SetType(NJson::JSON_MAP);
        if (!schema->Families.contains(0)) {
            families["default"]["compression"] = "off";
            families["default"]["cacheMode"] = "regular";
        }
        for (const auto& [id, family] : schema->Families) {
            const auto name = family.GetName();
            Y_ENSURE(id == 0 || name != "default",
                "Cannot serialize unnamed non-default column family: " << id);
            auto& definition = families[name];
            const auto& data = family.StorageConfig.GetData();
            if (!data.GetAllowOtherKinds() && data.GetPreferredPoolKind()) {
                definition["data"]["media"] = data.GetPreferredPoolKind();
            }
            definition["compression"] = family.Codec == NTable::NPage::ECodec::Plain ? "off" : "lz4";
            definition["cacheMode"] = family.CacheMode == NTable::NPage::ECacheMode::Regular
                ? "regular" : "in_memory";
        }

        auto& indexes = table["indexes"];
        indexes.SetType(NJson::JSON_MAP);
        for (const auto& index : fullSchema.GetTableIndexes()) {
            if (index.GetState() != NKikimrSchemeOp::EIndexStateReady) {
                continue;
            }

            auto& value = indexes[index.GetName()];
            const auto type = index.GetType();
            const TString typeName = NKikimrSchemeOp::EIndexType_Name(type);
            TStringBuf typeSuffix(typeName);
            Y_ENSURE(type != NKikimrSchemeOp::EIndexTypeInvalid && typeSuffix.SkipPrefix("EIndexType"),
                "Unknown index type: " << type);
            value["type"] = type == NKikimrSchemeOp::EIndexTypeGlobal
                ? "GlobalSync" : TString(typeSuffix);
            auto& indexColumns = value["indexColumns"];
            indexColumns.SetType(NJson::JSON_ARRAY);
            for (const auto& column : index.GetKeyColumnNames()) {
                indexColumns.AppendValue(column);
            }
            auto& dataColumns = value["dataColumns"];
            dataColumns.SetType(NJson::JSON_ARRAY);
            for (const auto& column : index.GetDataColumnNames()) {
                dataColumns.AppendValue(column);
            }
        }

        SerializeVirtualTimestamp(json["ts"], {record.GetStep(), record.GetTxId()});
    }

    void SerializeHeartbeat(NJson::TJsonValue& json, const TChangeRecord& record) {
        SerializeVirtualTimestamp(json["resolved"], {record.GetStep(), record.GetTxId()});
    }

    void SerializeToJson(NJson::TJsonValue& json, const TChangeRecord& record) override {
        switch (record.GetKind()) {
        case TChangeRecord::EKind::AsyncIndex:
        case TChangeRecord::EKind::CdcDataChange:
            return SerializeDataChange(json, record);
        case TChangeRecord::EKind::CdcSchemaChange:
            return SerializeSchemaChange(json, record);
        case TChangeRecord::EKind::CdcHeartbeat:
            return SerializeHeartbeat(json, record);
        case TChangeRecord::EKind::IncrementalRestore:
            Y_ENSURE(false, "Unexpected");
        }
    }

public:
    using TJsonSerializer::TJsonSerializer;

    static TString RenderSchemaChangeJson(const TChangeRecord& record) {
        TChangeRecordSerializerOpts opts;
        TYdbJsonSerializer serializer(opts);
        NJson::TJsonValue json;
        serializer.SerializeSchemaChange(json, record);
        return serializer.JsonToString(json);
    }

}; // TYdbJsonSerializer

class TDynamoDBStreamsJsonSerializer: public TJsonSerializer {
    static void ToAttributeValues(TUserTable::TCPtr schema, NJson::TJsonValue& value,
        const NKikimrChangeExchange::TDataChange::TSerializedCells& in)
    {
        TSerializedCellVec cells;
        Y_ENSURE(TSerializedCellVec::TryParse(in.GetData(), cells));
        Y_ENSURE(in.TagsSize() == cells.GetCells().size());

        for (ui32 i = 0; i < in.TagsSize(); ++i) {
            const auto tag = in.GetTags(i);
            const auto& cell = cells.GetCells().at(i);

            if (cell.IsNull()) {
                continue;
            }

            auto it = schema->Columns.find(tag);
            Y_ENSURE(it != schema->Columns.end());

            const auto& column = it->second;
            const auto& name = column.Name;
            const auto type = column.Type.GetTypeId();

            if (name == "__Hash" || name == "__CreatedAt") {
                continue; // hidden column
            } else if (name.StartsWith("__Hash_")) {
                bool indexed = false;
                for (const auto& [_, index] : schema->Indexes) {
                    if (index.Type != TUserTable::TTableIndex::EType::EIndexTypeGlobalAsync) {
                        continue;
                    }
                    Y_ENSURE(index.KeyColumnIds.size() >= 1);
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
        Y_ENSURE(record.GetKind() == TChangeRecord::EKind::CdcDataChange);
        Y_ENSURE(record.GetSchema());

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
        const auto& body = record.GetDataChange();

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
            Y_ENSURE(false, "Unexpected row operation: " << static_cast<int>(body.GetRowOperationCase()));
        }

        auto userCtx = record.GetUserCtx();
        if (userCtx != nullptr) {
            if (Opts.UserSIDs && !userCtx->GetUserSID().empty()) {
                auto& userIdentityJson = json["userIdentity"];
                if (userCtx->GetUserSID() == BUILTIN_ACL_CDC_TTL) {
                    userIdentityJson["type"] = "Service";
                    userIdentityJson["principalId"] = "dynamodb.amazonaws.com";
                } else {
                    userIdentityJson["type"] = "User";
                    userIdentityJson["principalId"] = userCtx->GetUserSID();
                }
            }
        }
    }

public:
    using TJsonSerializer::TJsonSerializer;

}; // TDynamoDBStreamsJsonSerializer

class TDebeziumJsonSerializer: public TJsonSerializer {
protected:
    void SerializeToJson(NJson::TJsonValue& json, const TChangeRecord& record) override {
        Y_ENSURE(record.GetKind() == TChangeRecord::EKind::CdcDataChange);
        Y_ENSURE(record.GetSchema());

        const auto& body = record.GetDataChange();
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
                Y_ENSURE(false, "Unexpected row operation: " << static_cast<int>(body.GetRowOperationCase()));
            }
        }

        auto& sourceJson = valueJson["payload"]["source"];
        sourceJson = NJson::TJsonMap({
            {"version", "1.0.0"},
            {"connector", "ydb"},
            {"ts_ms", record.GetApproximateCreationDateTime().MilliSeconds()},
            {"snapshot", record.GetSource() == TChangeRecord::ESource::InitialScan},
            {"step", record.GetStep()},
            {"txId", record.GetTxId()},
            // TODO: db & table
        });

        auto userCtx = record.GetUserCtx();
        if (userCtx != nullptr) {
            if (Opts.UserSIDs && !userCtx->GetUserSID().empty()) {
                sourceJson["user"] = userCtx->GetUserSID();
            }
            if (Opts.TraceIds && userCtx->GetUserTraceId()) {
                sourceJson["traceId"] = userCtx->GetUserTraceId().GetHexTraceId();
            }
        }
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

class TBinaryCdcSerializer: public TBaseSerializer {
    // A view into a TSerializedCellVec that stays alive until the writer finishes.
    struct TValueRef {
        ui16 Index = 0;
        NScheme::TTypeInfo Type;
        TCell Cell;
    };

    struct TColumnSlot {
        ui16 Index = 0;
        NScheme::TTypeInfo Type;
    };

    struct TColumnMap {
        TVector<std::pair<ui32, NScheme::TTypeInfo>> Keys;
        TVector<TColumnSlot> Values;
        THashMap<ui32, ui16> TagToIndex;
        bool Wide = false;
    };

    static TColumnMap BuildColumnMap(const TUserTable& table) {
        TColumnMap map;
        map.Keys.reserve(table.KeyColumnIds.size());
        for (const auto tag : table.KeyColumnIds) {
            const auto it = table.Columns.find(tag);
            Y_ENSURE(it != table.Columns.end());
            map.Keys.emplace_back(tag, it->second.Type);
        }

        TVector<std::pair<TString, ui32>> names;
        names.reserve(table.Columns.size());
        for (const auto& [tag, column] : table.Columns) {
            if (!column.IsKey) {
                names.emplace_back(column.Name, tag);
            }
        }
        Sort(names);

        map.Values.reserve(names.size());
        for (ui16 index = 0; index < names.size(); ++index) {
            const auto tag = names[index].second;
            const auto& column = table.Columns.at(tag);
            map.Values.push_back(TColumnSlot{index, column.Type});
            map.TagToIndex.emplace(tag, index);
        }
        map.Wide = map.Values.size() > 127;
        return map;
    }

    const TColumnMap& ColumnMapFor(const TUserTable::TCPtr& table) {
        const ui64 version = table->GetTableSchemaVersion();
        if (CachedMap && CachedTable.Get() == table.Get() && CachedVersion == version) {
            return *CachedMap;
        }
        CachedTable = table;
        CachedVersion = version;
        CachedMap = BuildColumnMap(*table);
        return *CachedMap;
    }

    // storage outlives every TCell copied into out.
    static void CollectValues(const NKikimrChangeExchange::TDataChange::TSerializedCells& in,
            const TColumnMap& map, TSerializedCellVec& storage, TVector<TValueRef>& out)
    {
        if (!in.TagsSize()) {
            return;
        }

        Y_ENSURE(TSerializedCellVec::TryParse(in.GetData(), storage));
        Y_ENSURE(in.TagsSize() == storage.GetCells().size());

        out.reserve(in.TagsSize());
        const auto cells = storage.GetCells();
        for (ui32 i = 0; i < in.TagsSize(); ++i) {
            const auto tag = in.GetTags(i);
            const auto it = map.TagToIndex.find(tag);
            Y_ENSURE(it != map.TagToIndex.end(), "binary CDC value tag is not in the schema: " << tag);
            const auto index = it->second;
            out.push_back(TValueRef{index, map.Values[index].Type, cells[i]});
        }
        Sort(out, [](const TValueRef& left, const TValueRef& right) {
            return left.Index < right.Index;
        });
    }

    static size_t IndexedWireBytes(bool wide, const TValueRef& value) {
        size_t bytes = wide ? 2 : 1;
        if (!value.Cell.IsNull()) {
            bytes += NChangeExchange::BinaryCdcCellSize(value.Cell, value.Type);
        }
        return bytes;
    }

    void FillDataChunk(NKikimrPQClient::TDataChunk& data, const TChangeRecord& record) override {
        using NChangeExchange::EBinaryCdcFlag;
        using NChangeExchange::EBinaryCdcKind;
        using NChangeExchange::EBinaryCdcOp;
        using NChangeExchange::TBinaryCdcHeader;
        using NChangeExchange::TBinaryCdcWriter;

        switch (record.GetKind()) {
        case TChangeRecord::EKind::CdcSchemaChange: {
            const TString json = TYdbJsonSerializer::RenderSchemaChangeJson(record);
            TBinaryCdcHeader header;
            header.Version = NChangeExchange::BinaryCdcVersion;
            header.Kind = EBinaryCdcKind::Schema;
            TBinaryCdcWriter writer;
            writer.Start(header, 8 + sizeof(ui32) + json.size());
            writer.SetSchemaJson(json);
            data.SetData(writer.Finish());
            return;
        }
        case TChangeRecord::EKind::CdcHeartbeat: {
            TBinaryCdcHeader header;
            header.Version = NChangeExchange::BinaryCdcVersion;
            header.Kind = EBinaryCdcKind::Heartbeat;
            header.Flags = static_cast<ui8>(EBinaryCdcFlag::HasTs);
            header.Step = record.GetStep();
            header.TxId = record.GetTxId();
            TBinaryCdcWriter writer;
            writer.Start(header, 8 + sizeof(ui64) * 2);
            data.SetData(writer.Finish());
            return;
        }
        case TChangeRecord::EKind::CdcDataChange:
            break;
        case TChangeRecord::EKind::AsyncIndex:
        case TChangeRecord::EKind::IncrementalRestore:
            Y_ENSURE(false, "Unexpected");
        }

        Y_ENSURE(record.GetSchema());
        const auto& map = ColumnMapFor(record.GetSchema());
        const auto& body = record.GetDataChange();

        TSerializedCellVec keyCells;
        Y_ENSURE(TSerializedCellVec::TryParse(body.GetKey().GetData(), keyCells));
        Y_ENSURE(body.GetKey().TagsSize() == map.Keys.size());
        Y_ENSURE(keyCells.GetCells().size() == map.Keys.size());
        for (size_t i = 0; i < map.Keys.size(); ++i) {
            Y_ENSURE(body.GetKey().GetTags(i) == map.Keys[i].first);
        }

        const bool hasOld = body.HasOldImage();
        const bool hasNew = body.HasNewImage();
        const bool hasImage = hasOld || hasNew;
        TSerializedCellVec valueStorage;
        TSerializedCellVec oldStorage;
        TSerializedCellVec newStorage;
        TVector<TValueRef> values;
        TVector<TValueRef> oldImage;
        TVector<TValueRef> newImage;
        EBinaryCdcOp op = EBinaryCdcOp::None;
        switch (body.GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert:
            op = EBinaryCdcOp::Upsert;
            if (!hasImage) {
                CollectValues(body.GetUpsert(), map, valueStorage, values);
            }
            break;
        case NKikimrChangeExchange::TDataChange::kReset:
            op = EBinaryCdcOp::Reset;
            if (!hasImage) {
                CollectValues(body.GetReset(), map, valueStorage, values);
            }
            break;
        case NKikimrChangeExchange::TDataChange::kErase:
            op = EBinaryCdcOp::Erase;
            break;
        default:
            Y_ENSURE(false, "Unexpected row operation: " << static_cast<int>(body.GetRowOperationCase()));
        }
        if (hasOld) {
            CollectValues(body.GetOldImage(), map, oldStorage, oldImage);
        }
        if (hasNew) {
            CollectValues(body.GetNewImage(), map, newStorage, newImage);
        }

        TString user;
        TString trace;
        if (const auto userCtx = record.GetUserCtx()) {
            if (Opts.UserSIDs && !userCtx->GetUserSID().empty()) {
                user = userCtx->GetUserSID();
            }
            if (Opts.TraceIds && userCtx->GetUserTraceId()) {
                trace = userCtx->GetUserTraceId().GetHexTraceId();
            }
        }

        TBinaryCdcHeader header;
        header.Version = NChangeExchange::BinaryCdcVersion;
        header.Kind = EBinaryCdcKind::Data;
        header.Op = op;
        header.KeyCount = map.Keys.size();
        header.ValueCount = values.size();
        header.OldImageCount = oldImage.size();
        header.NewImageCount = newImage.size();
        if (map.Wide) {
            header.Flags |= static_cast<ui8>(EBinaryCdcFlag::WideIndex);
        }
        if (hasOld) {
            header.Flags |= static_cast<ui8>(EBinaryCdcFlag::HasOldImage);
        }
        if (hasNew) {
            header.Flags |= static_cast<ui8>(EBinaryCdcFlag::HasNewImage);
        }
        if (Opts.VirtualTimestamps) {
            header.Flags |= static_cast<ui8>(EBinaryCdcFlag::HasTs);
            header.Step = record.GetStep();
            header.TxId = record.GetTxId();
        }
        if (!user.empty()) {
            header.Flags |= static_cast<ui8>(EBinaryCdcFlag::HasUser);
        }
        if (!trace.empty()) {
            header.Flags |= static_cast<ui8>(EBinaryCdcFlag::HasTrace);
        }
        for (const auto& cell : keyCells.GetCells()) {
            if (cell.IsNull()) {
                header.Flags |= static_cast<ui8>(EBinaryCdcFlag::NullKeys);
                break;
            }
        }

        size_t bytes = 8;
        if (hasOld) {
            bytes += sizeof(ui16);
        }
        if (hasNew) {
            bytes += sizeof(ui16);
        }
        if (Opts.VirtualTimestamps) {
            bytes += sizeof(ui64) * 2;
        }
        const bool nullKeys = (header.Flags & static_cast<ui8>(EBinaryCdcFlag::NullKeys)) != 0;
        for (size_t i = 0; i < map.Keys.size(); ++i) {
            const auto& cell = keyCells.GetCells()[i];
            if (nullKeys) {
                bytes += 1;
            }
            if (!cell.IsNull()) {
                bytes += NChangeExchange::BinaryCdcCellSize(cell, map.Keys[i].second);
            }
        }
        for (const auto& value : values) {
            bytes += IndexedWireBytes(map.Wide, value);
        }
        for (const auto& value : oldImage) {
            bytes += IndexedWireBytes(map.Wide, value);
        }
        for (const auto& value : newImage) {
            bytes += IndexedWireBytes(map.Wide, value);
        }
        if (!user.empty()) {
            bytes += sizeof(ui16) + user.size();
        }
        if (!trace.empty()) {
            bytes += sizeof(ui16) + trace.size();
        }

        TBinaryCdcWriter writer;
        writer.Start(header, bytes);
        for (size_t i = 0; i < map.Keys.size(); ++i) {
            writer.AddKey(keyCells.GetCells()[i], map.Keys[i].second);
        }
        for (const auto& value : values) {
            writer.AddValue(value.Index, value.Cell, value.Type);
        }
        for (const auto& value : oldImage) {
            writer.AddOldImage(value.Index, value.Cell, value.Type);
        }
        for (const auto& value : newImage) {
            writer.AddNewImage(value.Index, value.Cell, value.Type);
        }
        if (!user.empty()) {
            writer.SetUser(user);
        }
        if (!trace.empty()) {
            writer.SetTrace(trace);
        }
        data.SetData(writer.Finish());
    }

    TUserTable::TCPtr CachedTable;
    TMaybe<TColumnMap> CachedMap;
    ui64 CachedVersion = 0;

public:
    using TBaseSerializer::TBaseSerializer;

    TString DebugString(const TChangeRecord& record) override {
        return TStringBuilder() << "binary CDC kind " << static_cast<int>(record.GetKind());
    }

}; // TBinaryCdcSerializer

TChangeRecordSerializerOpts TChangeRecordSerializerOpts::DebugOpts() {
    TChangeRecordSerializerOpts opts;
    opts.Debug = true;
    return opts;
}

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
    case TUserTable::TCdcStream::EFormat::ECdcStreamFormatBinary:
        return new TBinaryCdcSerializer(opts);
    default:
        Y_ENSURE(false, "Unsupported format");
    }
}

IChangeRecordSerializer* CreateChangeRecordDebugSerializer() {
    return new TYdbJsonSerializer(TChangeRecordSerializerOpts::DebugOpts());
}

TString TChangeRecord::GetPartitionKey() const {
    if (PartitionKey) {
        return *PartitionKey;
    }

    Y_ENSURE(Kind == EKind::CdcDataChange);
    Y_ENSURE(Schema);

    const auto& body = GetDataChange();

    NJson::TJsonValue key;
    TJsonSerializer::SerializeJsonKey(Schema, key, body.GetKey());

    PartitionKey.ConstructInPlace(MD5::Calc(WriteJson(key, false)));
    return *PartitionKey;
}

TString TChangeRecord::GetRawKeyPartitionKey() const {
    if (RawKeyPartitionKey) {
        return *RawKeyPartitionKey;
    }

    Y_ENSURE(Kind == EKind::CdcDataChange);
    const auto& key = GetDataChange().GetKey().GetData();
    RawKeyPartitionKey.ConstructInPlace(MD5::Calc(TStringBuf(key.data(), key.size())));
    return *RawKeyPartitionKey;
}

}
