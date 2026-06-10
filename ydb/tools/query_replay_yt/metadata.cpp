#include "metadata.h"

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/protos/kqp.pb.h>

TMetadataInfoHolder::TMetadataInfoHolder(THashMap<TString, NYql::TKikimrTableMetadataPtr>&& tableMetadata)
    : TableMetadata(std::move(tableMetadata))
{
    for (auto& [name, ptr] : TableMetadata) {
        Y_UNUSED(name);
        for (auto implTable : ptr->ImplTables) {
            YQL_ENSURE(implTable);
            do {
                auto nextImplTable = implTable->Next;
                Indexes.emplace(implTable->Name, std::move(implTable));
                implTable = std::move(nextImplTable);
            } while (implTable);
        }
    }
}

const NYql::TKikimrTableMetadataPtr* TMetadataInfoHolder::FindPtr(const TString& key) const {
    const auto* result = TableMetadata.FindPtr(key);
    if (result != nullptr) {
        return result;
    }
    return Indexes.FindPtr(key);
}

THashMap<TString, NYql::TKikimrTableMetadataPtr> ExtractStaticMetadata(const NJson::TJsonValue& data) {
    EMetaSerializationType metaType = EMetaSerializationType::EncodedProto;
    if (data.Has("table_meta_serialization_type")) {
        metaType = static_cast<EMetaSerializationType>(data["table_meta_serialization_type"].GetUIntegerSafe());
    }
    THashMap<TString, NYql::TKikimrTableMetadataPtr> meta;

    if (metaType == EMetaSerializationType::EncodedProto) {
        static NJson::TJsonReaderConfig readerConfig;
        NJson::TJsonValue tablemetajson;
        TStringInput in(data["table_metadata"].GetStringSafe());
        NJson::ReadJsonTree(&in, &readerConfig, &tablemetajson, false);
        Y_ENSURE(tablemetajson.IsArray());
        for (auto& node : tablemetajson.GetArray()) {
            NKikimrKqp::TKqpTableMetadataProto proto;
            TString decoded = Base64Decode(node.GetStringRobust());
            Y_ENSURE(proto.ParseFromString(decoded));
            NYql::TKikimrTableMetadataPtr ptr = MakeIntrusive<NYql::TKikimrTableMetadata>(&proto);
            meta.emplace(proto.GetName(), ptr);
        }
    } else {
        Y_ENSURE(data["table_metadata"].IsArray());
        for (auto& node : data["table_metadata"].GetArray()) {
            NKikimrKqp::TKqpTableMetadataProto proto;
            NProtobufJson::Json2Proto(node.GetStringRobust(), proto);
            NYql::TKikimrTableMetadataPtr ptr = MakeIntrusive<NYql::TKikimrTableMetadata>(&proto);
            meta.emplace(proto.GetName(), ptr);
        }
    }
    return meta;
}
