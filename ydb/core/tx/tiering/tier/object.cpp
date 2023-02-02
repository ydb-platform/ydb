#include "object.h"
#include "behaviour.h"

#include <ydb/core/tx/tiering/tier/checker.h>

#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NColumnShard::NTiers {

NMetadata::IClassBehaviour::TPtr TTierConfig::GetBehaviour() {
    static std::shared_ptr<TTierConfigBehaviour> result = std::make_shared<TTierConfigBehaviour>();
    return result;
}

NJson::TJsonValue TTierConfig::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::TierName, TierName);
    NProtobufJson::Proto2Json(ProtoConfig, result.InsertValue(TDecoder::TierConfig, NJson::JSON_MAP));
    return result;
}

bool TTierConfig::IsSame(const TTierConfig& item) const {
    return TierName == item.TierName && ProtoConfig.SerializeAsString() == item.ProtoConfig.SerializeAsString();
}

bool TTierConfig::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r) {
    if (!decoder.Read(decoder.GetTierNameIdx(), TierName, r)) {
        return false;
    }
    if (!decoder.ReadDebugProto(decoder.GetTierConfigIdx(), ProtoConfig, r)) {
        return false;
    }
    return true;
}

NMetadata::NInternal::TTableRecord TTierConfig::SerializeToRecord() const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TDecoder::TierName, NMetadata::NInternal::TYDBValue::Utf8(TierName));
    result.SetColumn(TDecoder::TierConfig, NMetadata::NInternal::TYDBValue::Utf8(ProtoConfig.DebugString()));
    return result;
}

NKikimrSchemeOp::TS3Settings TTierConfig::GetPatchedConfig(
    std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) const
{
    auto config = ProtoConfig.GetObjectStorage();
    if (secrets) {
        secrets->PatchString(*config.MutableAccessKey());
        secrets->PatchString(*config.MutableSecretKey());
    }
    return config;
}

}
