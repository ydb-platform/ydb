#include "object.h"
#include "behaviour.h"


namespace NKikimr::NKqp {

//// TResourcePoolClassifierConfig::TDecoder

TResourcePoolClassifierConfig::TDecoder::TDecoder(const Ydb::ResultSet& rawData)\
    : NameIdx(GetFieldIndex(rawData, Name))
    , RankIdx(GetFieldIndex(rawData, Rank))
    , ConfigIdx(GetFieldIndex(rawData, Config))
{}

//// TResourcePoolClassifierConfig

bool TResourcePoolClassifierConfig::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawData) {
    if (!decoder.Read(decoder.GetNameIdx(), Name, rawData)) {
        return false;
    }
    if (!decoder.Read(decoder.GetRankIdx(), Rank, rawData)) {
        return false;
    }
    if (!decoder.Read(decoder.GetConfigIdx(), ConfigJson, rawData)) {
        return false;
    }
    return true;
}

NMetadata::NInternal::TTableRecord TResourcePoolClassifierConfig::SerializeToRecord() const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TDecoder::Name, NMetadata::NInternal::TYDBValue::Utf8(Name));
    result.SetColumn(TDecoder::Rank, NMetadata::NInternal::TYDBValue::UInt64(Rank));
    result.SetColumn(TDecoder::Config, NMetadata::NInternal::TYDBValue::RawBytes(ConfigJson));
    return result;
}

NJson::TJsonValue TResourcePoolClassifierConfig::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::Name, Name);
    result.InsertValue(TDecoder::Rank, Rank);
    result.InsertValue(TDecoder::Config, ConfigJson);
    return result;
}

NMetadata::IClassBehaviour::TPtr TResourcePoolClassifierConfig::GetBehaviour() {
    return TResourcePoolClassifierBehaviour::GetInstance();
}

TString TResourcePoolClassifierConfig::GetTypeId() {
    return "RESOURCE_POOL_CLASSIFIER";
}

}  // namespace NKikimr::NKqp
