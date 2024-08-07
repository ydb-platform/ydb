#include "object.h"
#include "behaviour.h"

#include <ydb/core/resource_pools/resource_pool_settings.h>


namespace NKikimr::NKqp {

using namespace NResourcePool;


//// TResourcePoolClassifierConfig::TDecoder

TResourcePoolClassifierConfig::TDecoder::TDecoder(const Ydb::ResultSet& rawData)
    : DatabaseIdx(GetFieldIndex(rawData, Database))
    , NameIdx(GetFieldIndex(rawData, Name))
    , RankIdx(GetFieldIndex(rawData, Rank))
    , ResourcePoolIdx(GetFieldIndex(rawData, ResourcePool))
    , MembernameIdx(GetFieldIndex(rawData, Membername))
{}

//// TResourcePoolClassifierConfig

bool TResourcePoolClassifierConfig::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawData) {
    if (!decoder.Read(decoder.GetDatabaseIdx(), Database, rawData)) {
        return false;
    }
    if (!decoder.Read(decoder.GetNameIdx(), Name, rawData)) {
        return false;
    }
    if (!decoder.Read(decoder.GetRankIdx(), Rank, rawData)) {
        Rank = -1;
    }
    if (!decoder.Read(decoder.GetResourcePoolIdx(), ResourcePool, rawData)) {
        ResourcePool = DEFAULT_POOL_ID;
    }
    if (!decoder.Read(decoder.GetMembernameIdx(), Membername, rawData)) {
        Membername = "";
    }
    return true;
}

NMetadata::NInternal::TTableRecord TResourcePoolClassifierConfig::SerializeToRecord() const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TDecoder::Database, NMetadata::NInternal::TYDBValue::Utf8(Database));
    result.SetColumn(TDecoder::Name, NMetadata::NInternal::TYDBValue::Utf8(Name));
    result.SetColumn(TDecoder::Rank, NMetadata::NInternal::TYDBValue::Int64(Rank));
    result.SetColumn(TDecoder::ResourcePool, NMetadata::NInternal::TYDBValue::Utf8(ResourcePool));
    result.SetColumn(TDecoder::Membername, NMetadata::NInternal::TYDBValue::Utf8(Membername));
    return result;
}

NJson::TJsonValue TResourcePoolClassifierConfig::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::Database, Database);
    result.InsertValue(TDecoder::Name, Name);
    result.InsertValue(TDecoder::Rank, Rank);
    result.InsertValue(TDecoder::ResourcePool, ResourcePool);
    result.InsertValue(TDecoder::Membername, Membername);
    return result;
}

bool TResourcePoolClassifierConfig::operator==(const TResourcePoolClassifierConfig& other) const {
    return std::tie(Database, Name, Rank, ResourcePool, Membername)
        == std::tie(other.Database, other.Name, other.Rank, other.ResourcePool, other.Membername);
}

NMetadata::IClassBehaviour::TPtr TResourcePoolClassifierConfig::GetBehaviour() {
    return TResourcePoolClassifierBehaviour::GetInstance();
}

TString TResourcePoolClassifierConfig::GetTypeId() {
    return "RESOURCE_POOL_CLASSIFIER";
}

}  // namespace NKikimr::NKqp
