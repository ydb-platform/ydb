#include "checker_secret.h"
#include "secret.h"
#include "secret_behaviour.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadata::NSecret {

bool TSecret::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetOwnerUserIdIdx(), OwnerUserId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetSecretIdIdx(), SecretId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetValueIdx(), Value, rawValue)) {
        return false;
    }
    return true;
}

NInternal::TTableRecord TSecret::SerializeToRecord() const {
    NInternal::TTableRecord result;
    result.SetColumn(TDecoder::OwnerUserId, NInternal::TYDBValue::Bytes(OwnerUserId));
    result.SetColumn(TDecoder::SecretId, NInternal::TYDBValue::Bytes(SecretId));
    result.SetColumn(TDecoder::Value, NInternal::TYDBValue::Bytes(Value));
    return result;
}

IClassBehaviour::TPtr TSecret::GetBehaviour() {
    static std::shared_ptr<NSecret::TSecretBehaviour> result = std::make_shared<NSecret::TSecretBehaviour>();
    return result;
}

TString TSecretId::SerializeToString() const {
    TStringBuilder sb;
    sb << "USId:" << OwnerUserId << ":" << SecretId;
    return sb;
}

bool TSecretId::DeserializeFromString(const TString& info) {
    static const TString prefix = "USId:";
    if (!info.StartsWith(prefix)) {
        return false;
    }
    TStringBuf sb(info.data(), info.size());
    sb.Skip(prefix.size());
    TStringBuf uId;
    TStringBuf sId;
    if (!sb.TrySplit(':', uId, sId)) {
        return false;
    }
    OwnerUserId = uId;
    SecretId = sId;
    return true;
}

}
