#include "checker_secret.h"
#include "secret.h"
#include "secret_behaviour.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <library/cpp/digest/md5/md5.h>

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
    result.SetColumn(TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(OwnerUserId));
    result.SetColumn(TDecoder::SecretId, NInternal::TYDBValue::Utf8(SecretId));
    result.SetColumn(TDecoder::Value, NInternal::TYDBValue::Utf8(Value));
    return result;
}

IClassBehaviour::TPtr TSecret::GetBehaviour() {
    return TSecretBehaviour::GetInstance();
}

TString TSecretId::SerializeToString() const {
    TStringBuilder sb;
    sb << "USId:" << OwnerUserId << ":" << SecretId;
    return sb;
}


TString TSecretIdOrValue::DebugString() const {
    if (SecretId) {
        return SecretId->SerializeToString();
    } else if (Value) {
        return MD5::Calc(*Value);
    }
    return "";
}

}
