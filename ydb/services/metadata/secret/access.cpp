#include "access.h"
#include "access_behaviour.h"
#include "checker_access.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadata::NSecret {

IClassBehaviour::TPtr TAccess::GetBehaviour() {
    return TAccessBehaviour::GetInstance();
}

bool TAccess::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetOwnerUserIdIdx(), OwnerUserId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetSecretIdIdx(), SecretId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetAccessSIDIdx(), AccessSID, rawValue)) {
        return false;
    }
    return true;
}

NInternal::TTableRecord TAccess::SerializeToRecord() const {
    NInternal::TTableRecord result;
    result.SetColumn(TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(OwnerUserId));
    result.SetColumn(TDecoder::SecretId, NInternal::TYDBValue::Utf8(SecretId));
    result.SetColumn(TDecoder::AccessSID, NInternal::TYDBValue::Utf8(AccessSID));
    return result;
}

}
