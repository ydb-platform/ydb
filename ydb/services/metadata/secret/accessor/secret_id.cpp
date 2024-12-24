#include "secret_id.h"

#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <library/cpp/digest/md5/md5.h>

namespace NKikimr::NMetadata::NSecret {

TString TSecretId::SerializeToString() const {
    TStringBuilder sb;
    sb << "USId:" << OwnerUserId << ":" << SecretId;
    return sb;
}

TString TSecretIdOrValue::DebugString() const {
    return std::visit(TOverloaded(
        [](std::monostate) -> TString{
            return "__NONE__";
        },
        [](const TSecretId& id) -> TString{
            return id.SerializeToString();
        },
        [](const TSecretName& name) -> TString{
            return name.SerializeToString();
        },
        [](const TString& value) -> TString{
            return MD5::Calc(value);
        }
    ),
    State);
}

}
