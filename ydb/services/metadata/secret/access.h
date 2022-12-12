#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/secret/secret.h>

namespace NKikimr::NMetadata::NSecret {

class TAccess: public TSecretId, public NModifications::TObject<TAccess> {
private:
    using TBase = NModifications::TObject<TAccess>;
    YDB_ACCESSOR_DEF(TString, AccessSID);
public:
    static IClassBehaviour::TPtr GetBehaviour();

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, OwnerUserIdIdx, -1);
        YDB_ACCESSOR(i32, SecretIdIdx, -1);
        YDB_ACCESSOR(i32, AccessSIDIdx, -1);
    public:
        static inline const TString OwnerUserId = "ownerUserId";
        static inline const TString SecretId = "secretId";
        static inline const TString AccessSID = "accessSID";

        TDecoder(const Ydb::ResultSet& rawData) {
            OwnerUserIdIdx = GetFieldIndex(rawData, OwnerUserId);
            SecretIdIdx = GetFieldIndex(rawData, SecretId);
            AccessSIDIdx = GetFieldIndex(rawData, AccessSID);
        }
    };

    using TBase::TBase;

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NInternal::TTableRecord SerializeToRecord() const;

    static TString GetTypeId() {
        return "SECRET_ACCESS";
    }
};

}
