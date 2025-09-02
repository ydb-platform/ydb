#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/secret/accessor/secret_id.h>

namespace NKikimr::NMetadata::NSecret {

class TSecret: public TSecretId, public NModifications::TObject<TSecret> {
private:
    using TBase = TSecretId;
    YDB_ACCESSOR_DEF(TString, Value);
public:
    static IClassBehaviour::TPtr GetBehaviour();

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, OwnerUserIdIdx, -1);
        YDB_ACCESSOR(i32, SecretIdIdx, -1);
        YDB_ACCESSOR(i32, ValueIdx, -1);
    public:
        static inline const TString OwnerUserId = "ownerUserId";
        static inline const TString SecretId = "secretId";
        static inline const TString Value = "value";

        TDecoder(const Ydb::ResultSet& rawData) {
            OwnerUserIdIdx = GetFieldIndex(rawData, OwnerUserId);
            SecretIdIdx = GetFieldIndex(rawData, SecretId);
            ValueIdx = GetFieldIndex(rawData, Value);
        }
    };

    using TBase::TBase;

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NInternal::TTableRecord SerializeToRecord() const;
    static TString GetTypeId() {
        return "SECRET";
    }

};

}
