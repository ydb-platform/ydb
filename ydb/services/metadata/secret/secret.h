#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/abstract/manager.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretId {
private:
    YDB_READONLY_PROTECT_DEF(TString, OwnerUserId);
    YDB_READONLY_PROTECT_DEF(TString, SecretId);
public:
    TSecretId() = default;
    TSecretId(const TString& ownerUserId, const TString& secretId)
        : OwnerUserId(ownerUserId)
        , SecretId(secretId)
    {
    }

    bool operator<(const TSecretId& item) const {
        return std::tie(OwnerUserId, SecretId) < std::tie(item.OwnerUserId, item.SecretId);
    }
};

class TSecret: public TSecretId, public NMetadataManager::TObject<TSecret> {
private:
    using TBase = TSecretId;
    YDB_ACCESSOR_DEF(TString, Value);
public:
    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, OwnerUserIdIdx, -1);
        YDB_ACCESSOR(i32, SecretIdIdx, -1);
        YDB_ACCESSOR(i32, ValueIdx, -1);
    public:
        static inline const TString OwnerUserId = "ownerUserId";
        static inline const TString SecretId = "secretId";
        static inline const TString Value = "value";
        static std::vector<TString> GetPKColumnIds();
        static std::vector<Ydb::Column> GetPKColumns();
        static std::vector<Ydb::Column> GetColumns();

        TDecoder(const Ydb::ResultSet& rawData) {
            OwnerUserIdIdx = GetFieldIndex(rawData, OwnerUserId);
            SecretIdIdx = GetFieldIndex(rawData, SecretId);
            ValueIdx = GetFieldIndex(rawData, Value);
        }
    };

    using TBase::TBase;

    static void AlteringPreparation(std::vector<TSecret>&& objects,
        NMetadataManager::IAlterPreparationController<TSecret>::TPtr controller,
        const NMetadata::IOperationsManager::TModificationContext& context);
    static TString GetStorageTablePath();
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NMetadataManager::TTableRecord SerializeToRecord() const;
    static NMetadata::TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IOperationsManager::TModificationContext& context);
    static TString GetTypeId() {
        return "SECRET";
    }

};

}
