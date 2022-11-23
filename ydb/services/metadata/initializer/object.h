#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/abstract/manager.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

namespace NKikimr::NMetadataInitializer {

class TDBInitializationKey {
private:
    YDB_READONLY_PROTECT_DEF(TString, ComponentId);
    YDB_READONLY_PROTECT_DEF(TString, ModificationId);
public:
    TDBInitializationKey() = default;
    TDBInitializationKey(const TString& componentId, const TString& modificationId)
        : ComponentId(componentId)
        , ModificationId(modificationId)
    {
    }

    bool operator<(const TDBInitializationKey& item) const {
        return std::tie(ComponentId, ModificationId) < std::tie(item.ComponentId, item.ModificationId);
    }
};

class TDBInitialization: public TDBInitializationKey, public NMetadataManager::TObject<TDBInitialization> {
private:
    using TBase = TDBInitializationKey;
    YDB_READONLY(TInstant, Instant, AppData()->TimeProvider->Now());
public:
    static NMetadata::TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& /*settings*/,
        const NMetadata::IOperationsManager::TModificationContext& /*context*/) {
        NMetadataManager::TTableRecord result;
        return result;
    }

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, ComponentIdIdx, -1);
        YDB_ACCESSOR(i32, ModificationIdIdx, -1);
        YDB_ACCESSOR(i32, InstantIdx, -1);
    public:
        static inline const TString ComponentId = "componentId";
        static inline const TString ModificationId = "modificationId";
        static inline const TString Instant = "instant";
        static std::vector<TString> GetPKColumnIds();
        static std::vector<Ydb::Column> GetPKColumns();
        static std::vector<Ydb::Column> GetColumns();

        TDecoder(const Ydb::ResultSet& rawData) {
            ComponentIdIdx = GetFieldIndex(rawData, ComponentId);
            ModificationIdIdx = GetFieldIndex(rawData, ModificationId);
            InstantIdx = GetFieldIndex(rawData, Instant);
        }
    };

    using TBase::TBase;

    static void AlteringPreparation(std::vector<TDBInitialization>&& objects,
        NMetadataManager::IAlterPreparationController<TDBInitialization>::TPtr controller,
        const NMetadata::IOperationsManager::TModificationContext& context);
    static TString GetStorageTablePath();
    static TString GetStorageHistoryTablePath() {
        return "";
    }
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NKikimr::NMetadataManager::TTableRecord SerializeToRecord() const;
    static TString GetTypeId() {
        return "INITIALIZATION";
    }
};

}
