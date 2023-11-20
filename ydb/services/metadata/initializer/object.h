#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <library/cpp/time_provider/time_provider.h>


namespace NKikimr::NMetadata::NInitializer {

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

class TDBInitialization: public TDBInitializationKey, public NModifications::TObject<TDBInitialization> {
private:
    using TBase = TDBInitializationKey;
    YDB_READONLY(TInstant, Instant, AppData()->TimeProvider->Now());
public:
    static IClassBehaviour::TPtr GetBehaviour();

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, ComponentIdIdx, -1);
        YDB_ACCESSOR(i32, ModificationIdIdx, -1);
        YDB_ACCESSOR(i32, InstantIdx, -1);
    public:
        static inline const TString ComponentId = "componentId";
        static inline const TString ModificationId = "modificationId";
        static inline const TString Instant = "instant";

        TDecoder(const Ydb::ResultSet& rawData) {
            ComponentIdIdx = GetFieldIndex(rawData, ComponentId);
            ModificationIdIdx = GetFieldIndex(rawData, ModificationId);
            InstantIdx = GetFieldIndex(rawData, Instant);
        }
    };

    using TBase::TBase;

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NInternal::TTableRecord SerializeToRecord() const;
    static TString GetTypeId() {
        return "INITIALIZATION";
    }
};

}
