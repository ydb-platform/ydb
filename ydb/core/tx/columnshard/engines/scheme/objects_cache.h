#pragma once
#include "column_features.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TSchemaObjectsCache {
public:
    class TSchemaVersionId {
    private:
        YDB_READONLY_DEF(ui64, PresetId);
        YDB_READONLY_DEF(ui64, Version);

    public:
        struct THash {
            ui64 operator()(const TSchemaVersionId& object) const {
                return CombineHashes(object.PresetId, object.Version);
            }
        };

        bool operator==(const TSchemaVersionId& other) const {
            return std::tie(PresetId, Version) == std::tie(other.PresetId, other.Version);
        }

        TSchemaVersionId(const ui64 presetId, const ui64 version)
            : PresetId(presetId)
            , Version(version) {
        }
    };

private:
    THashMap<TString, std::shared_ptr<arrow::Field>> Fields;
    mutable ui64 AcceptionFieldsCount = 0;
    mutable TMutex FieldsMutex;

    THashMap<TString, std::shared_ptr<TColumnFeatures>> ColumnFeatures;
    mutable ui64 AcceptionFeaturesCount = 0;
    mutable TMutex FeaturesMutex;

    THashMap<TSchemaVersionId, std::weak_ptr<const TIndexInfo>, TSchemaVersionId::THash> SchemasByVersion;
    mutable TMutex SchemasMutex;

    THashSet<TString> StringsCache;
    mutable TMutex StringsMutex;

public:
    const TString& GetStringCache(const TString& original) {
        TGuard lock(StringsMutex);
        auto it = StringsCache.find(original);
        if (it == StringsCache.end()) {
            it = StringsCache.emplace(original).first;
        }
        return *it;
    }

    void RegisterField(const TString& fingerprint, const std::shared_ptr<arrow::Field>& f) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "register_field")("fp", fingerprint)("f", f->ToString());
        TGuard lock(FieldsMutex);
        AFL_VERIFY(Fields.emplace(fingerprint, f).second);
    }
    void RegisterColumnFeatures(const TString& fingerprint, const std::shared_ptr<TColumnFeatures>& f) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "register_column_features")("fp", fingerprint)("info", f->DebugString());
        TGuard lock(FeaturesMutex);
        AFL_VERIFY(ColumnFeatures.emplace(fingerprint, f).second);
    }
    std::shared_ptr<arrow::Field> GetField(const TString& fingerprint) const {
        TGuard lock(FieldsMutex);
        auto it = Fields.find(fingerprint);
        if (it == Fields.end()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_field_miss")("fp", fingerprint)("count", Fields.size())(
                "acc", AcceptionFieldsCount);
            return nullptr;
        }
        if (++AcceptionFieldsCount % 1000 == 0) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_field_accept")("fp", fingerprint)("count", Fields.size())(
                "acc", AcceptionFieldsCount);
        }
        return it->second;
    }
    template <class TConstructor>
    TConclusion<std::shared_ptr<TColumnFeatures>> GetOrCreateColumnFeatures(const TString& fingerprint, const TConstructor& constructor) {
        TGuard lock(FeaturesMutex);
        auto it = ColumnFeatures.find(fingerprint);
        if (it == ColumnFeatures.end()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_column_features_miss")("fp", UrlEscapeRet(fingerprint))(
                "count", ColumnFeatures.size())("acc", AcceptionFeaturesCount);
            TConclusion<std::shared_ptr<TColumnFeatures>> resultConclusion = constructor();
            if (resultConclusion.IsFail()) {
                return resultConclusion;
            }
            it = ColumnFeatures.emplace(fingerprint, resultConclusion.DetachResult()).first;
            AFL_VERIFY(it->second);
        } else {
            if (++AcceptionFeaturesCount % 1000 == 0) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_column_features_accept")("fp", UrlEscapeRet(fingerprint))(
                    "count", ColumnFeatures.size())("acc", AcceptionFeaturesCount);
            }
        }
        return it->second;
    }

    std::shared_ptr<const TIndexInfo> UpsertIndexInfo(const ui64 presetId, TIndexInfo&& indexInfo);
};

class TSchemaCachesManager {
private:
    THashMap<ui64, std::shared_ptr<TSchemaObjectsCache>> CacheByTableOwner;
    TMutex Mutex;

    std::shared_ptr<TSchemaObjectsCache> GetCacheImpl(const ui64 ownerPathId) {
        if (!ownerPathId) {
            return std::make_shared<TSchemaObjectsCache>();
        }
        TGuard lock(Mutex);
        auto findCache = CacheByTableOwner.FindPtr(ownerPathId);
        if (findCache) {
            return *findCache;
        }
        return CacheByTableOwner.emplace(ownerPathId, std::make_shared<TSchemaObjectsCache>()).first->second;
    }

public:
    static std::shared_ptr<TSchemaObjectsCache> GetCache(const ui64 ownerPathId) {
        return Singleton<TSchemaCachesManager>()->GetCacheImpl(ownerPathId);
    }
};

}   // namespace NKikimr::NOlap
