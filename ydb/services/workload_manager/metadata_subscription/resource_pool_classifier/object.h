#pragma once

#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>

namespace NYql {
class TFeaturesExtractor;
}


namespace NKikimr::NWorkloadManager {

class TResourcePoolClassifierConfig : public NMetadata::NModifications::TObject<TResourcePoolClassifierConfig> {
    using TBase = NMetadata::NModifications::TObject<TResourcePoolClassifierConfig>;

    YDB_ACCESSOR_DEF(TString, Database);
    YDB_ACCESSOR_DEF(TString, Name);

    NResourcePool::TClassifierSettings Settings;

public:
    // JSON keys for classifier settings serialization
    static inline const TString JSON_KEY_RANK = "rank";
    static inline const TString JSON_KEY_RESOURCE_POOL = "resource_pool";
    static inline const TString JSON_KEY_MEMBER_NAME = "member_name";
    static inline const TString JSON_KEY_HAS_APP_NAME = "has_app_name";
    static inline const TString JSON_KEY_HAS_FULL_SCAN = "has_full_scan";
    static inline const TString JSON_KEY_HAS_PATH = "has_path";
    static inline const TString JSON_KEY_HAS_STREAM = "has_stream";
    static inline const TString JSON_KEY_ACTION = "action";

    class TDecoder : public NMetadata::NInternal::TDecoderBase {
    private:
        YDB_READONLY(i32, DatabaseIdx, -1);
        YDB_READONLY(i32, NameIdx, -1);
        YDB_READONLY(i32, RankIdx, -1);
        YDB_READONLY(i32, ConfigJsonIdx, -1);

    public:
        static inline const TString Database = "database";
        static inline const TString Name = "name";
        static inline const TString Rank = "rank";
        static inline const TString ConfigJson = "config";

        explicit TDecoder(const Ydb::ResultSet& rawData);
    };

    virtual NMetadata::NModifications::IColumnValuesMerger::TPtr BuildMerger(const TString& columnName) const override;
    NMetadata::NInternal::TTableRecord SerializeToRecord() const;
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawData);
    i64 GetRank() const {
        return Settings.Rank;
    }
    void SetRank(i64 rank) {
        Settings.Rank = rank;
    }
    const NResourcePool::TClassifierSettings& GetClassifierSettings() const {
        return Settings;
    }
    void SetClassifierSettings(const NResourcePool::TClassifierSettings& settings) {
        Settings = settings;
    }

    NJson::TJsonValue GetDebugJson() const;

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();
    static TString GetTypeId();

    // Serialize TClassifierSettings to JSON (excludes rank, which is stored separately)
    static NJson::TJsonValue SerializeToJson(const NResourcePool::TClassifierSettings& settings);
    // Deserialize TClassifierSettings from JSON, with optional rank override
    static NResourcePool::TClassifierSettings DeserializeFromJson(const NJson::TJsonValue& json, i64 rank = -1);

    // Result of parsing classifier settings from DDL features extractor
    struct TParseResult {
        NResourcePool::TClassifierSettings Settings;
        NJson::TJsonValue ConfigPatch = NJson::JSON_MAP;
        bool HasRank = false;
        bool HasResourcePool = false;
        bool HasConfigPatch = false;
    };

    // Parse TClassifierSettings from features extractor (DDL properties).
    // Returns std::nullopt on success (settings filled in *result).
    // Returns error string on failure.
    static std::optional<TString> ParseFromFeaturesExtractor(
        NYql::TFeaturesExtractor& featuresExtractor,
        TParseResult* result);
};

}  // namespace NKikimr::NWorkloadManager
