#pragma once

#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>


namespace NKikimr::NKqp {

class TResourcePoolClassifierConfig : public NMetadata::NModifications::TObject<TResourcePoolClassifierConfig> {
    using TBase = NMetadata::NModifications::TObject<TResourcePoolClassifierConfig>;

    YDB_ACCESSOR_DEF(TString, Database);
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(i64, Rank);
    YDB_ACCESSOR_DEF(NJson::TJsonValue, ConfigJson);

public:
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

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawData);
    NMetadata::NInternal::TTableRecord SerializeToRecord() const;
    NResourcePool::TClassifierSettings GetClassifierSettings() const;
    NJson::TJsonValue GetDebugJson() const;

    bool operator==(const TResourcePoolClassifierConfig& other) const;

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();
    static TString GetTypeId();

    static NMetadata::NModifications::NColumnMerger::TMerger MergerFactory(const TString& columnName);
    static bool JsonConfigsMerger(Ydb::Value& self, const Ydb::Value& other);
};

}  // namespace NKikimr::NKqp
