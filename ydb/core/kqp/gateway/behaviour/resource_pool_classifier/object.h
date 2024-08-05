#pragma once

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>


namespace NKikimr::NKqp {

class TResourcePoolClassifierConfig : public NMetadata::NModifications::TObject<TResourcePoolClassifierConfig> {
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(ui64, Rank);
    YDB_ACCESSOR_DEF(TString, ConfigJson);

public:
    class TDecoder : public NMetadata::NInternal::TDecoderBase {
    private:
        YDB_READONLY(i32, NameIdx, -1);
        YDB_READONLY(i32, RankIdx, -1);
        YDB_READONLY(i32, ConfigIdx, -1);

    public:
        static inline const TString Name = "name";
        static inline const TString Rank = "rank";
        static inline const TString Config = "config";

        explicit TDecoder(const Ydb::ResultSet& rawData);
    };

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawData);
    NMetadata::NInternal::TTableRecord SerializeToRecord() const;
    NJson::TJsonValue GetDebugJson() const;

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();
    static TString GetTypeId();
};

}  // namespace NKikimr::NKqp
