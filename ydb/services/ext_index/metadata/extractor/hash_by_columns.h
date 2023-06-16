#pragma once
#include "abstract.h"
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NCSIndex {

class TExtractorField {
private:
    YDB_ACCESSOR_DEF(TString, FieldId);
    YDB_ACCESSOR_DEF(TString, JsonPath);
public:
    TString GetFullId() const {
        if (JsonPath) {
            return "json:" + FieldId + ":" + JsonPath;
        } else {
            return FieldId;
        }
    }
    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result.InsertValue("id", FieldId);
        result.InsertValue("path", JsonPath);
        return result;
    }
    bool DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        if (!jsonInfo["id"].GetString(&FieldId)) {
            return false;
        }
        if (jsonInfo.Has("path")) {
            if (!jsonInfo["path"].GetString(&JsonPath)) {
                return false;
            }
        }
        return true;
    }
};

class THashByColumns: public IIndexExtractor {
public:
    enum class EHashType {
        XX64 /* "xx64" */
    };
private:
    YDB_READONLY_DEF(std::vector<TExtractorField>, Fields);
    YDB_READONLY(EHashType, HashType, EHashType::XX64);
    static TFactory::TRegistrator<THashByColumns> Registrator;
    static TFactory::TRegistrator<THashByColumns> RegistratorDeprecated;
protected:
    virtual std::vector<ui64> DoExtractIndex(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
    virtual bool DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;
    virtual NJson::TJsonValue DoSerializeToJson() const override;
public:
    static inline TString ClassName = "hash_by_columns";

    virtual TString GetClassName() const override {
        return ClassName;
    }
};

}
