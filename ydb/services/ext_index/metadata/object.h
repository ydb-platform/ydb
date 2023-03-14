#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/object_factory/object_factory.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <util/string/cast.h>

namespace NKikimr::NMetadata::NCSIndex {

class IIndexExtractor {
protected:
    virtual std::vector<ui64> DoExtractIndex(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;
    virtual bool DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) = 0;
    virtual NJson::TJsonValue DoSerializeToJson() const = 0;
public:
    using TPtr = std::shared_ptr<IIndexExtractor>;
    using TFactory = NObjectFactory::TObjectFactory<IIndexExtractor, TString>;

    virtual ~IIndexExtractor() = default;

    std::vector<ui64> ExtractIndex(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        return DoExtractIndex(batch);
    }

    bool DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        return DoDeserializeFromJson(jsonInfo);
    }

    NJson::TJsonValue SerializeToJson() const {
        return DoSerializeToJson();
    }

    virtual TString GetClassName() const = 0;
};

class TExtractorCityHash64: public IIndexExtractor {
private:
    YDB_READONLY_DEF(std::vector<TString>, Fields);
    static TFactory::TRegistrator<TExtractorCityHash64> Registrator;
protected:
    virtual std::vector<ui64> DoExtractIndex(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
    virtual bool DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override {
        const NJson::TJsonValue::TArray* jsonFields;
        if (!jsonInfo["fields"].GetArrayPointer(&jsonFields)) {
            return false;
        }
        for (auto&& i : *jsonFields) {
            TString fieldId;
            if (!i["id"].GetString(&fieldId)) {
                return false;
            }
            Fields.emplace_back(fieldId);
        }
        if (Fields.size() == 0) {
            return false;
        }
        return true;
    }
    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result;
        auto& jsonFields = result.InsertValue("fields", NJson::JSON_ARRAY);
        for (auto&& i : Fields) {
            auto& jsonField = jsonFields.AppendValue(NJson::JSON_MAP);
            jsonField.InsertValue("id", i);
        }
        return result;
    }
public:
    static inline TString ClassName = "city64";

    virtual TString GetClassName() const override {
        return ClassName;
    }
};

template <class TInterface>
class TInterfaceContainer {
private:
    using TPtr = typename TInterface::TPtr;
    using TFactory = typename TInterface::TFactory;
    TPtr Object;
public:
    TInterfaceContainer() = default;

    explicit TInterfaceContainer(TPtr object)
        : Object(object)
    {

    }

    const TInterface* operator->() const {
        return Object.get();
    }

    TInterface* operator->() {
        return Object.get();
    }

    TString DebugString() const {
        return SerializeToJson().GetStringRobust();
    }

    bool DeserializeFromJson(const TString& jsonString) {
        NJson::TJsonValue jsonInfo;
        if (!NJson::ReadJsonFastTree(jsonString, &jsonInfo)) {
            return false;
        }
        return DeserializeFromJson(jsonInfo);
    }

    bool DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        TString className;
        if (!jsonInfo["class_name"].GetString(&className)) {
            return false;
        }
        TPtr result(TFactory::Construct(className));
        if (!result) {
            return false;
        }
        if (!result->DeserializeFromJson(jsonInfo["object"])) {
            return false;
        }
        Object = result;
        return true;
    }

    NJson::TJsonValue SerializeToJson() const {
        if (!Object) {
            return NJson::JSON_NULL;
        }
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("class_name", Object->GetClassName());
        result.InsertValue("object", Object->SerializeToJson());
        return result;
    }
};

class TObject: public NModifications::TObject<TObject> {
private:
    YDB_READONLY_DEF(TString, IndexId);

    YDB_READONLY_DEF(TString, TablePath);
    YDB_READONLY_FLAG(Active, false);
    YDB_READONLY_FLAG(Delete, false);
    YDB_READONLY_DEF(TInterfaceContainer<IIndexExtractor>, Extractor);
public:

    bool TryProvideTtl(const NKikimrSchemeOp::TColumnTableDescription& csDescription, Ydb::Table::CreateTableRequest* cRequest);

    TString DebugString() const {
        return TablePath + ";" + IndexId + ";" + Extractor.DebugString() + ";" + ::ToString(ActiveFlag) + ";" + ::ToString(DeleteFlag);
    }

    TObject() = default;
    static std::optional<TObject> Build(const TString& tablePath, const TString& indexId, IIndexExtractor::TPtr extractor);

    TString GetIndexTablePath() const;

    TString GetUniqueId() const {
        return TablePath + "/" + IndexId;
    }

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, IndexIdIdx, -1);
        YDB_ACCESSOR(i32, TablePathIdx, -1);
        YDB_ACCESSOR(i32, ExtractorIdx, -1);
        YDB_ACCESSOR(i32, ActiveIdx, -1);
        YDB_ACCESSOR(i32, DeleteIdx, -1);
    public:
        static inline const TString IndexId = "indexId";
        static inline const TString TablePath = "tablePath";
        static inline const TString Extractor = "extractor";
        static inline const TString Active = "active";
        static inline const TString Delete = "delete";

        TDecoder(const Ydb::ResultSet& rawData) {
            IndexIdIdx = GetFieldIndex(rawData, IndexId);
            TablePathIdx = GetFieldIndex(rawData, TablePath);
            ExtractorIdx = GetFieldIndex(rawData, Extractor);
            ActiveIdx = GetFieldIndex(rawData, Active);
            DeleteIdx = GetFieldIndex(rawData, Delete);
        }
    };

    static IClassBehaviour::TPtr GetBehaviour();

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);

    NInternal::TTableRecord SerializeToRecord() const;

    static TString GetTypeId() {
        return "CS_EXT_INDEX";
    }
};

}
