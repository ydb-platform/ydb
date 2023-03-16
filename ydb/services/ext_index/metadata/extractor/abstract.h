#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/object_factory/object_factory.h>
#include <library/cpp/json/writer/json_value.h>

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

}
