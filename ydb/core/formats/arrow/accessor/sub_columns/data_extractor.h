#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

namespace NKikimr::NArrow::NAccessor {

class IDataAdapter {
private:
    virtual TConclusion<std::shared_ptr<arrow::Schema>> DoBuildSchemaForData(const std::shared_ptr<IChunkedArray>& sourceArray) const = 0;
    virtual TConclusionStatus DoAddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray,
        const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const = 0;

public:
    virtual ~IDataAdapter() = default;

    TConclusion<std::shared_ptr<arrow::Schema>> BuildSchemaForData(const std::shared_ptr<IChunkedArray>& sourceArray) const {
        return DoBuildSchemaForData(sourceArray);
    }
    TConclusionStatus AddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const;
};

class TFirstLevelSchemaData: public IDataAdapter {
private:
    virtual TConclusion<std::shared_ptr<arrow::Schema>> DoBuildSchemaForData(const std::shared_ptr<IChunkedArray>& sourceArray) const override;

    virtual TConclusionStatus DoAddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray,
        const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const override;

public:
};

}   // namespace NKikimr::NArrow::NAccessor
