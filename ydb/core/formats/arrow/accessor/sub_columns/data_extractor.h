#pragma once
#include "direct_builder.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class IDataAdapter {
private:
    virtual TConclusionStatus DoAddDataToBuilders(
        const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept = 0;

public:
    virtual ~IDataAdapter() = default;

    TConclusionStatus AddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept;
};

class TFirstLevelSchemaData: public IDataAdapter {
private:
    virtual TConclusionStatus DoAddDataToBuilders(
        const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept override;

public:
};

}   // namespace NKikimr::NArrow::NAccessor
