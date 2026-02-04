#include "yt_arrow_output_converter.h"

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/arrow/defs.h>
#include <yql/essentials/utils/yql_panic.h>

#include <arrow/compute/cast.h>
#include <arrow/util/key_value_metadata.h>

namespace NYql {

class TBasicOutputConverter : public IYtOutputColumnConverter {
public:
    TBasicOutputConverter(std::shared_ptr<arrow::DataType> arrowType, bool optional)
        : ArrowType_(arrowType)
        , Optional_(optional)
    {}

    std::shared_ptr<arrow::Field> BuildSchemaField(std::string name) override {
        return arrow::field(std::move(name), ArrowType_, Optional_);
    }

    std::shared_ptr<arrow::ArrayData> Convert(std::shared_ptr<arrow::ArrayData> block) override {
        YQL_ENSURE(ArrowType_->Equals(block->type), "block type differs from expected arrow output type");
        return block;
    }

private:
    std::shared_ptr<arrow::DataType> ArrowType_;
    bool Optional_;
};

class TBoolOutputConverter : public IYtOutputColumnConverter {
public:
    TBoolOutputConverter(bool optional, arrow::MemoryPool* pool)
        : Optional_(optional)
        , ExecContext_(pool)
    {}

    std::shared_ptr<arrow::Field> BuildSchemaField(std::string name) override {
        return arrow::field(std::move(name), arrow::boolean(), Optional_);
    }

    std::shared_ptr<arrow::ArrayData> Convert(std::shared_ptr<arrow::ArrayData> block) override {
        YQL_ENSURE(block->type->Equals(arrow::uint8()));
        auto convertedDatum = ARROW_RESULT(arrow::compute::Cast(block, arrow::boolean(), arrow::compute::CastOptions::Safe(), &ExecContext_));
        return convertedDatum.array();
    }

private:
    bool Optional_;
    arrow::compute::ExecContext ExecContext_;
};

class TYsonOutputConverter : public IYtOutputColumnConverter {
public:
    TYsonOutputConverter(bool optional)
        : Optional_(optional)
    {}

    std::shared_ptr<arrow::Field> BuildSchemaField(std::string name) override {
        auto metadata = arrow::KeyValueMetadata::Make({"YtType"}, {"yson"});
        return arrow::field(std::move(name), arrow::binary(), Optional_, std::move(metadata));
    }

    std::shared_ptr<arrow::ArrayData> Convert(std::shared_ptr<arrow::ArrayData> block) override {
        YQL_ENSURE(block->type->Equals(arrow::binary()));
        return block;
    }

private:
    bool Optional_;
};

IYtOutputColumnConverter::TPtr MakeYtOutputColumnConverter(NKikimr::NMiniKQL::TType* type, arrow::MemoryPool* pool) {
    std::shared_ptr<arrow::DataType> arrowType;
    YQL_ENSURE(ConvertArrowOutputType(type, arrowType), "unsupported arrow output type");

    // only data and optional data types are supported at the moment
    // TODO: support complex types

    bool optional = false;
    if (type->IsOptional()) {
        optional = true;
        type = AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType();
    }

    YQL_ENSURE(type->IsData(), "unsupported type");
    switch (*AS_TYPE(NKikimr::NMiniKQL::TDataType, type)->GetDataSlot()) {
    case NUdf::EDataSlot::Bool:
        return std::make_unique<TBoolOutputConverter>(optional, pool);

    case NUdf::EDataSlot::Yson:
        return std::make_unique<TYsonOutputConverter>(optional);

    default:
        return std::make_unique<TBasicOutputConverter>(std::move(arrowType), optional);
    }
}

}
