#include "conversion.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/library/formats/arrow/simple_builder/array.h>

namespace NKikimr::NArrow {

std::shared_ptr<arrow::Array> DictionaryToArray(const std::shared_ptr<arrow::DictionaryArray>& data) {
    Y_ABORT_UNLESS(data);
    return DictionaryToArray(*data);
}

std::shared_ptr<arrow::Array> DictionaryToArray(const arrow::DictionaryArray& data) {
    std::shared_ptr<arrow::Array> result;
    SwitchType(data.dictionary()->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TDictionaryValue = typename TWrap::T;
        using TDictionary = typename arrow::TypeTraits<TDictionaryValue>::ArrayType;
        constexpr bool noParams = arrow::TypeTraits<TDictionaryValue>::is_parameter_free;
        if constexpr (!noParams) {
            Y_ABORT_UNLESS(false);
            return true;
        }
        if constexpr (noParams) {
            auto& columnDictionary = static_cast<const TDictionary&>(*data.dictionary());
            SwitchType(data.indices()->type_id(), [&](const auto& type) {
                using TWrapIndices = std::decay_t<decltype(type)>;
                constexpr bool hasCType = arrow::has_c_type<typename TWrapIndices::T>::value;
                if constexpr (hasCType) {
                    constexpr bool indicesIntegral = std::is_integral<typename TWrapIndices::T::c_type>::value;
                    if constexpr (indicesIntegral && hasCType) {
                        using TIndices = typename arrow::TypeTraits<typename TWrapIndices::T>::ArrayType;
                        auto& columnIndices = static_cast<const TIndices&>(*data.indices());
                        constexpr bool hasStrView = arrow::has_string_view<TDictionaryValue>::value;
                        if constexpr (hasStrView) {
                            using TBinaryDictionaryAccessor = NConstruction::TBinaryDictionaryArrayAccessor<TDictionaryValue, TIndices>;
                            result = NConstruction::TBinaryArrayConstructor("absent", TBinaryDictionaryAccessor(columnDictionary, columnIndices)).BuildArray(data.length());
                        } else {
                            using TDictionaryAccessor = NConstruction::TDictionaryArrayAccessor<TDictionaryValue, TIndices>;
                            result = NConstruction::TSimpleArrayConstructor("absent", TDictionaryAccessor(columnDictionary, columnIndices)).BuildArray(data.length());
                        }
                        return true;
                    }
                }
                Y_ABORT_UNLESS(false);
                return true;
            });
        }
        return true;
    });
    Y_ABORT_UNLESS(result);
    return result;
}

std::shared_ptr<arrow::RecordBatch> DictionaryToArray(const std::shared_ptr<arrow::RecordBatch>& data) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    bool hasDict = false;
    for (auto&& f : data->schema()->fields()) {
        if (f->type()->id() == arrow::Type::DICTIONARY) {
            auto& dType = static_cast<const arrow::DictionaryType&>(*f->type());
            fields.emplace_back(std::make_shared<arrow::Field>(f->name(), dType.value_type()));
            hasDict = true;
        } else {
            fields.emplace_back(f);
        }
    }
    if (!hasDict) {
        return data;
    }
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& c : data->columns()) {
        if (c->type_id() == arrow::Type::DICTIONARY) {
            auto& dColumn = static_cast<const arrow::DictionaryArray&>(*c);
            columns.emplace_back(DictionaryToArray(dColumn));
        } else {
            columns.emplace_back(c);
        }
    }
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);
    return arrow::RecordBatch::Make(schema, data->num_rows(), columns);
}

std::shared_ptr<arrow::DictionaryArray> ArrayToDictionary(const std::shared_ptr<arrow::Array>& data) {
    Y_ABORT_UNLESS(IsDictionableArray(data));
    std::shared_ptr<arrow::DictionaryArray> result;
    SwitchType(data->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        if constexpr (arrow::has_string_view<typename TWrap::T>::value && arrow::TypeTraits<typename TWrap::T>::is_parameter_free) {
            auto resultArray = NConstruction::TDictionaryArrayConstructor<NConstruction::TLinearArrayAccessor<typename TWrap::T>>("absent", *data).BuildArray(data->length());
            Y_ABORT_UNLESS(resultArray->type()->id() == arrow::Type::DICTIONARY);
            result = static_pointer_cast<arrow::DictionaryArray>(resultArray);
        } else {
            Y_ABORT_UNLESS(false);
        }
        return true;
    });
    Y_ABORT_UNLESS(result);
    return result;
}

std::shared_ptr<arrow::RecordBatch> ArrayToDictionary(const std::shared_ptr<arrow::RecordBatch>& data) {
    if (!data) {
        return data;
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    ui32 idx = 0;
    for (auto&& i : data->schema()->fields()) {
        if (i->type()->id() == arrow::Type::DICTIONARY) {
            fields.emplace_back(i);
            columns.emplace_back(data->column(idx));
        } else {
            columns.emplace_back(ArrayToDictionary(data->column(idx)));
            fields.emplace_back(std::make_shared<arrow::Field>(i->name(), columns.back()->type()));
        }
        ++idx;
    }
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);
    return arrow::RecordBatch::Make(schema, data->num_rows(), columns);
}

bool IsDictionableArray(const std::shared_ptr<arrow::Array>& data) {
    Y_ABORT_UNLESS(data);
    bool result = false;
    SwitchType(data->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        result = arrow::has_string_view<typename TWrap::T>::value;
        return true;
    });
    return result;
}

}
