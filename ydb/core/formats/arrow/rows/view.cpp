#include "view.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NArrow {

template <class TCustomActor>
class TGeneralActor {
private:
    TCustomActor CustomActor;

public:
    TCustomActor& GetCustomActor() {
        return CustomActor;
    }

    template <class TWrap>
    bool OnValue(const ui32 fieldIndex, const char* data, const ui32 size) {
        if (!data) {
            return CustomActor.OnNull(fieldIndex);
        }
        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            return CustomActor.template OnBinary<TWrap>(fieldIndex, data, size);
        }
        if constexpr (arrow::has_c_type<typename TWrap::T>() && !std::is_base_of<arrow::HalfFloatType, typename TWrap::T>()) {
            using CType = typename arrow::TypeTraits<typename TWrap::T>::CType;
            return CustomActor.template OnValue<TWrap>(fieldIndex, *(CType*)(data));
        }
        AFL_VERIFY(false);
        return false;
    }

    TGeneralActor(TCustomActor&& actor)
        : CustomActor(std::move(actor)) {
    }
};

class TDebugStringActor {
private:
    YDB_READONLY_DEF(TStringBuilder, Result);

public:
    bool OnNull(const ui32 /*fieldIndex*/) {
        Result << "NULL,";
        return false;
    }

    template <class TWrap>
    bool OnBinary(const ui32 /*fieldIndex*/, const char* data, const ui32 size) {
        Result << std::string_view(data, size) << ",";
        return false;
    }

    template <class TWrap, class CType = typename arrow::TypeTraits<typename TWrap::T>::CType>
    bool OnValue(const ui32 /*fieldIndex*/, const CType value) {
        Result << value << ",";
        return false;
    }
};

TConclusion<TString> TSimpleRowView::DebugString(const std::shared_ptr<arrow::Schema>& schema) const {
    TDebugStringActor custom;
    TGeneralActor<TDebugStringActor> resultActor(std::move(custom));
    auto conclusion = Scan(schema, resultActor);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    return resultActor.GetCustomActor().GetResult();
}

class TBuildersActor {
private:
    const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& Builders;

public:
    bool OnNull(const ui32 fieldIndex) {
        TStatusValidator::Validate(Builders[fieldIndex]->AppendNull());
        return false;
    }

    template <class TWrap>
    bool OnBinary(const ui32 fieldIndex, const char* data, const ui32 size) {
        using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;
        TStatusValidator::Validate((static_cast<TBuilder*>(Builders[fieldIndex].get()))->Append(arrow::util::string_view(data, size)));
        return false;
    }

    template <class TWrap, class CType = typename arrow::TypeTraits<typename TWrap::T>::CType>
    bool OnValue(const ui32 fieldIndex, const CType value) {
        using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;
        TStatusValidator::Validate((static_cast<TBuilder*>(Builders[fieldIndex].get()))->Append(value));
        return false;
    }

    TBuildersActor(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders)
        : Builders(builders) {
    }
};

TConclusionStatus TSimpleRowView::AddToBuilders(
    const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const std::shared_ptr<arrow::Schema>& schema) const {
    AFL_VERIFY((ui32)schema->num_fields() == builders.size());
    TBuildersActor custom(builders);
    TGeneralActor<TBuildersActor> resultActor(std::move(custom));
    return Scan(schema, resultActor);
}

TConclusion<std::partial_ordering> TSimpleRowView::Compare(
    const TSimpleRowView& item, const std::shared_ptr<arrow::Schema>& schema, const std::optional<ui32> columnsCount) const {
    AFL_VERIFY(!columnsCount || *columnsCount <= (ui32)schema->num_fields());
    TStringBuf valuesSelf = Values;
    TStringBuf valuesItem = item.Values;
    for (ui32 fieldIdx = 0; fieldIdx < columnsCount.value_or((ui32)schema->num_fields()); ++fieldIdx) {
        if (1 > valuesSelf.size()) {
            return TConclusionStatus::Fail("unexpected string end");
        }
        if (1 > valuesItem.size()) {
            return TConclusionStatus::Fail("unexpected string end");
        }
        const ui8 selfFillByte = (ui8)valuesSelf[0];
        const ui8 itemFillByte = (ui8)valuesItem[0];
        valuesSelf.Skip(1);
        valuesItem.Skip(1);
        if (!selfFillByte && !itemFillByte) {
            continue;
        }
        if (selfFillByte && !itemFillByte) {
            AFL_ERROR(NKikimrServices::ARROW_HELPER)("self", selfFillByte)("item", itemFillByte);
            return std::partial_ordering::greater;
        }
        if (!selfFillByte && itemFillByte) {
            AFL_ERROR(NKikimrServices::ARROW_HELPER)("self", selfFillByte)("item", itemFillByte);
            return std::partial_ordering::less;
        }
        TString errorMessage;
        std::optional<std::partial_ordering> result;
        if (!NArrow::SwitchType(schema->field(fieldIdx)->type()->id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                    if (sizeof(ui32) > valuesSelf.size()) {
                        errorMessage = "cannot read string size for " + schema->field(fieldIdx)->name();
                        return false;
                    }
                    if (sizeof(ui32) > valuesItem.size()) {
                        errorMessage = "cannot read string size for " + schema->field(fieldIdx)->name();
                        return false;
                    }
                    const ui32 sizeSelf = *(ui32*)(valuesSelf.data());
                    const ui32 sizeItem = *(ui32*)(valuesItem.data());
                    valuesSelf.Skip(sizeof(ui32));
                    valuesItem.Skip(sizeof(ui32));
                    if (sizeSelf > valuesSelf.size()) {
                        errorMessage = "cannot read string for " + schema->field(fieldIdx)->name();
                        return false;
                    }
                    if (sizeItem > valuesItem.size()) {
                        errorMessage = "cannot read string for " + schema->field(fieldIdx)->name();
                        return false;
                    }
                    const TStringBuf selfString = valuesSelf.SubStr(0, sizeSelf);
                    const TStringBuf itemString = valuesItem.SubStr(0, sizeItem);
                    const int cmp = selfString.compare(itemString);
                    if (cmp < 0) {
                        result = std::partial_ordering::less;
                    } else if (cmp > 0) {
                        result = std::partial_ordering::greater;
                    }
                    valuesSelf.Skip(sizeSelf);
                    valuesItem.Skip(sizeItem);
                    return true;
                }
                if constexpr (arrow::has_c_type<typename TWrap::T>() && !std::is_base_of<arrow::HalfFloatType, typename TWrap::T>()) {
                    using CType = typename arrow::TypeTraits<typename TWrap::T>::CType;
                    if (sizeof(CType) > valuesSelf.size()) {
                        errorMessage = "cannot read ctype size for " + schema->field(fieldIdx)->name();
                    }
                    if (sizeof(CType) > valuesItem.size()) {
                        errorMessage = "cannot read ctype size for " + schema->field(fieldIdx)->name();
                    }
                    const CType valueSelf = *(CType*)(valuesSelf.data());
                    const CType valueItem = *(CType*)(valuesItem.data());
                    if (valueSelf < valueItem) {
                        result = std::partial_ordering::less;
                    } else if (valueItem < valueSelf) {
                        result = std::partial_ordering::greater;
                    }
                    valuesSelf.Skip(sizeof(CType));
                    valuesItem.Skip(sizeof(CType));
                    return true;
                }
                AFL_VERIFY(false);
                return false;
            })) {
            return TConclusionStatus::Fail(errorMessage);
        } else if (result) {
            return *result;
        }
    }
    return std::partial_ordering::equivalent;
}

TString TSimpleRowView::BuildString(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 recordIndex) {
    TString result;
    for (ui32 i = 0; i < (ui32)batch->num_columns(); ++i) {
        NArrow::SwitchType(batch->column(i)->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
            const auto& arrImpl = static_cast<const TArray&>(*batch->column(i));
            const ui8 byte = arrImpl.IsNull(recordIndex) ? 0 : 1;
            result.append((const char*)&byte, sizeof(ui8));
            if (!byte) {
                return true;
            }
            if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                const arrow::util::string_view sv = arrImpl.GetView(recordIndex);
                const ui32 size = sv.size();
                result.append((const char*)&size, sizeof(size));
                result.append(sv.data(), sv.size());
                return true;
            }
            if constexpr (arrow::has_c_type<typename TWrap::T>() && !std::is_base_of<arrow::HalfFloatType, typename TWrap::T>()) {
                using CType = typename arrow::TypeTraits<typename TWrap::T>::CType;
                const CType value = arrImpl.Value(recordIndex);
                result.append((const char*)&value, sizeof(CType));
                return true;
            }
            AFL_VERIFY(false)("type", batch->column(i)->type()->ToString());
            return false;
        });
    }
    return result;
}

class TGetScalarActor {
private:
    const ui32 ColumnIndex;
    const std::shared_ptr<arrow::Schema> Schema;
    YDB_ACCESSOR_DEF(std::shared_ptr<arrow::Scalar>, Result);

public:
    template <class TWrap>
    bool OnValue(const ui32 fieldIndex, const char* data, const ui32 size) {
        if (fieldIndex != ColumnIndex) {
            return false;
        }
        AFL_VERIFY(fieldIndex == ColumnIndex);
        if (!data) {
            Result = std::make_shared<arrow::NullScalar>();
            return true;
        }
        using ScalarType = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            Result = std::make_shared<arrow::StringScalar>(std::string(data, size));
            return true;
        }
        if constexpr (arrow::has_c_type<typename TWrap::T>() && !std::is_base_of<arrow::HalfFloatType, typename TWrap::T>()) {
            using CType = typename arrow::TypeTraits<typename TWrap::T>::CType;
            const CType value = *(CType*)(data);
            Result = std::make_shared<ScalarType>(value, Schema->field(fieldIndex)->type());
            return true;
        }
        return true;
    }
    TGetScalarActor(const ui32 columnIndex, const std::shared_ptr<arrow::Schema>& schema)
        : ColumnIndex(columnIndex)
        , Schema(schema) {
    }
};

TConclusion<std::shared_ptr<arrow::Scalar>> TSimpleRowView::GetScalar(
    const ui32 columnIndex, const std::shared_ptr<arrow::Schema>& schema) const {
    TGetScalarActor resultActor(columnIndex, schema);
    auto conclusion = Scan(schema, resultActor);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    AFL_VERIFY(resultActor.GetResult());
    return resultActor.GetResult();
}

std::partial_ordering TSimpleRow::operator<=>(const TSimpleRow& item) const {
    AFL_VERIFY_DEBUG(Schema->Equals(*item.Schema));
    return TSimpleRowView(Data).Compare(TSimpleRowView(item.Data), Schema).GetResult();
}

std::shared_ptr<arrow::RecordBatch> TSimpleRow::ToBatch() const {
    auto builders = NArrow::MakeBuilders(Schema);
    AddToBuilders(builders).Validate();
    return arrow::RecordBatch::Make(Schema, 1, FinishBuilders(std::move(builders)));
}

}   // namespace NKikimr::NArrow
