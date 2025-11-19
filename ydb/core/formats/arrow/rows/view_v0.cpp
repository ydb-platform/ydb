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
        if constexpr (std::is_base_of<arrow::FixedSizeBinaryType, typename TWrap::T>()) {
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

TConclusion<TString> TSimpleRowViewV0::DebugString(const std::shared_ptr<arrow::Schema>& schema) const {
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

TConclusionStatus TSimpleRowViewV0::AddToBuilders(
    const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const std::shared_ptr<arrow::Schema>& schema) const {
    AFL_VERIFY((ui32)schema->num_fields() == builders.size());
    TBuildersActor custom(builders);
    TGeneralActor<TBuildersActor> resultActor(std::move(custom));
    return Scan(schema, resultActor);
}

class TCompareActor {
private:
    const std::optional<std::string_view> ItemView;
    YDB_READONLY(std::partial_ordering, Result, std::partial_ordering::equivalent);

public:
    bool OnNull(const ui32 /*fieldIndex*/) {
        if (!ItemView) {
            return false;
        } else {
            Result = std::partial_ordering::less;
            return false;
        }
    }

    template <class TWrap>
    bool OnBinary(const ui32 /*fieldIndex*/, const char* data, const ui32 size) {
        if (!ItemView) {
            Result = std::partial_ordering::greater;
            return false;
        }
        const int cmp = std::string_view(data, size).compare(*ItemView);
        if (cmp < 0) {
            Result = std::partial_ordering::less;
        } else if (cmp > 0) {
            Result = std::partial_ordering::greater;
        }
        return false;
    }

    template <class TWrap, class CType = typename arrow::TypeTraits<typename TWrap::T>::CType>
    bool OnValue(const ui32 /*fieldIndex*/, const CType value) {
        if (!ItemView) {
            Result = std::partial_ordering::greater;
            return false;
        }
        AFL_VERIFY(ItemView->size() == sizeof(CType));
        const CType itemValue = *(CType*)(ItemView->data());
        if (value < itemValue) {
            Result = std::partial_ordering::less;
        } else if (itemValue < value) {
            Result = std::partial_ordering::greater;
        }
        return false;
    }

    TCompareActor(const std::optional<std::string_view>& itemView)
        : ItemView(itemView) {
    }
};

TConclusion<std::partial_ordering> TSimpleRowViewV0::Compare(
    const TSimpleRowViewV0& item, const std::shared_ptr<arrow::Schema>& schema, const std::optional<ui32> columnsCount) const {
    AFL_VERIFY(!columnsCount || *columnsCount <= (ui32)schema->num_fields());
    TIterator itSelf(Values, schema);
    TIterator itItem(item.Values, schema);
    {
        auto conclusion = itSelf.Start();
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    {
        auto conclusion = itItem.Start();
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }

    ui32 idx = columnsCount.value_or((ui32)schema->num_fields());
    while (!itSelf.IsFinished()) {
        AFL_VERIFY(!itItem.IsFinished());

        TCompareActor customActor(itItem.GetCurrentValue());
        TGeneralActor<TCompareActor> actor(std::move(customActor));
        Y_UNUSED(itSelf.ApplyActor(actor));
        if (actor.GetCustomActor().GetResult() != std::partial_ordering::equivalent) {
            return actor.GetCustomActor().GetResult();
        }

        if (--idx == 0) {
            break;
        }

        {
            auto conclusion = itSelf.Next();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        {
            auto conclusion = itItem.Next();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
    }
    return std::partial_ordering::equivalent;
}

TString TSimpleRowViewV0::BuildString(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 recordIndex) {
    std::string result;
    const ui8 ver = 0;
    result.append((const char*)&ver, sizeof(ui8));
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
            if constexpr (std::is_base_of<arrow::FixedSizeBinaryType, typename TWrap::T>()) {
                const arrow::util::string_view sv = arrImpl.GetView(recordIndex);
                const ui32 size = static_cast<const arrow::FixedSizeBinaryType*>(batch->schema()->field(i)->type().get())->byte_width();
                AFL_VERIFY(size == sv.size());
                result.append(sv.data(), sv.size());
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
    result.shrink_to_fit();
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

TConclusion<std::shared_ptr<arrow::Scalar>> TSimpleRowViewV0::GetScalar(
    const ui32 columnIndex, const std::shared_ptr<arrow::Schema>& schema) const {
    TGetScalarActor resultActor(columnIndex, schema);
    auto conclusion = Scan(schema, resultActor);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    AFL_VERIFY(resultActor.GetResult());
    return resultActor.GetResult();
}

TConclusionStatus TSimpleRowViewV0::TIterator::InitCurrentData() {
    AFL_VERIFY(StartedFlag);
    AFL_VERIFY(!IsFinishedFlag);
    if (1 > Values.size()) {
        IsFinishedFlag = true;
        return TConclusionStatus::Fail("unexpected string end");
    }

    const ui8 fillByte = (ui8)Values[0];
    const bool isNullFlag = (fillByte == 0);
    Values.Skip(1);
    TString errorMessage;
    const auto predScan = [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        if (isNullFlag) {
            CurrentValue.reset();
            return false;
        }
        if constexpr (std::is_base_of<arrow::FixedSizeBinaryType, typename TWrap::T>()) {
            const ui32 size = static_cast<const arrow::FixedSizeBinaryType*>(Schema[FieldIndex]->type().get())->byte_width();
            if (size > Values.size()) {
                errorMessage = "cannot read fix_size_binary size for " + Schema[FieldIndex]->name();
                return false;
            }
            CurrentValue = std::string_view(Values.data(), size);
            return false;
        }
        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            if (sizeof(ui32) > Values.size()) {
                errorMessage = "cannot read string size for " + Schema[FieldIndex]->name();
                return false;
            }
            const ui32 size = *(ui32*)(Values.data());
            Values.Skip(sizeof(ui32));
            if (size > Values.size()) {
                errorMessage = "cannot read string for " + Schema[FieldIndex]->name();
                return false;
            }
            CurrentValue = std::string_view(Values.data(), size);
            return false;
        }
        if constexpr (arrow::has_c_type<typename TWrap::T>() && !std::is_base_of<arrow::HalfFloatType, typename TWrap::T>()) {
            using CType = typename arrow::TypeTraits<typename TWrap::T>::CType;
            const ui32 size = sizeof(CType);
            if (size > Values.size()) {
                errorMessage = "cannot read ctype size for " + Schema[FieldIndex]->name();
                return false;
            }
            CurrentValue = std::string_view(Values.data(), size);
            return false;
        }
        AFL_VERIFY(false);
        return false;
    };

    NArrow::SwitchType(Schema[FieldIndex]->type()->id(), predScan);
    if (errorMessage) {
        IsFinishedFlag = true;
        return TConclusionStatus::Fail(errorMessage);
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow
