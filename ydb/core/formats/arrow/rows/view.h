#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

class TSimpleRowView {
private:
    TStringBuf Values;

    template <class TActor>
    [[nodiscard]] TConclusionStatus Scan(const std::shared_ptr<arrow::Schema>& schema, TActor& actor) const {
        TStringBuf currentValues = Values;
        for (ui32 fieldIdx = 0; fieldIdx < (ui32)schema->num_fields(); ++fieldIdx) {
            if (1 > currentValues.size()) {
                return TConclusionStatus::Fail("unexpected string end");
            }
            const ui8 fillByte = (ui8)currentValues[0];
            currentValues.Skip(1);
            TString errorMessage;
            const auto predScan = [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                if (fillByte == 0) {
                    return actor.template OnValue<TWrap>(fieldIdx, nullptr, 0);
                }
                if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                    if (sizeof(ui32) > currentValues.size()) {
                        errorMessage = "cannot read string size for " + schema->field(fieldIdx)->name();
                        return false;
                    }
                    const ui32 size = *(ui32*)(currentValues.data());
                    currentValues.Skip(sizeof(ui32));
                    if (size > currentValues.size()) {
                        errorMessage = "cannot read string for " + schema->field(fieldIdx)->name();
                        return false;
                    }
                    if (actor.template OnValue<TWrap>(fieldIdx, currentValues.data(), size)) {
                        return true;
                    }
                    currentValues.Skip(size);
                    return false;
                }
                if constexpr (arrow::has_c_type<typename TWrap::T>() && !std::is_base_of<arrow::HalfFloatType, typename TWrap::T>()) {
                    using CType = typename arrow::TypeTraits<typename TWrap::T>::CType;
                    if (sizeof(CType) > currentValues.size()) {
                        errorMessage = "cannot read ctype size for " + schema->field(fieldIdx)->name();
                    }
                    if (actor.template OnValue<TWrap>(fieldIdx, currentValues.data(), sizeof(CType))) {
                        return true;
                    }
                    currentValues.Skip(sizeof(CType));
                    return false;
                }
                AFL_VERIFY(false);
                return false;
            };

            const bool finished = NArrow::SwitchType(schema->field(fieldIdx)->type()->id(), predScan);
            if (errorMessage) {
                return TConclusionStatus::Fail(errorMessage);
            }
            if (finished) {
                return TConclusionStatus::Success();
            }
        }
        return TConclusionStatus::Success();
    }

public:
    TSimpleRowView(const TStringBuf sb)
        : Values(sb) {
    }

    class TActorValidator {
    public:
        template <class TWrap>
        bool OnValue(const ui32 /*fieldIndex*/, const char* /*data*/, const ui32 /*size*/) {
            return false;
        }
    };
    [[nodiscard]] TConclusionStatus Validate(const std::shared_ptr<arrow::Schema>& schema) const {
        TActorValidator actorValidation;
        return Scan(schema, actorValidation);
    }

    [[nodiscard]] bool DoValidate(const std::shared_ptr<arrow::Schema>& schema) const {
        TActorValidator actorValidation;
        auto conclusion = Scan(schema, actorValidation);
        if (conclusion.IsFail()) {
            AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "simple_row_validation")("reason", conclusion.GetErrorMessage());
        }
        return !conclusion.IsFail();
    }

    static TString BuildString(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 recordIndex);

    [[nodiscard]] TConclusionStatus AddToBuilders(
        const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const std::shared_ptr<arrow::Schema>& schema) const;
    [[nodiscard]] TConclusion<std::partial_ordering> Compare(
        const TSimpleRowView& item, const std::shared_ptr<arrow::Schema>& schema, const std::optional<ui32> columnsCount = {}) const;
    [[nodiscard]] TConclusion<TString> DebugString(const std::shared_ptr<arrow::Schema>& schema) const;

    template <class T>
    class TGetValueActor {
    private:
        const ui32 ColumnIndex;
        YDB_ACCESSOR_DEF(std::optional<T>, Result);

    public:
        template <class TWrap>
        bool OnValue(const ui32 fieldIndex, const char* data, const ui32 size) {
            if (fieldIndex != ColumnIndex) {
                return false;
            }
            AFL_VERIFY(fieldIndex == ColumnIndex);
            if (!data) {
                return true;
            }
            if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                if constexpr (std::is_same_v<T, std::string_view>) {
                    Result = std::string_view(data, size);
                    return true;
                }
                AFL_VERIFY(false);
            }
            if constexpr (arrow::has_c_type<typename TWrap::T>() && !std::is_base_of<arrow::HalfFloatType, typename TWrap::T>()) {
                using CType = typename arrow::TypeTraits<typename TWrap::T>::CType;
                if constexpr (std::is_same_v<T, CType>) {
                    Result = *(CType*)(data);
                    return true;
                }
                AFL_VERIFY(false);
            }
            return true;
        }
        TGetValueActor(const ui32 columnIndex)
            : ColumnIndex(columnIndex) {
        }
    };

    template <class T>
    TConclusion<std::optional<T>> GetValue(const ui32 columnIndex, const std::shared_ptr<arrow::Schema>& schema) const {
        TGetValueActor<T> resultActor(columnIndex);
        auto conclusion = Scan(schema, resultActor);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        return resultActor.GetResult();
    }

    TConclusion<std::shared_ptr<arrow::Scalar>> GetScalar(const ui32 columnIndex, const std::shared_ptr<arrow::Schema>& schema) const;
};

class TSimpleRow {
private:
    YDB_READONLY_DEF(TString, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);

public:
    ui32 GetMemorySize() const {
        return Data.capacity() + sizeof(std::shared_ptr<arrow::Schema>);
    }

    TSimpleRow(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 recordIndex) {
        AFL_VERIFY(batch);
        Schema = batch->schema();
        Data = TSimpleRowView::BuildString(batch, recordIndex);
        AFL_VERIFY_DEBUG(TSimpleRowView(Data).DoValidate(Schema));
    }

    std::shared_ptr<arrow::RecordBatch> ToBatch() const;
    [[nodiscard]] TConclusionStatus AddToBuilders(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const {
        return TSimpleRowView(Data).AddToBuilders(builders, Schema);
    }

    ui32 GetColumnsCount() const {
        return Schema->num_fields();
    }

    template <class T>
    std::optional<T> GetValue(const TString& columnName) const {
        const int fIndex = Schema->GetFieldIndex(columnName);
        AFL_VERIFY(fIndex >= 0);
        return GetValue<T>(fIndex);
    }

    std::shared_ptr<arrow::Scalar> GetScalar(const ui32 columnIndex) const {
        return TSimpleRowView(Data).GetScalar(columnIndex, Schema).DetachResult();
    }

    template <class T>
    T GetValueVerified(const TString& columnName) const {
        const int fIndex = Schema->GetFieldIndex(columnName);
        AFL_VERIFY(fIndex >= 0);
        return GetValueVerified<T>(fIndex);
    }

    template <class T>
    std::optional<T> GetValue(const ui32 columnIndex) const {
        return TSimpleRowView(Data).GetValue<T>(columnIndex, Schema).DetachResult();
    }

    template <class T>
    T GetValueVerified(const ui32 columnIndex) const {
        const auto result = TSimpleRowView(Data).GetValue<T>(columnIndex, Schema).DetachResult();
        AFL_VERIFY(!!result);
        return *result;
    }

    TSimpleRow(const TString& data, const std::shared_ptr<arrow::Schema>& schema)
        : Data(data)
        , Schema(schema) {
        AFL_VERIFY_DEBUG(TSimpleRowView(Data).DoValidate(Schema));
    }

    TString DebugString() const {
        return TSimpleRowView(Data).DebugString(Schema).GetResult();
    }

    std::partial_ordering ComparePartNotNull(const TSimpleRow& item, const ui32 columnsCount) const {
        AFL_VERIFY(columnsCount <= GetColumnsCount());
        AFL_VERIFY(columnsCount <= item.GetColumnsCount());
        return TSimpleRowView(Data).Compare(TSimpleRowView(item.Data), Schema, columnsCount).GetResult();
    }

    std::partial_ordering CompareNotNull(const TSimpleRow& item) const {
        AFL_VERIFY(GetColumnsCount() <= item.GetColumnsCount());
        return TSimpleRowView(Data).Compare(TSimpleRowView(item.Data), Schema).GetResult();
    }

    std::partial_ordering operator<=>(const TSimpleRow& item) const;
};

}   // namespace NKikimr::NArrow
