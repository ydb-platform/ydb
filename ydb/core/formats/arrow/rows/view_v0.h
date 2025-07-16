#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

class TSimpleRowViewV0 {
private:
    TStringBuf Values;

    class TIterator {
    private:
        TStringBuf Values;
        const std::vector<std::shared_ptr<arrow::Field>>& Schema;
        ui32 FieldIndex = 0;
        bool IsFinishedFlag = false;
        bool StartedFlag = false;

        YDB_READONLY_DEF(std::optional<std::string_view>, CurrentValue);

        TConclusionStatus InitCurrentData();

    public:
        TIterator(const TStringBuf values, const std::shared_ptr<arrow::Schema>& schema)
            : Values(values)
            , Schema(schema->fields()) {
        }

        [[nodiscard]] TConclusionStatus Start() {
            if (1 > Values.size()) {
                return TConclusionStatus::Fail("unexpected string end on version reading");
            }
            if ((ui8)Values[0] != 0) {
                return TConclusionStatus::Fail("uncompatible version for 0 viewer");
            }
            Values.Skip(1);
            StartedFlag = true;
            return InitCurrentData();
        }

        bool IsFinished() const {
            return IsFinishedFlag || FieldIndex == (ui32)Schema.size();
        }

        template <class TActor>
        [[nodiscard]] bool ApplyActor(TActor& actor) {
            const auto predScan = [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                if (!CurrentValue) {
                    return actor.template OnValue<TWrap>(FieldIndex, nullptr, 0);
                } else {
                    return actor.template OnValue<TWrap>(FieldIndex, CurrentValue->data(), CurrentValue->size());
                }
            };

            IsFinishedFlag = NArrow::SwitchType(Schema[FieldIndex]->type()->id(), predScan);
            return !IsFinishedFlag;
        }

        [[nodiscard]] TConclusionStatus Next() {
            AFL_VERIFY(!IsFinishedFlag);
            if (!!CurrentValue) {
                Values.Skip(CurrentValue->size());
            }
            if (++FieldIndex == (ui32)Schema.size()) {
                IsFinishedFlag = true;
                return TConclusionStatus::Success();
            }
            return InitCurrentData();
        }
    };

    template <class TActor>
    [[nodiscard]] TConclusionStatus Scan(const std::shared_ptr<arrow::Schema>& schema, TActor& actor) const {
        TIterator it(Values, schema);
        auto startConclusion = it.Start();
        if (startConclusion.IsFail()) {
            return startConclusion;
        }
        while (!it.IsFinished()) {
            if (!it.ApplyActor(actor)) {
                break;
            }

            auto conclusion = it.Next();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        return TConclusionStatus::Success();
    }

public:
    TSimpleRowViewV0(const TStringBuf sb)
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

    class TWriter {
    private:
        TString Data;
    public:
        TWriter(const ui32 reserveSize = 0) {
            Data.reserve(reserveSize + sizeof(ui8));
            const ui8 ver = 0;
            Data.append((const char*)&ver, sizeof(ui8));
        }
        template <class T>
        void Append(const T& value, const bool nullable = true) {
            static_assert(std::is_integral_v<T>);
            if constexpr (std::is_integral_v<T>) {
                if (nullable) {
                    const ui8 byte = 1;
                    Data.append((const char*)&byte, sizeof(ui8));
                }
                Data.append((const char*)&value, sizeof(T));
            }
        }

        void Append(const char* value, const ui32 size, const bool nullable = true) {
            if (nullable) {
                const ui8 byte = 1;
                Data.append((const char*)&byte, sizeof(ui8));
            }
            Data.append(value, size);
        }
        TString Finish() {
            return std::move(Data);
        }
    };

    [[nodiscard]] TConclusionStatus AddToBuilders(
        const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const std::shared_ptr<arrow::Schema>& schema) const;
    [[nodiscard]] TConclusion<std::partial_ordering> Compare(
        const TSimpleRowViewV0& item, const std::shared_ptr<arrow::Schema>& schema, const std::optional<ui32> columnsCount = {}) const;
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

}   // namespace NKikimr::NArrow
