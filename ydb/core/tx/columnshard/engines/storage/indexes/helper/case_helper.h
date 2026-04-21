#pragma once

#include <ydb/core/formats/arrow/hash/calcer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <util/generic/buffer.h>
#include <util/string/ascii.h>

namespace NKikimr::NOlap::NIndexes {

// old value cannot be used after the next Normalize call
class TCaseStringNormalizer {
private:
    const bool CaseSensitive;
    TBuffer LowerStringBuffer;

public:
    explicit TCaseStringNormalizer(const bool caseSensitive)
        : CaseSensitive(caseSensitive) {
    }

    bool IsCaseSensitive() const {
        return CaseSensitive;
    }

    TStringBuf Normalize(const TStringBuf value) {
        if (CaseSensitive) {
            return value;
        }

        LowerStringBuffer.Clear();
        LowerStringBuffer.Resize(value.size());
        for (ui32 i = 0; i < value.size(); ++i) {
            LowerStringBuffer.Data()[i] = AsciiToLower(value[i]);
        }

        return TStringBuf(LowerStringBuffer.Data(), value.size());
    }
};

class TCaseAwareHashCalcer {
private:
    TCaseStringNormalizer StringNormalizer;

public:
    explicit TCaseAwareHashCalcer(const bool caseSensitive)
        : StringNormalizer(caseSensitive) {
    }

    ui64 CalcString(const TStringBuf value, const ui64 seed) {
        return NArrow::NHash::TXX64::CalcSimple(StringNormalizer.Normalize(value), seed);
    }

    ui64 CalcJsonScalar(const TStringBuf value, const ui64 seed) {
        return CalcString(value, seed);
    }

    ui64 CalcScalar(const std::shared_ptr<arrow::Scalar>& scalar, const ui64 seed) {
        if (StringNormalizer.IsCaseSensitive()) {
            return NArrow::NHash::TXX64::CalcForScalar(scalar, seed);
        }

        AFL_VERIFY(scalar);
        ui64 result = 0;
        NArrow::SwitchType(scalar->type->id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using T = typename TWrap::T;
            using TScalar = typename arrow::TypeTraits<T>::ScalarType;

            auto& typedScalar = static_cast<const TScalar&>(*scalar);
            if constexpr (arrow::has_string_view<T>()) {
                result = CalcString(TStringBuf((const char*)typedScalar.value->data(), typedScalar.value->size()), seed);
            } else {
                result = NArrow::NHash::TXX64::CalcForScalar(scalar, seed);
            }

            return true;
        });

        return result;
    }

    template <class TAction>
    void CalcForAll(const std::shared_ptr<arrow::Array>& array, const ui64 seed, const TAction& action) {
        if (StringNormalizer.IsCaseSensitive()) {
            NArrow::NHash::TXX64::CalcForAll(array, seed, action);
            return;
        }

        AFL_VERIFY(array);
        NArrow::SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using T = typename TWrap::T;
            using TArray = typename arrow::TypeTraits<T>::ArrayType;
            auto& typedArray = static_cast<const TArray&>(*array);

            for (ui32 idx = 0; idx < array->length(); ++idx) {
                if (array->IsNull(idx)) {
                    action(NArrow::NHash::TXX64::CalcSimple(TStringBuf(), seed), idx);
                    continue;
                }

                auto value = typedArray.GetView(idx);
                if constexpr (arrow::has_string_view<T>()) {
                    action(CalcString(TStringBuf(value.data(), value.size()), seed), idx);
                } else if constexpr (arrow::has_c_type<T>()) {
                    action(NArrow::NHash::TXX64::CalcSimple(&value, sizeof(value), seed), idx);
                } else {
                    static_assert(arrow::is_decimal_type<T>());
                    AFL_VERIFY(false);
                }
            }

            return true;
        });
    }
};

}   // namespace NKikimr::NOlap::NIndexes
