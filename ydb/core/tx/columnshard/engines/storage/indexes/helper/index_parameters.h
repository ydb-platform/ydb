#pragma once

#include <optional>
#include <utility>

#include <library/cpp/json/writer/json_value.h>
#include <util/generic/strbuf.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NIndexes::NIndexParameters {

// Common
inline constexpr const char* ColumnName = "column_name";

// Bloom filter
inline constexpr const char* FalsePositiveProbability = "false_positive_probability";
inline constexpr const char* CaseSensitive = "case_sensitive";

// Bloom ngram filter
inline constexpr const char* NGrammSize = "ngramm_size";
inline constexpr const char* HashesCount = "hashes_count";
inline constexpr const char* FilterSizeBytes = "filter_size_bytes";
inline constexpr const char* RecordsCount = "records_count";

template <class TValue, class TCheckType, class TGetter>
class TOptionalJsonField {
public:
    TOptionalJsonField(const TStringBuf key, std::optional<TValue>& target, TCheckType&& checkType, TGetter&& getter,
        const TStringBuf typeError)
        : Key(key)
        , Target(target)
        , CheckType(std::forward<TCheckType>(checkType))
        , Getter(std::forward<TGetter>(getter))
        , TypeError(typeError)
    {
    }

    TConclusionStatus Parse(const NJson::TJsonValue& json) const {
        if (!json.Has(Key)) {
            return TConclusionStatus::Success();
        }

        if (!CheckType(json[Key])) {
            return TConclusionStatus::Fail(TString(TypeError));
        }

        Target = Getter(json[Key]);
        return TConclusionStatus::Success();
    }

private:
    TStringBuf Key;
    std::optional<TValue>& Target;
    TCheckType CheckType;
    TGetter Getter;
    TStringBuf TypeError;
};

template <class TValue, class TCheckType, class TGetter>
TOptionalJsonField<TValue, std::decay_t<TCheckType>, std::decay_t<TGetter>> MakeOptionalJsonField(
    const TStringBuf key, std::optional<TValue>& target, TCheckType&& checkType, TGetter&& getter, const TStringBuf typeError) {
    return TOptionalJsonField<TValue, std::decay_t<TCheckType>, std::decay_t<TGetter>>(
        key, target, std::forward<TCheckType>(checkType), std::forward<TGetter>(getter), typeError);
}

template <class... TFields>
TConclusionStatus ParseOptionalJsonFields(const NJson::TJsonValue& json, const TFields&... fields) {
    TConclusionStatus result = TConclusionStatus::Success();
    const auto parseOne = [&](const auto& field) {
        if (result.IsSuccess()) {
            result = field.Parse(json);
        }
    };

    (parseOne(fields), ...);
    return result;
}

template <class TValue, class TSetter>
void SetProtoIfPresent(const std::optional<TValue>& value, TSetter&& setter) {
    if (value) {
        std::forward<TSetter>(setter)(*value);
    }
}

}   // namespace NKikimr::NOlap::NIndexes::NIndexParameters
