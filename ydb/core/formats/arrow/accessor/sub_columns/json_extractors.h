#pragma once
#include "direct_builder.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/simdjson/include/simdjson/dom/array-inl.h>
#include <contrib/libs/simdjson/include/simdjson/dom/document-inl.h>
#include <contrib/libs/simdjson/include/simdjson/dom/element-inl.h>
#include <contrib/libs/simdjson/include/simdjson/dom/object-inl.h>
#include <contrib/libs/simdjson/include/simdjson/dom/parser-inl.h>
#include <contrib/libs/simdjson/include/simdjson/ondemand.h>
#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class IJsonObjectExtractor {
private:
    const TStringBuf Prefix;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) = 0;

protected:
    const bool FirstLevelOnly = false;
    TStringBuf GetPrefix() const {
        return Prefix;
    }

    [[nodiscard]] TConclusionStatus AddDataToBuilder(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators,
        const TStringBuf key, NBinaryJson::TEntryCursor& value) const;

public:
    virtual ~IJsonObjectExtractor() = default;

    IJsonObjectExtractor(const TStringBuf prefix, const bool firstLevelOnly)
        : Prefix(prefix)
        , FirstLevelOnly(firstLevelOnly) {
    }

    [[nodiscard]] TConclusionStatus Fill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) {
        return DoFill(dataBuilder, iterators);
    }
};

class TKVExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    NBinaryJson::TObjectIterator Iterator;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) override;

public:
    TKVExtractor(const NBinaryJson::TObjectIterator& iterator, const TStringBuf prefix, const bool firstLevelOnly = false)
        : TBase(prefix, firstLevelOnly)
        , Iterator(iterator) {
    }
};

class TArrayExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    NBinaryJson::TArrayIterator Iterator;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) override;

public:
    TArrayExtractor(const NBinaryJson::TArrayIterator& iterator, const TStringBuf prefix, const bool firstLevelOnly = false)
        : TBase(prefix, firstLevelOnly)
        , Iterator(iterator) {
    }
};

class TSIMDExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    simdjson::simdjson_result<simdjson::ondemand::document>& Document;

#define RETURN_IF_NOT_SUCCESS(expr)                                                                        \
    if (const auto& status = expr; Y_UNLIKELY(status != simdjson::SUCCESS)) {                              \
        return TConclusionStatus::Fail("json parsing error: " + TString(simdjson::error_message(status))); \
    }

    TConclusion<std::string_view> PrintObject(simdjson::ondemand::value& value) const {
        switch (value.type()) {
            case simdjson::ondemand::json_type::string: {
                auto sv = (std::string_view)value.raw_json_token();
                AFL_VERIFY(sv.size() >= 2);
                return std::string_view(sv.data() + 1, sv.size() - 2);
            }
            case simdjson::ondemand::json_type::null: {
                return TDataBuilder::GetNullStringView();
            }
            case simdjson::ondemand::json_type::number:
            case simdjson::ondemand::json_type::boolean: {
                return (std::string_view)value.raw_json_token();
            }
            case simdjson::ondemand::json_type::object: {
                simdjson::ondemand::object v;
                RETURN_IF_NOT_SUCCESS(value.get(v));
                return v.raw_json();
            }
            case simdjson::ondemand::json_type::array: {
                simdjson::ondemand::array v;
                RETURN_IF_NOT_SUCCESS(value.get(v));
                return v.raw_json();
            }
        }
    }

    template <typename TOnDemandValue>
        requires std::is_same_v<TOnDemandValue, simdjson::ondemand::value> || std::is_same_v<TOnDemandValue, simdjson::ondemand::document>
    [[nodiscard]] TConclusionStatus ProcessValue(TDataBuilder& dataBuilder, TOnDemandValue& value, const TStringBuf currentKey) {
        switch (value.type()) {
            case simdjson::ondemand::json_type::string: {
                auto sv = (std::string_view)value.raw_json_token();
                AFL_VERIFY(sv.size() >= 2);
                dataBuilder.AddKV(currentKey, TStringBuf(sv.data() + 1, sv.size() - 2));
                break;
            }
            case simdjson::ondemand::json_type::null: {
                dataBuilder.AddKVNull(currentKey);
                break;
            }
            case simdjson::ondemand::json_type::number:
            case simdjson::ondemand::json_type::boolean: {
                dataBuilder.AddKV(currentKey, (std::string_view)value.raw_json_token());
                break;
            }
            case simdjson::ondemand::json_type::array: {
                simdjson::ondemand::array v;
                RETURN_IF_NOT_SUCCESS(value.get(v));
                ui32 idx = 0;
                for (auto item : v) {
                    RETURN_IF_NOT_SUCCESS(item.error());
                    const TStringBuf sbKey = dataBuilder.AddKeyOwn(currentKey, "[" + std::to_string(idx++) + "]");
                    if (FirstLevelOnly) {
                        auto conclusion = PrintObject(item.value_unsafe());
                        if (conclusion.IsFail()) {
                            return conclusion;
                        }
                        dataBuilder.AddKV(sbKey, conclusion.DetachResult());
                    } else {
                        auto conclusion =
                            ProcessValue(dataBuilder, item.value_unsafe(), sbKey);
                        if (conclusion.IsFail()) {
                            return conclusion;
                        }
                    }
                }
                break;
            }
            case simdjson::ondemand::json_type::object: {
                simdjson::ondemand::object v;
                RETURN_IF_NOT_SUCCESS(value.get(v));
                for (auto item : v) {
                    RETURN_IF_NOT_SUCCESS(item.error());
                    auto& keyValue = item.value_unsafe();
                    const auto key = keyValue.escaped_key();
                    const auto sbKey = dataBuilder.AddKey(currentKey, key);
                    if (FirstLevelOnly) {
                        auto conclusion = PrintObject(keyValue.value());
                        if (conclusion.IsFail()) {
                            return conclusion;
                        }
                        dataBuilder.AddKV(sbKey, conclusion.DetachResult());
                    } else {
                        auto conclusion = ProcessValue(dataBuilder, keyValue.value(), sbKey);
                        if (conclusion.IsFail()) {
                            return conclusion;
                        }
                    }
                }
                break;
            }
        }

        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& /*iterators*/) override {
        RETURN_IF_NOT_SUCCESS(Document.error());
        return ProcessValue(dataBuilder, Document.value_unsafe(), TStringBuf());
    }

public:
#undef RETURN_IF_NOT_SUCCESS
    TSIMDExtractor(simdjson::simdjson_result<simdjson::ondemand::document>& document, const bool firstLevelOnly = false)
        : TBase(TStringBuf(), firstLevelOnly)
        , Document(document) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
