#pragma once
#include "column_features.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap {

class TSchemaObjectsCache {
private:
    THashMap<TString, std::shared_ptr<arrow::Field>> Fields;
    THashMap<TString, std::shared_ptr<TColumnFeatures>> ColumnFeatures;
    THashSet<TString> StringsCache;
    mutable ui64 AcceptionFieldsCount = 0;
    mutable ui64 AcceptionFeaturesCount = 0;

public:
    const TString& GetStringCache(const TString& original) {
        auto it = StringsCache.find(original);
        if (it == StringsCache.end()) {
            it = StringsCache.emplace(original).first;
        }
        return *it;
    }

    void RegisterField(const TString& fingerprint, const std::shared_ptr<arrow::Field>& f) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "register_field")("fp", fingerprint)("f", f->ToString());
        AFL_VERIFY(Fields.emplace(fingerprint, f).second);
    }
    void RegisterColumnFeatures(const TString& fingerprint, const std::shared_ptr<TColumnFeatures>& f) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "register_column_features")("fp", fingerprint)("info", f->DebugString());
        AFL_VERIFY(ColumnFeatures.emplace(fingerprint, f).second);
    }
    std::shared_ptr<arrow::Field> GetField(const TString& fingerprint) const {
        auto it = Fields.find(fingerprint);
        if (it == Fields.end()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_field_miss")("fp", fingerprint)("count", Fields.size())(
                "acc", AcceptionFieldsCount);
            return nullptr;
        }
        if (++AcceptionFieldsCount % 1000 == 0) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_field_accept")("fp", fingerprint)("count", Fields.size())(
                "acc", AcceptionFieldsCount);
        }
        return it->second;
    }
    template <class TConstructor>
    TConclusion<std::shared_ptr<TColumnFeatures>> GetOrCreateColumnFeatures(const TString& fingerprint, const TConstructor& constructor) {
        auto it = ColumnFeatures.find(fingerprint);
        if (it == ColumnFeatures.end()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_column_features_miss")("fp", UrlEscapeRet(fingerprint))(
                "count", ColumnFeatures.size())("acc", AcceptionFeaturesCount);
            TConclusion<std::shared_ptr<TColumnFeatures>> resultConclusion = constructor();
            if (resultConclusion.IsFail()) {
                return resultConclusion;
            }
            it = ColumnFeatures.emplace(fingerprint, resultConclusion.DetachResult()).first;
            AFL_VERIFY(it->second);
        } else {
            if (++AcceptionFeaturesCount % 1000 == 0) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_column_features_accept")("fp", UrlEscapeRet(fingerprint))(
                    "count", ColumnFeatures.size())("acc", AcceptionFeaturesCount);
            }
        }
        return it->second;
    }
};

}   // namespace NKikimr::NOlap
