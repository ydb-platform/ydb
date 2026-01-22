#pragma once

#include "request_features.h"

#include <ydb/library/accessor/accessor.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/string.h>
#include <util/generic/typetraits.h>

namespace NYql {

namespace NObjectOptionsParsing {

Y_HAS_MEMBER(ExistingOk); // for create
Y_HAS_MEMBER(MissingOk); // for alter, drop
Y_HAS_MEMBER(ReplaceIfExists); // for create
Y_HAS_MEMBER(ResetFeatures); // for alter

} // namespace NObjectOptionsParsing

class TObjectSettingsImpl {
public:
    using TFeaturesExtractor = TFeaturesExtractor;

private:
    using TResetFeatures = std::unordered_set<TString>;
    YDB_READONLY_DEF(TString, TypeId);
    YDB_READONLY_DEF(TString, ObjectId);
    YDB_READONLY_DEF(bool, ExistingOk); // for create
    YDB_READONLY_DEF(bool, MissingOk); // for alter, drop
    YDB_READONLY_DEF(bool, ReplaceIfExists); // for create
    TFeaturesExtractor::TFeatures Features;
    TResetFeatures ResetFeatures;
    std::shared_ptr<TFeaturesExtractor> FeaturesExtractor;

public:
    TObjectSettingsImpl() = default;

    TObjectSettingsImpl(const TString& typeId, const TString& objectId, const THashMap<TString, TString>& features, const TResetFeatures& resetFeatures = {});

    TFeaturesExtractor& GetFeaturesExtractor() const;

    template <class TKiObject>
    bool DeserializeFromKi(const TKiObject& data) {
        ObjectId = data.ObjectId();
        TypeId = data.TypeId();
        if constexpr (NObjectOptionsParsing::THasReplaceIfExists<TKiObject>::value) {
            ReplaceIfExists = (data.ReplaceIfExists().Value() == "1");
        }
        if constexpr (NObjectOptionsParsing::THasExistingOk<TKiObject>::value) {
            ExistingOk = (data.ExistingOk().Value() == "1");
        }
        if constexpr (NObjectOptionsParsing::THasMissingOk<TKiObject>::value) {
            MissingOk = (data.MissingOk().Value() == "1");
        }
        if constexpr (NObjectOptionsParsing::THasResetFeatures<TKiObject>::value) {
            for (auto&& i : data.ResetFeatures()) {
                if (auto maybeAtom = i.template Maybe<NNodes::TCoAtom>()) {
                    ResetFeatures.emplace(maybeAtom.Cast().StringValue());
                }
            }
        }
        DeserializeFeatures(data.Features(), Features);
        FeaturesExtractor = std::make_shared<TFeaturesExtractor>(Features, ResetFeatures);
        return true;
    }

private:
    static void DeserializeFeatures(const NNodes::TCoNameValueTupleList& features, TFeaturesExtractor::TFeatures& result);
};

using TCreateObjectSettings = TObjectSettingsImpl;
using TUpsertObjectSettings = TObjectSettingsImpl;
using TAlterObjectSettings = TObjectSettingsImpl;
using TDropObjectSettings = TObjectSettingsImpl;

} // namespace NYql
