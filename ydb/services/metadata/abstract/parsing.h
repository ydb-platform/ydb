#pragma once
#include "request_features.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/string.h>
#include <map>
#include <optional>

namespace NYql {
class TObjectSettingsImpl {
public:
    using TFeaturesExtractor = NYql::TFeaturesExtractor;
private:
    using TFeatures = THashMap<TString, TString>;
    YDB_READONLY_DEF(TString, TypeId);
    YDB_READONLY_DEF(TString, ObjectId);
    TFeatures Features;
    std::shared_ptr<TFeaturesExtractor> FeaturesExtractor;
public:
    TObjectSettingsImpl() = default;

    TObjectSettingsImpl(const TString& typeId, const TString& objectId, const TFeatures& features)
        : TypeId(typeId)
        , ObjectId(objectId)
        , Features(features)
        , FeaturesExtractor(std::make_shared<TFeaturesExtractor>(Features))
        {}

    TFeaturesExtractor& GetFeaturesExtractor() const {
        Y_VERIFY(!!FeaturesExtractor);
        return *FeaturesExtractor;
    }

    template <class TKiObject>
    bool DeserializeFromKi(const TKiObject& data) {
        ObjectId = data.ObjectId();
        TypeId = data.TypeId();
        for (auto&& i : data.Features()) {
            if (auto maybeAtom = i.template Maybe<NYql::NNodes::TCoAtom>()) {
                Features.emplace(maybeAtom.Cast().StringValue(), "");
            } else if (auto maybeTuple = i.template Maybe<NNodes::TCoNameValueTuple>()) {
                auto tuple = maybeTuple.Cast();
                if (auto tupleValue = tuple.Value().template Maybe<NNodes::TCoAtom>()) {
                    Features.emplace(tuple.Name().Value(), tupleValue.Cast().Value());
                }
            }
        }
        FeaturesExtractor = std::make_shared<TFeaturesExtractor>(Features);
        return true;
    }
};

using TUpsertObjectSettings = TObjectSettingsImpl;
using TCreateObjectSettings = TObjectSettingsImpl;
using TAlterObjectSettings = TObjectSettingsImpl;
using TDropObjectSettings = TObjectSettingsImpl;

}
