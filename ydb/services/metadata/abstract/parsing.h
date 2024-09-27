#pragma once
#include "request_features.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/string.h>
#include <util/generic/typetraits.h>

#include <map>
#include <optional>

namespace NYql {

namespace NObjectOptionsParsing {
Y_HAS_MEMBER(ExistingOk); // for create
Y_HAS_MEMBER(MissingOk); // for drop
Y_HAS_MEMBER(ReplaceIfExists); // for create
Y_HAS_MEMBER(ResetFeatures); // for alter
} // namespace NObjectOptionsParsing

class TObjectSettingsImpl {
public:
    using TFeaturesExtractor = NYql::TFeaturesExtractor;
    using TProto = NKikimrSchemeOp::TAbstractObjectDescription;

private:
    using TFeatures = THashMap<TString, TString>;
    using TResetFeatures = std::unordered_set<TString>;
    YDB_READONLY_DEF(TString, TypeId);
    YDB_READONLY_DEF(TString, ObjectId);
    YDB_READONLY_DEF(bool, ExistingOk); // for create
    YDB_READONLY_DEF(bool, MissingOk); // for drop
    YDB_READONLY_DEF(bool, ReplaceIfExists); // for create
    TFeatures Features;
    TResetFeatures ResetFeatures;
    std::shared_ptr<TFeaturesExtractor> FeaturesExtractor;

private:
    void InitFeaturesExtractor() {
        FeaturesExtractor = std::make_shared<TFeaturesExtractor>(Features, ResetFeatures);
    }

public:
    TObjectSettingsImpl() = default;

    TObjectSettingsImpl(const TString& typeId, const TString& objectId, const TFeatures& features, const TResetFeatures& resetFeatures = {})
        : TypeId(typeId)
        , ObjectId(objectId)
        , Features(features)
        , ResetFeatures(resetFeatures)
        , FeaturesExtractor(std::make_shared<TFeaturesExtractor>(Features, ResetFeatures))
        {}

    TFeaturesExtractor& GetFeaturesExtractor() const {
        Y_ABORT_UNLESS(!!FeaturesExtractor);
        return *FeaturesExtractor;
    }

    const TString* ReadFeature(const TString& key) const {
        return Features.FindPtr(key);
    }

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
                if (auto maybeAtom = i.template Maybe<NYql::NNodes::TCoAtom>()) {
                    ResetFeatures.emplace(maybeAtom.Cast().StringValue());
                }
            }
        }
        for (auto&& i : data.Features()) {
            if (auto maybeAtom = i.template Maybe<NYql::NNodes::TCoAtom>()) {
                Features.emplace(maybeAtom.Cast().StringValue(), "");
            } else if (auto maybeTuple = i.template Maybe<NNodes::TCoNameValueTuple>()) {
                NNodes::TCoNameValueTuple tuple = maybeTuple.Cast();
                if (auto maybeAtom = tuple.Value().template Maybe<NNodes::TCoAtom>()) {
                    Features.emplace(tuple.Name().Value(), maybeAtom.Cast().Value());
                } else if (auto maybeInt = tuple.Value().template Maybe<NNodes::TCoIntegralCtor>()) {
                    Features.emplace(tuple.Name().Value(), maybeInt.Cast().Literal().Cast<NNodes::TCoAtom>().Value());
                } else if (auto maybeBool = tuple.Value().template Maybe<NNodes::TCoBool>()) {
                    Features.emplace(tuple.Name().Value(), maybeBool.Cast().Literal().Cast<NNodes::TCoAtom>().Value());
                }
            }
        }
        InitFeaturesExtractor();
        return true;
    }

    bool DeserializeFromProto(const TProto& data) {
        ObjectId = data.GetObject();
        TypeId = data.GetType();
        ReplaceIfExists = data.GetReplaceIfExists();
        ExistingOk = data.GetExistingOk();
        MissingOk = data.GetMissingOk();
        for (const auto& feature : data.GetResetFeatures()) {
            ResetFeatures.emplace(feature);
        }
        for (const auto& [key, value] : data.GetFeatures()) {
            if (!value.has_text_value()) {
                return false;
            }
            Features.emplace(key, value.Gettext_value());
        }
        InitFeaturesExtractor();
        return true;
    }

    TProto SerializeToProto() const {
        TProto result;
        result.SetType(TypeId);
        result.SetObject(ObjectId);
        result.SetExistingOk(ExistingOk);
        result.SetMissingOk(MissingOk);
        result.SetReplaceIfExists(ReplaceIfExists);
        for (const TString& feature : ResetFeatures) {
            result.AddResetFeatures(feature);
        }
        for (const auto& [name, value] : Features) {
            Ydb::Value serializedValue;
            serializedValue.set_text_value(value);
            result.MutableFeatures()->emplace(name, std::move(serializedValue));
        }
        return result;
    }
};

using TCreateObjectSettings = TObjectSettingsImpl;
using TUpsertObjectSettings = TObjectSettingsImpl;
using TAlterObjectSettings = TObjectSettingsImpl;
using TDropObjectSettings = TObjectSettingsImpl;

}
