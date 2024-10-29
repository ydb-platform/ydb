#pragma once
#include "request_features.h"

#include <ydb/core/protos/kqp_physical.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
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
    void ResetFeaturesExtractor() {
        FeaturesExtractor = std::make_shared<TFeaturesExtractor>(Features, ResetFeatures);
    }

public:
    TObjectSettingsImpl() = default;

    TObjectSettingsImpl(const TString& typeId, const TString& objectId, const TFeatures& features, const TResetFeatures& resetFeatures = {})
        : TypeId(typeId)
        , ObjectId(objectId)
        , Features(features)
        , ResetFeatures(resetFeatures) {
        ResetFeaturesExtractor();
    }

    TFeaturesExtractor& GetFeaturesExtractor() const {
        Y_ABORT_UNLESS(!!FeaturesExtractor);
        return *FeaturesExtractor;
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
        FeaturesExtractor = std::make_shared<TFeaturesExtractor>(Features, ResetFeatures);
        return true;
    }

    NKqpProto::TKqpPhyMetadataObjectSettings SerializeToProto() const {
        NKqpProto::TKqpPhyMetadataObjectSettings serialized;
        serialized.SetObjectId(ObjectId);
        serialized.SetExistingOk(ExistingOk);
        serialized.SetMissingOk(MissingOk);
        serialized.SetReplaceIfExists(ReplaceIfExists);
        auto* serializedFeatures = serialized.MutableFeatures();
        for (const auto& [key, value] : Features) {
            NKqpProto::TKqpPhyMetadataObjectSettings::TFeatureValue serializedValue;
            serializedValue.SetStringValue(value);
            AFL_VERIFY(serializedFeatures->emplace(key, std::move(serializedValue)).second);
        }
        for (const TString& feature : ResetFeatures) {
            serialized.AddResetFeatures(feature);
        }
        return serialized;
    }

    bool ParseFromProto(const NKqpProto::TKqpPhyMetadataObjectSettings& serialized) {
        ObjectId = serialized.GetObjectId();
        ExistingOk = serialized.GetExistingOk();
        MissingOk = serialized.GetMissingOk();
        ReplaceIfExists = serialized.GetReplaceIfExists();
        const auto& serializedFeatures = serialized.GetFeatures();
        for (const auto& [key, value] : serializedFeatures) {
            AFL_VERIFY(Features.emplace(key, value.GetStringValue()).second);
        }
        for (const TString& feature : ResetFeatures) {
            if (!ResetFeatures.emplace(feature).second) {
                return false;
            }
        }
        ResetFeaturesExtractor();
        return true;
    }
};

using TCreateObjectSettings = TObjectSettingsImpl;
using TUpsertObjectSettings = TObjectSettingsImpl;
using TAlterObjectSettings = TObjectSettingsImpl;
using TDropObjectSettings = TObjectSettingsImpl;

}
