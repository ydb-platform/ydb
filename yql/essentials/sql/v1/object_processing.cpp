#include "object_processing.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>

using namespace NYql;

namespace NSQLTranslationV1 {

namespace {

bool InitFeatures(TContext& ctx, ISource* src, const std::map<TString, TDeferredAtom>& features) {
    for (const auto& [key, value] : features) {
        if (value.HasNode() && !value.Build()->Init(ctx, src)) {
            return false;
        }
    }

    return true;
}

} // anonymous namespace

TObjectOperatorContext::TObjectOperatorContext(TScopedStatePtr scoped)
    : Scoped_(scoped)
    , ServiceId(Scoped_->CurrService)
    , Cluster(Scoped_->CurrCluster)
{
}

INode::TPtr TObjectProcessorImpl::BuildKeys() const {
    auto keys = Y("Key");
    keys = L(keys, Q(Y(Q("objectId"), Y("String", BuildQuotedAtom(Pos_, ObjectId_)))));
    keys = L(keys, Q(Y(Q("typeId"), Y("String", BuildQuotedAtom(Pos_, TypeId_)))));
    return keys;
}

TObjectProcessorImpl::TObjectProcessorImpl(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context)
    : TBase(pos)
    , TObjectOperatorContext(context)
    , ObjectId_(objectId)
    , TypeId_(typeId)
{
}

bool TObjectProcessorImpl::DoInit(TContext& ctx, ISource* src) {
    Scoped_->UseCluster(ServiceId, Cluster);
    auto options = FillFeatures(BuildOptions());
    auto keys = BuildKeys();

    Add("block", Q(Y(
                     Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                     Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                     Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));
    return TAstListNode::DoInit(ctx, src);
}

TObjectProcessorImpl::TPtr TObjectProcessorImpl::DoClone() const {
    return {};
}

TObjectProcessorWithFeatures::TObjectProcessorWithFeatures(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                                                           TFeatureMap&& features)
    : TBase(pos, objectId, typeId, context)
    , Features_(std::move(features))
{
}

INode::TPtr TObjectProcessorWithFeatures::FillFeatures(INode::TPtr options) const {
    if (!Features_.empty()) {
        auto features = Y();
        for (auto&& i : Features_) {
            if (i.second.HasNode()) {
                features->Add(Q(Y(BuildQuotedAtom(Pos_, i.first), i.second.Build())));
            } else {
                features->Add(Q(Y(BuildQuotedAtom(Pos_, i.first))));
            }
        }
        options->Add(Q(Y(Q("features"), Q(features))));
    }

    return options;
}

bool TObjectProcessorWithFeatures::DoInit(TContext& ctx, ISource* src) {
    if (!InitFeatures(ctx, src, Features_)) {
        return false;
    }

    return TObjectProcessorImpl::DoInit(ctx, src);
}

INode::TPtr TCreateObject::BuildOptions() const {
    TString mode;

    if (ExistingOk_) {
        mode = "createObjectIfNotExists";
    } else if (ReplaceIfExists_) {
        mode = "createObjectOrReplace";
    } else {
        mode = "createObject";
    }

    return Y(Q(Y(Q("mode"), Q(mode))));
}

TCreateObject::TCreateObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                             TFeatureMap&& features, bool existingOk, bool replaceIfExists)
    : TBase(pos, objectId, typeId, context, std::move(features))
    , ExistingOk_(existingOk)
    , ReplaceIfExists_(replaceIfExists)
{
}

INode::TPtr TUpsertObject::BuildOptions() const {
    return Y(Q(Y(Q("mode"), Q("upsertObject"))));
}

TAlterObject::TAlterObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                           TFeatureMap&& features, std::set<TString>&& featuresToReset, bool missingOk)
    : TBase(pos, objectId, typeId, context, std::move(features))
    , FeaturesToReset_(std::move(featuresToReset))
    , MissingOk_(missingOk)
{
}

INode::TPtr TAlterObject::FillFeatures(INode::TPtr options) const {
    options = TBase::FillFeatures(options);

    if (!FeaturesToReset_.empty()) {
        auto reset = Y();
        for (const auto& featureName : FeaturesToReset_) {
            reset->Add(BuildQuotedAtom(Pos_, featureName));
        }
        options->Add(Q(Y(Q("resetFeatures"), Q(reset))));
    }
    return options;
}

INode::TPtr TAlterObject::BuildOptions() const {
    return Y(Q(Y(Q("mode"), Q(MissingOk_ ? "alterObjectIfExists" : "alterObject"))));
}

TDropObject::TDropObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                         TFeatureMap&& features, bool missingOk)
    : TBase(pos, objectId, typeId, context, std::move(features))
    , MissingOk_(missingOk)
{
}

INode::TPtr TDropObject::BuildOptions() const {
    return Y(Q(Y(Q("mode"), Q(MissingOk_ ? "dropObjectIfExists" : "dropObject"))));
}

} // namespace NSQLTranslationV1
