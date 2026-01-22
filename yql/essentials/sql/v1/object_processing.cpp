#include "object_processing.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>

using namespace NYql;

namespace NSQLTranslationV1 {

TObjectFeatureNode::TObjectFeatureNode(TPosition pos, const std::map<TString, TDeferredAtom>& features)
    : TBase(std::move(pos))
{
    Add("quote");

    for (const auto& [name, value] : features) {
        Features_.emplace(name, TFeature{.Pos = Pos_, .Node = value.Build()});
    }
}

TNodePtr TObjectFeatureNode::SkipEmpty(TObjectFeatureNodePtr features) {
    if (!features || features->Features_.empty()) {
        return nullptr;
    }
    return features;
}

std::pair<TNodePtr&, bool> TObjectFeatureNode::AddFeature(TStringBuf feature, TPosition pos) {
    const auto [it, inserted] = Features_.emplace(feature, TFeature{.Pos = std::move(pos)});
    return {it->second.Node, inserted};
}

bool TObjectFeatureNode::DoInit(TContext& ctx, ISource* src) {
    auto featuresList = Y();
    for (const auto& [name, value] : Features_) {
        const auto& pos = value.Pos;

        if (const auto& node = value.Node) {
            if (!node->Init(ctx, src)) {
                return false;
            }

            featuresList->Add(Q(Y(BuildQuotedAtom(pos, name), node)));
        } else {
            featuresList->Add(Q(Y(BuildQuotedAtom(pos, name))));
        }
    }

    Add(std::move(featuresList));

    return TBase::DoInit(ctx, src);
}

TNodePtr TObjectFeatureNode::DoClone() const {
    TObjectFeatureNodePtr result = new TObjectFeatureNode(Pos_);

    for (const auto& [name, value] : Features_) {
        result->AddFeature(name, value.Pos).first = SafeClone(value.Node);
    }

    return result;
}

TObjectOperatorContext::TObjectOperatorContext(TScopedStatePtr scoped)
    : Scoped_(std::move(scoped))
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

TObjectProcessorWithFeatures::TObjectProcessorWithFeatures(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                                                           TNodePtr features)
    : TBase(pos, objectId, typeId, context)
    , Features_(std::move(features))
{
}

INode::TPtr TObjectProcessorWithFeatures::FillFeatures(INode::TPtr options) const {
    if (Features_) {
        options->Add(Q(Y(Q("features"), Features_)));
    }

    return options;
}

bool TObjectProcessorWithFeatures::DoInit(TContext& ctx, ISource* src) {
    if (Features_ && !Features_->Init(ctx, src)) {
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
                             TNodePtr features, bool existingOk, bool replaceIfExists)
    : TBase(pos, objectId, typeId, context, std::move(features))
    , ExistingOk_(existingOk)
    , ReplaceIfExists_(replaceIfExists)
{
}

TNodePtr TCreateObject::DoClone() const {
    return new TCreateObject(Pos_, ObjectId_, TypeId_, TObjectOperatorContext(Scoped_), SafeClone(Features_), ExistingOk_, ReplaceIfExists_);
}

INode::TPtr TUpsertObject::BuildOptions() const {
    return Y(Q(Y(Q("mode"), Q("upsertObject"))));
}

TNodePtr TUpsertObject::DoClone() const {
    return new TUpsertObject(Pos_, ObjectId_, TypeId_, TObjectOperatorContext(Scoped_), SafeClone(Features_));
}

TAlterObject::TAlterObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                           TNodePtr features, std::set<TString>&& featuresToReset, bool missingOk)
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

TNodePtr TAlterObject::DoClone() const {
    return new TAlterObject(Pos_, ObjectId_, TypeId_, TObjectOperatorContext(Scoped_), SafeClone(Features_), std::set(FeaturesToReset_), MissingOk_);
}

TDropObject::TDropObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                         TNodePtr features, bool missingOk)
    : TBase(pos, objectId, typeId, context, std::move(features))
    , MissingOk_(missingOk)
{
}

INode::TPtr TDropObject::BuildOptions() const {
    return Y(Q(Y(Q("mode"), Q(MissingOk_ ? "dropObjectIfExists" : "dropObject"))));
}

TNodePtr TDropObject::DoClone() const {
    return new TDropObject(Pos_, ObjectId_, TypeId_, TObjectOperatorContext(Scoped_), SafeClone(Features_), MissingOk_);
}

} // namespace NSQLTranslationV1
