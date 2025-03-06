#include "object_processing.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>

namespace NSQLTranslationV1 {
using namespace NYql;

INode::TPtr TObjectProcessorImpl::BuildKeys() const {
    auto keys = Y("Key");
    keys = L(keys, Q(Y(Q("objectId"), Y("String", BuildQuotedAtom(Pos, ObjectId)))));
    keys = L(keys, Q(Y(Q("typeId"), Y("String", BuildQuotedAtom(Pos, TypeId)))));
    return keys;
}

TObjectProcessorImpl::TObjectProcessorImpl(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context)
    : TBase(pos)
    , TObjectOperatorContext(context)
    , ObjectId(objectId)
    , TypeId(typeId)
{

}

bool TObjectProcessorImpl::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    Scoped->UseCluster(ServiceId, Cluster);
    auto options = FillFeatures(BuildOptions());
    AddNodeFeatures(options);
    auto keys = BuildKeys();

    Add("block", Q(Y(
        Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos, ServiceId), Scoped->WrapCluster(Cluster, ctx))),
        Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
        Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world"))
    )));
    return TAstListNode::DoInit(ctx, src);
}

INode::TPtr TCreateObject::FillFeatures(INode::TPtr options) const {
    if (!Features.empty()) {
        auto features = Y();
        for (auto&& i : Features) {
            if (i.second.HasNode()) {
                features->Add(Q(Y(BuildQuotedAtom(Pos, i.first), i.second.Build())));
            } else {
                features->Add(Q(Y(BuildQuotedAtom(Pos, i.first))));
            }
        }
        options->Add(Q(Y(Q("features"), Q(features))));
    }
    if (!FeaturesToReset.empty()) {
        auto reset = Y();
        for (const auto& featureName : FeaturesToReset) {
            reset->Add(BuildQuotedAtom(Pos, featureName));
        }
        options->Add(Q(Y(Q("resetFeatures"), Q(reset))));
    }
    return options;
}

namespace {

bool InitFeatures(TContext& ctx, ISource* src, std::map<TString, TDeferredAtom>& features) {
    for (auto& [name, feature] : features) {
        if (feature.HasNode() && !feature.Build()->Init(ctx, src)) {
            return false;
        }
    }
    return true;
}

bool InitNodeFeatures(TContext& ctx, ISource* src, std::map<TString, TNodePtr>& nodeFeatures) {
    for (auto& [name, nodeFeature] : nodeFeatures) {
        if (!nodeFeature->Init(ctx, src)) {
            return false;
        }
    }
    return true;
}

}

bool TCreateObject::DoInit(TContext& ctx, ISource* src) {
    if (!InitFeatures(ctx, src, Features)) {
        return false;
    }
    if (!InitNodeFeatures(ctx, src, NodeFeatures)) {
        return false;
    }
    return TObjectProcessorImpl::DoInit(ctx, src);
}

void TCreateObject::AddNodeFeatures(TNodePtr options) const {
    if (!NodeFeatures.empty()) {
        auto nodeFeatures = Y();
        for (const auto& [name, node] : NodeFeatures) {
            nodeFeatures->Add(Q(Y(BuildQuotedAtom(Pos, name), node)));
        }
        options->Add(Q(Y(Q("nodeFeatures"), Q(nodeFeatures))));
    }
}

void TCreateObject::AddNodeFeature(TStringBuf name, TNodePtr node) {
    NodeFeatures.emplace(name, node);
}

TObjectOperatorContext::TObjectOperatorContext(TScopedStatePtr scoped)
    : Scoped(scoped)
    , ServiceId(Scoped->CurrService)
    , Cluster(Scoped->CurrCluster)
{

}

}
