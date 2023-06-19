#pragma once
#include "node.h"
#include "context.h"

namespace NSQLTranslationV1 {

class TObjectOperatorContext {
protected:
    TScopedStatePtr Scoped;
public:
    TString ServiceId;
    TDeferredAtom Cluster;
    TObjectOperatorContext(const TObjectOperatorContext& baseItem) = default;
    TObjectOperatorContext(TScopedStatePtr scoped);
};

class TObjectProcessorImpl: public TAstListNode, public TObjectOperatorContext {
protected:
    using TBase = TAstListNode;
    TString ObjectId;
    TString TypeId;

    virtual INode::TPtr BuildOptions() const = 0;
    virtual INode::TPtr FillFeatures(INode::TPtr options) const = 0;
    INode::TPtr BuildKeys() const;
public:
    TObjectProcessorImpl(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context);

    bool DoInit(TContext& ctx, ISource* src) override;

    TPtr DoClone() const final {
        return {};
    }
};

class TCreateObject: public TObjectProcessorImpl {
private:
    using TBase = TObjectProcessorImpl;
    std::map<TString, TDeferredAtom> Features;
protected:
    virtual INode::TPtr BuildOptions() const override {
        return Y(Q(Y(Q("mode"), Q("createObject"))));
    }
    virtual INode::TPtr FillFeatures(INode::TPtr options) const override;
public:
    TCreateObject(TPosition pos, const TString& objectId,
        const TString& typeId, std::map<TString, TDeferredAtom>&& features, const TObjectOperatorContext& context)
        : TBase(pos, objectId, typeId, context)
        , Features(std::move(features)) {

    }
};

class TUpsertObject final: public TCreateObject {
private:
    using TBase = TCreateObject;
protected:
    virtual INode::TPtr BuildOptions() const override {
        return Y(Q(Y(Q("mode"), Q("upsertObject"))));
    }
public:
    using TBase::TBase;
};

class TAlterObject final: public TCreateObject {
private:
    using TBase = TCreateObject;
protected:
    virtual INode::TPtr BuildOptions() const override {
        return Y(Q(Y(Q("mode"), Q("alterObject"))));
    }
public:
    using TBase::TBase;
};

class TDropObject final: public TCreateObject {
private:
    using TBase = TCreateObject;
protected:
    virtual INode::TPtr BuildOptions() const override {
        return Y(Q(Y(Q("mode"), Q("dropObject"))));
    }
public:
    using TBase::TBase;
};

}
