#pragma once
#include "node.h"
#include "context.h"

namespace NSQLTranslationV1 {

class TObjectOperatorContext {
protected:
    TScopedStatePtr Scoped_;
public:
    TString ServiceId;
    TDeferredAtom Cluster;
    TObjectOperatorContext(const TObjectOperatorContext& baseItem) = default;
    TObjectOperatorContext(TScopedStatePtr scoped);
};

class TObjectProcessorImpl: public TAstListNode, public TObjectOperatorContext {
protected:
    using TBase = TAstListNode;
    TString ObjectId_;
    TString TypeId_;

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
    std::map<TString, TDeferredAtom> Features_;
    std::set<TString> FeaturesToReset_;
protected:
    bool ExistingOk_ = false;
    bool ReplaceIfExists_ = false;
protected:
    virtual INode::TPtr BuildOptions() const override {
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
    virtual INode::TPtr FillFeatures(INode::TPtr options) const override;
    bool DoInit(TContext& ctx, ISource* src) override;
public:
    TCreateObject(TPosition pos, const TString& objectId,
        const TString& typeId, bool existingOk, bool replaceIfExists, std::map<TString, TDeferredAtom>&& features, std::set<TString>&& featuresToReset, const TObjectOperatorContext& context)
        : TBase(pos, objectId, typeId, context)
        , Features_(std::move(features))
        , FeaturesToReset_(std::move(featuresToReset))
        , ExistingOk_(existingOk)
        , ReplaceIfExists_(replaceIfExists) {
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
    bool MissingOk() const {
        return ExistingOk_; // Because we were derived from TCreateObject
    }
protected:
    virtual INode::TPtr BuildOptions() const override {
        return Y(Q(Y(Q("mode"), Q(MissingOk() ? "dropObjectIfExists" : "dropObject"))));
    }
public:
    using TBase::TBase;
};

}
