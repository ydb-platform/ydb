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
    using TBase = TAstListNode;

    TString ObjectId_;
    TString TypeId_;

    INode::TPtr BuildKeys() const;

protected:
    virtual INode::TPtr BuildOptions() const = 0;

    virtual INode::TPtr FillFeatures(INode::TPtr options) const = 0;

public:
    TObjectProcessorImpl(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context);

    bool DoInit(TContext& ctx, ISource* src) override;

    TPtr DoClone() const final;
};

class TObjectProcessorWithFeatures: public TObjectProcessorImpl {
protected:
    using TFeatureMap = std::map<TString, TDeferredAtom>;

private:
    using TBase = TObjectProcessorImpl;

    TFeatureMap Features_;

protected:
    INode::TPtr FillFeatures(INode::TPtr options) const override;

public:
    bool DoInit(TContext& ctx, ISource* src) override;

    TObjectProcessorWithFeatures(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                                 TFeatureMap&& features);
};

class TCreateObject final: public TObjectProcessorWithFeatures {
    using TBase = TObjectProcessorWithFeatures;

    bool ExistingOk_ = false;
    bool ReplaceIfExists_ = false;

protected:
    INode::TPtr BuildOptions() const final;

public:
    TCreateObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                  TFeatureMap&& features, bool existingOk, bool replaceIfExists);
};

class TUpsertObject final: public TObjectProcessorWithFeatures {
    using TBase = TObjectProcessorWithFeatures;

protected:
    INode::TPtr BuildOptions() const final;

public:
    using TBase::TBase;
};

class TAlterObject final: public TObjectProcessorWithFeatures {
    using TBase = TObjectProcessorWithFeatures;

    std::set<TString> FeaturesToReset_;
    bool MissingOk_ = false;

protected:
    INode::TPtr BuildOptions() const final;

    INode::TPtr FillFeatures(INode::TPtr options) const final;

public:
    TAlterObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                 TFeatureMap&& features, std::set<TString>&& featuresToReset, bool missingOk);
};

class TDropObject final: public TObjectProcessorWithFeatures {
    using TBase = TObjectProcessorWithFeatures;

    bool MissingOk_ = false;

protected:
    INode::TPtr BuildOptions() const final;

public:
    TDropObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                TFeatureMap&& features, bool missingOk);
};

} // namespace NSQLTranslationV1
