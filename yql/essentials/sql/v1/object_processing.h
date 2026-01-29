#pragma once

#include "node.h"
#include "context.h"

namespace NSQLTranslationV1 {

class TObjectFeatureNode final: public TAstListNode {
    using TBase = TAstListNode;

    struct TFeature {
        TPosition Pos;
        TNodePtr Node;
    };

public:
    explicit TObjectFeatureNode(TPosition pos, const std::map<TString, TDeferredAtom>& features = {});

    static TNodePtr SkipEmpty(TObjectFeatureNodePtr features);

    std::pair<TNodePtr&, bool> AddFeature(TStringBuf feature, TPosition pos);

protected:
    bool DoInit(TContext& ctx, ISource* src) final;

    TPtr DoClone() const final;

private:
    std::map<TString, TFeature> Features_;
};

class TObjectOperatorContext {
protected:
    const TScopedStatePtr Scoped_;

public:
    TString ServiceId;
    TDeferredAtom Cluster;

    TObjectOperatorContext(const TObjectOperatorContext& baseItem) = default;

    explicit TObjectOperatorContext(TScopedStatePtr scoped);
};

class TObjectProcessorImpl: public TAstListNode, public TObjectOperatorContext {
    using TBase = TAstListNode;

    INode::TPtr BuildKeys() const;

protected:
    const TString ObjectId_;
    const TString TypeId_;

    virtual INode::TPtr BuildOptions() const = 0;

    virtual INode::TPtr FillFeatures(INode::TPtr options) const = 0;

public:
    TObjectProcessorImpl(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context);

    bool DoInit(TContext& ctx, ISource* src) override;
};

class TObjectProcessorWithFeatures: public TObjectProcessorImpl {
    using TBase = TObjectProcessorImpl;

protected:
    const TNodePtr Features_;

    INode::TPtr FillFeatures(INode::TPtr options) const override;

public:
    bool DoInit(TContext& ctx, ISource* src) override;

    TObjectProcessorWithFeatures(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                                 TNodePtr features);
};

class TCreateObject final: public TObjectProcessorWithFeatures {
    using TBase = TObjectProcessorWithFeatures;

    const bool ExistingOk_ = false;
    const bool ReplaceIfExists_ = false;

protected:
    INode::TPtr BuildOptions() const final;

    TPtr DoClone() const final;

public:
    TCreateObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                  TNodePtr features, bool existingOk, bool replaceIfExists);
};

class TUpsertObject final: public TObjectProcessorWithFeatures {
    using TBase = TObjectProcessorWithFeatures;

protected:
    INode::TPtr BuildOptions() const final;

    TPtr DoClone() const final;

public:
    using TBase::TBase;
};

class TAlterObject final: public TObjectProcessorWithFeatures {
    using TBase = TObjectProcessorWithFeatures;

    const std::set<TString> FeaturesToReset_;
    const bool MissingOk_ = false;

protected:
    INode::TPtr BuildOptions() const final;

    INode::TPtr FillFeatures(INode::TPtr options) const final;

    TPtr DoClone() const final;

public:
    TAlterObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                 TNodePtr features, std::set<TString>&& featuresToReset, bool missingOk);
};

class TDropObject final: public TObjectProcessorWithFeatures {
    using TBase = TObjectProcessorWithFeatures;

    const bool MissingOk_ = false;

protected:
    INode::TPtr BuildOptions() const final;

    TPtr DoClone() const final;

public:
    TDropObject(TPosition pos, const TString& objectId, const TString& typeId, const TObjectOperatorContext& context,
                TNodePtr features, bool missingOk);
};

} // namespace NSQLTranslationV1
