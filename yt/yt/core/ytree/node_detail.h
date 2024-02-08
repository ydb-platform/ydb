#pragma once

#include "convert.h"
#include "exception_helpers.h"
#include "node.h"
#include "permission.h"
#include "tree_builder.h"
#include "ypath_detail.h"
#include "ypath_service.h"

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public virtual TYPathServiceBase
    , public virtual TSupportsGetKey
    , public virtual TSupportsGet
    , public virtual TSupportsSet
    , public virtual TSupportsMultisetAttributes
    , public virtual TSupportsRemove
    , public virtual TSupportsList
    , public virtual TSupportsExists
    , public virtual TSupportsPermissions
    , public virtual INode
{
public:
#define IMPLEMENT_AS_METHODS(type) \
    TIntrusivePtr<I##type##Node> As##type() override \
    { \
        ThrowInvalidNodeType(this, ENodeType::type, GetType()); \
    } \
    \
    TIntrusivePtr<const I##type##Node> As##type() const override \
    { \
        ThrowInvalidNodeType(this, ENodeType::type, GetType()); \
    }

    IMPLEMENT_AS_METHODS(Entity)
    IMPLEMENT_AS_METHODS(Composite)
    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Int64)
    IMPLEMENT_AS_METHODS(Uint64)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(Boolean)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)
#undef IMPLEMENT_AS_METHODS

    TYPath GetPath() const override;

protected:
    bool DoInvoke(const IYPathServiceContextPtr& context) override;

    TResolveResult ResolveRecursive(const NYPath::TYPath& path, const IYPathServiceContextPtr& context) override;
    void GetKeySelf(TReqGetKey* request, TRspGetKey* response, const TCtxGetKeyPtr& context) override;
    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    void RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override;
    virtual void DoRemoveSelf(bool recursive, bool force);
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeNodeMixin
    : public virtual TYPathServiceBase
    , public virtual TSupportsSet
    , public virtual TSupportsMultisetAttributes
    , public virtual TSupportsRemove
    , public virtual TSupportsPermissions
    , public virtual ICompositeNode
{
protected:
    void RemoveRecursive(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    void SetRecursive(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    virtual void SetChild(
        INodeFactory* factory,
        const TYPath& path,
        INodePtr child,
        bool recursive) = 0;

    virtual int GetMaxChildCount() const;

    void ValidateChildCount(const TYPath& path, int childCount) const;
};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeMixin
    : public virtual TCompositeNodeMixin
    , public virtual TSupportsList
    , public virtual IMapNode
{
protected:
    IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const IYPathServiceContextPtr& context) override;

    void ListSelf(
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;

    void SetChild(
        INodeFactory* factory,
        const TYPath& path,
        INodePtr child,
        bool recursive) override;

    std::pair<TString, INodePtr> PrepareSetChild(
        INodeFactory* factory,
        const TYPath& path,
        INodePtr child,
        bool recursive);

    virtual int GetMaxKeyLength() const;

private:
    void ThrowMaxKeyLengthViolated() const;
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeMixin
    : public virtual TCompositeNodeMixin
    , public virtual IListNode
{
protected:
    IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const IYPathServiceContextPtr& context) override;

    void SetChild(
        INodeFactory* factory,
        const TYPath& path,
        INodePtr child,
        bool recursive) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSupportsSetSelfMixin
    : public virtual TNodeBase
{
protected:
    void SetSelf(TReqSet* request, TRspSet* /*response*/, const TCtxSetPtr& context) override;

    virtual void ValidateSetSelf(bool /*force*/) const;
};

////////////////////////////////////////////////////////////////////////////////

class TNonexistingService
    : public TYPathServiceBase
    , public TSupportsExists
{
public:
    static IYPathServicePtr Get();

private:
    bool DoInvoke(const IYPathServiceContextPtr& context) override;

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& context) override;

    void ExistsSelf(
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;

    void ExistsRecursive(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;

    void ExistsAttribute(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;

    void ExistsAny(const TCtxExistsPtr& context);
};

////////////////////////////////////////////////////////////////////////////////

#define YTREE_NODE_TYPE_OVERRIDES(type) \
public: \
    ::NYT::NYTree::ENodeType GetType() const override \
    { \
        return ::NYT::NYTree::ENodeType::type; \
    } \
    \
    TIntrusivePtr<const ::NYT::NYTree::I##type##Node> As##type() const override \
    { \
        return this; \
    } \
    \
    TIntrusivePtr<::NYT::NYTree::I##type##Node> As##type() override \
    { \
        return this; \
    }

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENodeFactoryState,
    (Active)
    (Committed)
    (RolledBack)
);

class TTransactionalNodeFactoryBase
    : public virtual ITransactionalNodeFactory
{
public:
    virtual ~TTransactionalNodeFactoryBase();

    void Commit() noexcept override;
    void Rollback() noexcept override;

protected:
    void RollbackIfNeeded();

private:
    using EState = ENodeFactoryState;
    EState State_ = EState::Active;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

