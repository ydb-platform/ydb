#pragma once

#include "system_attribute_provider.h"
#include "ypath_detail.h"

#include <yt/yt/core/yson/producer.h>
#include <yt/yt/core/yson/async_writer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TVirtualCompositeNodeReadOffloadParams
{
    NConcurrency::EWaitForStrategy WaitForStrategy = NConcurrency::EWaitForStrategy::WaitFor;
    i64 BatchSize = 10'000;
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public TSupportsAttributes
    , public ISystemAttributeProvider
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Opaque, true);

protected:
    explicit TVirtualMapBase(INodePtr owningNode = nullptr);

    virtual std::optional<TVirtualCompositeNodeReadOffloadParams> GetReadOffloadParams() const;

    virtual std::vector<TString> GetKeys(i64 limit = std::numeric_limits<i64>::max()) const = 0;

    virtual i64 GetSize() const = 0;

    virtual IYPathServicePtr FindItemService(TStringBuf key) const = 0;

    bool DoInvoke(const IYPathServiceContextPtr& context) override;

    TResolveResult ResolveRecursive(const TYPath& path, const IYPathServiceContextPtr& context) override;
    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override;
    void RemoveRecursive(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    // TSupportsAttributes overrides
    ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // ISystemAttributeProvider overrides
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    const THashSet<TInternedAttributeKey>& GetBuiltinAttributeKeys() override;
    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override;
    bool SetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override;

private:
    const INodePtr OwningNode_;

    TSystemBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeMapService
    : public TVirtualMapBase
{
public:
    TCompositeMapService();
    ~TCompositeMapService();

    std::vector<TString> GetKeys(i64 limit = std::numeric_limits<i64>::max()) const override;
    i64 GetSize() const override;
    IYPathServicePtr FindItemService(TStringBuf key) const override;
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;

    TIntrusivePtr<TCompositeMapService> AddChild(const TString& key, IYPathServicePtr service);
    TIntrusivePtr<TCompositeMapService> AddAttribute(TInternedAttributeKey key, NYson::TYsonCallback producer);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TCompositeMapService)

////////////////////////////////////////////////////////////////////////////////

INodePtr CreateVirtualNode(IYPathServicePtr service);

////////////////////////////////////////////////////////////////////////////////

class TVirtualListBase
    : public TSupportsAttributes
    , public ISystemAttributeProvider
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Opaque, true);

protected:
    virtual std::optional<TVirtualCompositeNodeReadOffloadParams> GetReadOffloadParams() const;

    virtual i64 GetSize() const = 0;

    virtual IYPathServicePtr FindItemService(int index) const = 0;

    bool DoInvoke(const IYPathServiceContextPtr& context) override;

    TResolveResult ResolveRecursive(const TYPath& path, const IYPathServiceContextPtr& context) override;
    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;

    // TSupportsAttributes overrides
    ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // ISystemAttributeProvider overrides
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    const THashSet<TInternedAttributeKey>& GetBuiltinAttributeKeys() override;
    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override;
    bool SetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override;

private:
    TSystemBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define VIRTUAL_INL_H_
#include "virtual-inl.h"
#undef VIRTUAL_INL_H_
