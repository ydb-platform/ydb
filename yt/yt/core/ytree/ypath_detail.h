#pragma once

#include "attributes.h"
#include "permission.h"
#include "tree_builder.h"
#include "ypath_service.h"
#include "system_attribute_provider.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/producer.h>
#include <yt/yt/core/yson/forwarding_consumer.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

#include <library/cpp/yt/misc/cast.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IYPathServiceContext
    : public virtual NRpc::IServiceContext
{
    virtual void SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header) = 0;

    virtual void SetReadRequestComplexityLimiter(
        const TReadRequestComplexityLimiterPtr& limiter) = 0;
    virtual TReadRequestComplexityLimiterPtr GetReadRequestComplexityLimiter() = 0;
};

DEFINE_REFCOUNTED_TYPE(IYPathServiceContext)

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceContextWrapper
    : public NRpc::TServiceContextWrapper
    , public IYPathServiceContext
{
public:
    explicit TYPathServiceContextWrapper(IYPathServiceContextPtr underlyingContext);

    void SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header) override;

    void SetReadRequestComplexityLimiter(const TReadRequestComplexityLimiterPtr& limiter) override;
    TReadRequestComplexityLimiterPtr GetReadRequestComplexityLimiter() override;

    const IYPathServiceContextPtr& GetUnderlyingContext() const;

private:
    const IYPathServiceContextPtr UnderlyingContext_;
};

DEFINE_REFCOUNTED_TYPE(TYPathServiceContextWrapper)

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_YPATH_SERVICE_METHOD(ns, method) \
    using TCtx##method = TYPathTypedServiceContextImpl<ns::TReq##method, ns::TRsp##method>; \
    using TCtx##method##Ptr = ::NYT::TIntrusivePtr<TCtx##method>; \
    using TReq##method = TCtx##method::TTypedRequest; \
    using TRsp##method = TCtx##method::TTypedResponse; \
    \
    void method##Thunk( \
        const ::NYT::TIntrusivePtr<IYPathServiceContextImpl>& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(context, options); \
        if (typedContext->DeserializeRequest()) { \
            this->method( \
                &typedContext->Request(), \
                &typedContext->Response(), \
                typedContext); \
        } \
    } \
    \
    void method( \
        [[maybe_unused]] TReq##method* request, \
        [[maybe_unused]] TRsp##method* response, \
        [[maybe_unused]] const TCtx##method##Ptr& context)

#define DEFINE_YPATH_SERVICE_METHOD(type, method) \
    void type::method( \
        [[maybe_unused]] TReq##method* request, \
        [[maybe_unused]] TRsp##method* response, \
        [[maybe_unused]] const TCtx##method##Ptr& context)

////////////////////////////////////////////////////////////////////////////////

#define DISPATCH_YPATH_SERVICE_METHOD(method, ...) \
    if (context->GetMethod() == #method) { \
        auto options = ::NYT::NRpc::THandlerInvocationOptions() __VA_ARGS__; \
        method##Thunk(context, options); \
        return true; \
    }

////////////////////////////////////////////////////////////////////////////////

//! These aliases provide an option to replace default typed service context class
//! with a custom one which may be richer in some way.
#define DEFINE_YPATH_CONTEXT_IMPL(serviceContext, typedServiceContext) \
    using IYPathServiceContextImpl = serviceContext; \
    \
    template <class RequestMessage, class ResponseMessage> \
    using TYPathTypedServiceContextImpl = typedServiceContext<RequestMessage, ResponseMessage>;

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceBase
    : public virtual IYPathService
{
public:
    void Invoke(const IYPathServiceContextPtr& context) override;
    TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& context) override;
    void DoWriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const TAttributeFilter& attributeFilter,
        bool stable) override;
    bool ShouldHideAttributes() override;

protected:
    DEFINE_YPATH_CONTEXT_IMPL(IYPathServiceContext, TTypedYPathServiceContext);

    virtual void BeforeInvoke(const IYPathServiceContextPtr& context);
    virtual bool DoInvoke(const IYPathServiceContextPtr& context);
    virtual void AfterInvoke(const IYPathServiceContextPtr& context);

    virtual TResolveResult ResolveSelf(const TYPath& path, const IYPathServiceContextPtr& context);
    virtual TResolveResult ResolveAttributes(const TYPath& path, const IYPathServiceContextPtr& context);
    virtual TResolveResult ResolveRecursive(const TYPath& path, const IYPathServiceContextPtr& context);
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SUPPORTS_METHOD(method, ...) \
    class TSupports##method \
        __VA_OPT__(: public)  __VA_ARGS__ \
    { \
    protected: \
        DECLARE_YPATH_SERVICE_METHOD(::NYT::NYTree::NProto, method); \
        virtual void method##Self(TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context); \
        virtual void method##Recursive(const NYPath::TYPath& path, TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context); \
        virtual void method##Attribute(const NYPath::TYPath& path, TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context); \
    }

#define IMPLEMENT_SUPPORTS_METHOD_RESOLVE(method, onPathError) \
    DEFINE_RPC_SERVICE_METHOD(TSupports##method, method) \
    { \
        NYPath::TTokenizer tokenizer(GetRequestTargetYPath(context->RequestHeader())); \
        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) { \
            method##Self(request, response, context); \
            return; \
        } \
        tokenizer.Skip(NYPath::ETokenType::Ampersand); \
        if (tokenizer.GetType() != NYPath::ETokenType::Slash) { \
            onPathError \
            return; \
        } \
        if (tokenizer.Advance() == NYPath::ETokenType::At) { \
            method##Attribute(NYPath::TYPath(tokenizer.GetSuffix()), request, response, context); \
        } else { \
            method##Recursive(NYPath::TYPath(tokenizer.GetInput()), request, response, context); \
        } \
    }

#define IMPLEMENT_SUPPORTS_METHOD(method) \
    IMPLEMENT_SUPPORTS_METHOD_RESOLVE( \
        method, \
        { \
            tokenizer.ThrowUnexpected(); \
        }) \
    \
    void TSupports##method::method##Attribute(const NYPath::TYPath& /*path*/, TReq##method* /*request*/, TRsp##method* /*response*/, const TCtx##method##Ptr& context) \
    { \
        ThrowMethodNotSupported(context->GetMethod(), TString("attribute")); \
    } \
    \
    void TSupports##method::method##Self(TReq##method* /*request*/, TRsp##method* /*response*/, const TCtx##method##Ptr& context) \
    { \
        ThrowMethodNotSupported(context->GetMethod(), TString("self")); \
    } \
    \
    void TSupports##method::method##Recursive(const NYPath::TYPath& /*path*/, TReq##method* /*request*/, TRsp##method* /*response*/, const TCtx##method##Ptr& context) \
    { \
        ThrowMethodNotSupported(context->GetMethod(), TString("recursive")); \
    }

////////////////////////////////////////////////////////////////////////////////

DEFINE_YPATH_CONTEXT_IMPL(IYPathServiceContext, TTypedYPathServiceContext);

class TSupportsExistsBase
{
protected:
    template <class TContextPtr>
    void Reply(const TContextPtr& context, bool exists);
};

class TSupportsMultisetAttributes
    : public virtual TRefCounted
{
protected:
    DECLARE_YPATH_SERVICE_METHOD(NProto, Multiset);
    DECLARE_YPATH_SERVICE_METHOD(NProto, MultisetAttributes);

    virtual void SetAttributes(
        const TYPath& path,
        TReqMultisetAttributes* request,
        TRspMultisetAttributes* response,
        const TCtxMultisetAttributesPtr& context);

private:
    // COMPAT(gritukan) Move it to MultisetAttributes.
    void DoSetAttributes(
        const TYPath& path,
        TReqMultisetAttributes* request,
        TRspMultisetAttributes* response,
        const TCtxMultisetAttributesPtr& context);
};

DECLARE_SUPPORTS_METHOD(GetKey, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Get, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Set, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(List, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Remove, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Exists, TSupportsExistsBase);

////////////////////////////////////////////////////////////////////////////////

class TSupportsPermissions
{
protected:
    virtual ~TSupportsPermissions() = default;

    // The last argument will be empty for contexts where authenticated user is known
    // a-priori (like in object proxies in master), otherwise it will be set to user name
    // (like in operation controller orchid).
    virtual void ValidatePermission(
        EPermissionCheckScope scope,
        EPermission permission,
        const TString& user = {});

    class TCachingPermissionValidator
    {
    public:
        TCachingPermissionValidator(
            TSupportsPermissions* owner,
            EPermissionCheckScope scope);

        void Validate(EPermission permission, const TString& user = {});

    private:
        TSupportsPermissions* const Owner_;
        const EPermissionCheckScope Scope_;

        THashMap<TString, EPermissionSet> ValidatedPermissions_;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TSupportsAttributes
    : public virtual TYPathServiceBase
    , public virtual TSupportsGet
    , public virtual TSupportsList
    , public virtual TSupportsSet
    , public virtual TSupportsMultisetAttributes
    , public virtual TSupportsRemove
    , public virtual TSupportsExists
    , public virtual TSupportsPermissions
{
protected:
    TSupportsAttributes();

    IAttributeDictionary* GetCombinedAttributes();

    //! Can be |nullptr|.
    virtual IAttributeDictionary* GetCustomAttributes();

    //! Can be |nullptr|.
    virtual ISystemAttributeProvider* GetBuiltinAttributeProvider();

    TResolveResult ResolveAttributes(
        const NYPath::TYPath& path,
        const IYPathServiceContextPtr& context) override;

    void GetAttribute(
        const TYPath& path,
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;

    void ListAttribute(
        const TYPath& path,
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;

    void ExistsAttribute(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;

    void SetAttribute(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    void RemoveAttribute(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    void SetAttributes(
        const TYPath& path,
        TReqMultisetAttributes* request,
        TRspMultisetAttributes* response,
        const TCtxMultisetAttributesPtr& context) override;

private:
    class TCombinedAttributeDictionary
        : public IAttributeDictionary
    {
    public:
        explicit TCombinedAttributeDictionary(TSupportsAttributes* owner);

        std::vector<TString> ListKeys() const override;
        std::vector<TKeyValuePair> ListPairs() const override;
        NYson::TYsonString FindYson(TStringBuf key) const override;
        void SetYson(const TString& key, const NYson::TYsonString& value) override;
        bool Remove(const TString& key) override;

    private:
        TSupportsAttributes* const Owner_;
    };

    using TCombinedAttributeDictionaryPtr = TIntrusivePtr<TCombinedAttributeDictionary>;

    TCombinedAttributeDictionaryPtr CombinedAttributes_;

    TFuture<NYson::TYsonString> DoFindAttribute(TStringBuf key);

    static NYson::TYsonString DoGetAttributeFragment(
        TStringBuf key,
        const TYPath& path,
        const NYson::TYsonString& wholeYson);
    TFuture<NYson::TYsonString> DoGetAttribute(
        const TYPath& path,
        const TAttributeFilter& attributeFilter);

    static bool DoExistsAttributeFragment(
        TStringBuf key,
        const TYPath& path,
        const TErrorOr<NYson::TYsonString>& wholeYsonOrError);
    TFuture<bool> DoExistsAttribute(const TYPath& path);

    static NYson::TYsonString DoListAttributeFragment(
        TStringBuf key,
        const TYPath& path,
        const NYson::TYsonString& wholeYson);
    TFuture<NYson::TYsonString> DoListAttribute(const TYPath& path);

    void DoSetAttribute(const TYPath& path, const NYson::TYsonString& newYson, bool force = false);
    void DoRemoveAttribute(const TYPath& path, bool force);

    bool GuardedGetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer);
    bool GuardedSetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value, bool force);
    bool GuardedRemoveBuiltinAttribute(TInternedAttributeKey key);

    void ValidateAttributeKey(TStringBuf key) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSystemBuiltinAttributeKeysCache
{
public:
    const THashSet<TInternedAttributeKey>& GetBuiltinAttributeKeys(ISystemAttributeProvider* provider);

private:
    std::atomic<bool> Initialized_ = false;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InitializationLock_);

    THashSet<TInternedAttributeKey> BuiltinKeys_;
};

////////////////////////////////////////////////////////////////////////////////

class TSystemCustomAttributeKeysCache
{
public:
    const THashSet<TString>& GetCustomAttributeKeys(ISystemAttributeProvider* provider);

private:
    std::atomic<bool> Initialized_ = false;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InitializationLock_);

    THashSet<TString> CustomKeys_;
};

////////////////////////////////////////////////////////////////////////////////

class TOpaqueAttributeKeysCache
{
public:
    const THashSet<TString>& GetOpaqueAttributeKeys(ISystemAttributeProvider* provider);

private:
    std::atomic<bool> Initialized_ = false;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InitializationLock_);

    THashSet<TString> OpaqueKeys_;
};

////////////////////////////////////////////////////////////////////////////////

class TTypedConsumer
    : public NYson::TForwardingYsonConsumer
{
protected:
    void ThrowInvalidType(ENodeType actualType);
    virtual ENodeType GetExpectedType() = 0;

    void OnMyStringScalar(TStringBuf value) override;
    void OnMyInt64Scalar(i64 value) override;
    void OnMyUint64Scalar(ui64 value) override;
    void OnMyDoubleScalar(double value) override;
    void OnMyBooleanScalar(bool value) override;
    void OnMyEntity() override;

    void OnMyBeginList() override;

    void OnMyBeginMap() override;
};

void SetNodeFromProducer(
    const INodePtr& node,
    const NYson::TYsonProducer& producer,
    ITreeBuilder* builder);

////////////////////////////////////////////////////////////////////////////////

IYPathServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    NLogging::TLogger logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

IYPathServiceContextPtr CreateYPathContext(
    std::unique_ptr<NRpc::NProto::TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    NLogging::TLogger logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree


#define YPATH_DETAIL_INL_H_
#include "ypath_detail-inl.h"
#undef YPATH_DETAIL_INL_H_
