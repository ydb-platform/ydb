#pragma once

#include "public.h"
#include "ypath_service.h"
#include "attribute_filter.h"

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/rpc/client.h>

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYPathRequest
    : public NRpc::IClientRequest
{
public:
    //! Enables tagging requests with arbitrary payload.
    //! These tags are propagated to the respective responses (if a particular implementation supports this).
    //! This simplifies correlating requests with responses within a batch.
    DEFINE_BYREF_RW_PROPERTY(std::any, Tag);

public:
    NRpc::TRequestId GetRequestId() const override;
    NRpc::TRealmId GetRealmId() const override;
    std::string GetMethod() const override;
    std::string GetService() const override;

    using NRpc::IClientRequest::DeclareClientFeature;
    using NRpc::IClientRequest::RequireServerFeature;

    void DeclareClientFeature(int featureId) override;
    void RequireServerFeature(int featureId) override;

    const TString& GetUser() const override;
    void SetUser(const TString& user) override;

    const TString& GetUserTag() const override;
    void SetUserTag(const TString& tag) override;

    void SetUserAgent(const TString& userAgent) override;

    bool GetRetry() const override;
    void SetRetry(bool value) override;

    NRpc::TMutationId GetMutationId() const override;
    void SetMutationId(NRpc::TMutationId id) override;

    size_t GetHash() const override;

    const NRpc::NProto::TRequestHeader& Header() const override;
    NRpc::NProto::TRequestHeader& Header() override;

    bool IsStreamingEnabled() const override;

    const NRpc::TStreamingParameters& ClientAttachmentsStreamingParameters() const override;
    NRpc::TStreamingParameters& ClientAttachmentsStreamingParameters() override;

    const NRpc::TStreamingParameters& ServerAttachmentsStreamingParameters() const override;
    NRpc::TStreamingParameters& ServerAttachmentsStreamingParameters() override;

    NConcurrency::IAsyncZeroCopyOutputStreamPtr GetRequestAttachmentsStream() const override;
    NConcurrency::IAsyncZeroCopyInputStreamPtr GetResponseAttachmentsStream() const override;

    bool IsLegacyRpcCodecsEnabled() override;

    TSharedRefArray Serialize() override;

protected:
    explicit TYPathRequest(const NRpc::NProto::TRequestHeader& header);

    TYPathRequest(
        std::string service,
        std::string method,
        NYPath::TYPath path,
        bool mutating);

    NRpc::NProto::TRequestHeader Header_;
    std::vector<TSharedRef> Attachments_;

    virtual TSharedRef SerializeBody() const = 0;
};

DEFINE_REFCOUNTED_TYPE(TYPathRequest)

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathRequest
    : public TYPathRequest
    , public TRequestMessage
{
public:
    using TTypedResponse = TTypedYPathResponse<TRequestMessage, TResponseMessage>;

    explicit TTypedYPathRequest(const NRpc::NProto::TRequestHeader& header)
        : TYPathRequest(header)
    { }

    TTypedYPathRequest(
        std::string service,
        std::string method,
        const NYPath::TYPath& path,
        bool mutating)
        : TYPathRequest(
            std::move(service),
            std::move(method),
            path,
            mutating)
    { }

protected:
    TSharedRef SerializeBody() const override
    {
        // COPMAT(danilalexeev): legacy RPC codecs
        if (Header_.has_request_codec()) {
            YT_VERIFY(Header_.request_codec() == NYT::ToProto<int>(NCompression::ECodec::None));
            return SerializeProtoToRefWithCompression(*this);
        } else {
            return SerializeProtoToRefWithEnvelope(*this);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYPathResponse
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

    //! A copy of the request's tag.
    DEFINE_BYREF_RW_PROPERTY(std::any, Tag);

public:
    void Deserialize(const TSharedRefArray& message);

protected:
    virtual bool TryDeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = {});
};

DEFINE_REFCOUNTED_TYPE(TYPathResponse)

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse
    : public TYPathResponse
    , public TResponseMessage
{
protected:
    bool TryDeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = {}) override
    {
        return codecId
            ? TryDeserializeProtoWithCompression(this, data, *codecId)
            : TryDeserializeProtoWithEnvelope(this, data);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_YPATH_PROXY(name) \
    static const ::NYT::NRpc::TServiceDescriptor& GetDescriptor() \
    { \
        static const auto Descriptor = ::NYT::NRpc::TServiceDescriptor(#name); \
        return Descriptor; \
    } \
    static_assert(true)

#define DEFINE_YPATH_PROXY_METHOD_IMPL(ns, method, isMutating) \
    using TReq##method = ::NYT::NYTree::TTypedYPathRequest<ns::TReq##method, ns::TRsp##method>; \
    using TRsp##method = ::NYT::NYTree::TTypedYPathResponse<ns::TReq##method, ns::TRsp##method>; \
    using TReq##method##Ptr = ::NYT::TIntrusivePtr<TReq##method>; \
    using TRsp##method##Ptr = ::NYT::TIntrusivePtr<TRsp##method>; \
    using TErrorOrRsp##method##Ptr = ::NYT::TErrorOr<TRsp##method##Ptr>; \
    \
    static TReq##method##Ptr method(const NYT::NYPath::TYPath& path = NYT::NYPath::TYPath()) \
    { \
        return New<TReq##method>(GetDescriptor().ServiceName, #method, path, isMutating); \
    } \
    static_assert(true)

#define DEFINE_YPATH_PROXY_METHOD(ns, method) \
    DEFINE_YPATH_PROXY_METHOD_IMPL(ns, method, false)

#define DEFINE_MUTATING_YPATH_PROXY_METHOD(ns, method) \
    DEFINE_YPATH_PROXY_METHOD_IMPL(ns, method, true)

////////////////////////////////////////////////////////////////////////////////

// TODO(gritukan): It's not easy to find a proper return type for these functions
// that is suitable both for vanilla and patched protobufs. In an ideal world,
// it would be TYPathBuf, but for now it breaks the advantages for CoW of the
// TString. Rethink it if and when YT will try to use std::string or non-CoW
// TString everywhere.
#ifdef YT_USE_VANILLA_PROTOBUF

TYPath GetRequestTargetYPath(const NRpc::NProto::TRequestHeader& header);
TYPath GetOriginalRequestTargetYPath(const NRpc::NProto::TRequestHeader& header);

#else

const TYPath& GetRequestTargetYPath(const NRpc::NProto::TRequestHeader& header);
const TYPath& GetOriginalRequestTargetYPath(const NRpc::NProto::TRequestHeader& header);

#endif

void SetRequestTargetYPath(NRpc::NProto::TRequestHeader* header, TYPath path);

bool IsRequestMutating(const NRpc::NProto::TRequestHeader& header);

//! Runs a sequence of IYPathService::Resolve calls aimed to discover the
//! ultimate endpoint responsible for serving a given request.
void ResolveYPath(
    const IYPathServicePtr& rootService,
    const IYPathServiceContextPtr& context,
    IYPathServicePtr* suffixService,
    TYPath* suffixPath);

//! Asynchronously executes an untyped request against a given service.
TFuture<TSharedRefArray>
ExecuteVerb(
    const IYPathServicePtr& service,
    const TSharedRefArray& requestMessage,
    NLogging::TLogger logger = {},
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

//! Asynchronously executes a request against a given service.
void ExecuteVerb(
    const IYPathServicePtr& service,
    const IYPathServiceContextPtr& context);

//! Asynchronously executes a typed YPath request against a given service.
template <class TTypedRequest>
TFuture<TIntrusivePtr<typename TTypedRequest::TTypedResponse>>
ExecuteVerb(
    const IYPathServicePtr& service,
    const TIntrusivePtr<TTypedRequest>& request,
    NLogging::TLogger logger = {},
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

//! Synchronously executes a typed YPath request against a given service.
//! Throws if an error has occurred.
template <class TTypedRequest>
TIntrusivePtr<typename TTypedRequest::TTypedResponse>
SyncExecuteVerb(
    const IYPathServicePtr& service,
    const TIntrusivePtr<TTypedRequest>& request,
    NLogging::TLogger logger = {},
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

//! Executes |GetKey| verb assuming #service handles requests synchronously. Throws if an error has occurred.
TString SyncYPathGetKey(
    const IYPathServicePtr& service,
    const TYPath& path);

//! Asynchronously executes |Get| verb.
TFuture<NYson::TYsonString> AsyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter = {});

//! Executes |Get| verb assuming #service handles requests synchronously. Throws if an error has occurred.
NYson::TYsonString SyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter = {});

//! Asynchronously executes |Exists| verb.
TFuture<bool> AsyncYPathExists(
    const IYPathServicePtr& service,
    const TYPath& path);

//! Executes |Exists| verb assuming #service handles requests synchronously. Throws if an error has occurred.
bool SyncYPathExists(
    const IYPathServicePtr& service,
    const TYPath& path);

//! Asynchronously executes |Set| verb.
TFuture<void> AsyncYPathSet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const NYson::TYsonString& value,
    bool recursive = false);

//! Executes |Set| verb assuming #service handles requests synchronously. Throws if an error has occurred.
void SyncYPathSet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const NYson::TYsonString& value,
    bool recursive = false);

//! Asynchronously executes |Remove| verb.
TFuture<void> AsyncYPathRemove(
    const IYPathServicePtr& service,
    const TYPath& path,
    bool recursive = true,
    bool force = false);

//! Executes |Remove| verb assuming #service handles requests synchronously. Throws if an error has occurred.
void SyncYPathRemove(
    const IYPathServicePtr& service,
    const TYPath& path,
    bool recursive = true,
    bool force = false);

//! Executes |List| verb assuming #service handles requests synchronously. Throws if an error has occurred.
std::vector<TString> SyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    std::optional<i64> limit = std::nullopt);

//! Asynchronously executes |List| verb.
TFuture<std::vector<TString>> AsyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    std::optional<i64> limit = std::nullopt);

//! Creates missing maps along #path.
/*!
 *  E.g. if #root is an empty map and #path is |/a/b/c| then
 *  nested maps |a| and |b| get created. Note that the final key (i.e. |c|)
 *  is not forced (since we have no idea of its type anyway).
 */
void ForceYPath(const INodePtr& root, const TYPath& path);

//! Constructs an ephemeral deep copy of #node.
INodePtr CloneNode(const INodePtr& node);

//! Applies changes given by #patch to #base.
//! Returns the resulting tree.
INodePtr PatchNode(const INodePtr& base, const INodePtr& patch);

struct TNodesEqualityOptions
{
    double DoubleTypePrecision = 1e-6;
};

//! Checks given nodes for deep equality.
bool AreNodesEqual(
    const INodePtr& lhs,
    const INodePtr& rhs,
    const TNodesEqualityOptions& options = {});

/////////////////////////////////////////////////////////////////////////////

struct TNodeWalkOptions
{
    std::function<INodePtr(const TString&)> MissingAttributeHandler;
    std::function<INodePtr(const IMapNodePtr&, const TString&)> MissingChildKeyHandler;
    std::function<INodePtr(const IListNodePtr&, int)> MissingChildIndexHandler;
    std::function<INodePtr(const INodePtr&)> NodeCannotHaveChildrenHandler;
};

extern TNodeWalkOptions GetNodeByYPathOptions;
extern TNodeWalkOptions FindNodeByYPathOptions;
extern TNodeWalkOptions FindNodeByYPathNoThrowOptions;

//! Generic function walking down the node according to given ypath.
INodePtr WalkNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    const TNodeWalkOptions& options);

/*!
 *  Throws exception if the specified node does not exist.
 */
INodePtr GetNodeByYPath(
    const INodePtr& root,
    const TYPath& path);

/*!
 *  Does not throw exception if the specified node does not exist, but still throws on attempt of
 *  moving to the child of a non-composite node.
 */
INodePtr FindNodeByYPath(
    const INodePtr& root,
    const TYPath& path);

/*!
 *  A version of #FindNodeByYPath that never throws.
 */
INodePtr FindNodeByYPathNoThrow(
    const INodePtr& root,
    const TYPath& path);

void SetNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    const INodePtr& value,
    bool force = false);

/*!
 *  Returns |false| if the specified node does not exists.
 */
bool RemoveNodeByYPath(
    const INodePtr& root,
    const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define YPATH_CLIENT_INL_H_
#include "ypath_client-inl.h"
#undef YPATH_CLIENT_INL_H_
