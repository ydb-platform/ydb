#include "ypath_client.h"
#include "helpers.h"
#include "exception_helpers.h"
#include "ypath_detail.h"
#include "ypath_proxy.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>
#include <yt/yt/core/rpc/server_detail.h>

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/format.h>
#include <yt/yt/core/yson/tokenizer.h>

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

#include <library/cpp/yt/misc/variant.h>

#include <cmath>

namespace NYT::NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const TRequestHeader& header)
    : Header_(header)
{ }

TYPathRequest::TYPathRequest(
    std::string service,
    std::string method,
    TYPath path,
    bool mutating)
{
    ToProto(Header_.mutable_service(), std::move(service));
    ToProto(Header_.mutable_method(), std::move(method));

    auto* ypathExt = Header_.MutableExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    ypathExt->set_mutating(mutating);
    ypathExt->set_target_path(std::move(path));
}

TRequestId TYPathRequest::GetRequestId() const
{
    return NullRequestId;
}

TRealmId TYPathRequest::GetRealmId() const
{
    return NullRealmId;
}

std::string TYPathRequest::GetMethod() const
{
    return FromProto<std::string>(Header_.method());
}

std::string TYPathRequest::GetService() const
{
    return FromProto<std::string>(Header_.service());
}

void TYPathRequest::DeclareClientFeature(int featureId)
{
    Header_.add_declared_client_feature_ids(featureId);
}

void TYPathRequest::RequireServerFeature(int featureId)
{
    Header_.add_required_server_feature_ids(featureId);
}

const TString& TYPathRequest::GetUser() const
{
    YT_ABORT();
}

void TYPathRequest::SetUser(const TString& /*user*/)
{
    YT_ABORT();
}

const TString& TYPathRequest::GetUserTag() const
{
    YT_ABORT();
}

void TYPathRequest::SetUserTag(const TString& /*tag*/)
{
    YT_ABORT();
}

void TYPathRequest::SetUserAgent(const TString& /*userAgent*/)
{
    YT_ABORT();
}

bool TYPathRequest::GetRetry() const
{
    return Header_.retry();
}

void TYPathRequest::SetRetry(bool value)
{
    Header_.set_retry(value);
}

TMutationId TYPathRequest::GetMutationId() const
{
    return NRpc::GetMutationId(Header_);
}

void TYPathRequest::SetMutationId(TMutationId id)
{
    if (id) {
        ToProto(Header_.mutable_mutation_id(), id);
    } else {
        Header_.clear_mutation_id();
    }
}

size_t TYPathRequest::GetHash() const
{
    return 0;
}

const NRpc::NProto::TRequestHeader& TYPathRequest::Header() const
{
    return Header_;
}

NRpc::NProto::TRequestHeader& TYPathRequest::Header()
{
    return Header_;
}

bool TYPathRequest::IsStreamingEnabled() const
{
    return false;
}

const NRpc::TStreamingParameters& TYPathRequest::ClientAttachmentsStreamingParameters() const
{
    YT_ABORT();
}

NRpc::TStreamingParameters& TYPathRequest::ClientAttachmentsStreamingParameters()
{
    YT_ABORT();
}

const NRpc::TStreamingParameters& TYPathRequest::ServerAttachmentsStreamingParameters() const
{
    YT_ABORT();
}

NRpc::TStreamingParameters& TYPathRequest::ServerAttachmentsStreamingParameters()
{
    YT_ABORT();
}

NConcurrency::IAsyncZeroCopyOutputStreamPtr TYPathRequest::GetRequestAttachmentsStream() const
{
    YT_ABORT();
}

NConcurrency::IAsyncZeroCopyInputStreamPtr TYPathRequest::GetResponseAttachmentsStream() const
{
    YT_ABORT();
}

bool TYPathRequest::IsLegacyRpcCodecsEnabled()
{
    YT_ABORT();
}

TSharedRefArray TYPathRequest::Serialize()
{
    auto bodyData = SerializeBody();
    return CreateRequestMessage(
        Header_,
        std::move(bodyData),
        Attachments_);
}

////////////////////////////////////////////////////////////////////////////////

void TYPathResponse::Deserialize(const TSharedRefArray& message)
{
    YT_ASSERT(message);

    NRpc::NProto::TResponseHeader header;
    if (!TryParseResponseHeader(message, &header)) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Error parsing response header");
    }

    // Check for error in header.
    if (header.has_error()) {
        FromProto<TError>(header.error())
            .ThrowOnError();
    }

    // Deserialize body.
    if (message.Size() < 2) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Too few response message parts: %v < 2",
            message.Size());
    }

    // COMPAT(danilalexeev): legacy RPC codecs
    auto codecId = header.has_codec()
        ? std::make_optional(CheckedEnumCast<NCompression::ECodec>(header.codec()))
        : std::nullopt;

    if (!TryDeserializeBody(message[1], codecId)) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Error deserializing response body");
    }

    // Load attachments.
    Attachments_ = std::vector<TSharedRef>(message.Begin() + 2, message.End());
}

bool TYPathResponse::TryDeserializeBody(TRef /*data*/, std::optional<NCompression::ECodec> /*codecId*/)
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_USE_VANILLA_PROTOBUF

TYPath GetRequestTargetYPath(const NRpc::NProto::TRequestHeader& header)
{
    const auto& ypathExt = header.GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    return FromProto<TYPath>(ypathExt.target_path());
}

TYPath GetOriginalRequestTargetYPath(const NRpc::NProto::TRequestHeader& header)
{
    const auto& ypathExt = header.GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    return ypathExt.has_original_target_path()
        ? FromProto<TYPath>(ypathExt.original_target_path())
        : FromProto<TYPath>(ypathExt.target_path());
}

#else

const TYPath& GetRequestTargetYPath(const NRpc::NProto::TRequestHeader& header)
{
    const auto& ypathExt = header.GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    return ypathExt.target_path();
}

const TYPath& GetOriginalRequestTargetYPath(const NRpc::NProto::TRequestHeader& header)
{
    const auto& ypathExt = header.GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    return ypathExt.has_original_target_path() ? ypathExt.original_target_path() : ypathExt.target_path();
}

#endif

void SetRequestTargetYPath(NRpc::NProto::TRequestHeader* header, TYPath path)
{
    auto* ypathExt = header->MutableExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    ypathExt->set_target_path(std::move(path));
}

bool IsRequestMutating(const NRpc::NProto::TRequestHeader& header)
{
    const auto& ext = header.GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    return ext.mutating();
}

void ResolveYPath(
    const IYPathServicePtr& rootService,
    const IYPathServiceContextPtr& context,
    IYPathServicePtr* suffixService,
    TYPath* suffixPath)
{
    YT_ASSERT(rootService);
    YT_ASSERT(suffixService);
    YT_ASSERT(suffixPath);

    auto currentService = rootService;

    const auto& originalPath = GetOriginalRequestTargetYPath(context->RequestHeader());
    auto currentPath = GetRequestTargetYPath(context->RequestHeader());

    int iteration = 0;
    while (true) {
        ValidateYPathResolutionDepth(originalPath, ++iteration);

        try {
            auto result = currentService->Resolve(currentPath, context);
            auto mustBreak = false;
            Visit(std::move(result),
                [&] (IYPathService::TResolveResultHere&& hereResult) {
                    *suffixService = std::move(currentService);
                    *suffixPath = std::move(hereResult.Path);
                    mustBreak = true;
                },
                [&] (IYPathService::TResolveResultThere&& thereResult) {
                    currentService = std::move(thereResult.Service);
                    currentPath = std::move(thereResult.Path);
                });

            if (mustBreak) {
                break;
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "Error resolving path %v",
                originalPath)
                << TErrorAttribute("method", context->GetMethod())
                << ex;
        }
    }
}

TFuture<TSharedRefArray> ExecuteVerb(
    const IYPathServicePtr& service,
    const TSharedRefArray& requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    IYPathServicePtr suffixService;
    TYPath suffixPath;
    try {
        auto resolveContext = CreateYPathContext(
            requestMessage,
            logger,
            logLevel);
        ResolveYPath(
            service,
            resolveContext,
            &suffixService,
            &suffixPath);
    } catch (const std::exception& ex) {
        return MakeFuture(CreateErrorResponseMessage(ex));
    }

    NRpc::NProto::TRequestHeader requestHeader;
    YT_VERIFY(ParseRequestHeader(requestMessage, &requestHeader));
    SetRequestTargetYPath(&requestHeader, suffixPath);

    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    auto invokeContext = CreateYPathContext(
        std::move(updatedRequestMessage),
        std::move(logger),
        logLevel);

    // NB: Calling GetAsyncResponseMessage after Invoke is not allowed.
    auto asyncResponseMessage = invokeContext->GetAsyncResponseMessage();

    // This should never throw.
    suffixService->Invoke(invokeContext);

    return asyncResponseMessage;
}

void ExecuteVerb(
    const IYPathServicePtr& service,
    const IYPathServiceContextPtr& context)
{
    IYPathServicePtr suffixService;
    TYPath suffixPath;
    try {
        ResolveYPath(
            service,
            context,
            &suffixService,
            &suffixPath);
    } catch (const std::exception& ex) {
        // NB: resolve failure may be caused by body serialization error.
        // In this case error is already set into context.
        if (!context->IsReplied()) {
            context->Reply(ex);
        }
        return;
    }

    auto requestMessage = context->GetRequestMessage();
    auto requestHeader = std::make_unique<NRpc::NProto::TRequestHeader>();
    YT_VERIFY(ParseRequestHeader(requestMessage, requestHeader.get()));
    SetRequestTargetYPath(requestHeader.get(), suffixPath);
    context->SetRequestHeader(std::move(requestHeader));

    // This should never throw.
    suffixService->Invoke(context);
}

TFuture<TYsonString> AsyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter)
{
    auto request = TYPathProxy::Get(path);
    if (attributeFilter) {
        ToProto(request->mutable_attributes(), attributeFilter);
    }
    return ExecuteVerb(service, request)
        .Apply(BIND([] (TYPathProxy::TRspGetPtr response) {
            return TYsonString(response->value());
        }));
}

TString SyncYPathGetKey(const IYPathServicePtr& service, const TYPath& path)
{
    auto request = TYPathProxy::GetKey(path);
    auto future = ExecuteVerb(service, request);
    auto optionalResult = future.TryGetUnique();
    YT_VERIFY(optionalResult);
    return optionalResult->ValueOrThrow()->value();
}

TYsonString SyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter)
{
    auto future = AsyncYPathGet(service, path, attributeFilter);
    auto optionalResult = future.TryGetUnique();
    YT_VERIFY(optionalResult);
    return optionalResult->ValueOrThrow();
}

TFuture<bool> AsyncYPathExists(
    const IYPathServicePtr& service,
    const TYPath& path)
{
    auto request = TYPathProxy::Exists(path);
    return ExecuteVerb(service, request)
        .Apply(BIND([] (TYPathProxy::TRspExistsPtr response) {
            return response->value();
        }));
}

bool SyncYPathExists(
    const IYPathServicePtr& service,
    const TYPath& path)
{
    auto future = AsyncYPathExists(service, path);
    auto optionalResult = future.TryGetUnique();
    YT_VERIFY(optionalResult);
    return optionalResult->ValueOrThrow();
}

TFuture<void> AsyncYPathSet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TYsonString& value,
    bool recursive)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value.ToString());
    request->set_recursive(recursive);
    return ExecuteVerb(service, request).AsVoid();
}

void SyncYPathSet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TYsonString& value,
    bool recursive)
{
    auto future = AsyncYPathSet(service, path, value, recursive);
    auto optionalResult = future.TryGetUnique();
    YT_VERIFY(optionalResult);
    optionalResult->ThrowOnError();
}

TFuture<void> AsyncYPathRemove(
    const IYPathServicePtr& service,
    const TYPath& path,
    bool recursive,
    bool force)
{
    auto request = TYPathProxy::Remove(path);
    request->set_recursive(recursive);
    request->set_force(force);
    return ExecuteVerb(service, request).AsVoid();
}

void SyncYPathRemove(
    const IYPathServicePtr& service,
    const TYPath& path,
    bool recursive,
    bool force)
{
    auto future = AsyncYPathRemove(service, path, recursive, force);
    auto optionalResult = future.TryGetUnique();
    YT_VERIFY(optionalResult);
    optionalResult->ThrowOnError();
}

std::vector<TString> SyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    std::optional<i64> limit)
{
    auto future = AsyncYPathList(service, path, limit);
    auto optionalResult = future.TryGetUnique();
    YT_VERIFY(optionalResult);
    return optionalResult->ValueOrThrow();
}

TFuture<std::vector<TString>> AsyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    std::optional<i64> limit)
{
    auto request = TYPathProxy::List(path);
    if (limit) {
        request->set_limit(*limit);
    }
    return ExecuteVerb(service, request)
        .Apply(BIND([] (TYPathProxy::TRspListPtr response) {
            return ConvertTo<std::vector<TString>>(TYsonString(response->value()));
        }));
}

INodePtr WalkNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    const TNodeWalkOptions& options)
{
    auto currentNode = root;
    NYPath::TTokenizer tokenizer(path);
    while (true) {
        tokenizer.Skip(NYPath::ETokenType::Ampersand);
        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            break;
        }
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        if (tokenizer.GetType() == NYPath::ETokenType::At) {
            tokenizer.Advance();
            if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                return currentNode->Attributes().ToMap();
            } else {
                tokenizer.Expect(NYPath::ETokenType::Literal);
                const auto key = tokenizer.GetLiteralValue();
                const auto& attributes = currentNode->Attributes();
                currentNode = attributes.Find<INodePtr>(key);
                if (!currentNode) {
                    return options.MissingAttributeHandler(key);
                }
            }
        } else {
            tokenizer.Expect(NYPath::ETokenType::Literal);
            switch (currentNode->GetType()) {
                case ENodeType::Map: {
                    auto currentMap = currentNode->AsMap();
                    auto key = tokenizer.GetLiteralValue();
                    currentNode = currentMap->FindChild(key);
                    if (!currentNode) {
                        return options.MissingChildKeyHandler(currentMap, key);
                    }
                    break;
                }
                case ENodeType::List: {
                    auto currentList = currentNode->AsList();
                    const auto& token = tokenizer.GetToken();
                    int index = ParseListIndex(token);
                    auto optionalAdjustedIndex = TryAdjustListIndex(index, currentList->GetChildCount());
                    currentNode = optionalAdjustedIndex ? currentList->FindChild(*optionalAdjustedIndex) : nullptr;
                    if (!currentNode) {
                        return options.MissingChildIndexHandler(currentList, optionalAdjustedIndex.value_or(index));
                    }
                    break;
                }
                default:
                    return options.NodeCannotHaveChildrenHandler(currentNode);
            }
        }
    }
    return currentNode;
}

void SetNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    const INodePtr& value,
    bool force)
{
    auto currentNode = root;

    NYPath::TTokenizer tokenizer(path);

    TString currentToken;
    TString currentLiteralValue;
    auto nextSegment = [&] () {
        tokenizer.Skip(NYPath::ETokenType::Ampersand);
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        currentToken = TString(tokenizer.GetToken());
        currentLiteralValue = tokenizer.GetLiteralValue();
    };

    tokenizer.Advance();
    nextSegment();

    auto factory = root->CreateFactory();
    while (tokenizer.Advance() != NYPath::ETokenType::EndOfStream) {
        switch (currentNode->GetType()) {
            case ENodeType::Map: {
                const auto& key = currentLiteralValue;
                auto currentMap = currentNode->AsMap();
                if (force) {
                    auto child = currentMap->FindChild(key);
                    if (!child) {
                        child = factory->CreateMap();
                        YT_VERIFY(currentMap->AddChild(key, child));
                    }
                    currentNode = child;
                } else {
                    currentNode = currentMap->GetChildOrThrow(key);
                }
                break;
            }

            case ENodeType::List: {
                auto currentList = currentNode->AsList();
                int index = ParseListIndex(currentToken);
                int adjustedIndex = currentList->AdjustChildIndexOrThrow(index);
                currentNode = currentList->GetChildOrThrow(adjustedIndex);
                break;
            }

            default:
                ThrowCannotHaveChildren(currentNode);
                YT_ABORT();
        }
        nextSegment();
    }

    // Set value.
    switch (currentNode->GetType()) {
        case ENodeType::Map: {
            const auto& key = currentLiteralValue;
            auto currentMap = currentNode->AsMap();
            auto child = currentMap->FindChild(key);
            if (child) {
                currentMap->ReplaceChild(child, value);
            } else {
                YT_VERIFY(currentMap->AddChild(key, value));
            }
            break;
        }

        case ENodeType::List: {
            auto currentList = currentNode->AsList();
            int index = ParseListIndex(currentToken);
            int adjustedIndex = currentList->AdjustChildIndexOrThrow(index);
            auto child = currentList->GetChildOrThrow(adjustedIndex);
            currentList->ReplaceChild(child, value);
            break;
        }

        default:
            ThrowCannotHaveChildren(currentNode);
            YT_ABORT();
    }

    factory->Commit();
}

bool RemoveNodeByYPath(
    const INodePtr& root,
    const TYPath& path)
{
    auto node = WalkNodeByYPath(root, path, FindNodeByYPathOptions);
    if (!node) {
        return false;
    }

    node->GetParent()->RemoveChild(node);

    return true;
}

void ForceYPath(
    const INodePtr& root,
    const TYPath& path)
{
    auto currentNode = root;

    NYPath::TTokenizer tokenizer(path);

    TString currentToken;
    TString currentLiteralValue;
    auto nextSegment = [&] () {
        tokenizer.Skip(NYPath::ETokenType::Ampersand);
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        currentToken = TString(tokenizer.GetToken());
        currentLiteralValue = tokenizer.GetLiteralValue();
    };

    tokenizer.Advance();
    nextSegment();

    auto factory = root->CreateFactory();

    while (tokenizer.Advance() != NYPath::ETokenType::EndOfStream) {
        INodePtr child;
        switch (currentNode->GetType()) {
            case ENodeType::Map: {
                auto currentMap = currentNode->AsMap();
                const auto& key = currentLiteralValue;
                child = currentMap->FindChild(key);
                if (!child) {
                    child = factory->CreateMap();
                    YT_VERIFY(currentMap->AddChild(key, child));
                }
                break;
            }

            case ENodeType::List: {
                auto currentList = currentNode->AsList();
                int index = ParseListIndex(currentToken);
                int adjustedIndex = currentList->AdjustChildIndexOrThrow(index);
                child = currentList->GetChildOrThrow(adjustedIndex);
                break;
            }

            default:
                ThrowCannotHaveChildren(currentNode);
                YT_ABORT();
        }

        nextSegment();
        currentNode = child;
    }

    factory->Commit();
}

INodePtr CloneNode(const INodePtr& node)
{
    return ConvertToNode(node);
}

INodePtr PatchNode(const INodePtr& base, const INodePtr& patch)
{
    if (base->GetType() == ENodeType::Map && patch->GetType() == ENodeType::Map) {
        auto result = CloneNode(base);
        auto resultMap = result->AsMap();
        auto patchMap = patch->AsMap();
        auto baseMap = base->AsMap();
        for (const auto& key : patchMap->GetKeys()) {
            if (baseMap->FindChild(key)) {
                resultMap->RemoveChild(key);
                YT_VERIFY(resultMap->AddChild(key, PatchNode(baseMap->GetChildOrThrow(key), patchMap->GetChildOrThrow(key))));
            } else {
                YT_VERIFY(resultMap->AddChild(key, CloneNode(patchMap->GetChildOrThrow(key))));
            }
        }
        result->MutableAttributes()->MergeFrom(patch->Attributes());
        return result;
    } else {
        auto result = CloneNode(patch);
        auto* resultAttributes = result->MutableAttributes();
        resultAttributes->Clear();
        if (base->GetType() == patch->GetType()) {
            resultAttributes->MergeFrom(base->Attributes());
        }
        resultAttributes->MergeFrom(patch->Attributes());
        return result;
    }
}

bool AreNodesEqual(
    const INodePtr& lhs,
    const INodePtr& rhs,
    const TNodesEqualityOptions& options)
{
    if (!lhs && !rhs) {
        return true;
    }

    if (!lhs || !rhs) {
        return false;
    }

    // Check types.
    auto lhsType = lhs->GetType();
    auto rhsType = rhs->GetType();
    if (lhsType != rhsType) {
        return false;
    }

    // Check attributes.
    const auto& lhsAttributes = lhs->Attributes();
    const auto& rhsAttributes = rhs->Attributes();
    if (lhsAttributes != rhsAttributes) {
        return false;
    }

    // Check content.
    switch (lhsType) {
        case ENodeType::Map: {
            auto lhsMap = lhs->AsMap();
            auto rhsMap = rhs->AsMap();

            auto lhsKeys = lhsMap->GetKeys();
            auto rhsKeys = rhsMap->GetKeys();

            if (lhsKeys.size() != rhsKeys.size()) {
                return false;
            }

            std::sort(lhsKeys.begin(), lhsKeys.end());
            std::sort(rhsKeys.begin(), rhsKeys.end());

            for (size_t index = 0; index < lhsKeys.size(); ++index) {
                const auto& lhsKey = lhsKeys[index];
                const auto& rhsKey = rhsKeys[index];
                if (lhsKey != rhsKey) {
                    return false;
                }
                if (!AreNodesEqual(lhsMap->FindChild(lhsKey), rhsMap->FindChild(rhsKey), options)) {
                    return false;
                }
            }

            return true;
        }

        case ENodeType::List: {
            auto lhsList = lhs->AsList();
            auto lhsChildren = lhsList->GetChildren();

            auto rhsList = rhs->AsList();
            auto rhsChildren = rhsList->GetChildren();

            if (lhsChildren.size() != rhsChildren.size()) {
                return false;
            }

            for (size_t index = 0; index < lhsChildren.size(); ++index) {
                if (!AreNodesEqual(lhsList->FindChild(index), rhsList->FindChild(index), options)) {
                    return false;
                }
            }

            return true;
        }

        case ENodeType::Int64:
            return lhs->AsInt64()->GetValue() == rhs->AsInt64()->GetValue();

        case ENodeType::Uint64:
            return lhs->AsUint64()->GetValue() == rhs->AsUint64()->GetValue();

        case ENodeType::Double:
            return std::abs(lhs->AsDouble()->GetValue() - rhs->AsDouble()->GetValue()) <= options.DoubleTypePrecision;

        case ENodeType::Boolean:
            return lhs->AsBoolean()->GetValue() == rhs->AsBoolean()->GetValue();

        case ENodeType::String:
            return lhs->AsString()->GetValue() == rhs->AsString()->GetValue();

        case ENodeType::Entity:
            return true;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TNodeWalkOptions GetNodeByYPathOptions {
    .MissingAttributeHandler = [] (const TString& key) {
        ThrowNoSuchAttribute(key);
        return nullptr;
    },
    .MissingChildKeyHandler = [] (const IMapNodePtr& node, const TString& key) {
        ThrowNoSuchChildKey(node, key);
        return nullptr;
    },
    .MissingChildIndexHandler = [] (const IListNodePtr& node, int index) {
        ThrowNoSuchChildIndex(node, index);
        return nullptr;
    },
    .NodeCannotHaveChildrenHandler = [] (const INodePtr& node) {
        ThrowCannotHaveChildren(node);
        return nullptr;
    }
};

TNodeWalkOptions FindNodeByYPathOptions {
    .MissingAttributeHandler = [] (const TString& /*key*/) {
        return nullptr;
    },
    .MissingChildKeyHandler = [] (const IMapNodePtr& /*node*/, const TString& /*key*/) {
        return nullptr;
    },
    .MissingChildIndexHandler = [] (const IListNodePtr& /*node*/, int /*index*/) {
        return nullptr;
    },
    .NodeCannotHaveChildrenHandler = GetNodeByYPathOptions.NodeCannotHaveChildrenHandler
};

TNodeWalkOptions FindNodeByYPathNoThrowOptions {
    .MissingAttributeHandler = [] (const TString& /*key*/) {
        return nullptr;
    },
    .MissingChildKeyHandler = [] (const IMapNodePtr& /*node*/, const TString& /*key*/) {
        return nullptr;
    },
    .MissingChildIndexHandler = [] (const IListNodePtr& /*node*/, int /*index*/) {
        return nullptr;
    },
    .NodeCannotHaveChildrenHandler = [] (const INodePtr& /*node*/) {
        return nullptr;
    },
};

INodePtr GetNodeByYPath(
    const INodePtr& root,
    const TYPath& path)
{
    return WalkNodeByYPath(root, path, GetNodeByYPathOptions);
}

INodePtr FindNodeByYPath(
    const INodePtr& root,
    const TYPath& path)
{
    return WalkNodeByYPath(root, path, FindNodeByYPathOptions);
}

INodePtr FindNodeByYPathNoThrow(
    const INodePtr& root,
    const TYPath& path)
{
    return WalkNodeByYPath(root, path, FindNodeByYPathNoThrowOptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
