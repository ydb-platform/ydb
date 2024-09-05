#include "ypath_detail.h"

#include "node_detail.h"
#include "helpers.h"
#include "request_complexity_limiter.h"
#include "system_attribute_provider.h"
#include "ypath_client.h"

#include <yt/yt/core/yson/attribute_consumer.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>
#include <yt/yt/core/rpc/server_detail.h>
#include <yt/yt/core/rpc/message.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NYTree {

using namespace NRpc;
using namespace NYPath;
using namespace NRpc::NProto;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto NoneYsonFuture = MakeFuture(TYsonString());

////////////////////////////////////////////////////////////////////////////////

TYPathServiceContextWrapper::TYPathServiceContextWrapper(IYPathServiceContextPtr underlyingContext)
    : TServiceContextWrapper(underlyingContext)
    , UnderlyingContext_(std::move(underlyingContext))
{ }

void TYPathServiceContextWrapper::SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header)
{
    UnderlyingContext_->SetRequestHeader(std::move(header));
}

void TYPathServiceContextWrapper::SetReadRequestComplexityLimiter(const TReadRequestComplexityLimiterPtr& limiter)
{
    UnderlyingContext_->SetReadRequestComplexityLimiter(limiter);
}

TReadRequestComplexityLimiterPtr TYPathServiceContextWrapper::GetReadRequestComplexityLimiter()
{
    return UnderlyingContext_->GetReadRequestComplexityLimiter();
}

const IYPathServiceContextPtr& TYPathServiceContextWrapper::GetUnderlyingContext() const
{
    return UnderlyingContext_;
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TYPathServiceBase::Resolve(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Skip(NYPath::ETokenType::Ampersand);
    if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
        return ResolveSelf(TYPath(tokenizer.GetSuffix()), context);
    }

    tokenizer.Expect(NYPath::ETokenType::Slash);

    if (tokenizer.Advance() == NYPath::ETokenType::At) {
        return ResolveAttributes(TYPath(tokenizer.GetSuffix()), context);
    } else {
        return ResolveRecursive(TYPath(tokenizer.GetInput()), context);
    }
}

IYPathService::TResolveResult TYPathServiceBase::ResolveSelf(
    const TYPath& path,
    const IYPathServiceContextPtr& /*context*/)
{
    return TResolveResultHere{path};
}

IYPathService::TResolveResult TYPathServiceBase::ResolveAttributes(
    const TYPath& /*path*/,
    const IYPathServiceContextPtr& /*context*/)
{
    THROW_ERROR_EXCEPTION("Object cannot have attributes");
}

IYPathService::TResolveResult TYPathServiceBase::ResolveRecursive(
    const TYPath& /*path*/,
    const IYPathServiceContextPtr& /*context*/)
{
    THROW_ERROR_EXCEPTION("Object cannot have children");
}

void TYPathServiceBase::Invoke(const IYPathServiceContextPtr& context)
{
    TError error;
    try {
        BeforeInvoke(context);
        if (!DoInvoke(context)) {
            ThrowMethodNotSupported(context->GetMethod());
        }
    } catch (const std::exception& ex) {
        error = ex;
    }

    AfterInvoke(context);

    if (!error.IsOK()) {
        context->Reply(error);
    }
}

void TYPathServiceBase::BeforeInvoke(const IYPathServiceContextPtr& /*context*/)
{ }

bool TYPathServiceBase::DoInvoke(const IYPathServiceContextPtr& /*context*/)
{
    return false;
}

void TYPathServiceBase::AfterInvoke(const IYPathServiceContextPtr& /*context*/)
{ }

void TYPathServiceBase::DoWriteAttributesFragment(
    NYson::IAsyncYsonConsumer* /*consumer*/,
    const TAttributeFilter& /*attributeFilter*/,
    bool /*stable*/)
{ }

bool TYPathServiceBase::ShouldHideAttributes()
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

IMPLEMENT_SUPPORTS_METHOD(GetKey)
IMPLEMENT_SUPPORTS_METHOD(Get)
IMPLEMENT_SUPPORTS_METHOD(Set)
IMPLEMENT_SUPPORTS_METHOD(List)
IMPLEMENT_SUPPORTS_METHOD(Remove)

IMPLEMENT_SUPPORTS_METHOD_RESOLVE(
    Exists,
    {
        context->SetRequestInfo();
        Reply(context, /*exists*/ false);
    })

void TSupportsExists::ExistsAttribute(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, /*exists*/ false);
}

void TSupportsExists::ExistsSelf(
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, /*exists*/ true);
}

void TSupportsExists::ExistsRecursive(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, /*exists*/ false);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TSupportsMultisetAttributes, Multiset)
{
    context->SetRequestInfo("KeyCount: %v", request->subrequests_size());

    auto ctx = New<TCtxMultisetAttributes>(
        context->GetUnderlyingContext(),
        context->GetOptions());
    ctx->DeserializeRequest();

    auto* req = &ctx->Request();
    auto* rsp = &ctx->Response();
    DoSetAttributes(GetRequestTargetYPath(context->RequestHeader()), req, rsp, ctx);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupportsMultisetAttributes, MultisetAttributes)
{
    context->SetRequestInfo("KeyCount: %v", request->subrequests_size());

    DoSetAttributes(GetRequestTargetYPath(context->RequestHeader()), request, response, context);

    context->Reply();
}

void TSupportsMultisetAttributes::DoSetAttributes(
    const TYPath& path,
    TReqMultisetAttributes* request,
    TRspMultisetAttributes* response,
    const TCtxMultisetAttributesPtr& context)
{
    NYPath::TTokenizer tokenizer(path);

    tokenizer.Advance();
    tokenizer.Skip(NYPath::ETokenType::Ampersand);
    tokenizer.Expect(NYPath::ETokenType::Slash);
    if (tokenizer.Advance() != NYPath::ETokenType::At) {
        tokenizer.ThrowUnexpected();
    }

    SetAttributes(TYPath(tokenizer.GetSuffix()), request, response, context);
}

void TSupportsMultisetAttributes::SetAttributes(
    const TYPath& path,
    TReqMultisetAttributes* request,
    TRspMultisetAttributes* response,
    const TCtxMultisetAttributesPtr& context)
{
    Y_UNUSED(path);
    Y_UNUSED(request);
    Y_UNUSED(response);
    Y_UNUSED(context);
    ThrowMethodNotSupported("MultisetAttributes", TString("attributes"));
}

////////////////////////////////////////////////////////////////////////////////

void TSupportsPermissions::ValidatePermission(
    EPermissionCheckScope /*scope*/,
    EPermission /*permission*/,
    const std::string& /*user*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TSupportsPermissions::TCachingPermissionValidator::TCachingPermissionValidator(
    TSupportsPermissions* owner,
    EPermissionCheckScope scope)
    : Owner_(owner)
    , Scope_(scope)
{ }

void TSupportsPermissions::TCachingPermissionValidator::Validate(EPermission permission, const std::string& user)
{
    auto& validatedPermissions = ValidatedPermissions_[user];
    if (None(validatedPermissions & permission)) {
        Owner_->ValidatePermission(Scope_, permission, user);
        validatedPermissions |= permission;
    }
}

////////////////////////////////////////////////////////////////////////////////

TSupportsAttributes::TCombinedAttributeDictionary::TCombinedAttributeDictionary(TSupportsAttributes* owner)
    : Owner_(owner)
{ }

std::vector<TString> TSupportsAttributes::TCombinedAttributeDictionary::ListKeys() const
{
    std::vector<TString> keys;

    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ReserveAndListSystemAttributes(&descriptors);
        for (const auto& descriptor : descriptors) {
            if (descriptor.Present && !descriptor.Custom && !descriptor.Opaque) {
                keys.push_back(descriptor.InternedKey.Unintern());
            }
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (customAttributes) {
        auto customKeys = customAttributes->ListKeys();
        for (auto&& key : customKeys) {
            keys.push_back(std::move(key));
        }
    }
    return keys;
}

std::vector<IAttributeDictionary::TKeyValuePair> TSupportsAttributes::TCombinedAttributeDictionary::ListPairs() const
{
    std::vector<TKeyValuePair> pairs;

    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ReserveAndListSystemAttributes(&descriptors);
        for (const auto& descriptor : descriptors) {
            if (descriptor.Present && !descriptor.Custom && !descriptor.Opaque) {
                auto value = provider->FindBuiltinAttribute(descriptor.InternedKey);
                if (value) {
                    auto key = descriptor.InternedKey.Unintern();
                    pairs.push_back(std::pair(std::move(key), std::move(value)));
                }
            }
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (customAttributes) {
        for (const auto& pair : customAttributes->ListPairs()) {
            pairs.push_back(pair);
        }
    }

    return pairs;
}

TYsonString TSupportsAttributes::TCombinedAttributeDictionary::FindYson(TStringBuf key) const
{
    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        auto internedKey = TInternedAttributeKey::Lookup(key);
        if (internedKey != InvalidInternedAttribute) {
            const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
            if (builtinKeys.find(internedKey) != builtinKeys.end()) {
                return provider->FindBuiltinAttribute(internedKey);
            }
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (!customAttributes) {
        return TYsonString();
    }
    return customAttributes->FindYson(key);
}

void TSupportsAttributes::TCombinedAttributeDictionary::SetYson(const TString& key, const TYsonString& value)
{
    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        auto internedKey = TInternedAttributeKey::Lookup(key);
        if (internedKey != InvalidInternedAttribute) {
            const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
            if (builtinKeys.find(internedKey) != builtinKeys.end()) {
                if (!provider->SetBuiltinAttribute(internedKey, value, false)) {
                    ThrowCannotSetBuiltinAttribute(key);
                }
                return;
            }
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (!customAttributes) {
        ThrowNoSuchBuiltinAttribute(key);
    }
    customAttributes->SetYson(key, value);
}

bool TSupportsAttributes::TCombinedAttributeDictionary::Remove(const TString& key)
{
    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        auto internedKey = TInternedAttributeKey::Lookup(key);
        if (internedKey != InvalidInternedAttribute) {
            const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
            if (builtinKeys.find(internedKey) != builtinKeys.end()) {
                return provider->RemoveBuiltinAttribute(internedKey);
            }
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (!customAttributes) {
        ThrowNoSuchBuiltinAttribute(key);
    }
    return customAttributes->Remove(key);
}

////////////////////////////////////////////////////////////////////////////////

TSupportsAttributes::TSupportsAttributes()
    : CombinedAttributes_(New<TSupportsAttributes::TCombinedAttributeDictionary>(this))
{ }

IYPathService::TResolveResult TSupportsAttributes::ResolveAttributes(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
{
    const auto& method = context->GetMethod();
    if (method != "Get" &&
        method != "Set" &&
        method != "List" &&
        method != "Remove" &&
        method != "Exists" &&
        method != "Multiset" &&
        method != "MultisetAttributes")
    {
        ThrowMethodNotSupported(method);
    }

    return TResolveResultHere{"/@" + path};
}

TFuture<TYsonString> TSupportsAttributes::DoFindAttribute(TStringBuf key)
{
    auto* customAttributes = GetCustomAttributes();
    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    if (customAttributes) {
        auto attribute = customAttributes->FindYson(key);
        if (attribute) {
            return MakeFuture(attribute);
        }
    }

    if (builtinAttributeProvider) {
        auto internedKey = TInternedAttributeKey::Lookup(key);
        if (internedKey != InvalidInternedAttribute) {
            if (auto builtinYson = builtinAttributeProvider->FindBuiltinAttribute(internedKey)) {
                return MakeFuture(builtinYson);
            }
        }

        auto asyncResult = builtinAttributeProvider->GetBuiltinAttributeAsync(internedKey);
        if (asyncResult) {
            return asyncResult;
        }
    }

    return std::nullopt;
}

TYsonString TSupportsAttributes::DoGetAttributeFragment(
    TStringBuf key,
    const TYPath& path,
    const TYsonString& wholeYson)
{
    if (!wholeYson) {
        ThrowNoSuchAttribute(key);
    }
    if (path.empty()) {
        return wholeYson;
    }
    auto node = ConvertToNode<TYsonString>(wholeYson);
    return SyncYPathGet(node, path, TAttributeFilter());
}

TFuture<TYsonString> TSupportsAttributes::DoGetAttribute(
    const TYPath& path,
    const TAttributeFilter& attributeFilter)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        TAsyncYsonWriter writer;

        writer.OnBeginMap();

        if (attributeFilter) {
            WriteAttributesFragment(&writer, attributeFilter, /*stable*/ false);
        } else {
            if (builtinAttributeProvider) {
                std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinDescriptors;
                builtinAttributeProvider->ListBuiltinAttributes(&builtinDescriptors);
                for (const auto& descriptor : builtinDescriptors) {
                    if (!descriptor.Present)
                        continue;

                    auto key = descriptor.InternedKey.Unintern();
                    TAttributeValueConsumer attributeValueConsumer(&writer, key);

                    if (descriptor.Opaque) {
                        attributeValueConsumer.OnEntity();
                        continue;
                    }

                    if (GuardedGetBuiltinAttribute(descriptor.InternedKey, &attributeValueConsumer)) {
                        continue;
                    }

                    auto asyncValue = builtinAttributeProvider->GetBuiltinAttributeAsync(descriptor.InternedKey);
                    if (asyncValue) {
                        attributeValueConsumer.OnRaw(std::move(asyncValue));
                    }
                }
            }

            auto* customAttributes = GetCustomAttributes();
            if (customAttributes) {
                for (const auto& [key, value] : customAttributes->ListPairs()) {
                    writer.OnKeyedItem(key);
                    Serialize(value, &writer);
                }
            }
        }

        writer.OnEndMap();

        return writer.Finish();
    } else {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            asyncYson = NoneYsonFuture;
        }

        tokenizer.Advance();
        return asyncYson.Apply(BIND(
            &TSupportsAttributes::DoGetAttributeFragment,
            key,
            TYPath(tokenizer.GetInput())));
    }
}

namespace {

void OnAttributeRead(auto* context, auto* response, const TErrorOr<TYsonString>& ysonOrError)
{
    if (!ysonOrError.IsOK()) {
        context->Reply(ysonOrError);
        return;
    }

    auto yson = ysonOrError.Value().ToString();

    if (auto limiter = context->GetReadRequestComplexityLimiter()) {
        limiter->Charge({
            .NodeCount = 1,
            .ResultSize = std::ssize(yson),
        });
        if (auto error = limiter->CheckOverdraught(); !error.IsOK()) {
            context->Reply(error);
            return;
        }
    }

    response->set_value(yson);
    context->Reply();
}

} // namespace

void TSupportsAttributes::GetAttribute(
    const TYPath& path,
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    context->SetRequestInfo();

    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    DoGetAttribute(path, attributeFilter).Subscribe(BIND([=] (const TErrorOr<TYsonString>& ysonOrError) {
        OnAttributeRead(context.Get(), response, ysonOrError);
    }));
}

TYsonString TSupportsAttributes::DoListAttributeFragment(
    TStringBuf key,
    const TYPath& path,
    const TYsonString& wholeYson)
{
    if (!wholeYson) {
        ThrowNoSuchAttribute(key);
    }

    auto node = ConvertToNode(wholeYson);
    auto listedKeys = SyncYPathList(node, path);

    TStringStream stream;
    TBufferedBinaryYsonWriter writer(&stream);
    writer.OnBeginList();
    for (const auto& listedKey : listedKeys) {
        writer.OnListItem();
        writer.OnStringScalar(listedKey);
    }
    writer.OnEndList();
    writer.Flush();

    return TYsonString(stream.Str());
}

TFuture<TYsonString> TSupportsAttributes::DoListAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);

        writer.OnBeginList();

        auto* customAttributes = GetCustomAttributes();
        if (customAttributes) {
            auto userKeys = customAttributes->ListKeys();
            for (const auto& key : userKeys) {
                writer.OnListItem();
                writer.OnStringScalar(key);
            }
        }

        auto* builtinAttributeProvider = GetBuiltinAttributeProvider();
        if (builtinAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinDescriptors;
            builtinAttributeProvider->ListBuiltinAttributes(&builtinDescriptors);
            for (const auto& descriptor : builtinDescriptors) {
                if (descriptor.Present) {
                    writer.OnListItem();
                    writer.OnStringScalar(descriptor.InternedKey.Unintern());
                }
            }
        }

        writer.OnEndList();
        writer.Flush();

        return MakeFuture(TYsonString(stream.Str()));
    } else  {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            asyncYson = NoneYsonFuture;
        }

        tokenizer.Advance();
        return asyncYson.Apply(BIND(
            &TSupportsAttributes::DoListAttributeFragment,
            key,
            TYPath(tokenizer.GetInput())));
    }
}

void TSupportsAttributes::ListAttribute(
    const TYPath& path,
    TReqList* /*request*/,
    TRspList* response,
    const TCtxListPtr& context)
{
    context->SetRequestInfo();

    DoListAttribute(path).Subscribe(BIND([=] (const TErrorOr<TYsonString>& ysonOrError) {
        OnAttributeRead(context.Get(), response, ysonOrError);
    }));
}

bool TSupportsAttributes::DoExistsAttributeFragment(
    TStringBuf /*key*/,
    const TYPath& path,
    const TErrorOr<TYsonString>& wholeYsonOrError)
{
    if (!wholeYsonOrError.IsOK()) {
        return false;
    }
    const auto& wholeYson = wholeYsonOrError.Value();
    if (!wholeYson) {
        return false;
    }
    auto node = ConvertToNode<TYsonString>(wholeYson);
    try {
        return SyncYPathExists(node, path);
    } catch (const std::exception&) {
        return false;
    }
}

TFuture<bool> TSupportsAttributes::DoExistsAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        return TrueFuture;
    }

    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        auto* customAttributes = GetCustomAttributes();
        if (customAttributes && customAttributes->FindYson(key)) {
            return TrueFuture;
        }

        auto* builtinAttributeProvider = GetBuiltinAttributeProvider();
        if (builtinAttributeProvider) {
            auto internedKey = TInternedAttributeKey::Lookup(key);
            if (internedKey != InvalidInternedAttribute) {
                auto optionalDescriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(internedKey);
                if (optionalDescriptor) {
                    const auto& descriptor = *optionalDescriptor;
                    return descriptor.Present ? TrueFuture : FalseFuture;
                }
            }
        }

        return FalseFuture;
    } else {
        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            return FalseFuture;
        }

        return asyncYson.Apply(BIND(
            &TSupportsAttributes::DoExistsAttributeFragment,
            key,
            TYPath(tokenizer.GetInput())));
    }
}

void TSupportsAttributes::ExistsAttribute(
    const TYPath& path,
    TReqExists* /*request*/,
    TRspExists* response,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    DoExistsAttribute(path).Subscribe(BIND([=] (const TErrorOr<bool>& result) {
        if (!result.IsOK()) {
            context->Reply(result);
            return;
        }
        bool exists = result.Value();
        response->set_value(exists);
        context->SetResponseInfo("Result: %v", exists);
        context->Reply();
    }));
}

void TSupportsAttributes::DoSetAttribute(const TYPath& path, const TYsonString& newYson, bool force)
{
    TCachingPermissionValidator permissionValidator(this, EPermissionCheckScope::This);

    auto* customAttributes = GetCustomAttributes();
    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::EndOfStream: {
            auto newAttributes = ConvertToAttributes(newYson);

            std::map<TInternedAttributeKey, ISystemAttributeProvider::TAttributeDescriptor> descriptorMap;
            if (builtinAttributeProvider) {
                builtinAttributeProvider->ListSystemAttributes(&descriptorMap);
            }

            // Set custom attributes.
            if (customAttributes) {

                auto modifyPermission = builtinAttributeProvider
                    ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                    : EPermission::Write;

                auto customKeys = customAttributes->ListKeys();
                std::sort(customKeys.begin(), customKeys.end());
                for (const auto& key : customKeys) {
                    if (!newAttributes->Contains(key)) {
                        permissionValidator.Validate(modifyPermission);

                        YT_VERIFY(customAttributes->Remove(key));
                    }
                }

                auto newPairs = newAttributes->ListPairs();
                std::sort(newPairs.begin(), newPairs.end(), [] (const auto& lhs, const auto& rhs) {
                    return lhs.first < rhs.first;
                });
                for (const auto& [key, value] : newPairs) {
                    auto internedKey = TInternedAttributeKey::Lookup(key);
                    auto it = (internedKey != InvalidInternedAttribute)
                        ? descriptorMap.find(internedKey)
                        : descriptorMap.end();
                    if (it == descriptorMap.end() || it->second.Custom) {
                        permissionValidator.Validate(modifyPermission);

                        customAttributes->SetYson(key, value);

                        YT_VERIFY(newAttributes->Remove(key));
                    }
                }
            }

            // Set builtin attributes.
            if (builtinAttributeProvider) {
                for (const auto& [internedKey, descriptor] : descriptorMap) {
                    const auto& key = internedKey.Unintern();

                    if (descriptor.Custom) {
                        continue;
                    }

                    auto newAttributeYson = newAttributes->FindYson(key);
                    if (newAttributeYson) {
                        if (!descriptor.Writable) {
                            ThrowCannotSetBuiltinAttribute(key);
                        }

                        permissionValidator.Validate(descriptor.ModifyPermission);

                        if (!GuardedSetBuiltinAttribute(internedKey, newAttributeYson, force)) {
                            ThrowCannotSetBuiltinAttribute(key);
                        }

                        YT_VERIFY(newAttributes->Remove(key));
                    } else if (descriptor.Removable) {
                        permissionValidator.Validate(descriptor.ModifyPermission);

                        if (!GuardedRemoveBuiltinAttribute(internedKey)) {
                            ThrowCannotRemoveAttribute(key);
                        }
                    }
                }
            }

            auto remainingNewKeys = newAttributes->ListKeys();
            if (!remainingNewKeys.empty()) {
                ThrowCannotSetBuiltinAttribute(remainingNewKeys[0]);
            }

            break;
        }

        case NYPath::ETokenType::Literal: {
            auto key = tokenizer.GetLiteralValue();
            ValidateAttributeKey(key);
            auto internedKey = TInternedAttributeKey::Lookup(key);

            std::optional<ISystemAttributeProvider::TAttributeDescriptor> descriptor;
            if (builtinAttributeProvider && internedKey != InvalidInternedAttribute) {
                descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(internedKey);
            }

            if (descriptor) {
                if (!descriptor->Writable) {
                    ThrowCannotSetBuiltinAttribute(key);
                }

                permissionValidator.Validate(descriptor->ModifyPermission);

                if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                    if (!GuardedSetBuiltinAttribute(internedKey, newYson, force)) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
                } else {
                    auto oldWholeYson = builtinAttributeProvider->FindBuiltinAttribute(internedKey);
                    if (!oldWholeYson) {
                        ThrowNoSuchBuiltinAttribute(key);
                    }

                    auto oldWholeNode = ConvertToNode(oldWholeYson);
                    SyncYPathSet(oldWholeNode, TYPath(tokenizer.GetInput()), newYson);
                    auto newWholeYson = ConvertToYsonString(oldWholeNode);

                    if (!GuardedSetBuiltinAttribute(internedKey, newWholeYson, force)) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
                }
            } else {
                if (!customAttributes) {
                    THROW_ERROR_EXCEPTION("Custom attributes are not supported");
                }
                auto modifyPermission = builtinAttributeProvider
                    ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                    : EPermission::Write;
                permissionValidator.Validate(modifyPermission);

                if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                    customAttributes->SetYson(key, newYson);
                } else {
                    auto oldWholeYson = customAttributes->FindYson(key);
                    if (!oldWholeYson) {
                        ThrowNoSuchCustomAttribute(key);
                    }

                    auto wholeNode = ConvertToNode(oldWholeYson);
                    SyncYPathSet(wholeNode, TYPath(tokenizer.GetInput()), newYson);
                    auto newWholeYson = ConvertToYsonString(wholeNode);

                    customAttributes->SetYson(key, newWholeYson);
                }
            }

            break;
        }

        default:
            tokenizer.ThrowUnexpected();
    }
}

void TSupportsAttributes::SetAttribute(
    const TYPath& path,
    TReqSet* request,
    TRspSet* /*response*/,
    const TCtxSetPtr& context)
{
    context->SetRequestInfo();

    // Request instances are pooled, and thus are request->values.
    // Check if this pooled string has a small overhead (<= 25%).
    // Otherwise make a deep copy.
    const auto& requestValue = request->value();
    const auto& safeValue = requestValue.capacity() <= requestValue.length() * 5 / 4
        ? requestValue
        : TString(TStringBuf(requestValue));
    DoSetAttribute(path, TYsonString(safeValue), request->force());
    context->Reply();
}

void TSupportsAttributes::DoRemoveAttribute(const TYPath& path, bool force)
{
    TCachingPermissionValidator permissionValidator(this, EPermissionCheckScope::This);

    auto* customAttributes = GetCustomAttributes();
    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Asterisk: {
            if (customAttributes) {
                auto modifyPermission = builtinAttributeProvider
                    ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                    : EPermission::Write;
                permissionValidator.Validate(modifyPermission);

                auto customKeys = customAttributes->ListKeys();
                std::sort(customKeys.begin(), customKeys.end());
                for (const auto& key : customKeys) {
                    YT_VERIFY(customAttributes->Remove(key));
                }
            }
            break;
        }

        case NYPath::ETokenType::Literal: {
            auto key = tokenizer.GetLiteralValue();
            auto internedKey = TInternedAttributeKey::Lookup(key);
            auto customYson = customAttributes ? customAttributes->FindYson(key) : TYsonString();
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                if (customYson) {
                    auto modifyPermission = builtinAttributeProvider
                        ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                        : EPermission::Write;
                    permissionValidator.Validate(modifyPermission);

                    YT_VERIFY(customAttributes->Remove(key));
                } else {
                    if (!builtinAttributeProvider) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchCustomAttribute(key);
                    }

                    auto descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(internedKey);
                    if (!descriptor) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }
                    if (!descriptor->Removable) {
                        ThrowCannotRemoveAttribute(key);
                    }

                    permissionValidator.Validate(descriptor->ModifyPermission);

                    if (!GuardedRemoveBuiltinAttribute(internedKey)) {
                        ThrowNoSuchBuiltinAttribute(key);
                    }
                }
            } else {
                if (customYson) {
                    auto modifyPermission = builtinAttributeProvider
                        ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                        : EPermission::Write;
                    permissionValidator.Validate(modifyPermission);

                    auto customNode = ConvertToNode(customYson);
                    SyncYPathRemove(customNode, TYPath(tokenizer.GetInput()), /*recursive*/ true, force);
                    auto updatedCustomYson = ConvertToYsonString(customNode);

                    customAttributes->SetYson(key, updatedCustomYson);
                } else {
                    if (!builtinAttributeProvider) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }

                    auto descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(internedKey);
                    if (!descriptor) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }

                    if (!descriptor->Writable) {
                        ThrowCannotRemoveAttribute(key);
                    }

                    permissionValidator.Validate(descriptor->ModifyPermission);

                    auto builtinYson = builtinAttributeProvider->FindBuiltinAttribute(internedKey);
                    if (!builtinYson) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }

                    auto builtinNode = ConvertToNode(builtinYson);
                    SyncYPathRemove(builtinNode, TYPath(tokenizer.GetInput()));
                    auto updatedSystemYson = ConvertToYsonString(builtinNode);

                    if (!GuardedSetBuiltinAttribute(internedKey, updatedSystemYson, force)) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
                }
            }
            break;
        }

        default:
            tokenizer.ThrowUnexpected();
            break;
    }
}

void TSupportsAttributes::RemoveAttribute(
    const TYPath& path,
    TReqRemove* request,
    TRspRemove* /*response*/,
    const TCtxRemovePtr& context)
{
    context->SetRequestInfo();

    bool force = request->force();
    DoRemoveAttribute(path, force);
    context->Reply();
}

void TSupportsAttributes::SetAttributes(
    const TYPath& path,
    TReqMultisetAttributes* request,
    TRspMultisetAttributes* /*response*/,
    const TCtxMultisetAttributesPtr& /*context*/)
{
    for (const auto& subrequest : request->subrequests()) {
        const auto& attribute = subrequest.attribute();
        const auto& value = subrequest.value();
        if (attribute.empty()) {
            THROW_ERROR_EXCEPTION("Empty attribute names are not allowed");
        }

        TYPath attributePath;
        if (path.empty()) {
            attributePath = attribute;
        } else {
            attributePath = path + "/" + attribute;
        }

        DoSetAttribute(attributePath, TYsonString(value), request->force());
    }
}

IAttributeDictionary* TSupportsAttributes::GetCombinedAttributes()
{
    return CombinedAttributes_.Get();
}

IAttributeDictionary* TSupportsAttributes::GetCustomAttributes()
{
    return nullptr;
}

ISystemAttributeProvider* TSupportsAttributes::GetBuiltinAttributeProvider()
{
    return nullptr;
}

bool TSupportsAttributes::GuardedGetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    auto* provider = GetBuiltinAttributeProvider();

    try {
        return provider->GetBuiltinAttribute(key, consumer);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error getting builtin attribute %Qv",
            ToYPathLiteral(key.Unintern()))
            << ex;
    }
}

bool TSupportsAttributes::GuardedSetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& yson, bool force)
{
    auto* provider = GetBuiltinAttributeProvider();

    try {
        return provider->SetBuiltinAttribute(key, yson, force);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error setting builtin attribute %Qv",
            ToYPathLiteral(key.Unintern()))
            << ex;
    }
}

bool TSupportsAttributes::GuardedRemoveBuiltinAttribute(TInternedAttributeKey key)
{
    auto* provider = GetBuiltinAttributeProvider();

    try {
        return provider->RemoveBuiltinAttribute(key);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error removing builtin attribute %Qv",
            ToYPathLiteral(key.Unintern()))
            << ex;
    }
}

void TSupportsAttributes::ValidateAttributeKey(TStringBuf key) const
{
    if (key.empty()) {
        THROW_ERROR_EXCEPTION("Attribute key cannot be empty");
    }
}

////////////////////////////////////////////////////////////////////////////////

const THashSet<TInternedAttributeKey>& TSystemBuiltinAttributeKeysCache::GetBuiltinAttributeKeys(
    ISystemAttributeProvider* provider)
{
    if (!Initialized_) {
        auto guard = Guard(InitializationLock_);
        if (Initialized_) {
            return BuiltinKeys_;
        }

        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ListSystemAttributes(&descriptors);
        BuiltinKeys_.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (!descriptor.Custom) {
                YT_VERIFY(BuiltinKeys_.insert(descriptor.InternedKey).second);
            }
        }
        Initialized_ = true;
    }
    return BuiltinKeys_;
}

////////////////////////////////////////////////////////////////////////////////

const THashSet<TString>& TSystemCustomAttributeKeysCache::GetCustomAttributeKeys(
    ISystemAttributeProvider* provider)
{
    if (!Initialized_) {
        auto guard = Guard(InitializationLock_);
        if (Initialized_) {
            return CustomKeys_;
        }

        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ListSystemAttributes(&descriptors);
        CustomKeys_.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (descriptor.Custom) {
                YT_VERIFY(CustomKeys_.insert(descriptor.InternedKey.Unintern()).second);
            }
        }
        Initialized_ = true;
    }
    return CustomKeys_;
}

////////////////////////////////////////////////////////////////////////////////

const THashSet<TString>& TOpaqueAttributeKeysCache::GetOpaqueAttributeKeys(
    ISystemAttributeProvider* provider)
{
    if (!Initialized_) {
        auto guard = Guard(InitializationLock_);
        if (Initialized_) {
            return OpaqueKeys_;
        }

        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ListSystemAttributes(&descriptors);
        OpaqueKeys_.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (descriptor.Opaque) {
                YT_VERIFY(OpaqueKeys_.insert(descriptor.InternedKey.Unintern()).second);
            }
        }
        Initialized_ = true;
    }
    return OpaqueKeys_;
}

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase
    : public TTypedConsumer
{
public:
    void Commit();

protected:
    TNodeSetterBase(INode* node, ITreeBuilder* builder);
    ~TNodeSetterBase();

    void OnMyBeginAttributes() override;
    void OnMyEndAttributes() override;

protected:
    class TAttributesSetter;

    INode* const Node_;
    ITreeBuilder* const TreeBuilder_;

    const std::unique_ptr<ITransactionalNodeFactory> NodeFactory_;

    std::unique_ptr<TAttributesSetter> AttributesSetter_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TNodeSetter
{ };

#define BEGIN_SETTER(name, type) \
    template <> \
    class TNodeSetter<I##name##Node> \
        : public TNodeSetterBase \
    { \
    public: \
        TNodeSetter(I##name##Node* node, ITreeBuilder* builder) \
            : TNodeSetterBase(node, builder) \
            , Node_(node) \
        { } \
    \
    private: \
        I##name##Node* const Node_; \
        \
        virtual ENodeType GetExpectedType() override \
        { \
            return ENodeType::name; \
        }

#define END_SETTER() \
    };

BEGIN_SETTER(String, TString)
    void OnMyStringScalar(TStringBuf value) override
    {
        Node_->SetValue(TString(value));
    }
END_SETTER()

BEGIN_SETTER(Int64, i64)
    void OnMyInt64Scalar(i64 value) override
    {
        Node_->SetValue(value);
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        Node_->SetValue(CheckedIntegralCast<i64>(value));
    }
END_SETTER()

BEGIN_SETTER(Uint64,  ui64)
    void OnMyInt64Scalar(i64 value) override
    {
        Node_->SetValue(CheckedIntegralCast<ui64>(value));
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        Node_->SetValue(value);
    }
END_SETTER()

BEGIN_SETTER(Double, double)
    void OnMyDoubleScalar(double value) override
    {
        Node_->SetValue(value);
    }
END_SETTER()

BEGIN_SETTER(Boolean, bool)
    void OnMyBooleanScalar(bool value) override
    {
        Node_->SetValue(value);
    }
END_SETTER()

#undef BEGIN_SETTER
#undef END_SETTER

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IMapNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IMapNode* map, ITreeBuilder* builder)
        : TNodeSetterBase(map, builder)
        , Map_(map)
    { }

private:
    IMapNode* const Map_;

    ENodeType GetExpectedType() override
    {
        return ENodeType::Map;
    }

    void OnMyBeginMap() override
    {
        Map_->Clear();
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        YT_VERIFY(TreeBuilder_);

        TreeBuilder_->BeginTree();
        Forward(TreeBuilder_, std::bind(&TNodeSetter::OnForwardingFinished, this, TString(key)));
    }

    void OnForwardingFinished(TString itemKey)
    {
        YT_VERIFY(Map_->AddChild(itemKey, TreeBuilder_->EndTree()));
    }

    void OnMyEndMap() override
    {
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IListNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IListNode* list, ITreeBuilder* builder)
        : TNodeSetterBase(list, builder)
        , List_(list)
    { }

private:
    IListNode* const List_;


    ENodeType GetExpectedType() override
    {
        return ENodeType::List;
    }

    void OnMyBeginList() override
    {
        List_->Clear();
    }

    void OnMyListItem() override
    {
        YT_VERIFY(TreeBuilder_);

        TreeBuilder_->BeginTree();
        Forward(TreeBuilder_, [this] {
            List_->AddChild(TreeBuilder_->EndTree());
        });
    }

    void OnMyEndList() override
    {
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IEntityNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IEntityNode* entity, ITreeBuilder* builder)
        : TNodeSetterBase(entity, builder)
    { }

private:
    ENodeType GetExpectedType() override
    {
        return ENodeType::Entity;
    }

    void OnMyEntity() override
    {
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase::TAttributesSetter
    : public TForwardingYsonConsumer
{
public:
    explicit TAttributesSetter(IAttributeDictionary* attributes)
        : Attributes_(attributes)
    { }

private:
    IAttributeDictionary* const Attributes_;

    TStringStream AttributeStream_;
    std::unique_ptr<TBufferedBinaryYsonWriter> AttributeWriter_;


    void OnMyKeyedItem(TStringBuf key) override
    {
        AttributeWriter_.reset(new TBufferedBinaryYsonWriter(&AttributeStream_));
        Forward(
            AttributeWriter_.get(),
            [this, key = TString(key)] {
                AttributeWriter_->Flush();
                AttributeWriter_.reset();
                Attributes_->SetYson(key, TYsonString(AttributeStream_.Str()));
                AttributeStream_.clear();
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

void TTypedConsumer::ThrowInvalidType(ENodeType actualType)
{
    THROW_ERROR_EXCEPTION("Cannot update %Qlv node with %Qlv value; types must match",
        GetExpectedType(),
        actualType);
}

void TTypedConsumer::OnMyStringScalar(TStringBuf /*exists*/)
{
    ThrowInvalidType(ENodeType::String);
}

void TTypedConsumer::OnMyInt64Scalar(i64 /*exists*/)
{
    ThrowInvalidType(ENodeType::Int64);
}

void TTypedConsumer::OnMyUint64Scalar(ui64 /*exists*/)
{
    ThrowInvalidType(ENodeType::Uint64);
}

void TTypedConsumer::OnMyDoubleScalar(double /*exists*/)
{
    ThrowInvalidType(ENodeType::Double);
}

void TTypedConsumer::OnMyBooleanScalar(bool /*exists*/)
{
    ThrowInvalidType(ENodeType::Boolean);
}

void TTypedConsumer::OnMyEntity()
{
    ThrowInvalidType(ENodeType::Entity);
}

void TTypedConsumer::OnMyBeginList()
{
    ThrowInvalidType(ENodeType::List);
}

void TTypedConsumer::OnMyBeginMap()
{
    ThrowInvalidType(ENodeType::Map);
}

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node_(node)
    , TreeBuilder_(builder)
    , NodeFactory_(node->CreateFactory())
{ }

TNodeSetterBase::~TNodeSetterBase() = default;

void TNodeSetterBase::OnMyBeginAttributes()
{
    AttributesSetter_.reset(new TAttributesSetter(Node_->MutableAttributes()));
    Forward(AttributesSetter_.get(), nullptr, EYsonType::MapFragment);
}

void TNodeSetterBase::OnMyEndAttributes()
{
    AttributesSetter_.reset();
}

void TNodeSetterBase::Commit()
{
    NodeFactory_->Commit();
}

////////////////////////////////////////////////////////////////////////////////

void SetNodeFromProducer(
    const INodePtr& node,
    const NYson::TYsonProducer& producer,
    ITreeBuilder* builder)
{
    YT_VERIFY(node);

    switch (node->GetType()) {
        #define XX(type) \
            case ENodeType::type: { \
                TNodeSetter<I##type##Node> setter(node->As##type().Get(), builder); \
                producer.Run(&setter); \
                setter.Commit(); \
                break; \
            }

        XX(String)
        XX(Int64)
        XX(Uint64)
        XX(Double)
        XX(Boolean)
        XX(Map)
        XX(List)
        XX(Entity)

        #undef XX

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceContext
    : public TServiceContextBase
    , public IYPathServiceContext
{
public:
    template <class... TArgs>
    TYPathServiceContext(TArgs&&... args)
        : TServiceContextBase(std::forward<TArgs>(args)...)
    { }

    void SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header) override
    {
        RequestHeader_ = std::move(header);
        RequestMessage_ = NRpc::SetRequestHeader(RequestMessage_, *RequestHeader_);
        CachedYPathExt_ = nullptr;
    }

    void SetReadRequestComplexityLimiter(const TReadRequestComplexityLimiterPtr& limiter) final
    {
        ReadComplexityLimiter_ = limiter;
    }

    TReadRequestComplexityLimiterPtr GetReadRequestComplexityLimiter() final
    {
        return ReadComplexityLimiter_;
    }

protected:
    TReadRequestComplexityLimiterPtr ReadComplexityLimiter_ = nullptr;

    std::optional<NProfiling::TWallTimer> Timer_;
    const NProto::TYPathHeaderExt* CachedYPathExt_ = nullptr;

    const NProto::TYPathHeaderExt& GetYPathExt()
    {
        if (!CachedYPathExt_) {
            CachedYPathExt_ = &RequestHeader_->GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
        }
        return *CachedYPathExt_;
    }

    void DoReply() override
    { }

    void LogRequest() override
    {
        const auto& ypathExt = GetYPathExt();

        TStringBuilder builder;
        builder.AppendFormat("%v.%v %v <- ",
            GetService(),
            GetMethod(),
            ypathExt.target_path());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        auto requestId = GetRequestId();
        if (requestId) {
            delimitedBuilder->AppendFormat("RequestId: %v", requestId);
        }

        delimitedBuilder->AppendFormat("Mutating: %v", ypathExt.mutating());

        auto mutationId = GetMutationId();
        if (mutationId) {
            delimitedBuilder->AppendFormat("MutationId: %v", mutationId);
        }

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        delimitedBuilder->AppendFormat("Retry: %v", IsRetry());

        for (const auto& info : RequestInfos_){
            delimitedBuilder->AppendString(info);
        }

        auto logMessage = builder.Flush();
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag(RequestInfoAnnotation, logMessage);
        });
        YT_LOG_DEBUG(logMessage);

        Timer_.emplace();
    }

    void LogResponse() override
    {
        const auto& ypathExt = GetYPathExt();

        TStringBuilder builder;
        builder.AppendFormat("%v.%v %v -> ",
            GetService(),
            GetMethod(),
            ypathExt.target_path());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        auto requestId = GetRequestId();
        if (requestId) {
            delimitedBuilder->AppendFormat("RequestId: %v", requestId);
        }

        delimitedBuilder->AppendFormat("Mutating: %v", ypathExt.mutating());

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        if (auto limiter = GetReadRequestComplexityLimiter()) {
            auto usage = limiter->GetUsage();
            delimitedBuilder->AppendFormat("ResponseNodeCount: %v, ResponseSize: %v",
                usage.NodeCount,
                usage.ResultSize);
        }

        for (const auto& info : ResponseInfos_) {
            delimitedBuilder->AppendString(info);
        }

        if (Timer_) {
            delimitedBuilder->AppendFormat("WallTime: %v", Timer_->GetElapsedTime());
        }

        delimitedBuilder->AppendFormat("Error: %v", Error_);

        auto logMessage = builder.Flush();
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag(ResponseInfoAnnotation, logMessage);
        });
        YT_LOG_DEBUG(logMessage);
    }
};

IYPathServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    YT_ASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestMessage),
        TMemoryUsageTrackerGuard(),
        GetNullMemoryUsageTracker(),
        std::move(logger),
        logLevel);
}

IYPathServiceContextPtr CreateYPathContext(
    std::unique_ptr<TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    YT_ASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestHeader),
        std::move(requestMessage),
        TMemoryUsageTrackerGuard(),
        GetNullMemoryUsageTracker(),
        std::move(logger),
        logLevel);
}

////////////////////////////////////////////////////////////////////////////////

class TRootService
    : public IYPathService
{
public:
    explicit TRootService(IYPathServicePtr underlyingService)
        : UnderlyingService_(std::move(underlyingService))
    { }

    void Invoke(const IYPathServiceContextPtr& /*context*/) override
    {
        YT_ABORT();
    }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        NYPath::TTokenizer tokenizer(path);
        if (tokenizer.Advance() != NYPath::ETokenType::Slash) {
            THROW_ERROR_EXCEPTION("YPath must start with \"/\"");
        }

        return TResolveResultThere{UnderlyingService_, TYPath(tokenizer.GetSuffix())};
    }

    void DoWriteAttributesFragment(
        IAsyncYsonConsumer* consumer,
        const TAttributeFilter& attributeFilter,
        bool stable) override
    {
        UnderlyingService_->WriteAttributesFragment(consumer, attributeFilter, stable);
    }

    bool ShouldHideAttributes() override
    {
        return false;
    }

private:
    const IYPathServicePtr UnderlyingService_;

};

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService)
{
    return New<TRootService>(std::move(underlyingService));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
