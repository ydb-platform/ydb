#include "virtual.h"
#include "ephemeral_attribute_owner.h"
#include "fluent.h"
#include "node_detail.h"
#include "ypath_client.h"
#include "ypath_detail.h"
#include "ypath_service.h"

#include <yt/yt/core/yson/tokenizer.h>
#include <yt/yt/core/yson/async_writer.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/yson/writer.h>

#include <util/generic/hash.h>

namespace NYT::NYTree {

using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TVirtualMapBase::TVirtualMapBase()
    : TVirtualMapBase(/*owningNode*/ nullptr)
{ }

TVirtualMapBase::TVirtualMapBase(INodePtr owningNode)
    : OwningNode_(std::move(owningNode))
{ }

bool TVirtualMapBase::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    return TSupportsAttributes::DoInvoke(context);
}

IYPathService::TResolveResult TVirtualMapBase::ResolveRecursive(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();
    auto service = FindItemService(key);
    if (!service) {
        const auto& method = context->GetMethod();
        if (method == "Exists" || method == "Remove") {
            return TResolveResultHere{"/" + path};
        }
        // TODO(babenko): improve diagnostics
        ThrowNoSuchChildKey(key);
    }

    return TResolveResultThere{std::move(service), TYPath(tokenizer.GetSuffix())};
}

void TVirtualMapBase::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    YT_ASSERT(!NYson::TTokenizer(GetRequestTargetYPath(context->RequestHeader())).ParseNext());

    auto attributeFilter = request->has_attributes()
        ? NYT::FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    i64 limit = request->has_limit()
        ? request->limit()
        : DefaultVirtualChildLimit;

    context->SetRequestInfo("AttributeFilter: %v, Limit: %v",
        attributeFilter,
        limit);

    auto keys = GetKeys(limit);
    i64 size = GetSize();

    TAsyncYsonWriter writer;

    // NB: we do not want empty attributes (<>) to appear in the result in order to comply
    // with current behaviour for some paths (like //sys/scheduler/orchid/scheduler/operations).
    if (std::ssize(keys) != size || OwningNode_) {
        writer.OnBeginAttributes();
        if (std::ssize(keys) != size) {
            writer.OnKeyedItem("incomplete");
            writer.OnBooleanScalar(true);
        }
        if (OwningNode_) {
            OwningNode_->WriteAttributesFragment(&writer, attributeFilter, false);
        }
        writer.OnEndAttributes();
    }

    writer.OnBeginMap();

    if (attributeFilter) {
        for (const auto& key : keys) {
            auto service = FindItemService(key);
            if (service) {
                writer.OnKeyedItem(key);
                if (Opaque_) {
                    service->WriteAttributes(&writer, attributeFilter, false);
                    writer.OnEntity();
                } else {
                    auto asyncResult = AsyncYPathGet(service, "", attributeFilter);
                    writer.OnRaw(asyncResult);
                }
            }
        }
    } else {
        for (const auto& key : keys) {
            if (Opaque_) {
                writer.OnKeyedItem(key);
                writer.OnEntity();
            } else {
                if (auto service = FindItemService(key)) {
                    writer.OnKeyedItem(key);
                    auto asyncResult = AsyncYPathGet(service, "");
                    writer.OnRaw(asyncResult);
                }
            }
        }
    }
    writer.OnEndMap();

    writer.Finish()
        .Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
            if (resultOrError.IsOK()) {
                response->set_value(resultOrError.Value().ToString());
                context->Reply();
            } else {
                context->Reply(resultOrError);
            }
        }));
}

void TVirtualMapBase::ListSelf(
    TReqList* request,
    TRspList* response,
    const TCtxListPtr& context)
{
    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    i64 limit = request->has_limit()
        ? request->limit()
        : DefaultVirtualChildLimit;

    context->SetRequestInfo("AttributeFilter: %v, Limit: %v",
        attributeFilter,
        limit);

    auto keys = GetKeys(limit);
    i64 size = GetSize();

    TAsyncYsonWriter writer;

    if (std::ssize(keys) != size) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnBooleanScalar(true);
        writer.OnEndAttributes();
    }

    writer.OnBeginList();
    if (attributeFilter) {
        for (const auto& key : keys) {
            auto service = FindItemService(key);
            if (service) {
                writer.OnListItem();
                service->WriteAttributes(&writer, attributeFilter, false);
                writer.OnStringScalar(key);
            }
        }
    } else {
        for (const auto& key : keys) {
            writer.OnListItem();
            writer.OnStringScalar(key);
        }
    }
    writer.OnEndList();

    writer.Finish()
        .Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
            if (resultOrError.IsOK()) {
                response->set_value(resultOrError.Value().ToString());
                context->Reply();
            } else {
                context->Reply(resultOrError);
            }
        }));
}

void TVirtualMapBase::RemoveRecursive(
    const TYPath& path,
    TReqRemove* request,
    TRspRemove* /*response*/,
    const TSupportsRemove::TCtxRemovePtr& context)
{
    context->SetRequestInfo();

    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    if (request->force()) {
        context->Reply();
    } else {
        // TODO(babenko): improve diagnostics
        ThrowNoSuchChildKey(tokenizer.GetLiteralValue());
    }
}

void TVirtualMapBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->push_back(CountInternedAttribute);
}

const THashSet<TInternedAttributeKey>& TVirtualMapBase::GetBuiltinAttributeKeys()
{
    return BuiltinAttributeKeysCache_.GetBuiltinAttributeKeys(this);
}

bool TVirtualMapBase::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    switch (key) {
        case CountInternedAttribute:
            BuildYsonFluently(consumer)
                .Value(GetSize());
            return true;
        default:
            return false;
    }
}

TFuture<TYsonString> TVirtualMapBase::GetBuiltinAttributeAsync(TInternedAttributeKey /*key*/)
{
    return std::nullopt;
}

ISystemAttributeProvider* TVirtualMapBase::GetBuiltinAttributeProvider()
{
    return this;
}

bool TVirtualMapBase::SetBuiltinAttribute(TInternedAttributeKey /*key*/, const TYsonString& /*value*/)
{
    return false;
}

bool TVirtualMapBase::RemoveBuiltinAttribute(TInternedAttributeKey /*key*/)
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

class TCompositeMapService::TImpl
    : public TRefCounted
{
public:
    std::vector<TString> GetKeys(i64 limit) const
    {
        std::vector<TString> keys;
        int index = 0;
        auto it = Services_.begin();
        while (it != Services_.end() && index < limit) {
            keys.push_back(it->first);
            ++it;
            ++index;
        }
        return keys;
    }

    i64 GetSize() const
    {
        return Services_.size();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const
    {
        auto it = Services_.find(key);
        return it != Services_.end() ? it->second : nullptr;
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) const
    {
        for (const auto& it : Attributes_) {
            descriptors->push_back(TAttributeDescriptor(it.first));
        }
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) const
    {
        auto it = Attributes_.find(key);
        if (it != Attributes_.end()) {
            it->second(consumer);
            return true;
        }

        return false;
    }

    void AddChild(const TString& key, IYPathServicePtr service)
    {
        YT_VERIFY(Services_.emplace(key, service).second);
    }

    void AddAttribute(TInternedAttributeKey key, TYsonCallback producer)
    {
        YT_VERIFY(Attributes_.emplace(key, producer).second);
    }

private:
    THashMap<TString, IYPathServicePtr> Services_;
    THashMap<TInternedAttributeKey, TYsonCallback> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

TCompositeMapService::TCompositeMapService()
    : Impl_(New<TImpl>())
{ }

TCompositeMapService::~TCompositeMapService()
{ }

std::vector<TString> TCompositeMapService::GetKeys(i64 limit) const
{
    return Impl_->GetKeys(limit);
}

i64 TCompositeMapService::GetSize() const
{
    return Impl_->GetSize();
}

IYPathServicePtr TCompositeMapService::FindItemService(TStringBuf key) const
{
   return Impl_->FindItemService(key);
}

void TCompositeMapService::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    Impl_->ListSystemAttributes(descriptors);

    TVirtualMapBase::ListSystemAttributes(descriptors);
}

bool TCompositeMapService::GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    if (Impl_->GetBuiltinAttribute(key, consumer)) {
        return true;
    }

    return TVirtualMapBase::GetBuiltinAttribute(key, consumer);
}

TIntrusivePtr<TCompositeMapService> TCompositeMapService::AddChild(const TString& key, IYPathServicePtr service)
{
    Impl_->AddChild(key, std::move(service));
    return this;
}

TIntrusivePtr<TCompositeMapService> TCompositeMapService::AddAttribute(TInternedAttributeKey key, TYsonCallback producer)
{
    Impl_->AddAttribute(key, producer);
    return this;
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualEntityNode
    : public TNodeBase
    , public TSupportsAttributes
    , public IEntityNode
    , public TEphemeralAttributeOwner
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    explicit TVirtualEntityNode(IYPathServicePtr underlyingService)
        : UnderlyingService_(std::move(underlyingService))
    { }

    std::unique_ptr<ITransactionalNodeFactory> CreateFactory() const override
    {
        YT_ASSERT(Parent_);
        return Parent_->CreateFactory();
    }

    ICompositeNodePtr GetParent() const override
    {
        return Parent_;
    }

    void SetParent(const ICompositeNodePtr& parent) override
    {
        Parent_ = parent.Get();
    }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        // TODO(babenko): handle ugly face
        return TResolveResultThere{UnderlyingService_, path};
    }

    void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TAttributeFilter& /*attributeFilter*/,
        bool /*stable*/) override
    { }

    bool ShouldHideAttributes() override
    {
        return UnderlyingService_->ShouldHideAttributes();
    }

private:
    const IYPathServicePtr UnderlyingService_;

    ICompositeNode* Parent_ = nullptr;

    // TSupportsAttributes members

    IAttributeDictionary* GetCustomAttributes() override
    {
        return MutableAttributes();
    }
};

INodePtr CreateVirtualNode(IYPathServicePtr service)
{
    return New<TVirtualEntityNode>(service);
}

////////////////////////////////////////////////////////////////////////////////

bool TVirtualListBase::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    return TSupportsAttributes::DoInvoke(context);
}

IYPathService::TResolveResult TVirtualListBase::ResolveRecursive(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    int index = ParseListIndex(tokenizer.GetToken());
    int originalIndex = index;
    if (index < 0) {
        index += GetSize();
    }
    IYPathServicePtr service = nullptr;
    if (index >= 0 && index < GetSize()) {
        service = FindItemService(index);
    }
    if (!service) {
        if (context->GetMethod() == "Exists") {
            return TResolveResultHere{path};
        }
        THROW_ERROR_EXCEPTION("Node has no child with index %v", originalIndex);
    }

    return TResolveResultThere{std::move(service), TYPath(tokenizer.GetSuffix())};
}

void TVirtualListBase::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    YT_ASSERT(!NYson::TTokenizer(GetRequestTargetYPath(context->RequestHeader())).ParseNext());

    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    i64 limit = request->has_limit()
        ? request->limit()
        : DefaultVirtualChildLimit;

    context->SetRequestInfo("AttributeFilter: %v, Limit: %v",
        attributeFilter,
        limit);

    i64 size = GetSize();

    TAsyncYsonWriter writer;

    // NB: we do not want empty attributes (<>) to appear in the result in order to comply
    // with current behaviour for some paths (like //sys/scheduler/orchid/scheduler/operations).
    if (limit < size) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnBooleanScalar(true);
        writer.OnEndAttributes();
    }

    writer.OnBeginList();

    if (attributeFilter) {
        for (int index = 0; index < limit && index < size; ++index) {
            auto service = FindItemService(index);
            writer.OnListItem();
            if (service) {
                service->WriteAttributes(&writer, attributeFilter, false);
                if (Opaque_) {
                    writer.OnEntity();
                } else {
                    auto asyncResult = AsyncYPathGet(service, "");
                    writer.OnRaw(asyncResult);
                }
            } else {
                writer.OnEntity();
            }
        }
    } else {
        for (int index = 0; index < limit && index < size; ++index) {
            writer.OnListItem();
            if (Opaque_) {
                writer.OnEntity();
            } else {
                if (auto service = FindItemService(index)) {
                    writer.OnListItem();
                    auto asyncResult = AsyncYPathGet(service, "");
                    writer.OnRaw(asyncResult);
                } else {
                    writer.OnEntity();
                }
            }
        }
    }

    writer.OnEndList();

    writer.Finish()
        .Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
            if (resultOrError.IsOK()) {
                response->set_value(resultOrError.Value().ToString());
                context->Reply();
            } else {
                context->Reply(resultOrError);
            }
        }));
}

void TVirtualListBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->push_back(CountInternedAttribute);
}

const THashSet<TInternedAttributeKey>& TVirtualListBase::GetBuiltinAttributeKeys()
{
    return BuiltinAttributeKeysCache_.GetBuiltinAttributeKeys(this);
}

bool TVirtualListBase::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    switch (key) {
        case CountInternedAttribute:
            BuildYsonFluently(consumer)
                .Value(GetSize());
            return true;
        default:
            return false;
    }
}

TFuture<TYsonString> TVirtualListBase::GetBuiltinAttributeAsync(TInternedAttributeKey /*key*/)
{
    return std::nullopt;
}

ISystemAttributeProvider* TVirtualListBase::GetBuiltinAttributeProvider()
{
    return this;
}

bool TVirtualListBase::SetBuiltinAttribute(TInternedAttributeKey /*key*/, const TYsonString& /*value*/)
{
    return false;
}

bool TVirtualListBase::RemoveBuiltinAttribute(TInternedAttributeKey /*key*/)
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
