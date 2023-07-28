#include "node_detail.h"
#include "tree_visitor.h"
#include "exception_helpers.h"
#include "attribute_filter.h"

#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/tokenizer.h>
#include <yt/yt/core/yson/async_writer.h>

namespace NYT::NYTree {

using namespace NRpc;
using namespace NYPath;
using namespace NYson;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

bool TNodeBase::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetKey);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    DISPATCH_YPATH_SERVICE_METHOD(Multiset);
    DISPATCH_YPATH_SERVICE_METHOD(MultisetAttributes);
    return TYPathServiceBase::DoInvoke(context);
}

void TNodeBase::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    // TODO(babenko): make use of limit
    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
        limit,
        attributeFilter);

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    TAsyncYsonWriter writer;

    VisitTree(
        this,
        &writer,
        false,
        attributeFilter);

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

void TNodeBase::GetKeySelf(
    TReqGetKey* /*request*/,
    TRspGetKey* response,
    const TCtxGetKeyPtr& context)
{
    context->SetRequestInfo();

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto parent = GetParent();
    if (!parent) {
        THROW_ERROR_EXCEPTION("Node has no parent");
    }

    TString key;
    switch (parent->GetType()) {
        case ENodeType::Map:
            key = parent->AsMap()->GetChildKeyOrThrow(this);
            break;

        case ENodeType::List:
            key = ToString(parent->AsList()->GetChildIndexOrThrow(this));
            break;

        default:
            YT_ABORT();
    }

    context->SetResponseInfo("Key: %v", key);
    response->set_value(ConvertToYsonString(key).ToString());

    context->Reply();
}

void TNodeBase::RemoveSelf(
    TReqRemove* request,
    TRspRemove* /*response*/,
    const TCtxRemovePtr& context)
{
    context->SetRequestInfo("Recursive: %v, Force: %v", request->recursive(), request->force());

    ValidatePermission(
        EPermissionCheckScope::This | EPermissionCheckScope::Descendants,
        EPermission::Remove);
    ValidatePermission(
        EPermissionCheckScope::Parent,
        EPermission::Write | EPermission::ModifyChildren);

    bool isComposite = (GetType() == ENodeType::Map || GetType() == ENodeType::List);
    if (!request->recursive() && isComposite && AsComposite()->GetChildCount() > 0) {
        THROW_ERROR_EXCEPTION("Cannot remove non-empty composite node");
    }

    DoRemoveSelf(request->recursive(), request->force());

    context->Reply();
}

void TNodeBase::DoRemoveSelf(bool /*recursive*/, bool /*force*/)
{
    auto parent = GetParent();
    if (!parent) {
        ThrowCannotRemoveNode(this);
    }
    parent->AsComposite()->RemoveChild(this);
}

IYPathService::TResolveResult TNodeBase::ResolveRecursive(
    const NYPath::TYPath& path,
    const IYPathServiceContextPtr& context)
{
    if (context->GetMethod() == "Exists") {
        return TResolveResultHere{path};
    }

    ThrowCannotHaveChildren(this);
    YT_ABORT();
}

TYPath TNodeBase::GetPath() const
{
    TCompactVector<TString, 64> tokens;
    IConstNodePtr current(this);
    while (true) {
        auto parent = current->GetParent();
        if (!parent) {
            break;
        }
        TString token;
        switch (parent->GetType()) {
            case ENodeType::List: {
                auto index = parent->AsList()->GetChildIndexOrThrow(current);
                token = ToYPathLiteral(index);
                break;
            }
            case ENodeType::Map: {
                auto key = parent->AsMap()->GetChildKeyOrThrow(current);
                token = ToYPathLiteral(key);
                break;
            }
            default:
                YT_ABORT();
        }
        tokens.emplace_back(std::move(token));
        current = parent;
    }

    TStringBuilder builder;
    for (auto it = tokens.rbegin(); it != tokens.rend(); ++it) {
        builder.AppendChar('/');
        builder.AppendString(*it);
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

void TCompositeNodeMixin::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* /*response*/,
    const TCtxSetPtr& context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto factory = CreateFactory();
    auto child = ConvertToNode(TYsonString(request->value()), factory.get());
    SetChild(factory.get(), "/" + path, child, request->recursive());
    factory->Commit();

    context->Reply();
}

void TCompositeNodeMixin::RemoveRecursive(
    const TYPath& path,
    TSupportsRemove::TReqRemove* request,
    TSupportsRemove::TRspRemove* /*response*/,
    const TSupportsRemove::TCtxRemovePtr& context)
{
    context->SetRequestInfo();

    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() == NYPath::ETokenType::Asterisk) {
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write | EPermission::ModifyChildren);
        ValidatePermission(EPermissionCheckScope::Descendants, EPermission::Remove);
        Clear();

        context->Reply();
    } else if (request->force()) {
        // There is no child node under the given path, so there is nothing to remove.
        context->Reply();
    } else {
        ThrowNoSuchChildKey(this, tokenizer.GetLiteralValue());
    }
}

int TCompositeNodeMixin::GetMaxChildCount() const
{
    return std::numeric_limits<int>::max();
}

void TCompositeNodeMixin::ValidateChildCount(const TYPath& path, int childCount) const
{
    int maxChildCount = GetMaxChildCount();
    if (childCount >= maxChildCount) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::MaxChildCountViolation,
            "Composite node %v is not allowed to contain more than %v items",
            path,
            maxChildCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TMapNodeMixin::ResolveRecursive(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
{
    const auto& method = context->GetMethod();

    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Asterisk: {
            if (method != "Remove") {
                THROW_ERROR_EXCEPTION("\"*\" is only allowed for Remove method");
            }

            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);

            return IYPathService::TResolveResultHere{"/" + path};
        }

        case NYPath::ETokenType::Literal: {
            auto key = tokenizer.GetLiteralValue();
            if (key.empty()) {
                THROW_ERROR_EXCEPTION("Child key cannot be empty");
            }

            auto suffix = TYPath(tokenizer.GetSuffix());

            auto child = FindChild(key);
            if (!child) {
                if (method == "Exists" ||
                    method == "Remove" ||
                    method == "Set" ||
                    method == "Create" ||
                    method == "Copy" ||
                    method == "EndCopy")
                {
                    return IYPathService::TResolveResultHere{"/" + path};
                } else {
                    ThrowNoSuchChildKey(this, key);
                }
            }

            return IYPathService::TResolveResultThere{std::move(child), std::move(suffix)};
        }

        default:
            tokenizer.ThrowUnexpected();
            YT_ABORT();
    }
}

void TMapNodeMixin::ListSelf(
    TReqList* request,
    TRspList* response,
    const TCtxListPtr& context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
        limit,
        attributeFilter);

    TAsyncYsonWriter writer;

    auto children = GetChildren();
    if (limit && std::ssize(children) > *limit) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnBooleanScalar(true);
        writer.OnEndAttributes();
    }

    i64 counter = 0;

    writer.OnBeginList();
    for (const auto& [key, child] : children) {
        writer.OnListItem();
        child->WriteAttributes(&writer, attributeFilter, false);
        writer.OnStringScalar(key);
        if (limit && ++counter >= *limit) {
            break;
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

std::pair<TString, INodePtr> TMapNodeMixin::PrepareSetChild(
    INodeFactory* factory,
    const TYPath& path,
    INodePtr child,
    bool recursive)
{
    YT_VERIFY(factory || !recursive);

    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        tokenizer.ThrowUnexpected();
    }

    IMapNodePtr rootNode = AsMap();
    INodePtr rootChild;
    TString rootKey;

    auto currentNode = rootNode;
    try {
        while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
            tokenizer.Skip(NYPath::ETokenType::Ampersand);
            tokenizer.Expect(NYPath::ETokenType::Slash);

            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);
            auto key = tokenizer.GetLiteralValue();

            int maxKeyLength = GetMaxKeyLength();
            if (std::ssize(key) > maxKeyLength) {
                ThrowMaxKeyLengthViolated();
            }

            tokenizer.Advance();

            bool lastStep = (tokenizer.GetType() == NYPath::ETokenType::EndOfStream);
            if (!recursive && !lastStep) {
                ThrowNoSuchChildKey(currentNode, key);
            }

            ValidateChildCount(GetPath(), currentNode->GetChildCount());

            auto newChild = lastStep ? child : factory->CreateMap();
            if (currentNode != rootNode) {
                YT_VERIFY(currentNode->AddChild(key, newChild));
            } else {
                rootChild = newChild;
                rootKey = key;
            }

            if (!lastStep) {
                currentNode = newChild->AsMap();
            }
        }
    } catch (const std::exception& ex) {
        if (recursive) {
            THROW_ERROR_EXCEPTION("Failed to set node recursively")
                << ex;
        } else {
            throw;
        }
    }

    YT_VERIFY(rootKey);
    return {rootKey, rootChild};
}

void TMapNodeMixin::SetChild(
    INodeFactory* factory,
    const TYPath& path,
    INodePtr child,
    bool recursive)
{
    const auto& [rootKey, rootChild] = PrepareSetChild(factory, path, child, recursive);
    AddChild(rootKey, rootChild);
}

int TMapNodeMixin::GetMaxKeyLength() const
{
    return std::numeric_limits<int>::max();
}

void TMapNodeMixin::ThrowMaxKeyLengthViolated() const
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::MaxKeyLengthViolation,
        "Map node %v is not allowed to contain items with keys longer than %v symbols",
        GetPath(),
        GetMaxKeyLength());
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TListNodeMixin::ResolveRecursive(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
{
    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Asterisk: {
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);

            return IYPathService::TResolveResultHere{"/" + path};
        }

        case NYPath::ETokenType::Literal: {
            const auto& token = tokenizer.GetToken();
            if (token == ListBeginToken ||
                token == ListEndToken)
            {
                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::EndOfStream);

                return IYPathService::TResolveResultHere{"/" + path};
            } else if (token.StartsWith(ListBeforeToken) ||
                       token.StartsWith(ListAfterToken))
            {
                auto indexToken = ExtractListIndex(token);
                int index = ParseListIndex(indexToken);
                AdjustChildIndexOrThrow(index);

                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::EndOfStream);

                return IYPathService::TResolveResultHere{"/" + path};
            } else {
                int index = ParseListIndex(token);
                INodePtr child;
                auto adjustedIndex = NYPath::TryAdjustListIndex(index, GetChildCount());
                if (adjustedIndex) {
                    child = FindChild(*adjustedIndex);
                }

                if (!child) {
                    const auto& method = context->GetMethod();
                    if (method == "Exists") {
                        return IYPathService::TResolveResultHere{"/" + path};
                    } else {
                        ThrowNoSuchChildIndex(this, adjustedIndex.value_or(index));
                    }
                }

                return IYPathService::TResolveResultThere{std::move(child), TYPath(tokenizer.GetSuffix())};
            }
        }

        default:
            tokenizer.ThrowUnexpected();
            YT_ABORT();
    }
}

void TListNodeMixin::SetChild(
    INodeFactory* /*factory*/,
    const TYPath& path,
    INodePtr child,
    bool recursive)
{
    if (recursive) {
        THROW_ERROR_EXCEPTION("List node %v does not support \"recursive\" option",
            GetPath());
    }

    int beforeIndex = -1;

    NYPath::TTokenizer tokenizer(path);

    tokenizer.Advance();
    tokenizer.Skip(NYPath::ETokenType::Ampersand);
    tokenizer.Expect(NYPath::ETokenType::Slash);

    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);

    const auto& token = tokenizer.GetToken();

    if (token.StartsWith(ListBeginToken)) {
        beforeIndex = 0;
    } else if (token.StartsWith(ListEndToken)) {
        beforeIndex = GetChildCount();
    } else if (token.StartsWith(ListBeforeToken) || token.StartsWith(ListAfterToken)) {
        auto indexToken = ExtractListIndex(token);
        int index = ParseListIndex(indexToken);
        beforeIndex = AdjustChildIndexOrThrow(index);
        if (token.StartsWith(ListAfterToken)) {
            ++beforeIndex;
        }
    } else {
        tokenizer.ThrowUnexpected();
    }

    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::EndOfStream);

    ValidateChildCount(GetPath(), GetChildCount());

    AddChild(child, beforeIndex);
}

////////////////////////////////////////////////////////////////////////////////

void TSupportsSetSelfMixin::SetSelf(
    TReqSet* request,
    TRspSet* /*response*/,
    const TCtxSetPtr &context)
{
    bool force = request->force();
    context->SetRequestInfo("Force: %v", force);

    ValidateSetSelf(force);
    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
    ValidatePermission(EPermissionCheckScope::Descendants, EPermission::Remove);

    auto factory = CreateFactory();
    auto builder = CreateBuilderFromFactory(factory.get());
    SetNodeFromProducer(
        this,
        ConvertToProducer(TYsonString(request->value())),
        builder.get());
    factory->Commit();

    context->Reply();
}

void TSupportsSetSelfMixin::ValidateSetSelf(bool /*force*/) const
{ }

////////////////////////////////////////////////////////////////////////////////

IYPathServicePtr TNonexistingService::Get()
{
    return LeakyRefCountedSingleton<TNonexistingService>();
}

bool TNonexistingService::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    return TYPathServiceBase::DoInvoke(context);
}

IYPathService::TResolveResult TNonexistingService::Resolve(
    const TYPath& path,
    const IYPathServiceContextPtr& /*context*/)
{
    return TResolveResultHere{path};
}

void TNonexistingService::ExistsSelf(
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    ExistsAny(context);
}

void TNonexistingService::ExistsRecursive(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    ExistsAny(context);
}

void TNonexistingService::ExistsAttribute(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    ExistsAny(context);
}

void TNonexistingService::ExistsAny(const TCtxExistsPtr& context)
{
    context->SetRequestInfo();
    Reply(context, false);
}

////////////////////////////////////////////////////////////////////////////////

TTransactionalNodeFactoryBase::~TTransactionalNodeFactoryBase()
{
    YT_VERIFY(State_ == EState::Committed || State_ == EState::RolledBack);
}

void TTransactionalNodeFactoryBase::Commit() noexcept
{
    YT_VERIFY(State_ == EState::Active);
    State_ = EState::Committed;
}

void TTransactionalNodeFactoryBase::Rollback() noexcept
{
    YT_VERIFY(State_ == EState::Active);
    State_ = EState::RolledBack;
}

void TTransactionalNodeFactoryBase::RollbackIfNeeded()
{
    if (State_ == EState::Active) {
        Rollback();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
