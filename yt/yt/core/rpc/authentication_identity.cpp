#include "authentication_identity.h"

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NRpc {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TAuthenticationIdentity::TAuthenticationIdentity(TString user, TString userTag)
    : User(std::move(user))
    , UserTag(std::move(userTag))
{ }

void Serialize(const TAuthenticationIdentity& identity, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("user").Value(identity.User)
            .DoIf(!identity.UserTag.empty() && identity.UserTag != identity.User, [&] (TFluentMap fluent) {
                fluent
                    .Item("user_tag").Value(identity.UserTag);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

const TAuthenticationIdentity** GetCurrentAuthenticationIdentityPtr()
{
    static NConcurrency::TFlsSlot<const TAuthenticationIdentity*> Slot;
    return Slot.GetOrCreate();
}

} // namespace

const TAuthenticationIdentity& GetCurrentAuthenticationIdentity()
{
    const auto* identity = *GetCurrentAuthenticationIdentityPtr();
    return identity ? *identity : GetRootAuthenticationIdentity();
}

const TAuthenticationIdentity& GetRootAuthenticationIdentity()
{
    static const TAuthenticationIdentity RootIdentity{
        RootUserName,
        RootUserName
    };
    return RootIdentity;
}

void SetCurrentAuthenticationIdentity(const TAuthenticationIdentity* identity)
{
    *GetCurrentAuthenticationIdentityPtr() = identity;
}

void FormatValue(TStringBuilderBase* builder, const TAuthenticationIdentity& value, TStringBuf /*spec*/)
{
    builder->AppendFormat("{User: %v", value.User);
    if (!value.UserTag.Empty() && value.UserTag != value.User) {
        builder->AppendFormat(", UserTag: %v", value.UserTag);
    }
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

TCurrentAuthenticationIdentityGuard::TCurrentAuthenticationIdentityGuard()
    : OldIdentity_(nullptr)
{ }

TCurrentAuthenticationIdentityGuard::TCurrentAuthenticationIdentityGuard(
    TCurrentAuthenticationIdentityGuard&& other)
    : OldIdentity_(other.OldIdentity_)
{
    other.OldIdentity_ = nullptr;
}

TCurrentAuthenticationIdentityGuard::TCurrentAuthenticationIdentityGuard(
    const TAuthenticationIdentity* newIdentity)
{
    OldIdentity_ = &GetCurrentAuthenticationIdentity();
    SetCurrentAuthenticationIdentity(newIdentity);
}

TCurrentAuthenticationIdentityGuard::~TCurrentAuthenticationIdentityGuard()
{
    Release();
}

TCurrentAuthenticationIdentityGuard& TCurrentAuthenticationIdentityGuard::operator=(
    TCurrentAuthenticationIdentityGuard&& other)
{
    Release();
    OldIdentity_ = other.OldIdentity_;
    other.OldIdentity_ = nullptr;
    return *this;
}

void TCurrentAuthenticationIdentityGuard::Release()
{
    if (OldIdentity_) {
        *GetCurrentAuthenticationIdentityPtr() = OldIdentity_;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

size_t THash<NYT::NRpc::TAuthenticationIdentity>::operator()(const NYT::NRpc::TAuthenticationIdentity& value) const
{
    size_t result = 0;
    NYT::HashCombine(result, value.User);
    NYT::HashCombine(result, value.UserTag);
    return result;
}
