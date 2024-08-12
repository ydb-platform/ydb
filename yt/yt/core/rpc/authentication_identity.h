#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticationIdentity
{
    TAuthenticationIdentity() = default;
    explicit TAuthenticationIdentity(TString user, TString userTag = {});

    bool operator==(const TAuthenticationIdentity& other) const = default;

    TString User;
    TString UserTag;
};

//! Returns the current identity.
//! If none is explicitly set then returns the root identity.
const TAuthenticationIdentity& GetCurrentAuthenticationIdentity();

//! Sets the current identity.
//! The passed identity is not copied, just a pointer is updated.
//! The caller must ensure a proper lifetime of the installed identity.
void SetCurrentAuthenticationIdentity(const TAuthenticationIdentity* identity);

//! Returns the root identity, which is the default one.
const TAuthenticationIdentity& GetRootAuthenticationIdentity();

void FormatValue(TStringBuilderBase* builder, const TAuthenticationIdentity& value, TStringBuf spec);

void Serialize(const TAuthenticationIdentity& identity, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TCurrentAuthenticationIdentityGuard
    : private TNonCopyable
{
public:
    TCurrentAuthenticationIdentityGuard();
    explicit TCurrentAuthenticationIdentityGuard(const TAuthenticationIdentity* newIdentity);
    TCurrentAuthenticationIdentityGuard(TCurrentAuthenticationIdentityGuard&& other);
    ~TCurrentAuthenticationIdentityGuard();

    TCurrentAuthenticationIdentityGuard& operator=(TCurrentAuthenticationIdentityGuard&& other);

private:
    const TAuthenticationIdentity* OldIdentity_;

    void Release();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

template <>
struct THash<NYT::NRpc::TAuthenticationIdentity>
{
    size_t operator()(const NYT::NRpc::TAuthenticationIdentity& value) const;
};
