#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TCypressCookie
    : public NYTree::TYsonStruct
{
    //! Value of the cookie.
    TString Value;

    //! User for which cookie is issued.
    TString User;

    //! Revision of password in the moment of cookie issue.
    ui64 PasswordRevision;

    //! Cookie expiration instant.
    TInstant ExpiresAt;

    //! Returns text representation of a cookie for SetCookie header.
    TString ToHeader(const TCypressCookieGeneratorConfigPtr& config) const;

    REGISTER_YSON_STRUCT(TCypressCookie);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressCookie)

////////////////////////////////////////////////////////////////////////////////

//! Generates new cookie value using cryptographically strong generator.
// NB: May throw on RNG failure.
TString GenerateCookieValue();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
