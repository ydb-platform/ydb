#pragma once

#include "cypress_cookie.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

//! This class stores user session cookies locally and periodically
//! synchronizes them with Cypress.
/*
 *  Thread affinity: any
 */
struct ICypressCookieStore
    : public TRefCounted
{
    //! Starts periodic cookie fetch.
    virtual void Start() = 0;

    //! Stops periodic cookie fetch.
    virtual void Stop() = 0;

    //! Finds cookie description by value. If cookie with given value is known,
    //! returns its description. Otherwise, tries to fetch cookie from Cypress.
    virtual TFuture<TCypressCookiePtr> GetCookie(const TString& value) = 0;

    //! Returns known cookie for given user with maximum |ExpiresAt|.
    //! If no cookies for user are known, returns |nullptr|.
    virtual TCypressCookiePtr GetLastCookieForUser(const TString& user) = 0;

    //! Invalidates last cookie for user.
    virtual void RemoveLastCookieForUser(const TString& user) = 0;

    //! Registers cookie in Cypress. If registration is successful, also stores
    //! cookie locally.
    virtual TFuture<void> RegisterCookie(const TCypressCookiePtr& cookie) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressCookieStore)

////////////////////////////////////////////////////////////////////////////////

ICypressCookieStorePtr CreateCypressCookieStore(
    TCypressCookieStoreConfigPtr config,
    NApi::IClientPtr client,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
