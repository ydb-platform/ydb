#include "cypress_cookie_manager.h"

#include "config.h"
#include "cookie_authenticator.h"
#include "cypress_cookie_authenticator.h"
#include "cypress_cookie_store.h"

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TCypressCookieManager
    : public ICypressCookieManager
{
public:
    TCypressCookieManager(
        TCypressCookieManagerConfigPtr config,
        IClientPtr client,
        TProfiler profiler)
        : CookieStore_(CreateCypressCookieStore(
            config->CookieStore,
            client,
            NRpc::TDispatcher::Get()->GetHeavyInvoker()))
        , CookieAuthenticator_(CreateCypressCookieAuthenticator(
            config->CookieGenerator,
            CookieStore_,
            client))
        , CachingCookieAuthenticator_(CreateCachingCookieAuthenticator(
            config->CookieAuthenticator,
            CookieAuthenticator_,
            profiler.WithPrefix("/cypress_cookie_authenticator/cache")))
        { }

    void Start()
    {
        CookieStore_->Start();
    }

    void Stop()
    {
        CookieStore_->Stop();
    }

    const ICypressCookieStorePtr& GetCookieStore() const
    {
        return CookieStore_;
    }

    const ICookieAuthenticatorPtr& GetCookieAuthenticator() const
    {
        return CachingCookieAuthenticator_;
    }

private:
    const ICypressCookieStorePtr CookieStore_;

    const ICookieAuthenticatorPtr CookieAuthenticator_;
    const ICookieAuthenticatorPtr CachingCookieAuthenticator_;
};

////////////////////////////////////////////////////////////////////////////////

ICypressCookieManagerPtr CreateCypressCookieManager(
    TCypressCookieManagerConfigPtr config,
    IClientPtr client,
    TProfiler profiler)
{
    return New<TCypressCookieManager>(
        std::move(config),
        std::move(client),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
