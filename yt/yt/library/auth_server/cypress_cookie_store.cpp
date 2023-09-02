#include "cypress_cookie_store.h"

#include "config.h"

#include <yt/yt/library/auth_server/private.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NThreading;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressCookieStore
    : public ICypressCookieStore
{
public:
    TCypressCookieStore(
        TCypressCookieStoreConfigPtr config,
        IClientPtr client,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , UpdateExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TCypressCookieStore::DoFetchAllCookies, MakeWeak(this)),
            Config_->FullFetchPeriod))
    { }

    void Start() override
    {
        UpdateExecutor_->Start();

        YT_LOG_DEBUG("Starting periodic updates in native cookie store");
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(UpdateExecutor_->Stop());

        YT_LOG_DEBUG("Stopping periodic updates in native cookie store");
    }

    TFuture<TCypressCookiePtr> GetCookie(const TString& value) override
    {
        {
            auto guard = ReaderGuard(CookiesLock_);
            auto cookieIt = Cookies_.find(value);
            if (cookieIt != Cookies_.end()) {
                auto entry = cookieIt->second;
                if (IsEntryActual(entry)) {
                    return entry->CookieFuture;
                }
            }
        }

        {
            auto guard = WriterGuard(CookiesLock_);
            auto cookieIt = Cookies_.find(value);
            if (cookieIt != Cookies_.end()) {
                auto entry = cookieIt->second;
                // Double check.
                if (IsEntryActual(entry)) {
                    return entry->CookieFuture;
                } else {
                    Cookies_.erase(cookieIt);
                }
            }

            auto cookieFuture = DoFetchCookie(value);
            auto entry = New<TEntry>();

            cookieFuture = cookieFuture
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TCypressCookiePtr>& cookieOrError) {
                    entry->FetchTime = TInstant::Now();

                    if (cookieOrError.IsOK()) {
                        DoRegisterCookie(cookieOrError.Value());
                    }

                    return cookieOrError;
                })).ToUncancelable();
            entry->CookieFuture = cookieFuture;

            EmplaceOrCrash(Cookies_, value, std::move(entry));

            return cookieFuture;
        }
    }

    TCypressCookiePtr GetLastCookieForUser(const TString& user) override
    {
        auto guard = ReaderGuard(UserToLastCookieLock_);
        auto userIt = UserToLastCookie_.find(user);
        if (userIt == UserToLastCookie_.end()) {
            return nullptr;
        } else {
            return userIt->second;
        }
    }

    void RemoveLastCookieForUser(const TString& user) override
    {
        auto guard = WriterGuard(UserToLastCookieLock_);
        UserToLastCookie_.erase(user);
    }

    TFuture<void> RegisterCookie(const TCypressCookiePtr& cookie) override
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("value", ConvertToYsonString(cookie));
        attributes->Set("expiration_time", ConvertToYsonString(cookie->ExpiresAt));

        TCreateNodeOptions createOptions;
        createOptions.Attributes = std::move(attributes);

        auto future = Client_->CreateNode(
            GetCookiePath(cookie->Value),
            EObjectType::Document,
            createOptions);
        return future.AsVoid().Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            if (error.IsOK()) {
                auto entry = New<TEntry>();
                entry->CookieFuture = MakeFuture<TCypressCookiePtr>(cookie);
                entry->FetchTime = TInstant::Now();

                {
                    // NB: Technically it is possible that cookie is already in |Cookies_|.
                    auto writeGuard = WriterGuard(CookiesLock_);
                    Cookies_[cookie->Value] = std::move(entry);
                }

                DoRegisterCookie(cookie);
            }
        }));
    }

private:
    const TCypressCookieStoreConfigPtr Config_;

    const IClientPtr Client_;

    const TPeriodicExecutorPtr UpdateExecutor_;

    struct TEntry final
    {
        TFuture<TCypressCookiePtr> CookieFuture;

        //! Time when this entry was fetched.
        TInstant FetchTime;
    };
    using TEntryPtr = TIntrusivePtr<TEntry>;

    THashMap<TString, TEntryPtr> Cookies_;
    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, CookiesLock_);

    THashMap<TString, TCypressCookiePtr> UserToLastCookie_;
    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, UserToLastCookieLock_);

    bool IsEntryActual(const TEntryPtr& entry)
    {
        // Cookie info is not fetched yet, so information cannot be stale.
        if (!entry->CookieFuture.IsSet()) {
            return true;
        }

        // Successes are stored forever and errors are cached for a configured time.
        return
            entry->CookieFuture.Get().IsOK() ||
            entry->FetchTime + Config_->ErrorEvictionTime > TInstant::Now();
    }

    TFuture<TCypressCookiePtr> DoFetchCookie(const TString& value)
    {
        YT_LOG_DEBUG("Fetching cookie from Cypress (Cookie: %v)",
            value);

        return Client_->GetNode(GetCookiePath(value))
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TYsonString& value) {
                return ConvertTo<TCypressCookiePtr>(value);
            }));
    }

    void DoFetchAllCookies()
    {
        try {
            GuardedFetchAllCookies();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to fetch native cookies from Cypress");
        }
    }

    void GuardedFetchAllCookies()
    {
        YT_LOG_DEBUG("Started fetching native cookies");

        constexpr TStringBuf ValueAttribute = "value";

        TListNodeOptions listOptions{
            .Attributes = std::vector<TString>({TString{ValueAttribute}}),
        };
        listOptions.ReadFrom = EMasterChannelKind::Cache;

        auto rawListResult = WaitFor(Client_->ListNode(
            "//sys/cypress_cookies",
            listOptions))
            .ValueOrThrow();
        auto listResult = ConvertTo<IListNodePtr>(rawListResult);

        YT_LOG_DEBUG("Native cookies fetched from Cypress (CookieCount: %v)",
            listResult->GetChildCount());

        THashMap<TString, TEntryPtr> newCookies;
        THashMap<TString, TCypressCookiePtr> newUserToLastCookie;
        for (const auto& child : listResult->GetChildren()) {
            try {
                auto cookie = child->Attributes().Get<TCypressCookiePtr>(ValueAttribute);

                auto entry = New<TEntry>();
                entry->CookieFuture = MakeFuture<TCypressCookiePtr>(cookie);
                entry->FetchTime = TInstant::Now();
                EmplaceOrCrash(newCookies, cookie->Value, entry);

                const auto& user = cookie->User;
                auto userIt = newUserToLastCookie.find(user);
                if (userIt == newUserToLastCookie.end()) {
                    newUserToLastCookie[user] = cookie;
                } else {
                    const auto& lastCookie = userIt->second;
                    if (cookie->ExpiresAt > lastCookie->ExpiresAt) {
                        newUserToLastCookie[user] = cookie;
                    }
                }
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to parse cookie (Cookie: %v)",
                    child->GetValue<TString>());
            }
        }

        {
            auto guard = WriterGuard(CookiesLock_);
            Cookies_ = std::move(newCookies);
        }
        {
            auto guard = WriterGuard(UserToLastCookieLock_);
            UserToLastCookie_ = std::move(newUserToLastCookie);
        }
    }

    void DoRegisterCookie(const TCypressCookiePtr& cookie)
    {
        auto guard = WriterGuard(UserToLastCookieLock_);

        const auto& user = cookie->User;
        auto userIt = UserToLastCookie_.find(user);
        if (userIt == UserToLastCookie_.end()) {
            UserToLastCookie_[user] = cookie;
        } else {
            const auto& lastCookie = userIt->second;
            if (cookie->ExpiresAt > lastCookie->ExpiresAt) {
                UserToLastCookie_[user] = cookie;
            }
        }
    }

    static TYPath GetCookiePath(const TString& value)
    {
        return Format("//sys/cypress_cookies/%v", ToYPathLiteral(value));
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressCookieStorePtr CreateCypressCookieStore(
    TCypressCookieStoreConfigPtr config,
    IClientPtr client,
    IInvokerPtr invoker)
{
    return New<TCypressCookieStore>(
        std::move(config),
        std::move(client),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
