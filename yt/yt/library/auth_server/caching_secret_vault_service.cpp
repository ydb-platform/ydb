#include "caching_secret_vault_service.h"
#include "secret_vault_service.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NAuth {

using namespace NConcurrency;

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TCachingSecretVaultService
    : public ISecretVaultService
    , public TAsyncExpiringCache<
        ISecretVaultService::TSecretSubrequest,
        ISecretVaultService::TSecretSubresponse
     >
{
public:
    TCachingSecretVaultService(
        TCachingSecretVaultServiceConfigPtr config,
        ISecretVaultServicePtr underlying,
        NProfiling::TProfiler profiler)
        : TAsyncExpiringCache(
            config->Cache,
            AuthLogger.WithTag("Cache: SecretVault"),
            std::move(profiler))
        , Underlying_(std::move(underlying))
    { }

    TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(const std::vector<TSecretSubrequest>& subrequests) override
    {
        std::vector<TFuture<TSecretSubresponse>> asyncResults;
        THashMap<TSecretSubrequest, TFuture<TSecretSubresponse>> subrequestToAsyncResult;
        for (const auto& subrequest : subrequests) {
            auto it = subrequestToAsyncResult.find(subrequest);
            if (it == subrequestToAsyncResult.end()) {
                auto asyncResult = Get(subrequest);
                YT_VERIFY(subrequestToAsyncResult.emplace(subrequest, asyncResult).second);
                asyncResults.push_back(std::move(asyncResult));
            } else {
                asyncResults.push_back(it->second);
            }
        }
        return AllSet(asyncResults);
    }

    TFuture<TString> GetDelegationToken(TDelegationTokenRequest request) override
    {
        return Underlying_->GetDelegationToken(std::move(request));
    }

protected:
    //! Called under write lock.
    void OnAdded(const ISecretVaultService::TSecretSubrequest& subrequest) noexcept override
    {
        YT_LOG_DEBUG("Secret added to cache (SecretId: %v, SecretVersion: %v)",
            subrequest.SecretId,
            subrequest.SecretVersion);
    }

    //! Called under write lock.
    void OnRemoved(const ISecretVaultService::TSecretSubrequest& subrequest) noexcept override
    {
        YT_LOG_DEBUG("Secret removed from cache (SecretId: %v, SecretVersion: %v)",
            subrequest.SecretId,
            subrequest.SecretVersion);
    }

private:
    const ISecretVaultServicePtr Underlying_;

    TFuture<TSecretSubresponse> DoGet(
        const TSecretSubrequest& subrequest,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return Underlying_->GetSecrets({subrequest})
            .Apply(BIND([] (const std::vector<TErrorOrSecretSubresponse>& result) {
                YT_VERIFY(result.size() == 1);
                return result[0].ValueOrThrow();
            }));
    }
};

ISecretVaultServicePtr CreateCachingSecretVaultService(
    TCachingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying,
    NProfiling::TProfiler profiler)
{
    return New<TCachingSecretVaultService>(
        std::move(config),
        std::move(underlying),
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
