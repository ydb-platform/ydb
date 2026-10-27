#include "local_address.h"
#include "address.h"

#include "config.h"

#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/system/handle_eintr.h>

#ifdef _unix_
    #include <sys/types.h>
    #include <sys/socket.h>
    #include <netdb.h>
    #include <errno.h>
#endif

#ifdef _win_
    #include <winsock2.h>
#endif

#include <array>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

namespace {

// All static variables below must be constinit.
constinit std::atomic<bool> IPv6Enabled = false;

} // namespace

////////////////////////////////////////////////////////////////////////////////

void UpdateLocalHostName(const TAddressResolverConfigPtr& config)
{
    NYT::NDetail::EnableErrorOriginOverrides();

    // See https://man7.org/linux/man-pages/man7/hostname.7.html
    std::array<char, 256> hostName{};

    auto onFail = [&] (const std::vector<TError>& errors) {
        THROW_ERROR_EXCEPTION("Failed to update localhost name").With(errors);
    };

    auto runWithRetries = [&] (auto func, auto onError) {
        std::vector<TError> errors;

        for (int retryIndex = 0; retryIndex < config->Retries; ++retryIndex) {
            auto result = func();
            if (result == 0) {
                return;
            }

            errors.push_back(onError(result));

            if (retryIndex + 1 == config->Retries) {
                onFail(errors);
            } else {
                Sleep(config->RetryDelay);
            }
        }
    };

    runWithRetries(
        [&] { return HandleEintr(::gethostname, hostName.data(), hostName.size() - 1); },
        [&] (int /*result*/) { return TError("gethostname failed: %v", strerror(errno)); });

    if (!config->ResolveHostNameIntoFqdn) {
        SetLocalHostName(TStringBuf(hostName.data()));
        return;
    }

    addrinfo request;
    ::memset(&request, 0, sizeof(request));
    request.ai_family = AF_UNSPEC;
    request.ai_socktype = SOCK_STREAM;
    request.ai_flags |= AI_CANONNAME;

    addrinfo* response = nullptr;

    runWithRetries(
        [&] { return getaddrinfo(hostName.data(), nullptr, &request, &response); },
        [&] (int result) { return TError("getaddrinfo failed: %v", gai_strerror(result)); });

    std::unique_ptr<addrinfo, void(*)(addrinfo*)> holder(response, &::freeaddrinfo);

    if (!response->ai_canonname) {
        auto error = TError("getaddrinfo failed: no canonical hostname");
        onFail({error});
    }

    SetLocalHostName(TStringBuf(response->ai_canonname));
}

////////////////////////////////////////////////////////////////////////////////

const std::string& GetLoopbackAddress()
{
    static const std::string ipv4result("[127.0.1.1]");
    static const std::string ipv6result("[::1]");
    return IPv6Enabled.load(std::memory_order::relaxed) ? ipv6result : ipv4result;
}

void UpdateLoopbackAddress(const TAddressResolverConfigPtr& config)
{
    IPv6Enabled = config->EnableIPv6;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
