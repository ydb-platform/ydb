#include "local_address.h"
#include "address.h"

#include "config.h"

#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

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

constexpr size_t MaxLocalFieldLength = 256;
constexpr size_t MaxLocalFieldDataSize = 1024;

// All static variables below must be const-initialized.
// - char[] is a POD, so it must be const-initialized.
// - std::atomic has constexpr value constructors.
// However, there is no way to enforce in compile-time that these variables
// are really const-initialized, so please double-check it with `objdump -s`.
char LocalHostNameData[MaxLocalFieldDataSize] = "(unknown)";
std::atomic<char*> LocalHostNamePtr;

char LocalYPClusterData[MaxLocalFieldDataSize] = "(unknown)";
std::atomic<char*> LocalYPClusterPtr;

std::atomic<bool> IPv6Enabled_ = false;

} // namespace

////////////////////////////////////////////////////////////////////////////////

const char* ReadLocalHostName() noexcept
{
    NYT::NOrigin::EnableOriginOverrides();
    // Writer-side imposes AcqRel ordering, so all preceding writes must be visible.
    char* ptr = LocalHostNamePtr.load(std::memory_order::relaxed);
    return ptr ? ptr : LocalHostNameData;
}

const char* ReadLocalYPCluster() noexcept
{
    // Writer-side imposes AcqRel ordering, so all preceding writes must be visible.
    char* ptr = LocalYPClusterPtr.load(std::memory_order::relaxed);
    return ptr ? ptr : LocalYPClusterData;
}

void GuardedWriteString(std::atomic<char*>& storage, char* initial, TStringBuf string)
{
    char* ptr = storage.load(std::memory_order::relaxed);
    ptr = ptr ? ptr : initial;

    if (::strncmp(ptr, string.data(), string.length()) == 0) {
        return; // No changes; just return.
    }

    ptr = ptr + strlen(ptr) + 1;

    if (ptr + string.length() + 1 >= initial + MaxLocalFieldDataSize) {
        ::abort(); // Once we crash here, we can start reusing space.
    }

    ::memcpy(ptr, string.data(), string.length());
    *(ptr + string.length()) = 0;

    storage.store(ptr, std::memory_order::seq_cst);
}

void WriteLocalHostName(TStringBuf hostName) noexcept
{
    NYT::NOrigin::EnableOriginOverrides();

    static NThreading::TForkAwareSpinLock Lock;
    auto guard = Guard(Lock);

    GuardedWriteString(LocalHostNamePtr, LocalHostNameData, hostName);

    if (auto ypCluster = InferYPClusterFromHostNameRaw(hostName)) {
        GuardedWriteString(LocalYPClusterPtr, LocalYPClusterData, *ypCluster);
    }
}

TString GetLocalHostName()
{
    return {ReadLocalHostName()};
}

TString GetLocalYPCluster()
{
    return {ReadLocalYPCluster()};
}

void UpdateLocalHostName(const TAddressResolverConfigPtr& config)
{
    std::array<char, MaxLocalFieldLength> hostName;
    hostName.fill(0);

    auto onFail = [&] (const std::vector<TError>& errors) {
        THROW_ERROR_EXCEPTION("Failed to update localhost name") << errors;
    };

    auto runWithRetries = [&] (std::function<int()> func, std::function<TError(int /*result*/)> onError) {
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
        WriteLocalHostName(TStringBuf(hostName.data()));
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

    WriteLocalHostName(TStringBuf(response->ai_canonname));
}

////////////////////////////////////////////////////////////////////////////////

const TString& GetLoopbackAddress()
{
    static const TString ipv4result("[127.0.1.1]");
    static const TString ipv6result("[::1]");
    return IPv6Enabled_.load(std::memory_order::relaxed) ? ipv6result : ipv4result;
}

void UpdateLoopbackAddress(const TAddressResolverConfigPtr& config)
{
    IPv6Enabled_ = config->EnableIPv6;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
