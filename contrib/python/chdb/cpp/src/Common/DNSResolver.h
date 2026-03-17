#pragma once
#include <CHDBPoco/Net/IPAddress.h>
#include <CHDBPoco/Net/SocketAddress.h>
#include <memory>
#include <base/types.h>
#include <Core/Names.h>
#include <boost/noncopyable.hpp>
#include <Common/LoggingFormatStringHelpers.h>


namespace CHDBPoco { class Logger; }

namespace DB_CHDB
{

/// A singleton implementing DNS names resolving with optional DNS cache
/// The cache is being updated asynchronous in separate thread (see DNSCacheUpdater)
/// or it could be updated manually via drop() method.
class DNSResolver : private boost::noncopyable
{
public:
    using IPAddresses = std::vector<CHDBPoco::Net::IPAddress>;
    using CacheEntry = struct
    {
        IPAddresses addresses;
        std::chrono::system_clock::time_point cached_at;
    };

    static DNSResolver & instance();

    DNSResolver(const DNSResolver &) = delete;

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolves its IP
    CHDBPoco::Net::IPAddress resolveHost(const std::string & host);

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolves all its IPs
    /// resolveHostAllInOriginOrder returns addresses with the same order as system call returns it
    IPAddresses resolveHostAllInOriginOrder(const std::string & host);
    /// resolveHostAll returns addresses in random order
    IPAddresses resolveHostAll(const std::string & host);

    /// Accepts host names like 'example.com:port' or '127.0.0.1:port' or '[::1]:port' and resolves its IP and port
    CHDBPoco::Net::SocketAddress resolveAddress(const std::string & host_and_port);

    CHDBPoco::Net::SocketAddress resolveAddress(const std::string & host, UInt16 port);

    std::vector<CHDBPoco::Net::SocketAddress> resolveAddressList(const std::string & host, UInt16 port);

    /// Accepts host IP and resolves its host names
    std::unordered_set<String> reverseResolve(const CHDBPoco::Net::IPAddress & address);

    /// Get this server host name
    String getHostName();

    /// Disables caching
    void setDisableCacheFlag(bool is_disabled = true);

    /// Set a limit of entries in cache
    void setCacheMaxEntries(UInt64 cache_max_entries);

    /// Drops all caches
    void dropCache();

    /// Removes an entry from cache or does nothing
    void removeHostFromCache(const std::string & host);

    /// Updates all known hosts in cache.
    /// Returns true if IP of any host has been changed or an element was dropped (too many failures)
    bool updateCache(UInt32 max_consecutive_failures);

    /// Returns a copy of cache entries
    std::vector<std::pair<std::string, CacheEntry>> cacheEntries() const;

    ~DNSResolver();

private:
    template <typename UpdateF, typename ElemsT>
    bool updateCacheImpl(
        UpdateF && update_func,
        ElemsT && elems,
        UInt32 max_consecutive_failures,
        FormatStringHelper<String> notfound_log_msg,
        FormatStringHelper<String> dropped_log_msg);

    DNSResolver();

    struct Impl;
    std::unique_ptr<Impl> impl;
    LoggerPtr log;

    /// Updates cached value and returns true it has been changed.
    bool updateHost(const String & host);
    bool updateAddress(const CHDBPoco::Net::IPAddress & address);

    void addToNewHosts(const String & host);
    void addToNewAddresses(const CHDBPoco::Net::IPAddress & address);
};

}
