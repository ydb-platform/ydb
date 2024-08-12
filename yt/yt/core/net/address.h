#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/dns/public.h>

#include <yt/yt/core/actions/future.h>

#include <util/generic/hash.h>

#ifdef _WIN32
    #include <ws2tcpip.h>
#else
    #include <sys/socket.h>
#endif

#include <array>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

//! Constructs an address of the form |hostName:port|.
TString BuildServiceAddress(TStringBuf hostName, int port);

//! Parses service address into host name and port number.
//! Both #hostName and #port can be |NULL|.
//! Throws if the address is malformed.
void ParseServiceAddress(
    TStringBuf address,
    TStringBuf* hostName,
    int* port);

//! Extracts port number from a service address.
//! Throws if the address is malformed.
int GetServicePort(TStringBuf address);

//! Extracts host name from a service address.
TStringBuf GetServiceHostName(TStringBuf address);

////////////////////////////////////////////////////////////////////////////////

class TIP6Address;

//! An opaque wrapper for |sockaddr| type.
class TNetworkAddress
{
public:
    TNetworkAddress();
    TNetworkAddress(const TNetworkAddress& other, int port);
    explicit TNetworkAddress(const sockaddr& other, socklen_t length = 0);
    TNetworkAddress(int family, const char* addr, size_t size);

    sockaddr* GetSockAddr();
    const sockaddr* GetSockAddr() const;
    socklen_t GetLength() const;
    socklen_t* GetLengthPtr();
    int GetPort() const;

    bool IsUnix() const;
    bool IsIP() const;
    bool IsIP4() const;
    bool IsIP6() const;

    static TErrorOr<TNetworkAddress> TryParse(TStringBuf address);
    static TNetworkAddress Parse(TStringBuf address);

    static TNetworkAddress CreateIPv6Any(int port);
    static TNetworkAddress CreateIPv6Loopback(int port);
    static TNetworkAddress CreateUnixDomainSocketAddress(const TString& socketPath);
    static TNetworkAddress CreateAbstractUnixDomainSocketAddress(const TString& socketName);

    TIP6Address ToIP6Address() const;

private:
    sockaddr_storage Storage_;
    socklen_t Length_;

    static socklen_t GetGenericLength(const sockaddr& sockAddr);

    friend void ToProto(TString* protoAddress, const TNetworkAddress& address);
    friend void FromProto(TNetworkAddress* address, const TString& protoAddress);
};

extern const TNetworkAddress NullNetworkAddress;

struct TNetworkAddressFormatOptions
{
    bool IncludePort = true;
    bool IncludeTcpProtocol = true;
};

void FormatValue(TStringBuilderBase* builder, const TNetworkAddress& address, TStringBuf spec);
TString ToString(const TNetworkAddress& address, const TNetworkAddressFormatOptions& options = {});

bool operator == (const TNetworkAddress& lhs, const TNetworkAddress& rhs);

////////////////////////////////////////////////////////////////////////////////

class TIP6Address
{
public:
    static constexpr size_t ByteSize = 16;

    TIP6Address() = default;

    static TIP6Address FromString(TStringBuf str);
    static bool FromString(TStringBuf str, TIP6Address* address);

    static TIP6Address FromRawBytes(const ui8* raw);
    static TIP6Address FromRawWords(const ui16* raw);
    static TIP6Address FromRawDWords(const ui32* raw);

    const ui8* GetRawBytes() const;
    ui8* GetRawBytes();

    const ui16* GetRawWords() const;
    ui16* GetRawWords();

    const ui32* GetRawDWords() const;
    ui32* GetRawDWords();

private:
    std::array<ui8, ByteSize> Raw_ = {};
};

void FormatValue(TStringBuilderBase* builder, const TIP6Address& address, TStringBuf spec);

bool operator == (const TIP6Address& lhs, const TIP6Address& rhs);

TIP6Address operator & (const TIP6Address& lhs, const TIP6Address& rhs);
TIP6Address operator | (const TIP6Address& lhs, const TIP6Address& rhs);
TIP6Address& operator &= (TIP6Address& lhs, const TIP6Address& rhs);
TIP6Address& operator |= (TIP6Address& lhs, const TIP6Address& rhs);

void Deserialize(TIP6Address& value, NYTree::INodePtr node);
void Deserialize(TIP6Address& value, NYson::TYsonPullParserCursor* cursor);
void Serialize(const TIP6Address& value, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TIP6Network
{
public:
    TIP6Network() = default;
    TIP6Network(const TIP6Address& network, const TIP6Address& mask);

    static TIP6Network FromString(TStringBuf str);
    static bool FromString(TStringBuf str, TIP6Network* network);

    bool Contains(const TIP6Address& address) const;

    const TIP6Address& GetAddress() const;
    const TIP6Address& GetMask() const;
    int GetMaskSize() const;

    //! Get project id as extracted by FromString().
    std::optional<ui32> GetProjectId() const;

private:
    TIP6Address Network_;
    TIP6Address Mask_;
    std::optional<ui32> ProjectId_;
};

void FormatValue(TStringBuilderBase* builder, const TIP6Network& network, TStringBuf spec);

void Deserialize(TIP6Network& value, NYTree::INodePtr node);
void Deserialize(TIP6Network& value, NYson::TYsonPullParserCursor* cursor);
void Serialize(const TIP6Network& value, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

//! Performs asynchronous host name resolution.
class TAddressResolver
{
public:
    //! Returns the singleton instance.
    static TAddressResolver* Get();

    //! Resolves #address asynchronously.
    /*!
     *  Calls |getaddrinfo| and returns the first entry belonging to |AF_INET| or |AF_INET6| family.
     *  Caches successful resolutions.
     */
    TFuture<TNetworkAddress> Resolve(const TString& address);

    //! Returns the currently installed global DNS resolver.
    NDns::IDnsResolverPtr GetDnsResolver();

    //! Sets the global DNS resolver.
    void SetDnsResolver(NDns::IDnsResolverPtr dnsResolver);

    //! Sets localhost name up if it was not provided via |localhost_name_override| config section.
    //! Depending on |resolve_localhost_into_fqdn| option, localhost name is either additionally resolved
    //! into FQDN or not.
    void EnsureLocalHostName();

    //! Returns |true| if #address matches one of local host addresses.
    bool IsLocalAddress(const TNetworkAddress& address);

    //! Removes all cached resolutions.
    void PurgeCache();

    //! Updates resolver configuration.
    void Configure(TAddressResolverConfigPtr config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

    TAddressResolver();

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

//! Class representing IPv6 address in MTN model.
//! Refer to (https://wiki.yandex-team.ru/noc/newnetwork/hbf/projectid/)
//! for model description.
class TMtnAddress
{
public:
    TMtnAddress() = default;

    TMtnAddress(TIP6Address address);

    ui64 GetPrefix() const;
    TMtnAddress& SetPrefix(ui64 prefix);

    ui64 GetGeo() const;
    TMtnAddress& SetGeo(ui64 geo);

    ui64 GetProjectId() const;
    TMtnAddress& SetProjectId(ui64 projectId);

    ui64 GetHost() const;
    TMtnAddress& SetHost(ui64 host);

    const TIP6Address& ToIP6Address() const;

private:
    ui64 GetBytesRangeValue(int leftIndex, int rightIndex) const;

    void SetBytesRangeValue(int leftIndex, int rightIndex, ui64 value);

    static constexpr int HostOffsetInBytes = 0;
    static constexpr int ProjectIdOffsetInBytes = 4;
    static constexpr int GeoOffsetInBytes = 8;
    static constexpr int PrefixOffsetInBytes = 11;
    static constexpr int TotalLenInBytes = 16;

    TIP6Address Address_;
};

////////////////////////////////////////////////////////////////////////////////

// Expected format: sas1-5535-9d7.sas-test.yp.gencfg-c.yandex.net, or noqpmfiudzbb4hvs.man.yp-c.yandex.net.
// YP pod id must not contain a '.' in its name.
std::optional<TStringBuf> InferYPClusterFromHostNameRaw(TStringBuf hostName);
std::optional<TString> InferYPClusterFromHostName(TStringBuf hostName);

////////////////////////////////////////////////////////////////////////////////

std::optional<TStringBuf> InferYTClusterFromClusterUrlRaw(TStringBuf clusterUrl);
std::optional<TString> InferYTClusterFromClusterUrl(TStringBuf clusterUrl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet

template <>
struct THash<NYT::NNet::TNetworkAddress>
{
    size_t operator()(const NYT::NNet::TNetworkAddress& address) const;
};
