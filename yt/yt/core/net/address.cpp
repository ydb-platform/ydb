#include "address.h"

#include "local_address.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/dns/dns_resolver.h>
#include <yt/yt/core/dns/ares_dns_resolver.h>
#include <yt/yt/core/dns/private.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <util/generic/singleton.h>

#include <util/network/interface.h>

#include <util/string/hex.h>

#ifdef _win_
    #include <ws2ipdef.h>
    #include <winsock2.h>
#endif

#ifdef _unix_
    #include <ifaddrs.h>
    #include <netdb.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <sys/socket.h>
    #include <sys/un.h>
    #include <unistd.h>
#endif

namespace NYT::NNet {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NDns;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = NetLogger;

////////////////////////////////////////////////////////////////////////////////
// These are implemented in local_address.cpp.

// Update* function interacts with the system to determine actual hostname
// of the local machine (by calling `gethostname` and `getaddrinfo`).
// On success, calls Write* with the hostname, throws on errors.
void UpdateLocalHostName(const TAddressResolverConfigPtr& config);

// Updates the loopback address based on available configuration.
void UpdateLoopbackAddress(const TAddressResolverConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

TString BuildServiceAddress(TStringBuf hostName, int port)
{
    return Format("%v:%v", hostName, port);
}

void ParseServiceAddress(TStringBuf address, TStringBuf* hostName, int* port)
{
    auto colonIndex = address.find_last_of(':');
    if (colonIndex == TString::npos) {
        THROW_ERROR_EXCEPTION("Service address %Qv is malformed, <host>:<port> format is expected",
            address);
    }

    if (hostName) {
        *hostName = address.substr(0, colonIndex);
    }

    if (port) {
        try {
            *port = FromString<int>(address.substr(colonIndex + 1));
        } catch (const std::exception) {
            THROW_ERROR_EXCEPTION("Port number in service address %Qv is malformed",
                address);
        }
    }
}

int GetServicePort(TStringBuf address)
{
    int result;
    ParseServiceAddress(address, nullptr, &result);
    return result;
}

TStringBuf GetServiceHostName(TStringBuf address)
{
    TStringBuf result;
    ParseServiceAddress(address, &result, nullptr);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

const TNetworkAddress NullNetworkAddress;

TNetworkAddress::TNetworkAddress()
{
    Storage_ = {};
    Storage_.ss_family = AF_UNSPEC;
    Length_ = sizeof(Storage_);
}

TNetworkAddress::TNetworkAddress(const TNetworkAddress& other, int port)
{
    memcpy(&Storage_, &other.Storage_, sizeof(Storage_));
    switch (Storage_.ss_family) {
        case AF_INET:
            reinterpret_cast<sockaddr_in*>(&Storage_)->sin_port = htons(port);
            Length_ = sizeof(sockaddr_in);
            break;
        case AF_INET6:
            reinterpret_cast<sockaddr_in6*>(&Storage_)->sin6_port = htons(port);
            Length_ = sizeof(sockaddr_in6);
            break;
        default:
            THROW_ERROR_EXCEPTION("Unknown network address family")
                << TErrorAttribute("family", Storage_.ss_family);
    }
}

TNetworkAddress::TNetworkAddress(const sockaddr& other, socklen_t length)
{
    Length_ = length == 0 ? GetGenericLength(other) : length;
    memcpy(&Storage_, &other, Length_);
}

TNetworkAddress::TNetworkAddress(int family, const char* addr, size_t size)
{
    Storage_ = {};
    Storage_.ss_family = family;
    switch (Storage_.ss_family) {
        case AF_INET: {
            auto* typedSockAddr = reinterpret_cast<sockaddr_in*>(&Storage_);
            if (size > sizeof(sockaddr_in)) {
                THROW_ERROR_EXCEPTION("Wrong size of AF_INET address")
                    << TErrorAttribute("size", size);
            }
            memcpy(&typedSockAddr->sin_addr, addr, size);
            Length_ = sizeof(sockaddr_in);
            break;
        }
        case AF_INET6: {
            auto* typedSockAddr = reinterpret_cast<sockaddr_in6*>(&Storage_);
            if (size > sizeof(sockaddr_in6)) {
                THROW_ERROR_EXCEPTION("Wrong size of AF_INET6 address")
                    << TErrorAttribute("size", size);
            }
            memcpy(&typedSockAddr->sin6_addr, addr, size);
            Length_ = sizeof(sockaddr_in6);
            break;
        }
        default:
            THROW_ERROR_EXCEPTION("Unknown network address family")
                << TErrorAttribute("family", family);
    }
}

sockaddr* TNetworkAddress::GetSockAddr()
{
    return reinterpret_cast<sockaddr*>(&Storage_);
}

const sockaddr* TNetworkAddress::GetSockAddr() const
{
    return reinterpret_cast<const sockaddr*>(&Storage_);
}

socklen_t TNetworkAddress::GetGenericLength(const sockaddr& sockAddr)
{
    switch (sockAddr.sa_family) {
#ifdef _unix_
        case AF_UNIX:
            return sizeof(sockaddr_un);
#endif
        case AF_INET:
            return sizeof(sockaddr_in);
        case AF_INET6:
            return sizeof(sockaddr_in6);
        default:
            // Don't know its actual size, report the maximum possible.
            return sizeof(sockaddr_storage);
    }
}

int TNetworkAddress::GetPort() const
{
    switch (Storage_.ss_family) {
        case AF_INET:
            return ntohs(reinterpret_cast<const sockaddr_in*>(&Storage_)->sin_port);
        case AF_INET6:
            return ntohs(reinterpret_cast<const sockaddr_in6*>(&Storage_)->sin6_port);
        default:
            THROW_ERROR_EXCEPTION("Address has no port");
    }
}

bool TNetworkAddress::IsUnix() const
{
    return Storage_.ss_family == AF_UNIX;
}

bool TNetworkAddress::IsIP() const
{
    return IsIP4() || IsIP6();
}

bool TNetworkAddress::IsIP4() const
{
    return Storage_.ss_family == AF_INET;
}

bool TNetworkAddress::IsIP6() const
{
    return Storage_.ss_family == AF_INET6;
}

TIP6Address TNetworkAddress::ToIP6Address() const
{
    if (Storage_.ss_family != AF_INET6) {
        THROW_ERROR_EXCEPTION("Address is not an IPv6 address");
    }

    auto addr = reinterpret_cast<const sockaddr_in6*>(&Storage_)->sin6_addr;
    std::reverse(addr.s6_addr, addr.s6_addr + sizeof(addr));
    return TIP6Address::FromRawBytes(addr.s6_addr);
}

socklen_t TNetworkAddress::GetLength() const
{
    return Length_;
}

socklen_t* TNetworkAddress::GetLengthPtr()
{
    return &Length_;
}

TErrorOr<TNetworkAddress> TNetworkAddress::TryParse(TStringBuf address)
{
    TString ipAddress(address);
    std::optional<int> port;

    auto closingBracketIndex = address.find(']');
    if (closingBracketIndex != TString::npos) {
        if (closingBracketIndex == TString::npos || address.empty() || address[0] != '[') {
            return TError("Address %Qv is malformed, expected [<addr>]:<port> or [<addr>] format",
                address);
        }

        auto colonIndex = address.find(':', closingBracketIndex + 1);
        if (colonIndex != TString::npos) {
            try {
                port = FromString<int>(address.substr(colonIndex + 1));
            } catch (const std::exception) {
                return TError("Port number in address %Qv is malformed",
                    address);
            }
        }

        ipAddress = TString(address.substr(1, closingBracketIndex - 1));
    } else {
        if (address.find('.') != TString::npos) {
            auto colonIndex = address.find(':', closingBracketIndex + 1);
            if (colonIndex != TString::npos) {
                try {
                    port = FromString<int>(address.substr(colonIndex + 1));
                    ipAddress = TString(address.substr(0, colonIndex));
                } catch (const std::exception) {
                    return TError("Port number in address %Qv is malformed",
                        address);
                }
            }
        }
    }

    {
        // Try to parse as IPv4.
        struct sockaddr_in sa = {};
        if (inet_pton(AF_INET, ipAddress.c_str(), &sa.sin_addr) == 1) {
            if (port) {
                sa.sin_port = htons(*port);
            }
            sa.sin_family = AF_INET;
            return TNetworkAddress(*reinterpret_cast<sockaddr*>(&sa));
        }
    }
    {
        // Try to parse as IPv6.
        struct sockaddr_in6 sa = {};
        if (inet_pton(AF_INET6, ipAddress.c_str(), &(sa.sin6_addr))) {
            if (port) {
                sa.sin6_port = htons(*port);
            }
            sa.sin6_family = AF_INET6;
            return TNetworkAddress(*reinterpret_cast<sockaddr*>(&sa));
        }
    }

    return TError("Address %Qv is neither a valid IPv4 nor a valid IPv6 address",
        ipAddress);
}

TNetworkAddress TNetworkAddress::CreateIPv6Any(int port)
{
    sockaddr_in6 serverAddress = {};
    serverAddress.sin6_family = AF_INET6;
    serverAddress.sin6_addr = in6addr_any;
    serverAddress.sin6_port = htons(port);

    return TNetworkAddress(reinterpret_cast<const sockaddr&>(serverAddress), sizeof(serverAddress));
}

TNetworkAddress TNetworkAddress::CreateIPv6Loopback(int port)
{
    sockaddr_in6 serverAddress = {};
    serverAddress.sin6_family = AF_INET6;
    serverAddress.sin6_addr = in6addr_loopback;
    serverAddress.sin6_port = htons(port);

    return TNetworkAddress(reinterpret_cast<const sockaddr&>(serverAddress), sizeof(serverAddress));
}

TNetworkAddress TNetworkAddress::CreateUnixDomainSocketAddress(const TString& socketPath)
{
#ifdef _linux_
    // Abstract unix sockets are supported only on Linux.
    sockaddr_un sockAddr = {};
    if (socketPath.size() > sizeof(sockAddr.sun_path)) {
        THROW_ERROR_EXCEPTION("Unix domain socket path is too long")
            << TErrorAttribute("socket_path", socketPath)
            << TErrorAttribute("max_socket_path_length", sizeof(sockAddr.sun_path));
    }

    sockAddr.sun_family = AF_UNIX;
    memcpy(sockAddr.sun_path, socketPath.data(), socketPath.length());
    return TNetworkAddress(
        *reinterpret_cast<sockaddr*>(&sockAddr),
        sizeof(sockAddr.sun_family) +
        socketPath.length());
#else
    Y_UNUSED(socketPath);
    YT_ABORT();
#endif
}

TNetworkAddress TNetworkAddress::CreateAbstractUnixDomainSocketAddress(const TString& socketName)
{
    return CreateUnixDomainSocketAddress(TString("\0", 1) + socketName);
}

TNetworkAddress TNetworkAddress::Parse(TStringBuf address)
{
    return TryParse(address).ValueOrThrow();
}

void ToProto(TString* protoAddress, const TNetworkAddress& address)
{
    *protoAddress = TString(reinterpret_cast<const char*>(&address.Storage_), address.Length_);
}

void FromProto(TNetworkAddress* address, const TString& protoAddress)
{
    if (protoAddress.size() > sizeof(address->Storage_)) {
        THROW_ERROR_EXCEPTION("Network address size is too big")
            << TErrorAttribute("size", protoAddress.size());
    }
    address->Storage_ = {};
    memcpy(&address->Storage_, protoAddress.data(), protoAddress.size());
    address->Length_ = protoAddress.size();
}

void FormatValue(TStringBuilderBase* builder, const TNetworkAddress& address, TStringBuf spec)
{
    // TODO(arkady-e1ppa): Optimize.
    FormatValue(builder, ToString(address), spec);
}

TString ToString(const TNetworkAddress& address, const TNetworkAddressFormatOptions& options)
{
    const auto& sockAddr = address.GetSockAddr();

    const void* ipAddr;
    int port = 0;
    bool ipv6 = false;
    switch (sockAddr->sa_family) {
#ifdef _unix_
        case AF_UNIX: {
            const auto* typedAddr = reinterpret_cast<const sockaddr_un*>(sockAddr);
            //! See `man unix`.
            if (address.GetLength() == sizeof(sa_family_t)) {
                return "unix://[*unnamed*]";
            } else if (typedAddr->sun_path[0] == 0) {
                auto addressRef = TStringBuf(typedAddr->sun_path + 1, address.GetLength() - 1 - sizeof(sa_family_t));
                auto quoted = Format("%Qv", addressRef);
                return Format("unix://[%v]", quoted.substr(1, quoted.size() - 2));
            } else {
                auto addressRef = TString(typedAddr->sun_path, address.GetLength() - sizeof(sa_family_t));
                return Format("unix://%v", NFS::GetRealPath(addressRef));
            }
        }
#endif
        case AF_INET: {
            const auto* typedAddr = reinterpret_cast<const sockaddr_in*>(sockAddr);
            ipAddr = &typedAddr->sin_addr;
            port = typedAddr->sin_port;
            ipv6 = false;
            break;
        }
        case AF_INET6: {
            const auto* typedAddr = reinterpret_cast<const sockaddr_in6*>(sockAddr);
            ipAddr = &typedAddr->sin6_addr;
            port = typedAddr->sin6_port;
            ipv6 = true;
            break;
        }
        default:
            return Format("unknown://family(%v)", sockAddr->sa_family);
    }

    std::array<char, std::max(INET6_ADDRSTRLEN, INET_ADDRSTRLEN)> buffer;
    YT_VERIFY(inet_ntop(
        sockAddr->sa_family,
        const_cast<void*>(ipAddr),
        buffer.data(),
        buffer.size()));

    TStringBuilder result;
    if (options.IncludeTcpProtocol) {
        result.AppendString(TStringBuf("tcp://"));
    }

    bool withBrackets = ipv6 && (options.IncludeTcpProtocol || options.IncludePort);
    if (withBrackets) {
        result.AppendChar('[');
    }

    result.AppendString(buffer.data());

    if (withBrackets) {
        result.AppendChar(']');
    }

    if (options.IncludePort) {
        result.AppendFormat(":%v", ntohs(port));
    }

    return result.Flush();
}

bool operator == (const TNetworkAddress& lhs, const TNetworkAddress& rhs)
{
    auto lhsAddr = lhs.GetSockAddr();
    auto lhsSize = lhs.GetLength();

    auto rhsAddr = rhs.GetSockAddr();
    auto rhsSize = rhs.GetLength();

    TStringBuf rawLhs{reinterpret_cast<const char*>(lhsAddr), static_cast<size_t>(lhsSize)};
    TStringBuf rawRhs{reinterpret_cast<const char*>(rhsAddr), static_cast<size_t>(rhsSize)};

    return rawLhs == rawRhs;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Parse project id notation as described here:
//! https://wiki.yandex-team.ru/noc/newnetwork/hbf/projectid/#faervolnajazapissetevogoprefiksasprojectidirabotasnim
bool ParseProjectId(TStringBuf* str, std::optional<ui32>* projectId)
{
    auto pos = str->find('@');
    if (pos == TStringBuf::npos) {
        // Project id not specified.
        return true;
    }

    if (pos == 0 || pos > 8) {
        // Project id occupies 32 bits of address, so it must be between 1 and 8 hex digits.
        return false;
    }

    ui32 value = 0;
    for (int i = 0; i < static_cast<int>(pos); ++i) {
        int digit = Char2DigitTable[static_cast<unsigned char>((*str)[i])];
        if (digit == '\xff') {
            return false;
        }

        value <<= 4;
        value += digit;
    }

    *projectId = value;
    str->Skip(pos + 1);
    return true;
}

bool ParseIP6Address(TStringBuf* str, TIP6Address* address)
{
    auto tokenizeWord = [&] (ui16* word) -> bool {
        int partLen = 0;
        ui16 wordValue = 0;

        if (str->empty()) {
            return false;
        }

        while (partLen < 4 && !str->empty()) {
            int digit = Char2DigitTable[static_cast<unsigned char>((*str)[0])];
            if (digit == '\xff' && partLen == 0) {
                return false;
            }
            if (digit == '\xff') {
                break;
            }

            str->Skip(1);
            wordValue <<= 4;
            wordValue += digit;
            ++partLen;
        }

        *word = wordValue;
        return true;
    };

    bool beforeAbbrev = true;
    int wordIndex = 0;
    int wordsPushed = 0;

    auto words = address->GetRawWords();
    std::fill_n(address->GetRawBytes(), TIP6Address::ByteSize, 0);

    auto isEnd = [&] {
        return str->empty() || (*str)[0] == '/';
    };

    auto tokenizeAbbrev = [&] {
        if (str->size() >= 2 && (*str)[0] == ':' && (*str)[1] == ':') {
            str->Skip(2);
            return true;
        }
        return false;
    };

    if (tokenizeAbbrev()) {
        beforeAbbrev = false;
        ++wordIndex;
    }

    if (isEnd() && !beforeAbbrev) {
        return true;
    }

    while (true) {
        if (beforeAbbrev) {
            ui16 newWord = 0;
            if (!tokenizeWord(&newWord)) {
                return false;
            }

            words[7 - wordIndex] = newWord;
            ++wordIndex;
        } else {
            ui16 newWord = 0;
            if (!tokenizeWord(&newWord)) {
                return false;
            }

            std::copy_backward(words, words + wordsPushed, words + wordsPushed + 1);
            words[0] = newWord;
            ++wordsPushed;
        }

        // end of full address
        if (wordIndex + wordsPushed == 8) {
            break;
        }

        // end of abbreviated address
        if (isEnd() && !beforeAbbrev) {
            break;
        }

        // ':' or '::'
        if (beforeAbbrev && tokenizeAbbrev()) {
            beforeAbbrev = false;
            ++wordIndex;

            if (isEnd()) {
                break;
            }
        } else if (!str->empty() && (*str)[0] == ':') {
            str->Skip(1);
        } else {
            return false;
        }
    }

    return true;
}

bool ParseMask(TStringBuf buf, int* maskSize)
{
    if (buf.size() < 2 || buf[0] != '/') {
        return false;
    }

    *maskSize = 0;
    for (int i = 1; i < 4; ++i) {
        if (i == std::ssize(buf)) {
            return true;
        }

        if (buf[i] < '0' || '9' < buf[i]) {
            return false;
        }

        *maskSize = (*maskSize * 10) + (buf[i] - '0');
    }

    return buf.size() == 4 && *maskSize <= 128;
}

} // namespace

const ui8* TIP6Address::GetRawBytes() const
{
    return Raw_.data();
}

ui8* TIP6Address::GetRawBytes()
{
    return Raw_.data();
}

const ui16* TIP6Address::GetRawWords() const
{
    return reinterpret_cast<const ui16*>(GetRawBytes());
}

ui16* TIP6Address::GetRawWords()
{
    return reinterpret_cast<ui16*>(GetRawBytes());
}

const ui32* TIP6Address::GetRawDWords() const
{
    return reinterpret_cast<const ui32*>(GetRawBytes());
}

ui32* TIP6Address::GetRawDWords()
{
    return reinterpret_cast<ui32*>(GetRawBytes());
}

TIP6Address TIP6Address::FromRawBytes(const ui8* raw)
{
    TIP6Address result;
    ::memcpy(result.Raw_.data(), raw, ByteSize);
    return result;
}

TIP6Address TIP6Address::FromRawWords(const ui16* raw)
{
    return FromRawBytes(reinterpret_cast<const ui8*>(raw));
}

TIP6Address TIP6Address::FromRawDWords(const ui32* raw)
{
    return FromRawBytes(reinterpret_cast<const ui8*>(raw));
}

TIP6Address TIP6Address::FromString(TStringBuf str)
{
    TIP6Address result;
    if (!FromString(str, &result)) {
        THROW_ERROR_EXCEPTION("Error parsing IP6 address %Qv", str);
    }
    return result;
}

bool TIP6Address::FromString(TStringBuf str, TIP6Address* address)
{
    TStringBuf buf = str;
    if (!ParseIP6Address(&buf, address) || !buf.empty()) {
        return false;
    }
    return true;
}

void FormatValue(TStringBuilderBase* builder, const TIP6Address& address, TStringBuf /*spec*/)
{
    const auto* parts = reinterpret_cast<const ui16*>(address.GetRawBytes());
    std::pair<int, int> maxRun = {-1, -1};
    int start = -1;
    int end = -1;
    auto endRun = [&] {
        if ((end - start) >= (maxRun.second - maxRun.first) && (end - start) > 1) {
            maxRun = {start, end};
        }

        start = -1;
        end = -1;
    };

    for (int index = 0; index < 8; ++index) {
        if (parts[index] == 0) {
            if (start == -1) {
                start = index;
            }
            end = index + 1;
        } else {
            endRun();
        }
    }
    endRun();

    for (int index = 7; index >= 0; --index) {
        if (maxRun.first <= index && index < maxRun.second) {
            if (index == maxRun.first) {
                builder->AppendChar(':');
                builder->AppendChar(':');
            }
        } else {
            if (index != 7 && index + 1 != maxRun.first) {
                builder->AppendChar(':');
            }
            builder->AppendFormat("%x", parts[index]);
        }
    }
}

bool operator == (const TIP6Address& lhs, const TIP6Address& rhs)
{
    return ::memcmp(lhs.GetRawBytes(), rhs.GetRawBytes(), TIP6Address::ByteSize) == 0;
}

TIP6Address operator|(const TIP6Address& lhs, const TIP6Address& rhs)
{
    auto result = lhs;
    result |= rhs;
    return result;
}

TIP6Address operator&(const TIP6Address& lhs, const TIP6Address& rhs)
{
    auto result = lhs;
    result &= rhs;
    return result;
}

TIP6Address& operator|=(TIP6Address& lhs, const TIP6Address& rhs)
{
    *reinterpret_cast<ui64*>(lhs.GetRawBytes()) |= *reinterpret_cast<const ui64*>(rhs.GetRawBytes());
    *reinterpret_cast<ui64*>(lhs.GetRawBytes() + 8) |= *reinterpret_cast<const ui64*>(rhs.GetRawBytes() + 8);
    return lhs;
}

TIP6Address& operator&=(TIP6Address& lhs, const TIP6Address& rhs)
{
    *reinterpret_cast<ui64*>(lhs.GetRawBytes()) &= *reinterpret_cast<const ui64*>(rhs.GetRawBytes());
    *reinterpret_cast<ui64*>(lhs.GetRawBytes() + 8) &= *reinterpret_cast<const ui64*>(rhs.GetRawBytes() + 8);
    return lhs;
}

void Deserialize(TIP6Address& value, INodePtr node)
{
    value = TIP6Address::FromString(node->AsString()->GetValue());
}

void Deserialize(TIP6Address& value, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("TIP6Address", *cursor, EYsonItemType::StringValue);
    value = TIP6Address::FromString((*cursor)->UncheckedAsString());
    cursor->Next();
}

void Serialize(const TIP6Address& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(ToString(value));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

int GetMaskSize(const TIP6Address& mask)
{
    int size = 0;
    const auto* parts = mask.GetRawDWords();
    for (size_t partIndex = 0; partIndex < 4; ++partIndex) {
        size += __builtin_popcount(parts[partIndex]);
    }
    return size;
}

} // namespace

TIP6Network::TIP6Network(const TIP6Address& network, const TIP6Address& mask)
    : Network_(network)
    , Mask_(mask)
{
    // We allow only masks that can be written by using prefix notation,
    // e.g. /64. In prefix notation no ones can go after zeros.
    bool seenOne = false;
    const auto* bytes = mask.GetRawBytes();
    for (int i = 0; i < static_cast<int>(TIP6Address::ByteSize * 8); ++i) {
        auto val = *(bytes + i / 8) & (1 << (i % 8));
        if (val) {
            seenOne = true;
        } else {
            if (seenOne) {
                THROW_ERROR_EXCEPTION("Invalid network mask %Qv", mask);
            }
        }
    }
}

const TIP6Address& TIP6Network::GetAddress() const
{
    return Network_;
}

const TIP6Address& TIP6Network::GetMask() const
{
    return Mask_;
}

int TIP6Network::GetMaskSize() const
{
    return ::NYT::NNet::GetMaskSize(Mask_);
}

std::optional<ui32> TIP6Network::GetProjectId() const
{
    return ProjectId_;
}

bool TIP6Network::Contains(const TIP6Address& address) const
{
    TIP6Address masked = address;
    masked &= Mask_;
    return masked == Network_;
}

TIP6Network TIP6Network::FromString(TStringBuf str)
{
    TIP6Network network;
    if (!FromString(str, &network)) {
        THROW_ERROR_EXCEPTION("Error parsing IP6 network %Qv", str);
    }
    return network;
}

bool TIP6Network::FromString(TStringBuf str, TIP6Network* network)
{
    auto buf = str;
    if (!ParseProjectId(&buf, &network->ProjectId_)) {
        return false;
    }

    if (!ParseIP6Address(&buf, &network->Network_)) {
        return false;
    }

    int maskSize = 0;
    if (!ParseMask(buf, &maskSize)) {
        return false;
    }

    // Set mask based on mask size.
    network->Mask_ = TIP6Address();
    auto* bytes = network->Mask_.GetRawBytes();
    for (int i = 0; i < static_cast<int>(TIP6Address::ByteSize * 8); ++i) {
        if (i >= static_cast<int>(TIP6Address::ByteSize * 8) - maskSize) {
            *(bytes + i / 8) |= (1 << (i % 8));
        } else {
            *(bytes + i / 8) &= ~(1 << (i % 8));
        }
    }

    if (network->ProjectId_) {
        static_assert(TIP6Address::ByteSize == 16);

        // Set 64-95 bits of address to ::::[project_id_low:project_id_high]::
        network->Network_.GetRawWords()[2] = *network->ProjectId_;
        network->Network_.GetRawWords()[3] = *network->ProjectId_ >> 16;

        // Set 64-95 bits of mask to ::::[ffff:ffff]::
        network->Mask_.GetRawWords()[2] = 0xffff;
        network->Mask_.GetRawWords()[3] = 0xffff;
    }

    return true;
}

void FormatValue(TStringBuilderBase* builder, const TIP6Network& network, TStringBuf /*spec*/)
{
    if (auto projectId = network.GetProjectId()) {
        // The network has been created from string in
        // project id notation. Save it just the way it came.

        // Recover original network address.
        auto address = network.GetAddress();
        address.GetRawWords()[2] = 0;
        address.GetRawWords()[3] = 0;

        // Recover original mask.
        auto mask = network.GetMask();
        mask.GetRawWords()[2] = 0;
        mask.GetRawWords()[3] = 0;

        builder->AppendFormat("%x@%v/%v",
            *projectId,
            address,
            GetMaskSize(mask));
    } else {
        builder->AppendFormat("%v/%v",
            network.GetAddress(),
            network.GetMaskSize());
    }
}

void Deserialize(TIP6Network& value, INodePtr node)
{
    value = TIP6Network::FromString(node->AsString()->GetValue());
}

void Deserialize(TIP6Network& value, NYson::TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("TIP6Network", *cursor, EYsonItemType::StringValue);
    value = TIP6Network::FromString((*cursor)->UncheckedAsString());
    cursor->Next();
}

void Serialize(const TIP6Network& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(ToString(value));
}

////////////////////////////////////////////////////////////////////////////////

//! Performs asynchronous host name resolution.
class TAddressResolver::TImpl
    : public virtual TRefCounted
    , private TAsyncExpiringCache<TString, TNetworkAddress>
{
public:
    explicit TImpl(TAddressResolverConfigPtr config)
        : TAsyncExpiringCache(
            config,
            /*logger*/ {},
            DnsProfiler.WithPrefix("/resolve_cache"))
    {
        Configure(std::move(config));
    }

    TFuture<TNetworkAddress> Resolve(const TString& hostName)
    {
        // Check if |address| parses into a valid IPv4 or IPv6 address.
        if (auto result = TNetworkAddress::TryParse(hostName); result.IsOK()) {
            return MakeFuture(result);
        }

        // Run async resolution.
        return Get(hostName);
    }

    IDnsResolverPtr GetDnsResolver()
    {
        return DnsResolver_.Acquire();
    }

    void SetDnsResolver(IDnsResolverPtr dnsResolver)
    {
        DnsResolver_.Store(std::move(dnsResolver));
    }

    void EnsureLocalHostName()
    {
        if (Config_->LocalHostNameOverride) {
            return;
        }

        UpdateLocalHostName(Config_);

        YT_LOG_INFO("Localhost name determined via system call (LocalHostName: %v, ResolveHostNameIntoFqdn: %v)",
            GetLocalHostName(),
            Config_->ResolveHostNameIntoFqdn);
    }

    bool IsLocalAddress(const TNetworkAddress& address)
    {
        TNetworkAddress localIP{address, 0};

        const auto& localAddresses = GetLocalAddresses();
        return std::find(localAddresses.begin(), localAddresses.end(), localIP) != localAddresses.end();
    }

    void PurgeCache()
    {
        Clear();
        YT_LOG_INFO("Address cache purged");
    }

    void Configure(TAddressResolverConfigPtr config)
    {
        Config_ = std::move(config);

        SetDnsResolver(CreateAresDnsResolver(Config_));
        TAsyncExpiringCache::Reconfigure(Config_);

        if (Config_->LocalHostNameOverride) {
            WriteLocalHostName(*Config_->LocalHostNameOverride);
            YT_LOG_INFO("Localhost name configured via config override (LocalHostName: %v)",
                Config_->LocalHostNameOverride);
        }

        UpdateLoopbackAddress(Config_);
    }

private:
    TAddressResolverConfigPtr Config_;

    std::atomic<bool> HasCachedLocalAddresses_ = false;
    std::vector<TNetworkAddress> CachedLocalAddresses_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CacheLock_);

    const TActionQueuePtr Queue_ = New<TActionQueue>("AddressResolver");

    TAtomicIntrusivePtr<IDnsResolver> DnsResolver_;

    TFuture<TNetworkAddress> DoGet(const TString& hostName, bool /*isPeriodicUpdate*/) noexcept override
    {
        TDnsResolveOptions options{
            .EnableIPv4 = Config_->EnableIPv4,
            .EnableIPv6 = Config_->EnableIPv6,
        };
        return GetDnsResolver()->Resolve(hostName, options)
            .Apply(BIND([=] (const TErrorOr<TNetworkAddress>& result) {
                // Empty callback just to forward future callbacks into proper thread.
                return result.ValueOrThrow();
            })
            .AsyncVia(Queue_->GetInvoker()));
    }

    const std::vector<TNetworkAddress>& GetLocalAddresses()
    {
        if (HasCachedLocalAddresses_) {
            return CachedLocalAddresses_;
        }

        std::vector<TNetworkAddress> localAddresses;
        for (const auto& interface : NAddr::GetNetworkInterfaces()) {
            localAddresses.push_back(TNetworkAddress(*interface.Address->Addr()));
        }

        {
            auto guard = WriterGuard(CacheLock_);
            // NB: Only update CachedLocalAddresses_ once.
            if (!HasCachedLocalAddresses_) {
                CachedLocalAddresses_ = std::move(localAddresses);
                HasCachedLocalAddresses_ = true;
            }
        }

        return CachedLocalAddresses_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TAddressResolver::TAddressResolver()
    : Impl_(New<TImpl>(New<TAddressResolverConfig>()))
{ }

TAddressResolver* TAddressResolver::Get()
{
    return LeakySingleton<TAddressResolver>();
}

TFuture<TNetworkAddress> TAddressResolver::Resolve(const TString& address)
{
    return Impl_->Resolve(address);
}

IDnsResolverPtr TAddressResolver::GetDnsResolver()
{
    return Impl_->GetDnsResolver();
}

void TAddressResolver::SetDnsResolver(IDnsResolverPtr dnsResolver)
{
    Impl_->SetDnsResolver(std::move(dnsResolver));
}

void TAddressResolver::EnsureLocalHostName()
{
    return Impl_->EnsureLocalHostName();
}

bool TAddressResolver::IsLocalAddress(const TNetworkAddress& address)
{
    return Impl_->IsLocalAddress(address);
}

void TAddressResolver::PurgeCache()
{
    return Impl_->PurgeCache();
}

void TAddressResolver::Configure(TAddressResolverConfigPtr config)
{
    return Impl_->Configure(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

TMtnAddress::TMtnAddress(TIP6Address address)
    : Address_(address)
{ }

ui64 TMtnAddress::GetPrefix() const
{
    return GetBytesRangeValue(PrefixOffsetInBytes, TotalLenInBytes);
}

TMtnAddress& TMtnAddress::SetPrefix(ui64 prefix)
{
    SetBytesRangeValue(PrefixOffsetInBytes, TotalLenInBytes, prefix);
    return *this;
}

ui64 TMtnAddress::GetGeo() const
{
    return GetBytesRangeValue(GeoOffsetInBytes, PrefixOffsetInBytes);
}

TMtnAddress& TMtnAddress::SetGeo(ui64 geo)
{
    SetBytesRangeValue(GeoOffsetInBytes, PrefixOffsetInBytes, geo);
    return *this;
}

ui64 TMtnAddress::GetProjectId() const
{
    return GetBytesRangeValue(ProjectIdOffsetInBytes, GeoOffsetInBytes);
}

TMtnAddress& TMtnAddress::SetProjectId(ui64 projectId)
{
    SetBytesRangeValue(ProjectIdOffsetInBytes, GeoOffsetInBytes, projectId);
    return *this;
}

ui64 TMtnAddress::GetHost() const
{
    return GetBytesRangeValue(HostOffsetInBytes, ProjectIdOffsetInBytes);
}

TMtnAddress& TMtnAddress::SetHost(ui64 host)
{
    SetBytesRangeValue(HostOffsetInBytes, ProjectIdOffsetInBytes, host);
    return *this;
}

const TIP6Address& TMtnAddress::ToIP6Address() const
{
    return Address_;
}

ui64 TMtnAddress::GetBytesRangeValue(int leftIndex, int rightIndex) const
{
    if (leftIndex > rightIndex) {
        THROW_ERROR_EXCEPTION("Left index is greater than right index (LeftIndex: %v, RightIndex: %v)",
            leftIndex,
            rightIndex);
    }

    const auto* addressBytes = Address_.GetRawBytes();

    ui64 result = 0;
    for (int index = rightIndex - 1; index >= leftIndex; --index) {
        result = (result << 8) | addressBytes[index];
    }
    return result;
}

void TMtnAddress::SetBytesRangeValue(int leftIndex, int rightIndex, ui64 value)
{
    if (leftIndex > rightIndex) {
        THROW_ERROR_EXCEPTION("Left index is greater than right index (LeftIndex: %v, RightIndex: %v)",
            leftIndex,
            rightIndex);
    }

    auto bytesInRange = rightIndex - leftIndex;
    if (value >= (1ull << (8 * bytesInRange))) {
        THROW_ERROR_EXCEPTION("Value is too large to be set in [leftIndex; rightIndex) interval (LeftIndex: %v, RightIndex: %v, Value %v)",
            leftIndex,
            rightIndex,
            value);
    }

    auto* addressBytes = Address_.GetRawBytes();
    for (int index = leftIndex; index < rightIndex; ++index) {
        addressBytes[index] = value & 255;
        value >>= 8;
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MaxYPClusterNameSize = 32;

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::optional<TStringBuf> InferYPClusterFromHostNameRaw(TStringBuf hostName)
{
    auto start = hostName.find_first_of('.');
    if (start == TStringBuf::npos) {
        return {};
    }
    auto end = hostName.find_first_of('.', start + 1);
    if (end == TStringBuf::npos) {
        return {};
    }
    auto cluster = hostName.substr(start + 1, end - start - 1);
    if (cluster.empty()) {
        return {};
    }
    if (cluster.length() > MaxYPClusterNameSize) {
        return {};
    }
    return {cluster};
}

std::optional<TString> InferYPClusterFromHostName(TStringBuf hostName)
{
    if (auto rawResult = InferYPClusterFromHostNameRaw(hostName)) {
        return TString{*rawResult};
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TStringBuf> InferYTClusterFromClusterUrlRaw(TStringBuf clusterUrl)
{
    clusterUrl.SkipPrefix("http://");
    clusterUrl.ChopSuffix(".yt.yandex.net");

    if (clusterUrl.find("localhost") != TStringBuf::npos || clusterUrl.find_first_of(".:/") != TStringBuf::npos) {
        return {};
    }

    return clusterUrl;
}

std::optional<TString> InferYTClusterFromClusterUrl(TStringBuf clusterUrl)
{
    if (auto rawResult = InferYTClusterFromClusterUrlRaw(clusterUrl)) {
        return TString{*rawResult};
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet

size_t THash<NYT::NNet::TNetworkAddress>::operator()(const NYT::NNet::TNetworkAddress& address) const
{
    TStringBuf rawAddress{
        reinterpret_cast<const char*>(address.GetSockAddr()),
        static_cast<size_t>(address.GetLength())};
    return ComputeHash(rawAddress);
}
