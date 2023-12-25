#pragma once

#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/network/ip.h>
#include <util/stream/input.h>

#include <library/cpp/int128/int128.h>

#if defined(_freebsd_)
// #include required to avoid problem with undefined 'socklen_t' on FreeBSD
#include <sys/socket.h>
#endif

#if defined(_win_)
// #include required to avoid problem with undefined 'socklen_t' on Windows
#include <util/network/socket.h>
#endif

namespace NAddr {
    class IRemoteAddr;
}
struct in6_addr;
struct in_addr;
struct sockaddr;
struct sockaddr_in;
struct sockaddr_in6;

// TODO (dimanne): rename to something like TIntInetAddress or THostAddress
class TIpv6Address {
public:
    enum TIpType { Ipv6,
                   Ipv4,
                   LAST };

    constexpr TIpv6Address() noexcept = default;
    constexpr TIpv6Address(const TIpv6Address&) noexcept = default;
    constexpr TIpv6Address& operator=(const TIpv6Address&) noexcept = default;

    constexpr TIpv6Address(const ui128& ip, TIpType Type) noexcept
        : Ip(ip), Type_(Type)
    {}

    constexpr TIpv6Address(ui8 a, ui8 b, ui8 c, ui8 d) noexcept
        : Ip((ui32(a) << 24) | (ui32(b) << 16) | (ui32(c) << 8) | ui32(d))
        , Type_(TIpv6Address::Ipv4)
    {}

    constexpr TIpv6Address(ui16 a, ui16 b, ui16 c, ui16 d, ui16 e, ui16 f, ui16 g, ui16 h, ui32 scope = 0) noexcept
        : Type_(TIpv6Address::Ipv6)
        , ScopeId_(scope)
    {
        auto hi = (ui64(a) << 48) | (ui64(b) << 32) | (ui64(c) << 16) | ui64(d);
        auto lo = (ui64(e) << 48) | (ui64(f) << 32) | (ui64(g) << 16) | ui64(h);
        Ip = {hi, lo};
    }

    explicit TIpv6Address(const NAddr::IRemoteAddr& addr);
    explicit TIpv6Address(const sockaddr_in6& Addr);
    explicit TIpv6Address(const sockaddr_in& Addr);
    explicit TIpv6Address(const in6_addr& addr, ui32 Scope);
    explicit TIpv6Address(const in_addr& addr);

    static TIpv6Address FromString(TStringBuf srcStr, bool& ok) noexcept;
    static TIpv6Address FromString(TStringBuf str) noexcept;

    constexpr bool IsNull() const noexcept {
        return Ip == 0;
    }

    constexpr bool IsValid() const noexcept {
        return Ip != 0 && (Type_ == Ipv6 || Type_ == Ipv4);
    }

    explicit constexpr operator bool() const noexcept {
        return IsValid();
    }

    constexpr bool operator ! () const noexcept {
        return !IsValid();
    }

    bool Isv4MappedTov6() const noexcept;
    TIpv6Address TryToExtractIpv4From6() const noexcept;
    TIpv6Address Normalized() const noexcept;

    TString ToString(bool* ok = nullptr) const noexcept;
    TString ToString(bool PrintScopeId, bool* ok = nullptr) const noexcept;

    void ToSockaddrAndSocklen(sockaddr_in& sockAddrIPv4,
                              sockaddr_in6& sockAddrIPv6, // in
                              const sockaddr*& sockAddrPtr,
                              socklen_t& sockAddrSize,
                              ui16 Port) const; // out

    void ToInAddr(in_addr& Addr4) const;
    void ToIn6Addr(in6_addr& Addr6) const;
    // int SocketFamily() const;

    constexpr bool operator<(const TIpv6Address& other) const noexcept {
        if (Type_ != other.Type_)
            return Type_ > other.Type_;
        else
            return Ip < other.Ip;
    }
    constexpr bool operator>(const TIpv6Address& other) const noexcept {
        if (Type_ != other.Type_)
            return Type_ < other.Type_;
        else
            return Ip > other.Ip;
    }
    constexpr bool operator==(const TIpv6Address& other) const noexcept {
        return Type_ == other.Type_ && Ip == other.Ip;
    }
    constexpr bool operator!=(const TIpv6Address& other) const noexcept {
        return Type_ != other.Type_ || Ip != other.Ip;
    }

    constexpr bool operator<=(const TIpv6Address& other) const noexcept {
        return !(*this > other);
    }

    constexpr bool operator>=(const TIpv6Address& other) const noexcept {
        return !(*this < other);
    }

    constexpr operator ui128() const noexcept {
        return Ip;
    }

    constexpr TIpType Type() const noexcept {
        return Type_;
    }

    void SetScopeId(ui32 New) noexcept {
        ScopeId_ = New;
    }
    constexpr ui32 ScopeId() const noexcept {
        return ScopeId_;
    }

    void Save(IOutputStream* out) const;
    void Load(IInputStream* in);

private:
    void InitFrom(const in6_addr& addr);
    void InitFrom(const in_addr& addr);

    void InitFrom(const sockaddr_in6& Addr);
    void InitFrom(const sockaddr_in& Addr);

    ui128 Ip{};
    TIpType Type_ = LAST;
    ui32 ScopeId_ = 0;
};
IOutputStream& operator<<(IOutputStream& Out, const TIpv6Address::TIpType Type) noexcept;
IOutputStream& operator<<(IOutputStream& Out, const TIpv6Address& ipv6Address) noexcept;

constexpr TIpv6Address Get127001() noexcept {
    return {127, 0, 0, 1};
}

constexpr TIpv6Address Get1() noexcept {
    return {1, TIpv6Address::Ipv6};
}

struct THostAddressAndPort {
    constexpr THostAddressAndPort() noexcept = default;
    constexpr THostAddressAndPort(const TIpv6Address& i, TIpPort p) noexcept {
        Ip = i;
        Port = p;
    }

    constexpr bool operator==(const THostAddressAndPort& Other) const noexcept {
        return Ip == Other.Ip && Port == Other.Port;
    }
    constexpr bool operator!=(const THostAddressAndPort& Other) const noexcept {
        return !(*this == Other);
    }
    constexpr bool IsValid() const noexcept {
        return Ip.IsValid() && Port != 0;
    }

    TString ToString() const noexcept;

    TIpv6Address Ip {};
    TIpPort Port {0};
};
IOutputStream& operator<<(IOutputStream& Out, const THostAddressAndPort& HostAddressAndPort) noexcept;

///
/// Returns
///   1. either valid THostAddressAndPort
///   2. or TString with hostname (which you should resolve) and TIpPort with port
///   3. or error, if Ok == false
///
/// Supported RawStrs are
///   1.2.3.4          // port wil be equal to DefaultPort
///   1.2.3.4:80
///   [2001::7348]     // port wil be equal to DefaultPort
///   2001::7348       // port wil be equal to DefaultPort
///   [2001::7348]:80
///
std::tuple<THostAddressAndPort, TString, TIpPort> ParseHostAndMayBePortFromString(const TStringBuf RawStr,
                                                                                  TIpPort DefaultPort,
                                                                                  bool& Ok) noexcept;

using TIpv6AddressesSet = THashSet<TIpv6Address>;

template <>
struct THash<TIpv6Address> {
    inline size_t operator()(const TIpv6Address& ip) const {
        const ui128& Tmp = static_cast<ui128>(ip);
        return CombineHashes(THash<ui128>()(Tmp), THash<ui8>()(static_cast<ui8>(ip.Type())));
    }
};
template <>
struct THash<THostAddressAndPort> {
    inline size_t operator()(const THostAddressAndPort& IpAndPort) const {
        return CombineHashes(THash<TIpv6Address>()(IpAndPort.Ip), THash<TIpPort>()(IpAndPort.Port));
    }
};

namespace std {
    template <>
    struct hash<TIpv6Address> {
        std::size_t operator()(const TIpv6Address& Ip) const noexcept {
            return THash<TIpv6Address>()(Ip);
        }
    };
}

NAddr::IRemoteAddr* ToIRemoteAddr(const TIpv6Address& Address, TIpPort Port);

// template <>
// class TSerializer<TIpv6Address> {
// public:
//    static void Save(IOutputStream *out, const TIpv6Address &ip);
//    static void Load(IInputStream *in, TIpv6Address &ip);
//};
