#include "ipv6_address.h"
#include "ipv6_address_p.h"

#ifdef _unix_
#include <netinet/in.h>
#endif

#include <util/network/address.h>
#include <util/network/init.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/byteorder.h>
#include <util/ysaveload.h>

#include <array>

namespace {
    // reinterpret_cast from memory, where most significant bit is first
    inline ui128 FromMemMSF(const char* memPtr) {
        Y_ABORT_UNLESS(memPtr, " ");
        return ui128{
            *reinterpret_cast<const ui64*>(memPtr),
            *(reinterpret_cast<const ui64*>(memPtr) + 1)
        };
    }

    // zero-terminated copy of address string
    template <size_t N>
    inline auto AddrBuf(TStringBuf str) noexcept {
        std::array<char, N+1> res;
        auto len = Min(str.size(), N);
        CopyN(str.begin(), len, res.begin());
        res[len] = '\0';
        return res;
    }
}

void TIpv6Address::InitFrom(const in6_addr& addr) {
    const ui64* const ui64Ptr = reinterpret_cast<const ui64*>(&addr);
    const ui64 raw[2] = {SwapBytes(*ui64Ptr), SwapBytes(*(ui64Ptr + 1))};
    Ip = FromMemMSF(reinterpret_cast<const char*>(raw));
    Type_ = Ipv6;
}
void TIpv6Address::InitFrom(const in_addr& addr) {
    unsigned long swapped = SwapBytes(addr.s_addr);
    Ip = ui128{0, swapped};
    Type_ = Ipv4;
}

void TIpv6Address::InitFrom(const sockaddr_in6& Addr) {
    InitFrom(Addr.sin6_addr);
    ScopeId_ = Addr.sin6_scope_id;
}
void TIpv6Address::InitFrom(const sockaddr_in& Addr) {
    InitFrom(Addr.sin_addr);
}

TIpv6Address::TIpv6Address(const NAddr::IRemoteAddr& addr) {
    if (addr.Addr()->sa_family == AF_INET) { // IPv4
        const sockaddr_in* Tmp = reinterpret_cast<const sockaddr_in*>(addr.Addr());
        InitFrom(*Tmp);
    } else { // IPv6
        const sockaddr_in6* Tmp = reinterpret_cast<const sockaddr_in6*>(addr.Addr());
        InitFrom(*Tmp);
    }
}
TIpv6Address::TIpv6Address(const sockaddr_in6& Addr) {
    InitFrom(Addr);
}
TIpv6Address::TIpv6Address(const sockaddr_in& Addr) {
    InitFrom(Addr);
}
TIpv6Address::TIpv6Address(const in6_addr& addr, ui32 Scope) {
    InitFrom(addr);
    ScopeId_ = Scope;
}
TIpv6Address::TIpv6Address(const in_addr& addr) {
    InitFrom(addr);
}

TIpv6Address TIpv6Address::FromString(TStringBuf str, bool& ok) noexcept {
    const TIpType ipType = FigureOutType(str);

    if (ipType == Ipv6) {
        ui32 scopeId = 0;
        if (size_t pos = str.find('%'); pos != TStringBuf::npos) {
            ::TryFromString(str.substr(pos + 1), scopeId);
            str.Trunc(pos);
        }

        const auto buf = AddrBuf<INET6_ADDRSTRLEN>(str);
        in6_addr addr;
        if (inet_pton(AF_INET6, buf.data(), &addr) != 1) {
            ok = false;
            return TIpv6Address();
        }

        ok = true;
        return TIpv6Address(addr, scopeId);
    } else { // if (ipType == Ipv4) {
        const auto buf = AddrBuf<INET_ADDRSTRLEN>(str);
        in_addr addr;
        if (inet_pton(AF_INET, buf.data(), &addr) != 1) {
            ok = false;
            return TIpv6Address();
        }

        ok = true;
        return TIpv6Address(addr);
    }
}

TIpv6Address TIpv6Address::FromString(TStringBuf str) noexcept {
    bool ok = false;
    return TIpv6Address::FromString(str, ok);
}

TString TIpv6Address::ToString(bool* ok) const noexcept {
    return ToString(true, ok);
}
TString TIpv6Address::ToString(bool PrintScopeId, bool* ok) const noexcept {
    TString result;
    bool isOk = true;

    if (Type_ == TIpv6Address::Ipv4) {
        result.resize(INET_ADDRSTRLEN + 2);
        in_addr addr;
        ToInAddr(addr);
        isOk = inet_ntop(AF_INET, &addr, result.begin(), INET_ADDRSTRLEN);
        result.resize(result.find('\0'));
    } else if (Type_ == TIpv6Address::Ipv6) {
        result.resize(INET6_ADDRSTRLEN + 2);
        in6_addr addr;
        ToIn6Addr(addr);
        isOk = inet_ntop(AF_INET6, &addr, result.begin(), INET6_ADDRSTRLEN);
        result.resize(result.find('\0'));
        if (PrintScopeId)
            result += "%" + ::ToString(ScopeId_);
    } else {
        result = "null";
        isOk = true;
    }

    if (ok) {
        *ok = isOk;
    }

    return result;
}

void TIpv6Address::ToSockaddrAndSocklen(sockaddr_in& sockAddrIPv4,
                                        sockaddr_in6& sockAddrIPv6, // in
                                        const sockaddr*& sockAddrPtr,
                                        socklen_t& sockAddrSize,
                                        ui16 Port) const { // out

    if (Type_ == Ipv4) {
        memset(&sockAddrIPv4, 0, sizeof(sockAddrIPv4));
        sockAddrIPv4.sin_family = AF_INET;
        sockAddrIPv4.sin_port = htons(Port);
        ToInAddr(sockAddrIPv4.sin_addr);

        sockAddrSize = sizeof(sockAddrIPv4);
        sockAddrPtr = reinterpret_cast<sockaddr*>(&sockAddrIPv4);

    } else if (Type_ == Ipv6) {
        memset(&sockAddrIPv6, 0, sizeof(sockAddrIPv6));
        sockAddrIPv6.sin6_family = AF_INET6;
        sockAddrIPv6.sin6_port = htons(Port);
        ToIn6Addr(sockAddrIPv6.sin6_addr);
        sockAddrIPv6.sin6_scope_id = ScopeId_;
        sockAddrIPv6.sin6_flowinfo = 0;

        sockAddrSize = sizeof(sockAddrIPv6);
        sockAddrPtr = reinterpret_cast<sockaddr*>(&sockAddrIPv6);
    } else
        Y_ABORT_UNLESS(false);
}

void TIpv6Address::ToInAddr(in_addr& Addr4) const {
    Y_ABORT_UNLESS(Type_ == TIpv6Address::Ipv4);

    Zero(Addr4);
    ui32 Value = GetLow(Ip);
    Y_ABORT_UNLESS(Value == GetLow(Ip), " ");
    Y_ABORT_UNLESS(GetHigh(Ip) == 0, " ");
    Addr4.s_addr = SwapBytes(Value);
}
void TIpv6Address::ToIn6Addr(in6_addr& Addr6) const {
    Y_ABORT_UNLESS(Type_ == TIpv6Address::Ipv6);

    Zero(Addr6);
    ui64 Raw[2] = {GetHigh(Ip), GetLow(Ip)};
    *Raw = SwapBytes(*Raw);
    Raw[1] = SwapBytes(1 [Raw]);
    memcpy(&Addr6, Raw, sizeof(Raw));
}

void TIpv6Address::Save(IOutputStream* out) const {
    ::Save(out, Ip);
    ::Save(out, static_cast<ui8>(Type_));
    ::Save(out, ScopeId_);
}
void TIpv6Address::Load(IInputStream* in) {
    ::Load(in, Ip);
    ui8 num;
    ::Load(in, num);
    Type_ = static_cast<TIpType>(num);
    ::Load(in, ScopeId_);
}

bool TIpv6Address::Isv4MappedTov6() const noexcept {
    /// http://en.wikipedia.org/wiki/IPv6
    /// Hybrid dual-stack IPv6/IPv4 implementations recognize a special class of addresses,
    /// the IPv4-mapped IPv6 addresses.  In these addresses, the first 80 bits are zero, the next 16 bits are one,
    /// and the remaining 32 bits are the IPv4 address.

    if (Type_ != Ipv6)
        return false;

    if (GetHigh(Ip) != 0)
        return false; // First 64 bit are not zero -> it is not ipv4-mapped-ipv6 address

    const ui64 Low = GetLow(Ip) >> 32;
    if (Low != 0x0000ffff)
        return false;

    return true;
}

TIpv6Address TIpv6Address::TryToExtractIpv4From6() const noexcept {
    if (Isv4MappedTov6() == false)
        return TIpv6Address();

    const ui64 NewLow = GetLow(Ip) & 0x00000000ffffffff;
    TIpv6Address Result(ui128(0, NewLow), Ipv4);
    return Result;
}

TIpv6Address TIpv6Address::Normalized() const noexcept {
    if (Isv4MappedTov6() == false)
        return *this;

    TIpv6Address Result = TryToExtractIpv4From6();
    Y_ABORT_UNLESS(Result.IsNull() == false);
    return Result;
}

IOutputStream& operator<<(IOutputStream& Out, const TIpv6Address::TIpType Type) noexcept {
    switch (Type) {
    case TIpv6Address::Ipv4:
        Out << "Ipv4";
        return Out;
    case TIpv6Address::Ipv6:
        Out << "Ipv6";
        return Out;
    default:
        Out << "UnknownType";
        return Out;
    }
}

IOutputStream& operator<<(IOutputStream& out, const TIpv6Address& ipv6Address) noexcept {
    bool ok;
    const TString& strIp = ipv6Address.ToString(&ok);
    if (!ok) {
        return out << "Can not convert ip to string";
    } else {
        return out << strIp;
    }
}

TString THostAddressAndPort::ToString() const noexcept {
    TStringStream Str;
    Str << *this;
    return Str.Str();
}

IOutputStream& operator<<(IOutputStream& Out, const THostAddressAndPort& HostAddressAndPort) noexcept {
    Out << HostAddressAndPort.Ip << ":" << HostAddressAndPort.Port;
    return Out;
}

namespace {
    class TRemoteAddr: public NAddr::IRemoteAddr {
    public:
        TRemoteAddr(const TIpv6Address& Address, TIpPort Port);
        const sockaddr* Addr() const override;
        socklen_t Len() const override;

    private:
        sockaddr_in SockAddrIPv4;
        sockaddr_in6 SockAddrIPv6;
        const sockaddr* SockAddrPtr = nullptr;
        socklen_t SockAddrSize = 0;
    };

    TRemoteAddr::TRemoteAddr(const TIpv6Address& Address, TIpPort Port) {
        Address.ToSockaddrAndSocklen(SockAddrIPv4, SockAddrIPv6, SockAddrPtr, SockAddrSize, Port);
    }
    const sockaddr* TRemoteAddr::Addr() const {
        return SockAddrPtr;
    }
    socklen_t TRemoteAddr::Len() const {
        return SockAddrSize;
    }
}

NAddr::IRemoteAddr* ToIRemoteAddr(const TIpv6Address& Address, TIpPort Port) {
    return new TRemoteAddr(Address, Port);
}

std::tuple<THostAddressAndPort, TString, TIpPort> ParseHostAndMayBePortFromString(const TStringBuf RawStr,
                                                                                  TIpPort DefaultPort,
                                                                                  bool& Ok) noexcept {
    // Cout << "ParseHostAndMayBePortFromString: " << RawStr << ", Port: " << DefaultPort << Endl;

    using TRes = std::tuple<THostAddressAndPort, TString, TIpPort>;

    // ---------------------------------------------------------------------

    const size_t BracketColPos = RawStr.find("]:");
    if (BracketColPos != TStringBuf::npos) {
        // [ipv6]:port
        if (!RawStr.StartsWith('[')) {
            Ok = false;
            return {};
        }
        const TStringBuf StrIpv6(RawStr.begin() + 1, RawStr.begin() + BracketColPos);
        const TStringBuf StrPort(RawStr.begin() + BracketColPos + 2, RawStr.end());

        bool IpConverted;
        const TIpv6Address Ip = TIpv6Address::FromString(StrIpv6, IpConverted);
        if (!IpConverted) {
            Ok = false;
            return {};
        }
        if (Ip.Type() != TIpv6Address::Ipv6) {
            Ok = false;
            return {};
        }
        TIpPort Port {};
        if (!::TryFromString(StrPort, Port)) {
            Ok = false;
            return {};
        }

        Ok = true;
        TRes Res{{Ip, Port}, {}, {}};
        return Res;
    }

    // ---------------------------------------------------------------------

    if (RawStr.StartsWith('[')) {
        // [ipv6]
        if (!RawStr.EndsWith(']')) {
            Ok = false;
            return {};
        }
        const TStringBuf StrIpv6(RawStr.begin() + 1, RawStr.end() - 1);

        bool IpConverted;
        const TIpv6Address Ip = TIpv6Address::FromString(StrIpv6, IpConverted);
        if (!IpConverted || Ip.Type() != TIpv6Address::Ipv6) {
            Ok = false;
            return {};
        }

        Ok = true;
        TRes Res{{Ip, DefaultPort}, {}, {}};
        return Res;
    }

    // ---------------------------------------------------------------------

    const size_t ColPos = RawStr.find(':');
    if (ColPos != TStringBuf::npos) {
        // host:port
        // ipv4:port
        // ipv6

        {
            bool IpConverted;
            const TIpv6Address Ipv6 = TIpv6Address::FromString(RawStr, IpConverted);
            if (IpConverted && Ipv6.Type() == TIpv6Address::Ipv6) {
                // ipv6
                Ok = true;
                TRes Res{{Ipv6, DefaultPort}, {}, {}};
                return Res;
            }
        }

        const TStringBuf StrPort(RawStr.begin() + ColPos + 1, RawStr.end());
        TIpPort Port {};
        if (!::TryFromString(StrPort, Port)) {
            Ok = false;
            return {};
        }

        const TStringBuf StrIpv4OrHost(RawStr.begin(), RawStr.begin() + ColPos);
        {
            bool IpConverted;
            const TIpv6Address Ipv4 = TIpv6Address::FromString(StrIpv4OrHost, IpConverted);
            if (IpConverted && Ipv4.Type() == TIpv6Address::Ipv4) {
                // ipv4:port
                Ok = true;
                TRes Res{{Ipv4, Port}, {}, {}};
                return Res;
            }
        }

        {
            // host:port
            Ok = true;
            TRes Res{THostAddressAndPort{}, TString(StrIpv4OrHost), Port};
            return Res;
        }
    }

    // ---------------------------------------------------------------------

    {
        // ipv4
        bool IpConverted;
        const TIpv6Address Ipv4 = TIpv6Address::FromString(RawStr, IpConverted);
        if (IpConverted && Ipv4.Type() == TIpv6Address::Ipv4) {
            Ok = true;
            TRes Res{{Ipv4, DefaultPort}, {}, {}};
            return Res;
        }
    }

    // ---------------------------------------------------------------------

    {
        // host
        Ok = true;
        TRes Res{THostAddressAndPort{}, TString(RawStr), DefaultPort};
        return Res;
    }
}
