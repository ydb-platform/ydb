#include "address.h"

#include <util/generic/mem_copy.h>
#include <util/stream/format.h>
#include <util/string/cast.h>
#include <util/string/hex.h>
#include <util/string/printf.h>
#include <util/string/split.h>
#include <util/string/type.h>
#include <util/string/vector.h>
#include <util/system/byteorder.h>
#include <util/network/socket.h>

#include <sstream>

namespace NIPREG {

TAddress TAddress::ParseAny(TStringBuf str) {
    if (str.find(':') != TStringBuf::npos) {
        return ParseIPv6(str);
    } else if (str.find('.') != TStringBuf::npos) {
        return ParseIPv4(str);
    } else if (IsNumber(str)) {
        return ParseIPv4Num(str); // TODO(dieash@) IPv6Num
    }

    ythrow yexception() << "Unrecognized IPREG address format: " << str;
}

TAddress TAddress::ParseIPv6(TStringBuf str) {
    TAddress addr;
    if (inet_pton(AF_INET6, TString(str).c_str(), &addr.Data) != 1)
        ythrow yexception() << "Failed to parse IPREG address " << str << " as IPv6";

    return addr;
}

TAddress TAddress::ParseIPv4(TStringBuf str) {
    struct in_addr ipv4;
    if (inet_aton(TString(str).c_str(), &ipv4) != 1)
        ythrow yexception() << "Failed to parse IPREG address " << str << " as IPv4";

    return FromIPv4Num(InetToHost(ipv4.s_addr));
}

TAddress TAddress::ParseIPv4Num(TStringBuf str) {
    return FromIPv4Num(FromString<ui32>(str));
}

TAddress TAddress::ParseIPv6Num(TStringBuf str) {
    return FromUint128(FromString<ui128>(str));
}

TAddress TAddress::FromBinary(unsigned char const * const data) {
    TAddress addr;
    MemCopy<unsigned char>(addr.Data, data, sizeof(addr.Data));
    return addr;
}

TAddress TAddress::FromBinaryIPv4(unsigned char const * const data) {
    return TAddress::FromIPv4Num(
        (static_cast<ui32>(data[0]) << 24) |
        (static_cast<ui32>(data[1]) << 16) |
        (static_cast<ui32>(data[2]) << 8) |
        (static_cast<ui32>(data[3]))
    );
}

TAddress TAddress::FromIPv4Num(ui32 num) {
    TAddress addr;
    memset((void*)&addr.Data, 0x00, 10);
    addr.Data[10] = 0xff;
    addr.Data[11] = 0xff;
    addr.Data[12] = (num >> 24) & 0xff;
    addr.Data[13] = (num >> 16) & 0xff;
    addr.Data[14] = (num >> 8) & 0xff;
    addr.Data[15] = (num) & 0xff;
    return addr;
}

TAddress TAddress::FromUint128(ui128 intAddr) {
    const auto hiBE = HostToInet(GetHigh(intAddr));
    const auto loBE = HostToInet(GetLow(intAddr));

    TAddress addr;
    ui64* dataPtr = reinterpret_cast<ui64*>(addr.Data);
    MemCopy<ui64>(dataPtr, &hiBE, 1);
    MemCopy<ui64>(dataPtr + 1, &loBE, 1);

    return addr;
}

namespace {
    void SetHostsBits(TAddress& addr, char value) {
        addr.Data[ 8] = value;
        addr.Data[ 9] = value;
        addr.Data[10] = value;
        addr.Data[11] = value;
        addr.Data[12] = value;
        addr.Data[13] = value;
        addr.Data[14] = value;
        addr.Data[15] = value;
    }
} // anon-ns

TAddress TAddress::MakeNet64Broadcast(TAddress base) {
    SetHostsBits(base, 0xff);
    return base;
}

TAddress TAddress::MakeNet64Prefix(TAddress base) {
    SetHostsBits(base, 0x00);
    return base;
}

const TAddress& TAddress::Lowest() {
    static const TAddress first{{}};
    return first;
}

const TAddress& TAddress::Highest() {
    static const TAddress last{{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}};
    return last;
}

TString TAddress::AsIPv4() const {
    return ToString(Data[12]) + "." + ToString(Data[13]) + "." + ToString(Data[14]) + "." + ToString(Data[15]);
}

TString TAddress::AsIPv4Num() const {
    ui32 addr = (ui32)Data[12] << 24 | (ui32)Data[13] << 16 | (ui32)Data[14] << 8 | Data[15];
    return ToString(addr);
}

TString TAddress::AsIPv6() const {
    TStringStream ss;

    for (size_t octet = 0; octet < sizeof(Data); octet++) {
        ss << Hex(Data[octet], HF_FULL);
        if (octet < 15 && octet & 1)
            ss << ':';
    }

    TString s = ss.Str();
    s.to_lower();

    return s;
}

TString TAddress::AsIPv6Num() const {
    return ToString(AsUint128());
}

TString TAddress::GetTextFromNetOrder() const {
    char buf[INET6_ADDRSTRLEN];
    if (inet_ntop(AF_INET6, (void*)(&Data), buf, sizeof(buf)) == NULL)
        ythrow yexception() << "Failed to stringify IPREG address";

    return buf;
}

namespace {
    TString GetHexStr(ui64 v) {
        return HexEncode(reinterpret_cast<const char*>(&v), sizeof(v));
    }

    void HexDumpToStream(std::stringstream& ss, ui64 beData) {
        const auto dataHexStr = GetHexStr(beData);
        const auto hostData = InetToHost(beData);
        const auto hostDataStr = GetHexStr(hostData);
        ss << "\t/big-end[" << beData << " / " << dataHexStr << "]\t/host[" << hostData << " / " << hostDataStr << "]\n";
    }
} // anon-ns

TString TAddress::GetHexString(const bool deepView) const {
    std::stringstream ss;
    ss << HexEncode(TStringBuf(reinterpret_cast<const char*>(Data), 16));
    if (deepView) {
        const ui64* dataPtr = reinterpret_cast<const ui64*>(Data);

        const auto hi = *dataPtr;
        ss << "\nhigh-data"; HexDumpToStream(ss, hi);

        const auto lo = *(dataPtr + 1);
        ss << "\nlow-data"; HexDumpToStream(ss, lo);
    }
    return ss.str().c_str();
}

TString TAddress::AsShortIP() const {
    if (IsIPv4())
        return AsIPv4();
    else
        return GetTextFromNetOrder();
}

TString TAddress::AsShortIPv6() const {
    if (IsIPv4())
        return Sprintf("::ffff:%x:%x", (ui32)Data[12] << 8 | (ui32)Data[13], (ui32)Data[14] << 8 | (ui32)Data[15]);
    else
        return GetTextFromNetOrder();
}

TString TAddress::AsLongIP() const {
    if (IsIPv4())
        return AsIPv4();
    else
        return AsIPv6();
}

ui128 TAddress::AsUint128() const {
    const ui64* dataPtr = reinterpret_cast<const ui64*>(Data);
    return ui128(InetToHost(*dataPtr), InetToHost(*(dataPtr + 1)));
}

ui64 TAddress::GetHigh64() const {
    const ui64* dataPtr = reinterpret_cast<const ui64*>(Data);
    return *dataPtr;
}

ui64 TAddress::GetLow64() const {
    const ui64* dataPtr = reinterpret_cast<const ui64*>(Data);
    return *(dataPtr + 1);
}

ui64 TAddress::GetHigh64LE() const {
    return InetToHost(GetHigh64());
}

ui64 TAddress::GetLow64LE() const {
    return InetToHost(GetLow64());
}

bool TAddress::IsNet64Broadcast() const {
    static const auto NET64_HOSTS_MASK = TAddress::ParseAny("::ffff:ffff:ffff:ffff").GetLow64();
    const auto ownHostsBits = GetLow64();
    return ownHostsBits == NET64_HOSTS_MASK;
}

bool TAddress::IsNet64Host() const {
    const auto isSomeOwnHostsBitsOn = GetLow64() > 0;
    return isSomeOwnHostsBitsOn && !IsNet64Broadcast();
}

TString TAddress::Format(EAddressFormat format) const {
    switch (format) {
    case EAddressFormat::IPV6:
        return AsIPv6();
    case EAddressFormat::LONG_IP:
        return AsLongIP();
    case EAddressFormat::SHORT_IP:
        return AsShortIP();
    case EAddressFormat::NUMERIC_IPV4:
        return AsIPv4Num();
    case EAddressFormat::NUMERIC_IPV6:
        return AsIPv6Num();
    case EAddressFormat::NTOA:
        return GetTextFromNetOrder();
    case EAddressFormat::SHORT_IPV6:
        return AsShortIPv6();
    }
}

bool TAddress::IsIPv4() const {
    static const unsigned char mask[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff };
    return memcmp(Data, mask, sizeof(mask)) == 0;
}

TAddress TAddress::Next() const {
    if (Highest() == *this) {
        return Highest();
    }

    TAddress addr;
    bool carry = 1;
    for (ssize_t octet = 15; octet >= 0; octet--) {
        addr.Data[octet] = Data[octet] + carry;
        carry = carry && !addr.Data[octet];
    }

    return addr;
}

TAddress TAddress::Prev() const {
    if (Lowest() == *this) {
        return Lowest();
    }

    TAddress addr{};
    bool carry = 1;
    for (ssize_t octet = 15; octet >= 0; octet--) {
        addr.Data[octet] = Data[octet] - carry;
        carry = carry && !Data[octet];
    }

    return addr;
}

double TAddress::operator-(const TAddress& rhs) const {
    double diff = 0.0;
    for (ssize_t octet = 0; octet < 16; octet++) {
        diff = diff * 256.0 + (static_cast<int>(Data[octet]) - static_cast<int>(rhs.Data[octet]));
    }
    return diff;
}

ui128 TAddress::Distance(const TAddress& a, const TAddress& b) {
    const auto& intA = a.AsUint128();
    const auto& intB = b.AsUint128();
    return (a > b) ? (intA - intB) : (intB - intA);
}

namespace {
    constexpr size_t MAX_IPV6_MASK_LEN = 16 * 8;
    constexpr size_t MAX_IPV4_MASK_LEN = 4 * 8;
    constexpr size_t IPV4_IN6_MASK_BASE = MAX_IPV6_MASK_LEN - MAX_IPV4_MASK_LEN;

    TAddress SetMaskBits(const TAddress& addr, const size_t wantedMaskLen) {
        auto maskLen = wantedMaskLen;
        if (addr.IsIPv4() && maskLen && maskLen <= MAX_IPV4_MASK_LEN) {
            maskLen += IPV4_IN6_MASK_BASE;
        }

        if (maskLen == 0 || maskLen > MAX_IPV6_MASK_LEN || (addr.IsIPv4() && maskLen < IPV4_IN6_MASK_BASE)) {
            ythrow yexception() << "strange mask (calc/wanted) " << maskLen << "/" << wantedMaskLen << "; " << addr;
        }

        const int octetsForUpdate = (MAX_IPV6_MASK_LEN - maskLen) / 8;
        const int bitsForUpdate = (MAX_IPV6_MASK_LEN - maskLen) % 8;

        size_t currOctet = 15;
        TAddress addrWithMask = addr;

        for (int octetNum = 0; octetNum != octetsForUpdate; ++octetNum) {
            addrWithMask.Data[currOctet--] = 0xff;
        }

        for (int bitNum = 0; bitNum != bitsForUpdate; ++bitNum) {
            addrWithMask.Data[currOctet] ^= 1 << bitNum;
        }

        return addrWithMask;
    }
} // anon-ns

TNetwork::TNetwork(const TString& str)
    : TNetwork(static_cast<TVector<TString>>(StringSplitter(str).Split('/').SkipEmpty()))
{}

TNetwork::TNetwork(const TVector<TString>& data)
    : TNetwork(data.size() ? data[0] : "",
               data.size() > 1 ? FromStringWithDefault<size_t>(data[1]) : 0)
{}

TNetwork::TNetwork(const TString& net, size_t maskLen)
    : begin(TAddress::ParseAny(net))
    , end(SetMaskBits(begin, maskLen))
{}

}

IOutputStream& operator<<(IOutputStream& output, const NIPREG::TAddress& addr) {
    output << addr.AsShortIPv6();
    return output;
}
