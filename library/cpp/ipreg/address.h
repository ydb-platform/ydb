#pragma once

#include <library/cpp/int128/int128.h>

#include <util/generic/string.h>
#include <util/digest/murmur.h>
#include <util/string/cast.h>

namespace NIPREG {

struct TAddress {
    enum class EAddressFormat {
        IPV6         = 0x00 /* "ipv6" */,
        LONG_IP      = 0x01 /* "long" */,
        SHORT_IP     = 0x02 /* "short" */,
        NUMERIC_IPV4 = 0x03 /* "num4" */,
        NTOA         = 0x04 /* "n2a" */,
        SHORT_IPV6   = 0x05 /* "short-ipv6" */,
        NUMERIC_IPV6 = 0x06 /* "num" */,
    };

    unsigned char Data[16] = {0}; // NOTA BENE: network byte order (Big-Endian)

    // Comparison
    bool operator==(const TAddress& other) const {
        return memcmp(Data, other.Data, sizeof(Data)) == 0;
    }

    bool operator<(const TAddress& other) const {
        return memcmp(Data, other.Data, sizeof(Data)) < 0;
    }

    bool operator>(const TAddress& other) const {
        return memcmp(Data, other.Data, sizeof(Data)) > 0;
    }

    bool operator!=(const TAddress& other) const {
        return !(*this == other);
    }

    bool operator<=(const TAddress& other) const {
        return !(*this > other);
    }

    bool operator>=(const TAddress& other) const {
        return !(*this < other);
    }

    double operator-(const TAddress& rhs) const;

    // Parsing
    static TAddress ParseAny(TStringBuf str);

    static TAddress ParseIPv6(TStringBuf str);
    static TAddress ParseIPv4(TStringBuf str);
    static TAddress ParseIPv4Num(TStringBuf str);
    static TAddress ParseIPv6Num(TStringBuf str);

    static TAddress FromIPv4Num(ui32 num);
    static TAddress FromUint128(ui128 addr);
    static TAddress FromBinary(unsigned char const * data);
    static TAddress FromBinaryIPv4(unsigned char const * const data);

    static TAddress MakeNet64Broadcast(TAddress base);
    static TAddress MakeNet64Prefix(TAddress base);

    static const TAddress& Lowest();
    static const TAddress& Highest();

    // Inspecting
    TString AsIPv4() const;
    TString AsIPv4Num() const;
    TString AsIPv6() const;
    TString AsIPv6Num() const;
    TString GetTextFromNetOrder() const;
    TString GetHexString(bool deepView = false) const;

    TString AsShortIP() const;
    TString AsShortIPv6() const;
    TString AsLongIP() const;

    ui128 AsUint128() const;
    ui64 GetHigh64() const;
    ui64 GetLow64() const;
    ui64 GetHigh64LE() const;
    ui64 GetLow64LE() const;

    bool IsNet64Broadcast() const;
    bool IsNet64Host() const;

    TAddress GetNet64() const {
        return TAddress::FromUint128(ui128{GetHigh64LE()} << 64);
    }

    TAddress GetPrevNet64() const {
        return TAddress::FromUint128(ui128{GetHigh64LE() - 1} << 64);
    }

    TAddress GetNextNet64() const {
        return TAddress::FromUint128(ui128{GetHigh64LE() + 1} << 64);
    }

    TString Format(EAddressFormat format) const;

    int GetType() const { return IsIPv4() ? 4 : 6; }
    bool IsIPv4() const;

    // Mutating
    TAddress Next() const;
    TAddress Prev() const;

    static ui128 Distance(const TAddress& a, const TAddress& b);
};

using EAddressFormat = TAddress::EAddressFormat;

struct TNetwork {
    TAddress begin;
    TAddress end;

    TNetwork(const TString& str = "0.0.0.0/32");

private:
    TNetwork(const TVector<TString>& data);
    TNetwork(const TString& net, size_t mask);
};

} // NIPREG

template <>
struct THash<NIPREG::TAddress> {
    inline size_t operator()(const NIPREG::TAddress& address) const {
        return MurmurHash<size_t>((const void*)address.Data, 16);
    }
};

IOutputStream& operator<<(IOutputStream& output, const NIPREG::TAddress& addr);
