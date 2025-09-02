#pragma once

#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/ipv6_address/ipv6_address.h>
#include <library/cpp/ipmath/ipmath.h>
#include <util/generic/buffer.h>

namespace {
    using TAutoMapString = NKikimr::NUdf::TAutoMap<char*>;
    using TAutoMapUint32 = NKikimr::NUdf::TAutoMap<ui32>;
    using TOptionalString = NKikimr::NUdf::TOptional<char*>;
    using TOptionalUint32 = NKikimr::NUdf::TOptional<ui32>;
    using TOptionalByte = NKikimr::NUdf::TOptional<ui8>;
    using TStringRef = NKikimr::NUdf::TStringRef;
    using TUnboxedValue = NKikimr::NUdf::TUnboxedValue;
    using TUnboxedValuePod = NKikimr::NUdf::TUnboxedValuePod;

    ui8 GetAddressRangePrefix(const TIpAddressRange& range) {
        if (range.Contains(TIpv6Address(ui128(0), TIpv6Address::Ipv6)) && range.Contains(TIpv6Address(ui128(-1), TIpv6Address::Ipv6))) {
            return 0;
        }
        if (range.Size() == 0) {
            return range.Type() == TIpv6Address::Ipv4 ? 32 : 128;
        }
        ui128 size = range.Size();
        size_t sizeLog = MostSignificantBit(size);
        return ui8((range.Type() == TIpv6Address::Ipv4 ? 32 : 128) - sizeLog);
    }

    struct TRawIp4 {
        ui8 A, B, C, D;

        static TRawIp4 FromIpAddress(const TIpv6Address& addr) {
            ui128 x = addr;
            return {
                ui8(x >> 24 & 0xff),
                ui8(x >> 16 & 0xff),
                ui8(x >> 8  & 0xff),
                ui8(x & 0xff)
           };
        }

        static TRawIp4 MaskFromPrefix(ui8 prefix) {
            ui128 x = ui128(-1) << int(32 - prefix);
            x &= ui128(ui32(-1));
            return FromIpAddress({x, TIpv6Address::Ipv4});
        }

        TIpv6Address ToIpAddress() const {
            return {A, B, C, D};
        }

        std::pair<TRawIp4, TRawIp4> ApplyMask(const TRawIp4& mask) const {
            return {{
                    ui8(A & mask.A),
                    ui8(B & mask.B),
                    ui8(C & mask.C),
                    ui8(D & mask.D)
                },{
                    ui8(A | ~mask.A),
                    ui8(B | ~mask.B),
                    ui8(C | ~mask.C),
                    ui8(D | ~mask.D)
            }};
        }
    };

    struct TRawIp4Subnet {
        TRawIp4 Base, Mask;

        static TRawIp4Subnet FromIpRange(const TIpAddressRange& range) {
            return {TRawIp4::FromIpAddress(*range.Begin()), TRawIp4::MaskFromPrefix(GetAddressRangePrefix(range))};
        }

        TIpAddressRange ToIpRange() const {
            auto range = Base.ApplyMask(Mask);
            return {range.first.ToIpAddress(), range.second.ToIpAddress()};
        }
    };

    struct TRawIp6 {
        ui8 A1, A0, B1, B0, C1, C0, D1, D0, E1, E0, F1, F0, G1, G0, H1, H0;

        static TRawIp6 FromIpAddress(const TIpv6Address& addr) {
            ui128 x = addr;
            return {
                ui8(x >> 120 & 0xff), ui8(x >> 112 & 0xff),
                ui8(x >> 104 & 0xff), ui8(x >> 96 & 0xff),
                ui8(x >> 88 & 0xff), ui8(x >> 80 & 0xff),
                ui8(x >> 72 & 0xff), ui8(x >> 64 & 0xff),
                ui8(x >> 56 & 0xff), ui8(x >> 48 & 0xff),
                ui8(x >> 40 & 0xff), ui8(x >> 32 & 0xff),
                ui8(x >> 24 & 0xff), ui8(x >> 16 & 0xff),
                ui8(x >> 8 & 0xff), ui8(x & 0xff)
            };
        }

        static TRawIp6 MaskFromPrefix(ui8 prefix) {
            ui128 x = prefix == 0 ? ui128(0) : ui128(-1) << int(128 - prefix);
            return FromIpAddress({x, TIpv6Address::Ipv6});
        }

        TIpv6Address ToIpAddress() const {
            return {ui16(ui32(A1) << ui32(8) | ui32(A0)),
               ui16(ui32(B1) << ui32(8) | ui32(B0)),
               ui16(ui32(C1) << ui32(8) | ui32(C0)),
               ui16(ui32(D1) << ui32(8) | ui32(D0)),
               ui16(ui32(E1) << ui32(8) | ui32(E0)),
               ui16(ui32(F1) << ui32(8) | ui32(F0)),
               ui16(ui32(G1) << ui32(8) | ui32(G0)),
               ui16(ui32(H1) << ui32(8) | ui32(H0)),
            };
        }

        std::pair<TRawIp6, TRawIp6> ApplyMask(const TRawIp6& mask) const {
            return { {
                    ui8(A1 & mask.A1),
                    ui8(A0 & mask.A0),
                    ui8(B1 & mask.B1),
                    ui8(B0 & mask.B0),
                    ui8(C1 & mask.C1),
                    ui8(C0 & mask.C0),
                    ui8(D1 & mask.D1),
                    ui8(D0 & mask.D0),
                    ui8(E1 & mask.E1),
                    ui8(E0 & mask.E0),
                    ui8(F1 & mask.F1),
                    ui8(F0 & mask.F0),
                    ui8(G1 & mask.G1),
                    ui8(G0 & mask.G0),
                    ui8(H1 & mask.H1),
                    ui8(H0 & mask.H0)
                }, {
                    ui8(A1 | ~mask.A1),
                    ui8(A0 | ~mask.A0),
                    ui8(B1 | ~mask.B1),
                    ui8(B0 | ~mask.B0),
                    ui8(C1 | ~mask.C1),
                    ui8(C0 | ~mask.C0),
                    ui8(D1 | ~mask.D1),
                    ui8(D0 | ~mask.D0),
                    ui8(E1 | ~mask.E1),
                    ui8(E0 | ~mask.E0),
                    ui8(F1 | ~mask.F1),
                    ui8(F0 | ~mask.F0),
                    ui8(G1 | ~mask.G1),
                    ui8(G0 | ~mask.G0),
                    ui8(H1 | ~mask.H1),
                    ui8(H0 | ~mask.H0)
            }};
        }
    };

    struct TRawIp6Subnet {
        TRawIp6 Base, Mask;

        static TRawIp6Subnet FromIpRange(const TIpAddressRange& range) {
            return {TRawIp6::FromIpAddress(*range.Begin()), TRawIp6::MaskFromPrefix(GetAddressRangePrefix(range))};
        }

        TIpAddressRange ToIpRange() const {
            auto range = Base.ApplyMask(Mask);
            return {range.first.ToIpAddress(), range.second.ToIpAddress()};
        }
    };

    TIpv6Address DeserializeAddress(const TStringRef& str) {
        TIpv6Address addr;
        if (str.Size() == 4) {
            TRawIp4 addr4;
            memcpy(&addr4, str.Data(), sizeof addr4);
            addr = addr4.ToIpAddress();
        } else if (str.Size() == 16) {
            TRawIp6 addr6;
            memcpy(&addr6, str.Data(), sizeof addr6);
            addr = addr6.ToIpAddress();
        } else {
            ythrow yexception() << "Incorrect size of input, expected "
            << "4 or 16, got " << str.Size();
        }
        return addr;
    }

    TIpAddressRange DeserializeSubnet(const TStringRef& str) {
        TIpAddressRange range;
        if (str.Size() == sizeof(TRawIp4Subnet)) {
            TRawIp4Subnet subnet4;
            memcpy(&subnet4, str.Data(), sizeof subnet4);
            range = subnet4.ToIpRange();
        } else if (str.Size() == sizeof(TRawIp6Subnet)) {
            TRawIp6Subnet subnet6;
            memcpy(&subnet6, str.Data(), sizeof subnet6);
            range = subnet6.ToIpRange();
        } else {
            ythrow yexception() << "Invalid binary representation";
        }
        return range;
    }

    TString SerializeAddress(const TIpv6Address& addr) {
        Y_ENSURE(addr.Type() == TIpv6Address::Ipv4 || addr.Type() == TIpv6Address::Ipv6);
        TString res;
        if (addr.Type() == TIpv6Address::Ipv4) {
            auto addr4 = TRawIp4::FromIpAddress(addr);
            res = TString(reinterpret_cast<const char *>(&addr4), sizeof addr4);
        } else if (addr.Type() == TIpv6Address::Ipv6) {
            auto addr6 = TRawIp6::FromIpAddress(addr);
            res = TString(reinterpret_cast<const char *>(&addr6), sizeof addr6);
        }
        return res;
    }

    TString SerializeSubnet(const TIpAddressRange& range) {
        TString res;
        if (range.Type() == TIpv6Address::Ipv4) {
            auto subnet4 = TRawIp4Subnet::FromIpRange(range);
            res = TString(reinterpret_cast<const char *>(&subnet4), sizeof subnet4);
        } else if (range.Type() == TIpv6Address::Ipv6) {
            auto subnet6 = TRawIp6Subnet::FromIpRange(range);
            res = TString(reinterpret_cast<const char *>(&subnet6), sizeof subnet6);
        }
        return res;
    }

    SIMPLE_STRICT_UDF(TFromString, TOptionalString(TAutoMapString)) {
        TIpv6Address addr = TIpv6Address::FromString(args[0].AsStringRef());
        if (addr.Type() != TIpv6Address::Ipv4 && addr.Type() != TIpv6Address::Ipv6) {
            return TUnboxedValue();
        }
        return valueBuilder->NewString(SerializeAddress(addr));
    }

    SIMPLE_STRICT_UDF_OPTIONS(TIpv4FromUint32, char*(TAutoMapUint32), builder.SetMinLangVer(NYql::MakeLangVersion(2025, 3))) {
        // in_addr expects bytes in network byte order.
        in_addr addr;
        addr.s_addr = htonl(args[0].Get<ui32>());
        return valueBuilder->NewString(SerializeAddress(TIpv6Address{addr}));
    }

    SIMPLE_STRICT_UDF(TSubnetFromString, TOptionalString(TAutoMapString)) {
        TIpAddressRange range = TIpAddressRange::FromCompactString(args[0].AsStringRef());
        auto res = SerializeSubnet(range);
        return res ? valueBuilder->NewString(res) : TUnboxedValue(TUnboxedValuePod());
    }

    SIMPLE_UDF(TToString, char*(TAutoMapString)) {
        return valueBuilder->NewString(DeserializeAddress(args[0].AsStringRef()).ToString(false));
    }

    SIMPLE_UDF_OPTIONS(TIpv4ToUint32, TOptionalUint32(TAutoMapString), builder.SetMinLangVer(NYql::MakeLangVersion(2025, 3))) {
        Y_UNUSED(valueBuilder);
        TIpv6Address addr = DeserializeAddress(args[0].AsStringRef());
        if (addr.Type() != TIpv6Address::Ipv4) {
            return TUnboxedValue();
        }

        in_addr tmp;
        addr.ToInAddr(tmp);
        ui32 ret = ntohl(tmp.s_addr);
        return TUnboxedValuePod(ret);
    }

    SIMPLE_UDF(TSubnetToString, char*(TAutoMapString)) {
        TStringBuilder result;
        auto range = DeserializeSubnet(args[0].AsStringRef());
        result << (*range.Begin()).ToString(false);
        result << '/';
        result << ToString(GetAddressRangePrefix(range));
        return valueBuilder->NewString(result);
    }

    SIMPLE_UDF(TSubnetMatch, bool(TAutoMapString, TAutoMapString)) {
        Y_UNUSED(valueBuilder);
        auto range1 = DeserializeSubnet(args[0].AsStringRef());
        if (args[1].AsStringRef().Size() == sizeof(TRawIp4) || args[1].AsStringRef().Size() == sizeof(TRawIp6)) {
            auto addr2 = DeserializeAddress(args[1].AsStringRef());
            return TUnboxedValuePod(range1.Contains(addr2));
        } else { // second argument is a whole subnet, not a single address
            auto range2 = DeserializeSubnet(args[1].AsStringRef());
            return TUnboxedValuePod(range1.Contains(range2));
        }
    }

    SIMPLE_STRICT_UDF(TIsIPv4, bool(TOptionalString)) {
        Y_UNUSED(valueBuilder);
        bool result = false;
        if (args[0]) {
            const auto ref = args[0].AsStringRef();
            result = ref.Size() == 4;
        }
        return TUnboxedValuePod(result);
    }

    SIMPLE_STRICT_UDF(TIsIPv6, bool(TOptionalString)) {
        Y_UNUSED(valueBuilder);
        bool result = false;
        if (args[0]) {
            const auto ref = args[0].AsStringRef();
            result = ref.Size() == 16;
        }
        return TUnboxedValuePod(result);
    }

    SIMPLE_STRICT_UDF(TIsEmbeddedIPv4, bool(TOptionalString)) {
        Y_UNUSED(valueBuilder);
        bool result = false;
        if (args[0]) {
            const auto ref = args[0].AsStringRef();
            if (ref.Size() == 16) {
                result = DeserializeAddress(ref).Isv4MappedTov6();
            }
        }
        return TUnboxedValuePod(result);
    }

    SIMPLE_UDF(TConvertToIPv6, char*(TAutoMapString)) {
        const auto& ref = args[0].AsStringRef();
        if (ref.Size() == 16) {
            return valueBuilder->NewString(ref);
        } else if (ref.Size() == 4) {
            TIpv6Address addr4 = DeserializeAddress(ref);
            auto addr6 = TIpv6Address(ui128(addr4) | ui128(0xFFFF) << 32, TIpv6Address::Ipv6);
            return valueBuilder->NewString(SerializeAddress(addr6));
        } else {
            ythrow yexception() << "Incorrect size of input, expected "
            << "4 or 16, got " << ref.Size();
        }
    }

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TGetSubnet, char*(TAutoMapString, TOptionalByte), 1) {
        const auto ref = args[0].AsStringRef();
        ui8 subnetSize = args[1].GetOrDefault<ui8>(0);
        TIpv6Address addr = DeserializeAddress(ref);
        if (ref.Size() == 4) {
            if (!subnetSize) {
                subnetSize = 24;
            }
            if (subnetSize > 32) {
                subnetSize = 32;
            }
        } else if (ref.Size() == 16) {
            if (!subnetSize) {
                subnetSize = 64;
            }
            if (subnetSize > 128) {
                subnetSize = 128;
            }
        } else {
            ythrow yexception() << "Incorrect size of input, expected "
            << "4 or 16, got " << ref.Size();
        }
        TIpv6Address beg = LowerBoundForPrefix(addr, subnetSize);
        return valueBuilder->NewString(SerializeAddress(beg));
    }

    SIMPLE_UDF(TGetSubnetByMask, char*(TAutoMapString, TAutoMapString)) {
        const auto refBase = args[0].AsStringRef();
        const auto refMask = args[1].AsStringRef();
        TIpv6Address addrBase = DeserializeAddress(refBase);
        TIpv6Address addrMask = DeserializeAddress(refMask);
        if (addrBase.Type() != addrMask.Type()) {
            ythrow yexception() << "Base and mask differ in length";
        }
        return valueBuilder->NewString(SerializeAddress(TIpv6Address(ui128(addrBase) & ui128(addrMask), addrBase.Type())));
    }

#define EXPORTED_IP_BASE_UDF \
    TFromString, \
    TIpv4FromUint32, \
    TSubnetFromString, \
    TToString, \
    TIpv4ToUint32, \
    TSubnetToString, \
    TIsIPv4, \
    TIsIPv6, \
    TIsEmbeddedIPv4, \
    TConvertToIPv6, \
    TGetSubnet, \
    TSubnetMatch, \
    TGetSubnetByMask
}
