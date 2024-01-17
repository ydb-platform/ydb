#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/ipv6_address/ipv6_address.h>
#include <library/cpp/ipmath/ipmath.h>
#include <util/generic/buffer.h>

namespace {
    using TAutoMapString = NKikimr::NUdf::TAutoMap<char*>;
    using TOptionalString = NKikimr::NUdf::TOptional<char*>;
    using TOptionalByte = NKikimr::NUdf::TOptional<ui8>;
    using TStringRef = NKikimr::NUdf::TStringRef;
    using TUnboxedValue = NKikimr::NUdf::TUnboxedValue;
    using TUnboxedValuePod = NKikimr::NUdf::TUnboxedValuePod;

    struct TRawIp4 {
        ui8 a, b, c, d;
    };

    struct TRawIp6 {
        ui8 a1, a0, b1, b0, c1, c0, d1, d0, e1, e0, f1, f0, g1, g0, h1, h0;
    };

    TIpv6Address DeserializeAddress(const TStringRef& str) {
        TIpv6Address addr;
        if (str.Size() == 4) {
            TRawIp4 addr4;
            memcpy(&addr4, str.Data(), sizeof addr4);
            addr = {addr4.a, addr4.b, addr4.c, addr4.d};
        } else if (str.Size() == 16) {
            TRawIp6 addr6;
            memcpy(&addr6, str.Data(), sizeof addr6);
            addr = {ui16(ui32(addr6.a1) << ui32(8) | ui32(addr6.a0)),
                    ui16(ui32(addr6.b1) << ui32(8) | ui32(addr6.b0)),
                    ui16(ui32(addr6.c1) << ui32(8) | ui32(addr6.c0)),
                    ui16(ui32(addr6.d1) << ui32(8) | ui32(addr6.d0)),
                    ui16(ui32(addr6.e1) << ui32(8) | ui32(addr6.e0)),
                    ui16(ui32(addr6.f1) << ui32(8) | ui32(addr6.f0)),
                    ui16(ui32(addr6.g1) << ui32(8) | ui32(addr6.g0)),
                    ui16(ui32(addr6.h1) << ui32(8) | ui32(addr6.h0)),
            };
        } else {
            ythrow yexception() << "Incorrect size of input, expected "
            << "4 or 16, got " << str.Size();
        }
        return addr;
    }

    TString SerializeAddress(const TIpv6Address& addr) {
        Y_ENSURE(addr.Type() == TIpv6Address::Ipv4 || addr.Type() == TIpv6Address::Ipv6);
        TString res;
        ui128 x = addr;
        if (addr.Type() == TIpv6Address::Ipv4) {
            TRawIp4 addr4 {
                ui8(x >> 24 & 0xff),
                ui8(x >> 16 & 0xff),
                ui8(x >> 8  & 0xff),
                ui8(x & 0xff)
            };
            res = TString(reinterpret_cast<const char *>(&addr4), sizeof addr4);
        } else if (addr.Type() == TIpv6Address::Ipv6) {
            TRawIp6 addr6 {
                ui8(x >> 120 & 0xff), ui8(x >> 112 & 0xff),
                ui8(x >> 104 & 0xff), ui8(x >> 96 & 0xff),
                ui8(x >> 88 & 0xff), ui8(x >> 80 & 0xff),
                ui8(x >> 72 & 0xff), ui8(x >> 64 & 0xff),
                ui8(x >> 56 & 0xff), ui8(x >> 48 & 0xff),
                ui8(x >> 40 & 0xff), ui8(x >> 32 & 0xff),
                ui8(x >> 24 & 0xff), ui8(x >> 16 & 0xff),
                ui8(x >> 8 & 0xff), ui8(x & 0xff)
            };
            res = TString(reinterpret_cast<const char *>(&addr6), sizeof addr6);
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

    SIMPLE_UDF(TToString, char*(TAutoMapString)) {
        return valueBuilder->NewString(DeserializeAddress(args[0].AsStringRef()).ToString(false));
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

#define EXPORTED_IP_BASE_UDF \
    TFromString, \
    TToString, \
    TIsIPv4, \
    TIsIPv6, \
    TIsEmbeddedIPv4, \
    TConvertToIPv6, \
    TGetSubnet
}


