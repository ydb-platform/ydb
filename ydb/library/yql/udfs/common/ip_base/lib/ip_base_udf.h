#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/draft/ip.h>
#include <util/generic/buffer.h>

namespace {
    using TAutoMapString = NKikimr::NUdf::TAutoMap<char*>;
    using TOptionalString = NKikimr::NUdf::TOptional<char*>;
    using TOptionalByte = NKikimr::NUdf::TOptional<ui8>;
    using TStringRef = NKikimr::NUdf::TStringRef;
    using TUnboxedValue = NKikimr::NUdf::TUnboxedValue;
    using TUnboxedValuePod = NKikimr::NUdf::TUnboxedValuePod;

    struct TSerializeIpVisitor {
        TStringRef operator()(const TIp4& ip) const {
            return TStringRef(reinterpret_cast<const char*>(&ip), 4);
        }
        TStringRef operator()(const TIp6& ip) const {
            return TStringRef(reinterpret_cast<const char*>(&ip.Data), 16);
        }
    };

    SIMPLE_STRICT_UDF(TFromString, TOptionalString(TAutoMapString)) {
        try {
            TString input(args[0].AsStringRef());
            const TIp4Or6& ip = Ip4Or6FromString(input.c_str());
            return valueBuilder->NewString(std::visit(TSerializeIpVisitor(), ip));
        } catch (TSystemError&) {
            return TUnboxedValue();
        }
    }

    SIMPLE_UDF(TToString, char*(TAutoMapString)) {
        const auto& ref = args[0].AsStringRef();
        if (ref.Size() == 4) {
            TIp4 ip;
            memcpy(&ip, ref.Data(), sizeof(ip));
            return valueBuilder->NewString(Ip4Or6ToString(ip));
        } else if (ref.Size() == 16) {
            TIp6 ip;
            memcpy(&ip.Data, ref.Data(), sizeof(ip.Data));
            return valueBuilder->NewString(Ip4Or6ToString(ip));
        } else {
            ythrow yexception() << "Incorrect size of input, expected "
            << "4 or 16, got " << ref.Size();
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
            if (ref.Size() == 16 && ref.Data()[10] == -1) {
                bool allZeroes = true;
                for (int i = 0; i < 10; ++i) {
                    if (ref.Data()[i] != 0) {
                        allZeroes = false;
                        break;
                    }
                }
                result = allZeroes;
            }
        }
        return TUnboxedValuePod(result);
    }

    SIMPLE_UDF(TConvertToIPv6, char*(TAutoMapString)) {
        const auto& ref = args[0].AsStringRef();
        if (ref.Size() == 16) {
            return valueBuilder->NewString(ref);
        } else if (ref.Size() == 4) {
            TIp4 ipv4;
            memcpy(&ipv4, ref.Data(), sizeof(ipv4));
            const TIp6 ipv6 = Ip6FromIp4(ipv4);
            return valueBuilder->NewString(TStringRef(reinterpret_cast<const char*>(&ipv6.Data), 16));
        } else {
            ythrow yexception() << "Incorrect size of input, expected "
            << "4 or 16, got " << ref.Size();
        }
    }

    SIMPLE_UDF_OPTIONS(TGetSubnet, char*(TAutoMapString, TOptionalByte),
                       builder.OptionalArgs(1)) {
        const auto ref = args[0].AsStringRef();
        ui8 subnetSize = args[1].GetOrDefault<ui8>(0);

        if (ref.Size() == 4) {
            if (!subnetSize) {
                subnetSize = 24;
            }
        } else if (ref.Size() == 16) {
            if (!subnetSize) {
                subnetSize = 64;
            }
        } else {
            ythrow yexception() << "Incorrect size of input, expected "
            << "4 or 16, got " << ref.Size();
        }
        TBuffer result(ref.Data(), ref.Size());
        int bytesToMask = ref.Size() * 8 - subnetSize;
        ui8 currentByte = ref.Size() - 1;
        while (bytesToMask > 0) {
            if (bytesToMask > 8) {
                result.Data()[currentByte] = 0;
            } else {
                result.Data()[currentByte] = result.Data()[currentByte] & (0xff << bytesToMask);
            }
            bytesToMask -= 8;
            --currentByte;
        }

        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
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


