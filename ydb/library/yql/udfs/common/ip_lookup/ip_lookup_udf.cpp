#include <ydb/library/yql/public/udf/udf_allocator.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>

#include <library/cpp/ipv6_address/ipv6_address.h>
#include <library/cpp/ipmath/ipmath.h>
#include <util/generic/buffer.h>

using namespace NKikimr;
using namespace NUdf;

namespace {
    constexpr unsigned Ipv6PrefixLenDefault = 24;
    constexpr unsigned Ipv4PrefixLenDefault = 8;

    ui64 GetPrefix(const auto& addr, ui32 ipv6PrefixLength, ui32 ipv4PrefixLength) {
        auto v6 = TIpv6Address::FromString(addr);
        if (ipv6PrefixLength + ipv4PrefixLength > 64) {
            ythrow yexception() << "ipv4 + ipv6 prefix length exceed 64 bits";
        }
        if (v6.IsIpv6()) {
            return ipv6PrefixLength == 0 ? 0 : GetHigh((ui128)v6) >> (64 - ipv6PrefixLength);
        } else {
            return (GetLow((ui128)v6) >> (32 - ipv4PrefixLength)) << ipv6PrefixLength;
        }
    }

    i64 LookupIp(const auto& addr, const auto& dictBlob) {
        ui64 high;
        {
            auto v6 = TIpv6Address::FromString(addr);
            if (v6.IsIpv6()) {
                high = GetHigh((ui128)v6);
            } else {
                high = GetLow((ui128)v6) << (64 - 32);
            }
        }
        constexpr size_t headerSize = 16;
        if (dictBlob.size() < headerSize) {
            ythrow yexception() << "Corrupt trie: too small: " << dictBlob.size() << " < " << 16;
        }
        if (!std::string_view(dictBlob).starts_with("Trie0000")) {
            ythrow yexception() << "Corrupt trie: invalid signature";
        }
        const auto trie = dictBlob.data();
        const ui32 dictSize = ReadUnaligned<ui32>(trie + 8);
        if (dictSize == 0)
            return -1;
        const ui8 trieBits = ReadUnaligned<ui8>(trie + 8 + 4);
        // there must be at least one bit to choose from
        if (trieBits < 1 || trieBits > 8 || 32 % trieBits != 0) {
            ythrow yexception() << "Corrupt trie: trieBits = " << trieBits;
        }
        const ui32 trieSize = ui32(1) << trieBits;
        const auto ItemSize = sizeof(ui32) << trieBits;
        if ((dictBlob.size() - headerSize) / ItemSize < dictSize) {
            ythrow yexception() << "Corrupt trie: too small: " << (dictBlob.size() - 16) / ItemSize << " < " << dictSize;
        }
        ui32 h = 0;
        constexpr ui32 eol = ui32(1)<<(32 - 1);
        auto readTrie = [&trie, trieBits](ui32 h, ui32 bit) {
            return ReadUnaligned<ui32>(trie + headerSize + ((h<<trieBits) + bit)*sizeof(ui32));
        };
        for (ui32 i = 64;; ) {
            auto item1 = readTrie(h, 1);
            if (item1 & eol) {
                return (int64_t(item1 ^ eol) << 32) ^ readTrie(h, 0);
            }
            if (i == 0)
                return -1;
            i -= trieBits;
            auto bit = (high >> i) & (trieSize - 1);
            auto next = readTrie(h, bit);
            if (next >= dictSize) {
                ythrow yexception() << "Corrupt trie: " << h << "[" << bit << "] = " << next << " >= " << dictSize;
            }
            if (next == 0)
                return -1;
            h = next;
        }
    }

    // Takes: ipv4/ipv6 address, dict binary blob as string
    // Produces: NULL on error, negative on not found, number between 0 and INT64_MAX for found
    BEGIN_SIMPLE_STRICT_ARROW_UDF(TLookupIp, TOptional<i64>(TAutoMap<char*>, char*)) {
        Y_UNUSED(valueBuilder);
        try {
            return TUnboxedValuePod(LookupIp(args[0].AsStringRef(), args[1].AsStringRef()));
        } catch (yexception&) {
            return TUnboxedValue();
        }
    }

    struct TLookupIpKernelExec
        : public TGenericKernelExec<TLookupIpKernelExec, 2>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem args, const TSink& sink) {
            try {
                auto result = LookupIp(args.GetElement(0).AsStringRef(), args.GetElement(1).AsStringRef());
                return sink(TBlockItem((ui64)result, (ui64)0));
            } catch (yexception&) {
                return sink(TBlockItem());
            }
        }
    };

    END_SIMPLE_ARROW_UDF(TLookupIp, TLookupIpKernelExec::Do)

    BEGIN_SIMPLE_ARROW_UDF_WITH_OPTIONAL_ARGS(TIpPrefix, TOptional<ui32>(TAutoMap<char*>, TOptional<ui32>, TOptional<ui32>), 2) {
        Y_UNUSED(valueBuilder);
        try {
            ui32 result = GetPrefix(args[0].AsStringRef(), args[1].GetOrDefault<ui64>(32), args[2].GetOrDefault<ui32>(8));
            return TUnboxedValuePod(result);
        } catch (yexception&) {
            return TUnboxedValue();
        }
    }

    struct TIpPrefixKernelExec
        : public TGenericKernelExec<TIpPrefixKernelExec, 3>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem args, const TSink& sink) {
            try {
                ui32 result = GetPrefix(args.GetElement(0).AsStringRef(),
                        args.GetElement(1) ? args.GetElement(1).Get<ui32>() : Ipv6PrefixLenDefault,
                        args.GetElement(2) ? args.GetElement(2).Get<ui32>() : Ipv4PrefixLenDefault
                        );
                return sink(TBlockItem{result, (ui64)0});
            } catch (yexception&) {
                return sink(TBlockItem());
            }
        }
    };

    END_SIMPLE_ARROW_UDF(TIpPrefix, TIpPrefixKernelExec::Do)

    SIMPLE_MODULE(TIpLookupModule,
        TLookupIp,
        TIpPrefix
    )
}

REGISTER_MODULES(TIpLookupModule)
