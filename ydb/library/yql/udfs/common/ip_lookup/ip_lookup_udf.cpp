#include <ydb/library/yql/public/udf/udf_allocator.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>

#include <library/cpp/ipv6_address/ipv6_address.h>
#include <library/cpp/ipmath/ipmath.h>
#include <util/generic/buffer.h>

#if 0
#include <util/charset/wide.h>
#include <util/generic/vector.h>
#include <util/stream/format.h>
#include <util/string/ascii.h>
#include <util/string/escape.h>
#include <util/string/hex.h>
#include <util/string/join.h>
#include <util/string/reverse.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/string/util.h>
#include <util/string/vector.h>
#endif

using namespace NKikimr;
using namespace NUdf;

namespace {
    ui64 GetPrefix(const auto& addr) {
        auto v6 = TIpv6Address::FromString(addr);
        if (v6.IsIpv6()) {
            auto high = GetHigh((ui128)v6); 
            if ((high >> 40) == 0xffffffu) {
                ythrow yexception() << "Unexpected ipv6 address " << addr;
            }
            return high;
        } else {
            return (ui64(0xffffffu) << 40) | (GetLow((ui128)v6) << 8);
        }
    }

    i64 LookupIp(const auto& addr, const auto& dictBlob) {
        ui64 high = GetPrefix(addr);
        if ((dictBlob.size() % sizeof(ui64)) != 0) {
            ythrow yexception() << "Corrupt trie: unaligned size " << dictBlob.size() << " % " << sizeof(ui64) << " != 0";
        }
        const auto trie = dictBlob.data();
        size_t trieSize = dictBlob.size()/sizeof(const ui64);
        ui32 h = 0;
        constexpr ui32 eol = ui32(1)<<(32 - 1);
        for (ui32 i = 64;; ) {
            auto item0 = ReadUnaligned<ui32>(trie + h*sizeof(ui64) + 0*sizeof(ui32));
            auto item1 = ReadUnaligned<ui32>(trie + h*sizeof(ui64) + 1*sizeof(ui32));
            if (item1 & eol) {
                return (i64(item1 ^ eol) << 32) ^ item0;
            }
            if (i-- == 0)
                return -1;
            auto bit = (high >> i) & 1;
            auto next = bit ? item1 : item0;
            if (next >= trieSize) {
                ythrow yexception() << "Corrupt trie: " << h << "[" << bit << "] = " << next << " >= " << trieSize;
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

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TIpPrefix, TOptional<ui32>(TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        try {
            ui32 result = GetPrefix(args[0].AsStringRef()) >> 32;
            return TUnboxedValuePod(result);
        } catch (yexception&) {
            return TUnboxedValue();
        }
    }

    struct TIpPrefixKernelExec
        : public TUnaryKernelExec<TIpPrefixKernelExec>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem args, const TSink& sink) {
            try {
                ui32 result = GetPrefix(args.GetElement(0).AsStringRef()) >> 32;
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
