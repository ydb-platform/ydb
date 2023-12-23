#include "wilson_trace.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/byteorder.h>

using namespace NWilson;

using TTrace = std::array<ui64, 2>;

TTrace GetTrace(const TTraceId& traceId) {
    static_assert(TTraceId::GetTraceIdSize() == sizeof(TTrace));
    return *reinterpret_cast<const TTrace*>(traceId.GetTraceIdPtr());
}

ui64 GetSpan(const TTraceId& traceId) {
    static_assert(TTraceId::GetSpanIdSize() == sizeof(ui64));
    return *reinterpret_cast<const ui64*>(traceId.GetSpanIdPtr());
}

bool CheckAllZero(const TTraceId& traceId) {
    auto trace = GetTrace(traceId);
    return trace[0] == 0 && trace[1] == 0 && GetSpan(traceId) == 0;
}

template<class T>
inline T HostToBig(T val) noexcept {
    return LittleToBig(HostToLittle(val));
}

Y_UNIT_TEST_SUITE(TTraceId) {
    Y_UNIT_TEST(OpenTelemetryHeaderParser) {
        const auto incorrectHeaders = std::array {
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-011", // Wrong length
            "0-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0",

            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // Wrong version

            "0r-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // Wrong charset
            "00-0af7651916xd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6y7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-t1",

            "00=0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // Wrong dash
            "00-0af7651916cd43dd8448eb211c80319c+b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331_01",

            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01", // One of the ids is zero
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",

            "00-0af7651916cd43dd8448eb211c8031-baa7ad6b7169203331-01", // Random errors
            "abacaba",
            "ababababab23131231231bababababababcdcdc123123123acdadab",
        };

        for (auto header : incorrectHeaders) {
            auto traceId = TTraceId::FromTraceparentHeader(header);
            UNIT_ASSERT(CheckAllZero(traceId) && !traceId);
        }

        const auto correctHeaders = std::array {
            std::tuple {
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                TTrace {0x0af7651916cd43dd, 0x8448eb211c80319c},
                0xb7ad6b7169203331,
            },
            std::tuple {
                "00-0AF7651916CD43DD8448EB211C80319C-b7Ad6B7169203331-01",
                TTrace {0x0af7651916cd43dd, 0x8448eb211c80319c},
                0xb7ad6b7169203331,
            },
            std::tuple {
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-03",
                TTrace {0x0af7651916cd43dd, 0x8448eb211c80319c},
                0xb7ad6b7169203331,
            },
            std::tuple {
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
                TTrace {0x0af7651916cd43dd, 0x8448eb211c80319c},
                0xb7ad6b7169203331,
            },
            std::tuple {
                "00-000101020305080d1522375990e97962-db3d18556dc22ff1-01",
                TTrace {0x000101020305080d, 0x1522375990e97962},
                0xdb3d18556dc22ff1,
            },
        };

        for (auto [header, expectedTrace, expectedSpan] : correctHeaders) {
            auto traceId = TTraceId::FromTraceparentHeader(header);
            auto realTrace = GetTrace(traceId);
            auto realSpan = GetSpan(traceId);
            for (auto& part : realTrace) {
                part = HostToBig(part);
            }
            realSpan = HostToBig(realSpan);
            UNIT_ASSERT_EQUAL(realTrace, expectedTrace);
            UNIT_ASSERT_EQUAL(realSpan, expectedSpan);
        }
    }
}
