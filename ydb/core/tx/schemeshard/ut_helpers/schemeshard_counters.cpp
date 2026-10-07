#include "schemeshard_counters.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/tablet.h>  // for TEvTablet
#include <ydb/core/tx/tx.h>  // for TTestTxConfig
#include <ydb/core/testlib/basics/runtime.h>  // for TTestBasicRuntime
#include <ydb/core/protos/tablet.pb.h>  // for NKikimrTabletBase::TEvGetCountersResponse
#include <ydb/core/protos/tablet_counters.pb.h>  // for NKikimrTabletBase::TTabletCounters



using namespace NActors;
using namespace NKikimr;

namespace {

NKikimrTabletBase::TEvGetCountersResponse GetCounters(TTestBasicRuntime& runtime) {
    const auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, new TEvTablet::TEvGetCounters);
    auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>(sender);

    UNIT_ASSERT(ev);
    return ev->Get()->Record;
}

auto GetPercentileCounters(TTestBasicRuntime& runtime, const TString& name) {
    const auto counters = GetCounters(runtime);
    for (const auto& counter : counters.GetTabletCounters().GetAppCounters().GetPercentileCounters()) {
        if (name != counter.GetName()) {
            continue;
        }

        return counter;
    }

    Y_FAIL("Counter not found: %s", name.c_str());
}

}  // anonymous namespace

namespace NSchemeShardUT_Private {

ui64 GetSimpleCounter(TTestBasicRuntime& runtime, const TString& name) {
    const auto counters = GetCounters(runtime);
    for (const auto& counter : counters.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
        if (name != counter.GetName()) {
            continue;
        }

        return counter.GetValue();
    }

    UNIT_ASSERT_C(false, "Counter not found: " << name);
    return 0; // unreachable
}

void CheckSimpleCounter(TTestBasicRuntime& runtime, const TString& name, ui64 value) {
    UNIT_ASSERT_VALUES_EQUAL(value, GetSimpleCounter(runtime, name));
}

ui64 GetCumulativeCounter(TTestBasicRuntime& runtime, const TString& name) {
    const auto counters = GetCounters(runtime);
    for (const auto& counter : counters.GetTabletCounters().GetAppCounters().GetCumulativeCounters()) {
        if (name != counter.GetName()) {
            continue;
        }

        return counter.GetValue();
    }

    UNIT_ASSERT_C(false, "Cumulative counter not found: " << name);
    return 0; // unreachable
}

ui64 GetPercentileCounter(const auto& counters, const TString& range) {
    for (ui32 i = 0; i < counters.RangesSize(); ++i) {
        if (range != counters.GetRanges(i)) {
            continue;
        }

        UNIT_ASSERT(i < counters.ValuesSize());
        return counters.GetValues(i);
    }

    Y_FAIL("Range not found: %s", range.c_str());
}

ui64 GetPercentileCounter(TTestBasicRuntime& runtime, const TString& name, const TString& range) {
    auto counters = GetPercentileCounters(runtime, name);
    return GetPercentileCounter(counters, range);
}

void CheckPercentileCounter(TTestBasicRuntime& runtime, const TString& name, const THashMap<TString, ui64>& rangeValues) {
    auto counters = GetPercentileCounters(runtime, name);
    Cerr << counters.DebugString();
    for (const auto& [range, value] : rangeValues) {
        const auto v = GetPercentileCounter(counters, range);
        UNIT_ASSERT_VALUES_EQUAL_C(v, value, "Unexpected value in range"
            << ": range# " << range
            << ", expected# " << value
            << ", got# " << v);
    }
}

}  // namespace NSchemeShardUT_Private
