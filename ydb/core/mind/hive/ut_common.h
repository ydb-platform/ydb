#pragma once

#include <ydb/core/protos/counters_hive.pb.h>
#include <ydb/core/protos/tablet_counters.pb.h>
#include <ydb/core/testlib/tenant_runtime.h>

#ifdef NDEBUG
static constexpr bool ENABLE_DETAILED_HIVE_LOG = false;
#else
static constexpr bool ENABLE_DETAILED_HIVE_LOG = true;
#endif

namespace NKikimr {

NKikimrTabletBase::TEvGetCountersResponse GetCounters(TTestActorRuntime& runtime, ui64 tabletId);

ui64 GetSimpleCounter(TTestActorRuntime& runtime, ui64 tabletId,
    NHive::ESimpleCounters counter);

}
