#pragma once

#include <ydb/library/actors/core/manual_test/event_serialize_bench/bench.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_flat.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/subsystems/stats.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/compiler.h>
#include <util/system/sigset.h>

#include <array>
#include <chrono>
#include <future>

using namespace NActors;
using namespace NActors::NDnsResolver;

namespace {

enum EBenchEvents {
    EvBenchPbMessage = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvBenchFlatMessage,
    EvBenchAck,
    EvBenchFlush,
};

constexpr ui32 Node1 = 1;
constexpr ui32 Node2 = 2;
constexpr size_t Payload256 = 256;
constexpr size_t Payload4KB = 4 * 1024;
constexpr size_t VPutLikeExtraChecks = 4;

TActorId MakeBenchReceiverServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("bnchrecv"));
}

struct TBenchConfig {
    TString Scenario = "all";
    TString Format = "all";
    TDuration Duration = TDuration::Seconds(5);
    ui64 Window = 10000;
    ui32 AckBatch = 256;
    ui32 Threads = 2;
    ui16 PortBase = 19000;
};

struct TBenchResult {
    TString Scenario;
    TString Format;
    TString Test;
    ui64 EffectiveWindow = 0;
    ui64 SentMessages = 0;
    ui64 AckedMessages = 0;
    ui64 Checksum = 0;
    ui32 SerializedSizeStart = 0;
    ui32 SerializedSizeEnd = 0;
    double Seconds = 0;
    bool Connected = true;
    ui64 ActorCpuUs = 0;
    double ActorCpuUtilPct = 0.0;
};
