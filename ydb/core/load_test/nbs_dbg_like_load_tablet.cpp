#include "nbs_dbg_like_load_tablet.h"

#include "events.h"
#include "nbs_dbg_like_alloc_helper.h"
#include "nbs_dbg_like_load_defs.h"
#include "nbs_dbg_like_range_coordinator.h"

#include "service_actor.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/keyvalue/keyvalue_flat_impl.h>
#include <ydb/core/load_test/nbs_dbg_like_load_defs.h_serialized.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/load_test.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_executor_compaction_logic.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/http/fetch/httpheader.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/bitmap.h>
#include <util/random/fast.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>

#include <google/protobuf/text_format.h>

#include <algorithm>
#include <array>
#include <bitset>
#include <expected>
#include <map>
#include <set>
#include <tuple>
#include <utility>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTablet] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTablet] " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTablet] " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTablet] " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsLoadTablet] " << stream)

namespace NKikimr::NNbsDbgLike {

namespace {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Constants
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

constexpr ui32 kBufferStateCount = GetEnumItemsCount<EPBufferState>();
constexpr ui32 kOpCount = GetEnumItemsCount<EOp>();

constexpr ui32 kBscRetryLimit = 5;
constexpr TDuration kBscRetryInitialBackoff = TDuration::MilliSeconds(500);
constexpr TDuration kBscRetryMaxBackoff = TDuration::Seconds(10);
constexpr ui64 kInitialDDiskSessionSeqNo = 1;

TString DDiskStatusText(NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TStringBuf reason) {
    TStringBuilder out;
    out << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(status);
    if (reason) {
        out << ": " << reason;
    }
    return TString(out);
}

// Auto-disable per-peer counters at this many DBGs (10 wires per DBG quickly
// blows up Solomon scrape size). Honour an explicit user setting first.
constexpr ui32 kMaxPerPeerCounters = 10;

const TVector<double> kBatchSizeBounds = {
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Types and counters
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

using TPeerBitset = std::bitset<kHostsPerDbgMax>;

// Mask with bits [0, kPrimaryHostsPerDbg) set.
static const TPeerBitset kAllPrimaryHostsMask = [] {
    TPeerBitset mask;
    for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
        mask.set(k);
    }
    return mask;
}();

struct TWriteInfo {
    ui64 Lsn = 0;
    ui32 Size = 0;
    ui32 VChunkIndex = 0;
    ui64 OffsetInVChunk = 0;
    ui8  CoordinatorIndex = 0;     // index in PB[0..2] for this LSN's write
    EPBufferState State = EPBufferState::PBufferIncompleteWrite;

    NActors::TMonotonic WriteStart;

    TPeerBitset WriteRequested;
    TPeerBitset WriteResponded;
    TPeerBitset WriteAmbiguous;
    TPeerBitset WriteConfirmed;
    TPeerBitset FlushDesired;
    TPeerBitset FlushRequested;
    TPeerBitset FlushConfirmed;
    TPeerBitset EraseRequested;
    TPeerBitset EraseConfirmed;
    TPeerBitset EraseTarget;

    // Origin of the TNbsWrite that opened this LSN. Set in
    // HandleNbsWrite, consumed exactly once in OnWritePersistentBuffersResult.
    // Zero TActorId means "no one is waiting" (e.g. an LSN re-driven by drain).
    NActors::TActorId OriginActor;
    ui64              OriginCookie = 0;
    bool              ReplySent = false;
    bool WriteFailed = false;
    bool WriteFinalized = false;
    TActorId WriteReplyActor;
    TString WriteError;

    NWilson::TSpan Span;
};

// Each worker owns a single DBG, so the PB-write cookie carries only the LSN.

// Per-operation (write, flush, etc) counter group: tracks Requests/Replies/Bytes/Histograms.
struct TOpCounters {
    ::NMonitoring::TDynamicCounters::TCounterPtr Requests;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
    ::NMonitoring::TDynamicCounters::TCounterPtr SubReplyOk;
    ::NMonitoring::TDynamicCounters::TCounterPtr SubReplyErr;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retries;
    ::NMonitoring::TDynamicCounters::TCounterPtr Pending;          // gauge
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesInFlight;    // gauge
    ::NMonitoring::TDynamicCounters::TCounterPtr OldestPendingLsn; // gauge
    ::NMonitoring::TDynamicCounters::TCounterPtr Bytes;
    ::NMonitoring::THistogramPtr ResponseTimeMs;
    ::NMonitoring::THistogramPtr BatchSize;

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& opGroup,
              const NMonitoring::TBucketBounds& latencyBounds,
              bool wantBatchSize, bool wantBytes) {
        Requests        = opGroup->GetCounter("Requests", true);
        ReplyOk         = opGroup->GetCounter("ReplyOk", true);
        ReplyErr        = opGroup->GetCounter("ReplyErr", true);
        SubReplyOk      = opGroup->GetCounter("SubReplyOk", true);
        SubReplyErr     = opGroup->GetCounter("SubReplyErr", true);
        Retries         = opGroup->GetCounter("Retries", true);
        Pending         = opGroup->GetCounter("Pending", false);
        BytesInFlight   = opGroup->GetCounter("BytesInFlight", false);
        OldestPendingLsn = opGroup->GetCounter("OldestPendingLsn", false);
        if (wantBytes) {
            Bytes = opGroup->GetCounter("Bytes", true);
        }
        ResponseTimeMs = opGroup->GetHistogram(
            "ResponseTimeMs", NMonitoring::ExplicitHistogram(latencyBounds));
        if (wantBatchSize) {
            BatchSize = opGroup->GetHistogram(
                "BatchSize", NMonitoring::ExplicitHistogram(kBatchSizeBounds));
        }
    }
};

// `subsystem=request` counters. Completed/Failed/LatencyMs/WriteQuorumMs are
// per-LSN end-to-end; FlushMs/EraseMs are per flush/erase request round-trip.
struct TRequestCounters {
    ::NMonitoring::TDynamicCounters::TCounterPtr Completed;
    ::NMonitoring::TDynamicCounters::TCounterPtr Failed;
    ::NMonitoring::THistogramPtr LatencyMs;
    ::NMonitoring::THistogramPtr WriteQuorumMs;
    ::NMonitoring::THistogramPtr FlushMs;
    ::NMonitoring::THistogramPtr EraseMs;

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& g,
              const NMonitoring::TBucketBounds& latencyBounds) {
        Completed = g->GetCounter("Completed", true);
        Failed    = g->GetCounter("Failed", true);
        LatencyMs       = g->GetHistogram("LatencyMs",       NMonitoring::ExplicitHistogram(latencyBounds));
        WriteQuorumMs   = g->GetHistogram("WriteQuorumMs",   NMonitoring::ExplicitHistogram(latencyBounds));
        FlushMs         = g->GetHistogram("FlushMs",         NMonitoring::ExplicitHistogram(latencyBounds));
        EraseMs         = g->GetHistogram("EraseMs",         NMonitoring::ExplicitHistogram(latencyBounds));
    }
};

// `subsystem=lsns` (gauges, per-state counters, etc.).
struct TLsnsCounters {
    ::NMonitoring::TDynamicCounters::TCounterPtr Total;
    ::NMonitoring::TDynamicCounters::TCounterPtr MaxLsns;
    ::NMonitoring::TDynamicCounters::TCounterPtr BackpressureHits;
    ::NMonitoring::TDynamicCounters::TCounterPtr NewestLsn;
    std::array<::NMonitoring::TDynamicCounters::TCounterPtr, kBufferStateCount> BufferStateGauges;
    ::NMonitoring::TDynamicCounters::TCounterPtr AvgPbUsedPct;       // 100 - free
    ::NMonitoring::TDynamicCounters::TCounterPtr AvgPbFreeSpacePct;  // 100 - used (spec name)
    ::NMonitoring::TDynamicCounters::TCounterPtr AvgPbFreeSpacePctMin;
    ::NMonitoring::TDynamicCounters::TCounterPtr AvgPbFreeSpacePctMax;
    ::NMonitoring::TDynamicCounters::TCounterPtr SyncGateThreshold;
    ::NMonitoring::TDynamicCounters::TCounterPtr SyncGateFlushBlocked;
    ::NMonitoring::TDynamicCounters::TCounterPtr SyncGateEraseBlocked;

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& g, bool isRoot) {
        Total            = g->GetCounter("Total", false);
        MaxLsns          = g->GetCounter("MaxLsns", false);
        BackpressureHits = g->GetCounter("BackpressureHits", true);
        NewestLsn        = g->GetCounter("NewestLsn", false);
        for (ui32 i = 0; i < kBufferStateCount; ++i) {
            auto sub = g->GetSubgroup("state", ToString(static_cast<EPBufferState>(i)));
            BufferStateGauges[i] = sub->GetCounter("Count", false);
        }
        AvgPbUsedPct       = g->GetCounter("AvgPbUsedPct", false);
        AvgPbFreeSpacePct  = g->GetCounter("AvgPbFreeSpacePct", false);
        SyncGateThreshold    = g->GetCounter("SyncGateThreshold", false);
        SyncGateFlushBlocked = g->GetCounter("SyncGateFlushBlocked", true);
        SyncGateEraseBlocked = g->GetCounter("SyncGateEraseBlocked", true);
        if (isRoot) {
            AvgPbFreeSpacePctMin = g->GetCounter("AvgPbFreeSpacePctMin", false);
            AvgPbFreeSpacePctMax = g->GetCounter("AvgPbFreeSpacePctMax", false);
        }
    }
};

struct TPeerCounters {
    ::NMonitoring::TDynamicCounters::TCounterPtr Connected;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestsSent;
    ::NMonitoring::TDynamicCounters::TCounterPtr RepliesOk;
    ::NMonitoring::TDynamicCounters::TCounterPtr RepliesErr;
    ::NMonitoring::TDynamicCounters::TCounterPtr FreeSpacePct;
    ::NMonitoring::THistogramPtr ResponseTimeMs;
    std::array<::NMonitoring::TDynamicCounters::TCounterPtr, kOpCount> RequestsSentByOp;
    std::array<::NMonitoring::TDynamicCounters::TCounterPtr, kOpCount> RepliesOkByOp;
    std::array<::NMonitoring::TDynamicCounters::TCounterPtr, kOpCount> RepliesErrByOp;
};

struct TPerDbgCounters {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Root;
    std::array<TOpCounters, kOpCount> Op;
    TRequestCounters Request;
    TLsnsCounters Lsns;
    std::array<TPeerCounters, 2 * kHostsPerDbgMax> Peers;  // PB0..4, DD0..4
    bool PerPeerEnabled = false;
};

struct TDDiskIdHash {
    size_t operator()(const NKikimrBlobStorage::NDDisk::TDDiskId& id) const {
        return MultiHash(id.GetNodeId(), id.GetPDiskId(), id.GetDDiskSlotId());
    }
};

struct TDDiskIdEqual {
    bool operator()(
        const NKikimrBlobStorage::NDDisk::TDDiskId& lhs,
        const NKikimrBlobStorage::NDDisk::TDDiskId& rhs) const
    {
        return lhs.GetNodeId() == rhs.GetNodeId()
            && lhs.GetPDiskId() == rhs.GetPDiskId()
            && lhs.GetDDiskSlotId() == rhs.GetDDiskSlotId();
    }
};

struct TPerDbgState {
    ui32 DbgIndex = 0;
    ui64 DirectBlockGroupId = 0;

    std::array<NKikimrBlobStorage::NDDisk::TDDiskId, kHostsPerDbgMax> DDiskIdsPb;  // BSC TDDiskId values
    std::array<NKikimrBlobStorage::NDDisk::TDDiskId, kHostsPerDbgMax> PBIdsPb;
    std::array<TActorId, kHostsPerDbgMax> DDiskActor;
    std::array<TActorId, kHostsPerDbgMax> PBActor;
    std::array<ui64, kHostsPerDbgMax> DDGuid = {};
    std::array<ui64, kHostsPerDbgMax> PBGuid = {};
    std::array<std::optional<NDDisk::TConnectionToken>, kHostsPerDbgMax> DDToken;
    std::array<std::optional<NDDisk::TConnectionToken>, kHostsPerDbgMax> PBToken;
    std::bitset<kHostsPerDbgMax> DDConnected;
    std::bitset<kHostsPerDbgMax> PBConnected;

    // TDDiskId -> k in [0..kHostsPerDbgMax).
    absl::flat_hash_map<NKikimrBlobStorage::NDDisk::TDDiskId, ui32, TDDiskIdHash, TDDiskIdEqual> PbIndexById;

    absl::flat_hash_map<ui64, TWriteInfo> Lsns;

    // Acceptance order, visible versions, read pins, and cohort admission.
    // Actionable queues contain only the oldest eligible version of a slot.
    TSlotIoCoordinator Slots;
    TMaintenanceScheduler Scheduler;
    std::vector<TDynBitMap> FlushedSlots;

    struct TVChunkActivity {
        ui64 IncompleteWrites = 0;
        ui64 SyncSegments = 0;

        bool IsIdle() const {
            return !IncompleteWrites && !SyncSegments;
        }
    };
    std::vector<TVChunkActivity> VChunkActivity;

    std::array<ui32, kPrimaryHostsPerDbg> InFlightTo = {};
    ui32 WritesInFlight = 0;
    ui32 ReadsInFlight = 0;

    // Per-PB free-space ratio reported by the device, 0..1 (1 = empty).
    std::array<double, kHostsPerDbgMax> LastFreeSpace = {1.0, 1.0, 1.0, 1.0, 1.0};
    // Used % across the primary PBs (= 100 - free %). Monitoring metric only;
    // the spec-named "AvgPbFreeSpacePct" is exposed as 100 - this.
    ui32 AvgPbUsedPct = 0;

    std::array<ui32, kBufferStateCount> StateCount = {};        // # of LSNs in each state

    TPerDbgCounters Counters;
};

struct TRootCounters {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Root;

    // Lifecycle
    ::NMonitoring::TDynamicCounters::TCounterPtr ConnectOk;
    ::NMonitoring::TDynamicCounters::TCounterPtr ConnectErr;
    ::NMonitoring::TDynamicCounters::TCounterPtr DisconnectOk;
    ::NMonitoring::TDynamicCounters::TCounterPtr DbgsAllocated;

    std::array<TOpCounters, kOpCount> Op;
    TRequestCounters Request;
    TLsnsCounters Lsns;
};

// Per-batch flush/erase tracking. One entry per outgoing
// TEvSync / TEvBatchErasePersistentBuffer keyed by a
// fresh u64 cookie so retries from one batch never disturb another.
struct TFlushBatch {
    ui32 DbgIndex = 0;
    ui8  Sink = 0;            // k in [0..kPrimaryHostsPerDbg)
    std::vector<ui64> Lsns;
    NActors::TMonotonic SentAt;
    TActorId ReplyActor;
};

struct TEraseBatch {
    ui32 DbgIndex = 0;
    ui8  Sink = 0;            // k in [0..kHostsPerDbgMax)
    std::vector<ui64> Lsns;
    NActors::TMonotonic SentAt;
    TActorId ReplyActor;
};

struct TReadInflight {
    ui32 DbgIndex = 0;
    ui32 PeerK = 0;           // PB index for ReadPB; DD index for ReadDDisk.
    bool IsPb = true;
    ui32 Size = 0;
    NActors::TMonotonic SentAt;
    TActorId ReplyActor;
    TSlotIoCoordinator::TSlot Slot;
    ui64 Lsn = 0;

    // v2: load-actor origin so the worker can hand the TNbsReadResult back.
    // Zero TActorId means "internal read" (none today; reserved for future).
    NActors::TActorId OriginActor;
    ui64              OriginCookie = 0;

    NWilson::TSpan Span;
};

struct TDecodedAddress {
    ui32 DbgIndex = 0;
    ui32 VChunkIndex = 0;
    ui32 OffsetInVChunk = 0;
};

// Tablet-local drain protocol. Configuration generations identify installation
// acknowledgements; drain epochs identify the accepted work being retired.
struct TEvDbgDrain {
    enum EEv {
        EvDrain = EventSpaceBegin(TEvents::ES_PRIVATE) + 100,
        EvDrained,
        EvTick,
        EvContinue,
        EvIdleCleanup,
    };

    struct TEvDrain : TEventLocal<TEvDrain, EvDrain> {
        ui64 Epoch;
        bool Stop;

        TEvDrain(ui64 epoch, bool stop)
            : Epoch(epoch)
            , Stop(stop)
        {}
    };

    struct TEvDrained : TEventLocal<TEvDrained, EvDrained> {
        ui64 Epoch;

        explicit TEvDrained(ui64 epoch)
            : Epoch(epoch)
        {}
    };

    struct TEvTick : TEventLocal<TEvTick, EvTick> {};

    struct TEvContinue : TEventLocal<TEvContinue, EvContinue> {};

    struct TEvIdleCleanup : TEventLocal<TEvIdleCleanup, EvIdleCleanup> {
        ui64 Generation;

        explicit TEvIdleCleanup(ui64 generation)
            : Generation(generation) {
        }
    };
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

bool CanDropErasedLsn(const TPeerBitset& eraseTarget, const TPeerBitset& eraseConfirmed) {
    return eraseTarget.any() && eraseConfirmed == eraseTarget;
}

bool ShouldReadFromPBuffer(EPBufferState state) {
    switch (state) {
        case EPBufferState::PBufferWritten:
            [[fallthrough]];
        case EPBufferState::PBufferFlushing:
            return true;
        case EPBufferState::PBufferIncompleteWrite:
            [[fallthrough]];
        case EPBufferState::PBufferFlushed:
            [[fallthrough]];
        case EPBufferState::PBufferErasing:
            [[fallthrough]];
        case EPBufferState::PBufferErased:
            [[fallthrough]];
        default:
            return false;
    }
}

ui32 PickRandomSetBit(const TPeerBitset& bits, ui64 randNum) {
    ui32 n = randNum % bits.count();
    for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
        if (bits.test(k) && n-- == 0) {
            return k;
        }
    }
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tablet schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

using NTabletFlatExecutor::TTabletExecutedFlat;
using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

struct Schema : NIceDb::Schema {
    struct State : Table<100> {
        struct Key             : Column<1, NScheme::NTypeIds::Bool>   { static constexpr Type Default = {}; };
        struct AllocConfig     : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, AllocConfig>;
    };

    struct Dbgs : Table<101> {
        struct DbgIndex          : Column<1, NScheme::NTypeIds::Uint32> {};
        struct DirectBlockGroupId: Column<2, NScheme::NTypeIds::Uint64> {};
        // Legacy fixed-width packing (kept for backward read; never written).
        struct DDiskIds          : Column<3, NScheme::NTypeIds::String> {};
        struct PBIds             : Column<4, NScheme::NTypeIds::String> {};
        // Current: serialized NKikimr.TNbsDbgLikeLoad.TPersistedDbgIds proto.
        struct DbgIds            : Column<5, NScheme::NTypeIds::String> {};

        using TKey = TableKey<DbgIndex>;
        using TColumns = TableColumns<DbgIndex, DirectBlockGroupId, DDiskIds, PBIds, DbgIds>;
    };

    using TTables = SchemaTables<State, Dbgs>;

    struct EmptySettings {
        static void Materialize(NIceDb::TToughDb&) {}
    };
    using TSettings = SchemaSettings<EmptySettings>;
};

// --------------------------------------------------------------------------
// TNbsDbgLikeActor
//
// One worker actor per DBG. Owns the full per-DBG runtime: the 10 DDisk/PB
// peer connections, the write/flush/erase/read state machine and the per-DBG
// counters. The proxy tablet (TNbsDbgLikeLoadTablet) forwards each NbsWrite /
// NbsRead to the right worker (preserving the original requestor as Sender),
// so the worker replies straight back to the requestor over the same IC
// session. Workers are spawned at tablet boot and poisoned when the tablet
// dies.
// --------------------------------------------------------------------------

class TNbsDbgLikeActor : public TActorBootstrapped<TNbsDbgLikeActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_NBS_DBG_LIKE_TABLET;
    }

    TNbsDbgLikeActor(
        const TActorId& tabletActorId,
        ui32 dbgIndex,
        ui32 numDbgsTotal,
        ui32 generation,
        const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig& allocConfig,
        const TDirectBlockGroup& dbgInfo,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : TabletActorId(tabletActorId)
        , MyDbgIndex(dbgIndex)
        , NumDbgsTotal(numDbgsTotal)
        , Generation_(generation)
        , AllocConfig(allocConfig)
        , DbgInfo(dbgInfo)
        , Counters(std::move(counters))
    {}

    void Bootstrap();

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NDDisk::TEvConnectResult, HandlePeerConnect);
            hFunc(NDDisk::TEvGetPersistentBufferRegistrationTokenResult, HandlePeerRegistrationToken);
            hFunc(NDDisk::TEvRegisterPersistentBufferResult, HandlePeerRegistration);
            hFunc(NDDisk::TEvListPersistentBufferResult, HandlePeerRegistrationProbe);
            hFunc(TEvents::TEvWakeup, HandlePeerRegistrationRetry);
            hFunc(NDDisk::TEvDisconnectResult, HandlePeerDisconnect);

            HFunc(TEvLoad::TEvNbsWrite, HandleNbsWrite);
            HFunc(TEvLoad::TEvNbsRead, HandleNbsRead);
            HFunc(TEvLoad::TEvConfigureTablet, HandleConfigureTablet);
            HFunc(NDDisk::TEvWritePersistentBuffersResult, HandleWritePbsResult);
            HFunc(NDDisk::TEvSyncResult, HandleSyncResult);
            HFunc(NDDisk::TEvErasePersistentBufferResult, HandleEraseResult);
            HFunc(NDDisk::TEvReadPersistentBufferResult, HandlePbReadResult);
            HFunc(NDDisk::TEvReadResult, HandleDDiskReadResult);
            HFunc(NKikimr::TEvUpdateMonitoring, HandleUpdateMonitoring);

            hFunc(TEvDbgDrain::TEvDrain, HandleDrain);
            hFunc(TEvDbgDrain::TEvTick, HandleDrainTick);
            hFunc(TEvDbgDrain::TEvContinue, HandleMaintenance);
            hFunc(TEvDbgDrain::TEvIdleCleanup, HandleIdleCleanup);
            cFunc(TEvents::TEvPoison::EventType, HandlePoison);
        }
    }

private:
    ui32 HostsPerDbg() const {
        const ui32 hosts = GetHostsPerDbg(AllocConfig);
        return Max(hosts, kPrimaryHostsPerDbg);
    }

    ui32 Generation() const {
        return Generation_;
    }

    static NActors::TMonotonic MonotonicNow() {
        return NActors::TActivationContext::Monotonic();
    }

    ui32 SyncGateThreshold() const {
        return Max<ui32>(1, TabletConfig.GetSyncRequestsBatchSize());
    }

    ui64 TotalLsns() const;

    void HandlePoison();
    void HandleDrain(TEvDbgDrain::TEvDrain::TPtr& ev);
    void HandleDrainTick(TEvDbgDrain::TEvTick::TPtr& ev);
    void HandleMaintenance(TEvDbgDrain::TEvContinue::TPtr& ev);
    void ScheduleDrainTick();
    void ScheduleIdleCleanup();
    void InvalidateIdleCleanup();
    void HandleIdleCleanup(TEvDbgDrain::TEvIdleCleanup::TPtr& ev);
    void ScheduleMaintenanceContinuation();
    void CheckDrained();
    void FinishStop();

    // ---- Peer connect (the worker's own 10 peers) -------------------------
    void KickOffPeerConnect();
    void ConnectPeer(ui32 k, bool isPb);
    void HandlePeerConnect(NDDisk::TEvConnectResult::TPtr& ev);
    void HandlePeerRegistrationToken(NDDisk::TEvGetPersistentBufferRegistrationTokenResult::TPtr& ev);
    void HandlePeerRegistration(NDDisk::TEvRegisterPersistentBufferResult::TPtr& ev);
    void HandlePeerRegistrationProbe(NDDisk::TEvListPersistentBufferResult::TPtr& ev);
    void HandlePeerRegistrationRetry(TEvents::TEvWakeup::TPtr& ev) {
        if (!Stopping) {
            ConnectPeer(ev->Get()->Tag, true);
        }
    }
    void PeerConnected(ui32 k, bool isPb);
    void HandlePeerDisconnect(NDDisk::TEvDisconnectResult::TPtr& ev);
    void DisconnectAllPeers();
    void PopulateDbgState();
    void ReportReadiness();
    static ui64 PackPeerCookie(ui32 k, bool isPb) {
        return (static_cast<ui64>(k) << 1) | (isPb ? 1u : 0u);
    }
    static void UnpackPeerCookie(ui64 cookie, ui32& k, bool& isPb) {
        k = static_cast<ui32>(cookie >> 1);
        isPb = (cookie & 1u) != 0;
    }

    // ---- Request handlers + state machine ---------------------------------
    void HandleNbsWrite(TEvLoad::TEvNbsWrite::TPtr& ev, const TActorContext& ctx);
    void HandleNbsRead(TEvLoad::TEvNbsRead::TPtr& ev, const TActorContext& ctx);
    void HandleConfigureTablet(TEvLoad::TEvConfigureTablet::TPtr& ev, const TActorContext& ctx);
    void HandleWritePbsResult(NDDisk::TEvWritePersistentBuffersResult::TPtr& ev,
        const TActorContext& ctx);
    void HandleSyncResult(NDDisk::TEvSyncResult::TPtr& ev,
        const TActorContext& ctx);
    void HandleEraseResult(NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
        const TActorContext& ctx);
    void HandlePbReadResult(NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
        const TActorContext& ctx);
    void HandleDDiskReadResult(NDDisk::TEvReadResult::TPtr& ev, const TActorContext& ctx);
    void HandleUpdateMonitoring(NKikimr::TEvUpdateMonitoring::TPtr& ev,
        const TActorContext& ctx);

    void InitWorkerCounters();
    std::expected<TDecodedAddress, EDecodeAddressError> DecodeAddress(
        ui64 address, ui32 sizeBytes) const;
    ui32 ChooseCoordinator(const TPerDbgState& dbg) const;
    void EnterState(TPerDbgState& dbg, EPBufferState s, int delta = +1);
    void UpdateLsnsTotal(TPerDbgState& dbg);
    void UpdateAvgPbFreeSpacePct(TPerDbgState& dbg);
    TSlotIoCoordinator::TSlot SlotOf(const TWriteInfo& info) const;
    void DriveFlushAdmission(TPerDbgState& dbg);
    void DriveEraseAdmission(TPerDbgState& dbg);
    void WakeFlush(TPerDbgState& dbg, TSlotIoCoordinator::TSlot slot);
    void WakeErase(TPerDbgState& dbg, TSlotIoCoordinator::TSlot slot);
    bool PumpFlush(TPerDbgState& dbg);
    bool PumpErase(TPerDbgState& dbg);
    void AccountFlushGate(const TPerDbgState& dbg, bool scheduled);
    void AccountEraseGate(const TPerDbgState& dbg, bool scheduled);
    void ReleaseErasedLsn(TPerDbgState& dbg, ui64 lsn);
    void AccountReadRequest(TPerDbgState& dbg, EOp op, ui32 size);
    bool SendPbRead(TPerDbgState& dbg, ui32 dbgIndex, ui64 lsn,
        const TWriteInfo& info, const TActorId& origin, ui64 originCookie,
        NWilson::TSpan& span);
    bool SendDDiskRead(TPerDbgState& dbg, ui32 dbgIndex, ui32 vChunkIndex,
        ui64 offset, ui32 size, std::bitset<kHostsPerDbgMax> flushMask,
        const TActorId& origin, ui64 originCookie, NWilson::TSpan& span);
    bool CompleteRead(ui64 cookie, bool ok, TActorId& origin, ui64& originCookie,
        ui32& size, TStringBuf errorReason = {});
    void ReplyWriteErr(const TActorId& origin, ui64 cookie,
        ENbsIoResultStatus status, TString reason = {});
    void ReplyReadErr(const TActorId& origin, ui64 cookie,
        ENbsIoResultStatus status, TString reason = {});

    static ui32 LocateInPbIds(const TPerDbgState& dbg,
        const NKikimrBlobStorage::NDDisk::TDDiskId& id);
    static void RegisterPeerOpCounters(TPeerCounters& pc,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& peerGroup, EOp op);
    static void BumpPeerRequest(TPerDbgState& dbg, ui32 peerIndex, EOp op);
    static void BumpPeerReply(TPerDbgState& dbg, ui32 peerIndex, EOp op, bool ok);

    // ---- State ------------------------------------------------------------

    const TActorId TabletActorId;
    const ui32 MyDbgIndex = 0;
    const ui32 NumDbgsTotal = 1;
    const ui32 Generation_ = 0;
    const TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig AllocConfig;
    const TDirectBlockGroup DbgInfo;

    // Per-DBG connection state for this worker's own peers.
    struct TPeerConnState {
        ui64 Guid = 0;
        std::optional<NDDisk::TConnectionToken> Token;
        bool Connected = false;
        bool ConnectInFlight = false;
        bool DisconnectInFlight = false;
        TActorId RuntimeActor;
    };
    std::array<TPeerConnState, kHostsPerDbgMax> DD;
    std::array<TPeerConnState, kHostsPerDbgMax> PB;

    TPerDbgState Dbg;

    NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet TabletConfig;
    ui32 ActiveDbgs = 0;
    ui32 IoSizeBytes = 0;
    ui64 BytesPerDbg = 0;
    bool WorkerCountersInited = false;
    bool MonitoringScheduled = false;
    ui32 LastReportedPbConnected = Max<ui32>();

    ui64 LsnsTotalAll = 0;   // == Dbg.Lsns.size() for this worker
    ui64 SequenceGenerator = 0;

    bool Draining = false;
    bool Stopping = false;
    bool Disconnecting = false;
    bool DrainTickScheduled = false;
    bool ContinuationQueued = false;
    bool DrainAcknowledged = false;
    bool NotifyDrained = false;
    ui64 DrainEpoch = 0;
    ui64 IdleCleanupGeneration = 0;
    bool IdleCleanupScheduled = false;

    ui64 NextBatchCookie = 1;
    std::map<ui64, TFlushBatch> FlushInflight;
    std::map<ui64, TEraseBatch> EraseInflight;
    std::map<ui64, TReadInflight> ReadInflight;

    TFastRng64 Rng{MyDbgIndex};
    TRootCounters RootCnt;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
};

// --------------------------------------------------------------------------
// TNbsDbgLikeLoadTablet
//
// Derives from NKeyValue::TKeyValueFlat so we reuse KV's flat-executor
// bringup, channel/profile plumbing, monitoring, transaction machinery and
// tablet-pipe wiring. Our allocation/run metadata is stored in our own
// NIceDb tables (ids 100/101/102, see Schema below); KV uses table 0
// internally for its key-value index, so the table-id spaces do not collide
// (same idiom as NTestShard::TTestShard in ydb/core/test_tablet/).
// --------------------------------------------------------------------------

class TNbsDbgLikeLoadTablet : public NKeyValue::TKeyValueFlat {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_NBS_DBG_LIKE_TABLET;
    }

    TNbsDbgLikeLoadTablet(const TActorId& tablet, TTabletStorageInfo* info)
        : NKeyValue::TKeyValueFlat(tablet, info)
    {
        SetActivityType(ActorActivityType());
    }

    // KV's DefaultSignalTabletActive is final and empty - the tablet is
    // marked active by us from OnLoadComplete once Dbgs are restored.

    void OnActivateExecutor(const TActorContext& ctx) override {
        Generation_ = Executor()->Generation();
        NKeyValue::TKeyValueFlat::OnActivateExecutor(ctx);
    }

    void CreatedHook(const TActorContext& ctx) override {
        // KV base calls CreatedHook after it has finished its own init
        // (kvtable load, initial GC, etc). Chain into our schema bringup.
        Execute(CreateTxInitScheme(), ctx);
    }

    bool HandleHook(STFUNC_SIG) override {
        LOG_D("HandleHook ev type# " << ev->GetTypeRewrite()
            << " name# " << ev->GetTypeName()
            << " Sender# " << ev->Sender
            << " Cookie# " << ev->Cookie);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLoad::TEvNbsLoadTabletAllocateGroups, Handle);
            HFunc(TEvLoad::TEvNbsLoadTabletDelete, Handle);
            HFunc(TEvLoad::TEvNbsLoadTabletGetSummary, Handle);
            HFunc(TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult, Handle);
            HFunc(TEvLoad::TEvNbsDbgActorReady, Handle);
            HFunc(TEvDbgDrain::TEvDrained, HandleWorkerDrained);

            // Proxy: route the per-request work to the per-DBG worker actors.
            HFunc(TEvLoad::TEvNbsWrite, HandleNbsWrite);
            HFunc(TEvLoad::TEvNbsRead, HandleNbsRead);
            HFunc(TEvLoad::TEvConfigureTablet, HandleConfigureTablet);
            HFunc(TEvLoad::TEvConfigureTabletResult, HandleConfigurationResult);

            case TEvents::TEvWakeup::EventType: {
                auto* msg = ev->Get<TEvents::TEvWakeup>();
                if (msg->Tag != kWakeupBscRetry) {
                    return false;
                }
                auto p = TEvents::TEvWakeup::TPtr(
                    static_cast<TEventHandle<TEvents::TEvWakeup>*>(ev.Release()));
                HandleWakeup(p);
                return true;
            }
            // Pipe events: only intercept our own BSC pipe; let KV handle
            // anything else (KV opens its own pipes for backpressure).
            case TEvTabletPipe::TEvClientConnected::EventType: {
                auto* msg = ev->Get<TEvTabletPipe::TEvClientConnected>();
                if (msg->ClientId != BscPipeClient) {
                    return false;
                }
                auto p = TEvTabletPipe::TEvClientConnected::TPtr(static_cast<TEventHandle<TEvTabletPipe::TEvClientConnected>*>(ev.Release()));
                Handle(p, TActivationContext::AsActorContext());
                return true;
            }
            case TEvTabletPipe::TEvClientDestroyed::EventType: {
                auto* msg = ev->Get<TEvTabletPipe::TEvClientDestroyed>();
                if (msg->ClientId != BscPipeClient) {
                    return false;
                }
                auto p = TEvTabletPipe::TEvClientDestroyed::TPtr(static_cast<TEventHandle<TEvTabletPipe::TEvClientDestroyed>*>(ev.Release()));
                Handle(p, TActivationContext::AsActorContext());
                return true;
            }
            default:
                return false;
        }
        return true;
    }

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override {
        if (!ev) {
            return true;
        }
        // ?page=keyvalue routes to the KV base's mon page; anything else
        // renders our own status / DBG roster / run history.
        const auto& cgi = ev->Get()->Cgi();
        if (cgi.Get("page") == "keyvalue") {
            return NKeyValue::TKeyValueFlat::OnRenderAppHtmlPage(ev, ctx);
        }

        if (ev->Get()->GetMethod() == HTTP_METHOD_POST) {
            TStringStream html;
            HTML(html) {
                DIV_CLASS("alert alert-info") {
                    html << "POST to this tablet URL is not supported. "
                        << "Start workload runs via the load-test service "
                        << "(<code>POST /actors/load</code> with "
                        << "<code>NbsDbgLikeLoad</code> command).";
                }
                html << "<p><a href=''>Back to tablet page</a></p>";
            }
            ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
            return true;
        }

        TStringStream str;
        RenderHtml(str);
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    // Best-effort tablet-local cleanup that must run regardless of which
    // shutdown path (poison vs sys-tablet-driven TEvTabletDead vs direct
    // PassAway) took us out. Idempotent so the natural chain
    // OnDetach/OnTabletDead -> HandleDie -> Die -> PassAway can call it
    // from each step without doubling up.
    void Cleanup() {
        if (CleanedUp_) {
            return;
        }
        CleanedUp_ = true;
        PoisonDbgActors();
        if (BscPipeClient) {
            NTabletPipe::CloseClient(SelfId(), BscPipeClient);
            BscPipeClient = TActorId();
        }
        // Workers retain their counters until their accepted I/O retires.
    }

    void OnDetach(const TActorContext& ctx) override {
        LOG_N("OnDetach TabletId# " << TabletID()
            << " Generation# " << Generation());
        Cleanup();
        NKeyValue::TKeyValueFlat::OnDetach(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override {
        LOG_N("OnTabletDead TabletId# " << TabletID()
            << " Generation# " << Generation()
            << " Reason# " << ev->Get()->Reason);
        Cleanup();
        NKeyValue::TKeyValueFlat::OnTabletDead(ev, ctx);
    }

    void PassAway() override {
        LOG_N("PassAway TabletId# " << TabletID()
            << " Generation# " << Generation());
        Cleanup();
        NKeyValue::TKeyValueFlat::PassAway();
    }

private:
    // ---- Schema transactions ----------------------------------------------

    class TTxInitScheme;
    class TTxLoadEverything;
    class TTxStoreState;
    class TTxStoreDbgs;
    class TTxClearAll;

    ITransaction* CreateTxInitScheme();
    ITransaction* CreateTxLoadEverything();
    ITransaction* CreateTxStoreState(const TString& serialized);
    ITransaction* CreateTxStoreDbgs(std::vector<TDirectBlockGroup> dbgs);
    ITransaction* CreateTxClearAll();

    // ---- Lifecycle --------------------------------------------------------

    // ctx MUST come from the caller (the calling transaction's Complete
    // forwards its own ownerCtx). We cannot fall back to
    // TlsActivationContext->AsActorContext() here, because transaction
    // Complete callbacks run synchronously from inside the executor's event
    // handler, so TLS SelfID resolves to the *executor* actor id rather than
    // ours - and any message sent with that SelfID as sender (e.g. peer
    // TEvConnect from KickOffPeerConnect) gets its reply routed to the
    // executor, which has no handler for it and silently drops it.
    void OnLoadComplete(const TActorContext& ctx) {
        LOG_I("Load complete TabletId# " << TabletID()
            << " Phase# " << Phase
            << " Dbgs# " << Dbgs.size());
        SignalTabletActive(SelfId());
        EmitPhaseGauge();
        if (Phase == ETabletPhase::Ready) {
            SpawnDbgActors(ctx);
        }
    }

    // ---- Handlers ---------------------------------------------------------

    void Handle(TEvLoad::TEvNbsLoadTabletAllocateGroups::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLoad::TEvNbsLoadTabletDelete::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLoad::TEvNbsLoadTabletGetSummary::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLoad::TEvNbsDbgActorReady::TPtr& ev, const TActorContext& ctx);
    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev);

    ui32 HostsPerDbg() const {
        const ui32 hosts = GetHostsPerDbg(AllocConfig);
        return Max(hosts, kPrimaryHostsPerDbg);
    }

    // Returns the generation stamped at boot. Stays valid after the executor
    // has been detached (HandlePoison / HandleTabletDead null Executor() before
    // calling OnDetach/OnTabletDead/PassAway), which is required so the spawned
    // worker actors carry the right generation in their peer credentials.
    ui32 Generation() const {
        if (Generation_) {
            return Generation_;
        }
        return Executor() ? Executor()->Generation() : 0;
    }

    void EnsureCounters(const TActorContext& ctx);
    void EmitPhaseGauge();

    // ---- Proxy: spawn / poison workers and route requests ----------------
    void SpawnDbgActors(const TActorContext& ctx);
    void PoisonDbgActors();
    void RecomputeRouting(const NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet& cfg);

    void HandleNbsWrite(TEvLoad::TEvNbsWrite::TPtr& ev, const TActorContext& ctx);
    void HandleNbsRead(TEvLoad::TEvNbsRead::TPtr& ev, const TActorContext& ctx);
    TActorId ConfigurationRequester;
    ui64 ConfigurationCookie = 0;
    ui64 ConfigurationId = 0;
    ui64 ConfigurationGeneration = 0;
    THashSet<TActorId> ConfigurationPending;
    THashSet<TActorId> DrainPending;
    ui64 DrainEpoch = 0;
    bool Reconfiguring = false;
    bool BscDeallocAuthorized = false;
    std::optional<NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet> PendingConfiguration;

    void BeginWorkerDrain(const TActorContext& ctx);
    void HandleWorkerDrained(TEvDbgDrain::TEvDrained::TPtr& ev, const TActorContext& ctx);
    void FinishWorkerDrain(const TActorContext& ctx);
    void InstallConfiguration(const TActorContext& ctx);

    void ReplyConfiguration(bool success, const TString& error, const TActorContext& ctx) {
        if (ConfigurationRequester) {
            auto reply = std::make_unique<TEvLoad::TEvConfigureTabletResult>();
            reply->Record.SetConfigurationId(ConfigurationId);
            reply->Record.SetSuccess(success);
            reply->Record.SetError(error);
            ctx.Send(ConfigurationRequester, reply.release(), 0, ConfigurationCookie);
            ConfigurationRequester = {};
        }
    }

    void HandleConfigurationResult(TEvLoad::TEvConfigureTabletResult::TPtr& ev, const TActorContext& ctx) {
        if (!Reconfiguring || !PendingConfiguration || !DrainPending.empty()
                || ev->Cookie != ConfigurationGeneration
                || ev->Get()->Record.GetConfigurationId() != PendingConfiguration->GetConfigurationId()
                || !ConfigurationPending.erase(ev->Sender)) {
            return;
        }
        if (!ev->Get()->Record.GetSuccess()) {
            ReplyConfiguration(false, "DBG rejected configuration", ctx);
            ConfigurationPending.clear();
        } else if (ConfigurationPending.empty()) {
            RecomputeRouting(*PendingConfiguration);
            Reconfiguring = false;
            PendingConfiguration.reset();
            ReplyConfiguration(true, {}, ctx);
        }
    }

    void HandleConfigureTablet(TEvLoad::TEvConfigureTablet::TPtr& ev, const TActorContext& ctx);

    void SendBscAllocate(const TActorContext& ctx, bool dealloc);
    void OnBscPipeBroken(const TActorContext& ctx);
    void RetryBscOperation(const TActorContext& ctx);
    void ScheduleBscRetry(const TActorContext& ctx);
    void FailPendingCreate(const TActorContext& ctx, const TString& reason);
    void FailPendingDelete(const TActorContext& ctx, const TString& reason);

    void RenderHtml(IOutputStream& out) const;

    static constexpr ui64 kWakeupBscRetry = 1;

    // ---- State ------------------------------------------------------------

    ETabletPhase Phase = ETabletPhase::Uninitialized;

    // Cached at OnActivateExecutor so the spawned workers get the right
    // generation, even on the poison / TabletDead paths where
    // TTabletExecutedFlat nulls Executor() before our hooks run.
    ui32 Generation_ = 0;

    bool CleanedUp_ = false;

    TString AllocConfigSerialized;  // empty when uninitialized
    TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig AllocConfig;
    std::vector<TDirectBlockGroup> Dbgs;

    // One worker actor per DBG (parallel to Dbgs[]), spawned at boot.
    std::vector<TActorId> DbgActors;
    // Cached per-DBG primary-PB connectivity reported by the workers; used by
    // GetSummary to compute the ready-DBG prefix.
    std::vector<ui32> DbgPbConnected;

    // Pipe to BSController.
    TActorId BscPipeClient;
    bool BscRetryScheduled = false;
    ui32 BscRetryAttempts = 0;
    ui64 BscRequestGeneration = 0;
    bool BscDeallocInFlight = false;  // distinguishes alloc vs dealloc in retry
    TInstant BscRequestSentAt;

    // Pending Create reply target while BSC alloc is in flight.
    TActorId PendingCreateReplyTo;
    ui64 PendingCreateCookie = 0;
    // Pending Delete reply target while dealloc is in flight.
    TActorId PendingDeleteReplyTo;
    ui64 PendingDeleteCookie = 0;

    // Routing params computed from the last TConfigureTablet so the proxy can
    // map an address to the owning DBG worker.
    ui32 ActiveDbgs = 0;
    ui32 IoSizeBytes = 0;
    ui64 BytesPerDbg = 0;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr PhaseGauge;
    ::NMonitoring::TDynamicCounters::TCounterPtr BscAllocOk;
    ::NMonitoring::TDynamicCounters::TCounterPtr BscAllocErr;
    ::NMonitoring::TDynamicCounters::TCounterPtr BscDeallocOk;
    ::NMonitoring::TDynamicCounters::TCounterPtr BscDeallocErr;
};

// ---- Tx: InitScheme ------------------------------------------------------

class TNbsDbgLikeLoadTablet::TTxInitScheme : public TTransactionBase<TNbsDbgLikeLoadTablet> {
public:
    explicit TTxInitScheme(TNbsDbgLikeLoadTablet* self) : TTransactionBase(self) {}
    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }
    void Complete(const TActorContext& ctx) override {
        Self->Execute(Self->CreateTxLoadEverything(), ctx);
    }
};

ITransaction* TNbsDbgLikeLoadTablet::CreateTxInitScheme() {
    return new TTxInitScheme(this);
}

// ---- Tx: LoadEverything --------------------------------------------------

class TNbsDbgLikeLoadTablet::TTxLoadEverything : public TTransactionBase<TNbsDbgLikeLoadTablet> {
public:
    explicit TTxLoadEverything(TNbsDbgLikeLoadTablet* self) : TTransactionBase(self) {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        ui32 hostsPerDbg = kHostsPerDbgMax;

        // State
        {
            using T = Schema::State;
            auto row = db.Table<T>().Key(T::Key::Default).Select();
            if (!row.IsReady()) {
                return false;
            }
            if (row.IsValid()) {
                AllocConfigSerialized = row.GetValue<T::AllocConfig>();
                if (!AllocConfigSerialized.empty()) {
                    TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig cfg;
                    if (cfg.ParseFromString(AllocConfigSerialized)) {
                        hostsPerDbg = Max(GetHostsPerDbg(cfg), kPrimaryHostsPerDbg);
                    }
                }
            }
        }

        // Dbgs
        {
            using T = Schema::Dbgs;
            auto rows = db.Table<T>().Range().Select();
            if (!rows.IsReady()) {
                return false;
            }
            while (rows.IsValid()) {
                TDirectBlockGroup d;
                d.DbgIndex = rows.GetValue<T::DbgIndex>();
                d.DirectBlockGroupId = rows.GetValue<T::DirectBlockGroupId>();

                bool parsed = false;
                if (rows.HaveValue<T::DbgIds>()) {
                    NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TPersistedDbgIds ids;
                    if (ids.ParseFromString(rows.GetValue<T::DbgIds>())
                        && ids.DDiskIdsSize() == hostsPerDbg
                        && ids.PBIdsSize() == hostsPerDbg) {
                        for (ui32 k = 0; k < hostsPerDbg; ++k) {
                            d.DDiskIds[k].CopyFrom(ids.GetDDiskIds(k));
                            d.PBIds[k].CopyFrom(ids.GetPBIds(k));
                        }
                        parsed = true;
                    }
                }
                if (!parsed) {
                    DroppedDbgIndices.push_back(d.DbgIndex);
                    if (!rows.Next()) {
                        return false;
                    }
                    continue;
                }

                Dbgs.push_back(std::move(d));
                if (!rows.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!DroppedDbgIndices.empty()) {
            LOG_E("Dropped " << DroppedDbgIndices.size()
                << " malformed Dbgs row(s) at boot — first index "
                << DroppedDbgIndices.front()
                << "; tablet rolls back to UNINITIALIZED so the user can re-Create.");
        }
        Self->AllocConfigSerialized = std::move(AllocConfigSerialized);

        bool malformedConfig = false;
        if (!Self->AllocConfigSerialized.empty()) {
            if (!Self->AllocConfig.ParseFromString(Self->AllocConfigSerialized)) {
                LOG_E("Malformed AllocConfig blob in tablet schema; rolling back to UNINITIALIZED");
                malformedConfig = true;
            }
        }

        // Verify the surviving rows form a contiguous 0..N-1 sequence. A gap
        // (e.g. rows 0, 1, 3 with row 2 missing) would crash SpawnDbgActors
        // via Y_DEBUG_ABORT_UNLESS, so we must catch it here and roll back
        // gracefully instead. Since DbgIndex is the schema key the range scan
        // returns rows in order, so a single position check suffices.
        bool hasGap = false;
        for (ui32 i = 0; i < Dbgs.size(); ++i) {
            if (Dbgs[i].DbgIndex != i) {
                LOG_E("Gap in loaded Dbgs at position " << i
                    << " (DbgIndex=" << Dbgs[i].DbgIndex
                    << "); rolling back to UNINITIALIZED so the user can re-Create.");
                hasGap = true;
                break;
            }
        }

        // If we lost any rows, found a gap, or the config is malformed, force
        // UNINITIALIZED; operator must re-Create.
        if (hasGap || malformedConfig || !DroppedDbgIndices.empty()) {
            Self->AllocConfigSerialized.clear();
            Self->AllocConfig.Clear();
            Self->Dbgs.clear();
            Self->Phase = ETabletPhase::Uninitialized;
        } else {
            Self->Dbgs = std::move(Dbgs);
            if (!Self->AllocConfigSerialized.empty()) {
                Self->Phase = Self->Dbgs.empty() ? ETabletPhase::Allocating : ETabletPhase::Ready;
            } else {
                Self->Phase = ETabletPhase::Uninitialized;
            }
        }
        Self->OnLoadComplete(ctx);
    }

private:
    TString AllocConfigSerialized;
    std::vector<TDirectBlockGroup> Dbgs;
    std::vector<ui32> DroppedDbgIndices;
};

ITransaction* TNbsDbgLikeLoadTablet::CreateTxLoadEverything() {
    return new TTxLoadEverything(this);
}

// ---- Tx: StoreState ------------------------------------------------------

class TNbsDbgLikeLoadTablet::TTxStoreState : public TTransactionBase<TNbsDbgLikeLoadTablet> {
public:
    TTxStoreState(TNbsDbgLikeLoadTablet* self, TString serialized)
        : TTransactionBase(self)
        , Serialized(std::move(serialized))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        using T = Schema::State;
        db.Table<T>().Key(T::Key::Default).Update<T::AllocConfig>(Serialized);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->BscRetryAttempts = 0;
        Self->BscDeallocInFlight = false;
        Self->SendBscAllocate(ctx, /*dealloc=*/false);
    }

private:
    TString Serialized;
};

ITransaction* TNbsDbgLikeLoadTablet::CreateTxStoreState(const TString& serialized) {
    return new TTxStoreState(this, serialized);
}

// ---- Tx: StoreDbgs -------------------------------------------------------

class TNbsDbgLikeLoadTablet::TTxStoreDbgs : public TTransactionBase<TNbsDbgLikeLoadTablet> {
public:
    TTxStoreDbgs(TNbsDbgLikeLoadTablet* self, std::vector<TDirectBlockGroup> dbgs)
        : TTransactionBase(self)
        , Dbgs(std::move(dbgs))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        using T = Schema::Dbgs;
        const ui32 hostsPerDbg = Self->HostsPerDbg();
        for (const auto& d : Dbgs) {
            NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TPersistedDbgIds ids;
            for (ui32 k = 0; k < hostsPerDbg; ++k) {
                ids.AddDDiskIds()->CopyFrom(d.DDiskIds[k]);
                ids.AddPBIds()->CopyFrom(d.PBIds[k]);
            }
            TString blob;
            Y_PROTOBUF_SUPPRESS_NODISCARD ids.SerializeToString(&blob);
            db.Table<T>().Key(d.DbgIndex).Update<
                T::DirectBlockGroupId,
                T::DbgIds>(d.DirectBlockGroupId, blob);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->Dbgs = std::move(Dbgs);
        Self->Phase = ETabletPhase::Ready;
        if (Self->BscAllocOk) {
            Self->BscAllocOk->Inc();
        }
        Self->EmitPhaseGauge();
        if (Self->PendingCreateReplyTo) {
            auto reply = std::make_unique<TEvLoad::TEvNbsLoadTabletAllocateGroupsResult>();
            reply->Record.SetStatus(NKikimr::NBSLT_OK);
            ctx.Send(Self->PendingCreateReplyTo, reply.release(), 0, Self->PendingCreateCookie);
            Self->PendingCreateReplyTo = TActorId();
            Self->PendingCreateCookie = 0;
        }
        // Spawn the per-DBG worker actors for the freshly-allocated DBGs; each
        // worker pre-establishes its own peer connections at boot.
        Self->SpawnDbgActors(ctx);
    }

private:
    std::vector<TDirectBlockGroup> Dbgs;
};

ITransaction* TNbsDbgLikeLoadTablet::CreateTxStoreDbgs(std::vector<TDirectBlockGroup> dbgs) {
    return new TTxStoreDbgs(this, std::move(dbgs));
}

// ---- Tx: ClearAll --------------------------------------------------------

class TNbsDbgLikeLoadTablet::TTxClearAll : public TTransactionBase<TNbsDbgLikeLoadTablet> {
public:
    explicit TTxClearAll(TNbsDbgLikeLoadTablet* self) : TTransactionBase(self) {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        using TS = Schema::State;
        db.Table<TS>().Key(TS::Key::Default).Delete();

        for (const auto& d : Self->Dbgs) {
            db.Table<Schema::Dbgs>().Key(d.DbgIndex).Delete();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        // Poison the per-DBG workers (they own the peer connections, whose
        // credentials are derived from the BSC alloc state we just cleared)
        // and reset all tablet-side state.
        Self->PoisonDbgActors();
        Self->Reconfiguring = false;
        Self->PendingConfiguration.reset();
        Self->ConfigurationPending.clear();
        Self->DrainPending.clear();
        Self->BscDeallocAuthorized = false;
        Self->ActiveDbgs = 0;
        Self->IoSizeBytes = 0;
        Self->BytesPerDbg = 0;
        Self->Dbgs.clear();
        Self->DbgPbConnected.clear();
        Self->AllocConfigSerialized.clear();
        Self->AllocConfig.Clear();
        Self->Phase = ETabletPhase::Uninitialized;
        Self->EmitPhaseGauge();
        if (Self->PendingDeleteReplyTo) {
            auto reply = std::make_unique<TEvLoad::TEvNbsLoadTabletDeleteResult>();
            reply->Record.SetStatus(NBSLT_OK);
            ctx.Send(Self->PendingDeleteReplyTo, reply.release(), 0, Self->PendingDeleteCookie);
            Self->PendingDeleteReplyTo = TActorId();
            Self->PendingDeleteCookie = 0;
        }
    }
};

ITransaction* TNbsDbgLikeLoadTablet::CreateTxClearAll() {
    return new TTxClearAll(this);
}

// ---- Counter wiring ------------------------------------------------------

void TNbsDbgLikeLoadTablet::EnsureCounters(const TActorContext& ctx) {
    if (Counters) {
        return;
    }
    Counters = GetServiceCounters(AppData(ctx)->Counters, "load_actor")->GetSubgroup("load", "tablet");
    PhaseGauge = Counters->GetCounter("Phase", false);
    auto life = Counters->GetSubgroup("subsystem", "lifecycle");
    BscAllocOk    = life->GetCounter("BscAllocOk", true);
    BscAllocErr   = life->GetCounter("BscAllocErr", true);
    BscDeallocOk  = life->GetCounter("BscDeallocOk", true);
    BscDeallocErr = life->GetCounter("BscDeallocErr", true);
}

void TNbsDbgLikeLoadTablet::EmitPhaseGauge() {
    if (PhaseGauge) {
        PhaseGauge->Set(static_cast<ui32>(Phase));
    }
    if (Counters) {
        auto life = Counters->GetSubgroup("subsystem", "lifecycle_worker");
        life->GetCounter("DbgsAllocated", false)->Set(Dbgs.size());
    }
}

// ---- TEvCreate -----------------------------------------------------------

void TNbsDbgLikeLoadTablet::Handle(TEvLoad::TEvNbsLoadTabletAllocateGroups::TPtr& ev,
    const TActorContext& ctx)
{
    EnsureCounters(ctx);
    LOG_N("Create request TabletId# " << TabletID()
        << " Sender# " << ev->Sender
        << " Phase# " << Phase);

    auto reply = [&](NKikimr::ENbsLoadTabletStatus s, const TString& err = {}) {
        auto r = std::make_unique<TEvLoad::TEvNbsLoadTabletAllocateGroupsResult>();
        r->Record.SetStatus(s);
        if (err) {
            r->Record.SetErrorReason(err);
        }
        ctx.Send(ev->Sender, r.release(), 0, ev->Cookie);
    };

    if (Phase == ETabletPhase::Ready) {
        return reply(NBSLT_ALREADY_INITIALIZED);
    }
    if (Phase == ETabletPhase::Deleting || Phase == ETabletPhase::Allocating) {
        return reply(NBSLT_BUSY);
    }
    Y_DEBUG_ABORT_UNLESS(Phase == ETabletPhase::Uninitialized);

    if (!ev->Get()->Record.HasAllocConfig()) {
        return reply(NBSLT_INTERNAL_ERROR, "missing AllocConfig");
    }

    AllocConfig = ev->Get()->Record.GetAllocConfig();

    // Storage namespace owner must be unique per load tablet. A shared id with
    // matching generation, DBG index, and LSN makes two load
    // tablets collide ("duplicate record with incorrect data"). Always use our
    // own (Hive-assigned) TabletID() as the BSC allocation owner and PB/DD
    // credential TabletId.
    AllocConfig.SetTabletId(TabletID());

    // Validate the persisted allocation geometry.
    if (AllocConfig.GetNumDirectBlockGroups() == 0) {
        return reply(NBSLT_INTERNAL_ERROR, "NumDirectBlockGroups must be > 0");
    }
    if (AllocConfig.GetVChunkSizeBytes() == 0
        || AllocConfig.GetVChunkSizeBytes() % 4096 != 0) {
        return reply(NBSLT_INTERNAL_ERROR,
            "VChunkSizeBytes must be a positive multiple of 4096");
    }
    {
        const ui32 hostsPerDbg = GetHostsPerDbg(AllocConfig);
        if (hostsPerDbg < kPrimaryHostsPerDbg || hostsPerDbg > kHostsPerDbgMax) {
            return reply(NBSLT_INTERNAL_ERROR, TStringBuilder()
                << "HostsPerDbg must be in ["
                << kPrimaryHostsPerDbg << ", " << kHostsPerDbgMax << "]");
        }
    }

    Y_PROTOBUF_SUPPRESS_NODISCARD AllocConfig.SerializeToString(&AllocConfigSerialized);
    Phase = ETabletPhase::Allocating;
    EmitPhaseGauge();
    PendingCreateReplyTo = ev->Sender;
    PendingCreateCookie  = ev->Cookie;

    Execute(CreateTxStoreState(AllocConfigSerialized), ctx);
}

void TNbsDbgLikeLoadTablet::SendBscAllocate(const TActorContext& ctx, bool dealloc) {
    BscDeallocInFlight = dealloc;
    if (!BscPipeClient) {
        BscPipeClient = ctx.Register(NTabletPipe::CreateClient(
            ctx.SelfID, MakeBSControllerID()));
        LOG_D("Created BSC pipe client TabletId# " << TabletID()
            << " BscPipeClient# " << BscPipeClient);
    }

    LOG_D("Send BSC " << (dealloc ? "deallocate" : "allocate")
        << " request TabletId# " << TabletID()
        << " NumDirectBlockGroups# " << AllocConfig.GetNumDirectBlockGroups()
        << " Attempt# " << (BscRetryAttempts + 1));
    auto request = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    BuildAllocateRequest(request->Record, AllocConfig, dealloc);
    BscRequestSentAt = ctx.Now();
    NTabletPipe::SendData(ctx, BscPipeClient, request.release(), ++BscRequestGeneration);
}

void TNbsDbgLikeLoadTablet::OnBscPipeBroken(const TActorContext& ctx) {
    BscPipeClient = TActorId();
    if (Phase != ETabletPhase::Allocating && !(Phase == ETabletPhase::Deleting && BscDeallocAuthorized)) {
        return;
    }
    ScheduleBscRetry(ctx);
}

void TNbsDbgLikeLoadTablet::ScheduleBscRetry(const TActorContext& ctx) {
    if (BscRetryScheduled) {
        return;
    }
    if (++BscRetryAttempts > kBscRetryLimit) {
        const TString reason = TStringBuilder()
            << "BSC pipe failed after " << kBscRetryLimit << " attempts";
        if (Phase == ETabletPhase::Allocating) {
            FailPendingCreate(ctx, reason);
        } else if (Phase == ETabletPhase::Deleting) {
            FailPendingDelete(ctx, reason);
        }
        return;
    }
    TDuration backoff = kBscRetryInitialBackoff * (1u << Min<ui32>(BscRetryAttempts - 1, 5));
    if (backoff > kBscRetryMaxBackoff) {
        backoff = kBscRetryMaxBackoff;
    }
    LOG_N("BSC retry " << BscRetryAttempts << " in " << backoff);
    BscRetryScheduled = true;
    ctx.Schedule(backoff, new TEvents::TEvWakeup(kWakeupBscRetry));
}

void TNbsDbgLikeLoadTablet::RetryBscOperation(const TActorContext& ctx) {
    BscRetryScheduled = false;
    LOG_D("Retry BSC operation TabletId# " << TabletID()
        << " Phase# " << Phase
        << " Attempt# " << BscRetryAttempts);
    if (Phase == ETabletPhase::Allocating) {
        SendBscAllocate(ctx, /*dealloc=*/false);
    } else if (Phase == ETabletPhase::Deleting && BscDeallocAuthorized) {
        SendBscAllocate(ctx, /*dealloc=*/true);
    }
}

void TNbsDbgLikeLoadTablet::FailPendingCreate(const TActorContext& ctx, const TString& reason) {
    if (BscAllocErr) {
        BscAllocErr->Inc();
    }
    if (PendingCreateReplyTo) {
        auto reply = std::make_unique<TEvLoad::TEvNbsLoadTabletAllocateGroupsResult>();
        reply->Record.SetStatus(NBSLT_BSC_ERROR);
        reply->Record.SetErrorReason(reason);
        ctx.Send(PendingCreateReplyTo, reply.release(), 0, PendingCreateCookie);
        PendingCreateReplyTo = TActorId();
        PendingCreateCookie = 0;
    }
    Phase = ETabletPhase::Uninitialized;
    EmitPhaseGauge();
    BscRetryAttempts = 0;
}

void TNbsDbgLikeLoadTablet::FailPendingDelete(const TActorContext& ctx, const TString& reason) {
    if (BscDeallocErr) {
        BscDeallocErr->Inc();
    }
    if (PendingDeleteReplyTo) {
        auto reply = std::make_unique<TEvLoad::TEvNbsLoadTabletDeleteResult>();
        reply->Record.SetStatus(NBSLT_BSC_ERROR);
        reply->Record.SetErrorReason(reason);
        ctx.Send(PendingDeleteReplyTo, reply.release(), 0, PendingDeleteCookie);
        PendingDeleteReplyTo = TActorId();
        PendingDeleteCookie = 0;
    }
    // Drained delete workers have disconnected and exited. A new run must
    // configure replacement workers before the tablet admits more I/O.
    BscDeallocAuthorized = false;
    IoSizeBytes = 0;
    BytesPerDbg = 0;
    // Roll back to READY so user can retry Delete.
    Phase = ETabletPhase::Ready;
    SpawnDbgActors(ctx);
    EmitPhaseGauge();
    BscRetryAttempts = 0;
}

void TNbsDbgLikeLoadTablet::Handle(
    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Cookie != BscRequestGeneration
            || (Phase == ETabletPhase::Deleting && !BscDeallocAuthorized)) {
        return;
    }
    const auto& rec = ev->Get()->Record;

    if (Phase == ETabletPhase::Deleting) {
        const auto status = rec.GetStatus();
        const bool ok = (status == NKikimrProto::OK || status == NKikimrProto::ALREADY);
        LOG_D("BSC dealloc reply TabletId# " << TabletID()
            << " Status# " << NKikimrProto::EReplyStatus_Name(status));
        if (ok) {
            if (BscDeallocOk) {
                BscDeallocOk->Inc();
            }
        } else {
            if (BscDeallocErr) {
                BscDeallocErr->Inc();
            }
            // Continue and clear local state anyway — the user can re-run if BSC
            // still has a phantom DBG (BSC tolerates re-dealloc).
            LOG_E("BSC dealloc returned " << NKikimrProto::EReplyStatus_Name(status)
                << " " << rec.GetErrorReason() << "; clearing local state regardless.");
        }
        Execute(CreateTxClearAll(), ctx);
        return;
    }

    if (Phase != ETabletPhase::Allocating) {
        return;  // late reply
    }

    auto parsed = ParseAllocateResult(rec, AllocConfig);
    if (!parsed) {
        FailPendingCreate(ctx, parsed.error());
        return;
    }

    LOG_D("BSC alloc parsed TabletId# " << TabletID()
        << " Dbgs# " << parsed->size());
    Execute(CreateTxStoreDbgs(std::move(*parsed)), ctx);
}

// ---- TEvDelete -----------------------------------------------------------

void TNbsDbgLikeLoadTablet::Handle(TEvLoad::TEvNbsLoadTabletDelete::TPtr& ev,
    const TActorContext& ctx)
{
    EnsureCounters(ctx);
    LOG_N("Delete request TabletId# " << TabletID()
        << " Sender# " << ev->Sender
        << " Phase# " << Phase);
    auto reply = [&](NKikimr::ENbsLoadTabletStatus s, const TString& err = {}) {
        auto r = std::make_unique<TEvLoad::TEvNbsLoadTabletDeleteResult>();
        r->Record.SetStatus(s);
        if (err) {
            r->Record.SetErrorReason(err);
        }
        ctx.Send(ev->Sender, r.release(), 0, ev->Cookie);
    };

    if (Phase == ETabletPhase::Allocating || Phase == ETabletPhase::Deleting) {
        return reply(NBSLT_BUSY);
    }
    if (Phase == ETabletPhase::Uninitialized && AllocConfigSerialized.empty()) {
        return reply(NBSLT_OK);
    }
    Y_DEBUG_ABORT_UNLESS(Phase == ETabletPhase::Ready || Phase == ETabletPhase::Uninitialized);

    PendingDeleteReplyTo = ev->Sender;
    PendingDeleteCookie  = ev->Cookie;
    ReplyConfiguration(false, "tablet is deleting", ctx);
    ++ConfigurationGeneration;
    ConfigurationPending.clear();
    PendingConfiguration.reset();
    Reconfiguring = false;
    Phase = ETabletPhase::Deleting;
    BscDeallocAuthorized = false;
    ++BscRequestGeneration;
    EmitPhaseGauge();
    BscRetryAttempts = 0;
    BeginWorkerDrain(ctx);
}

void TNbsDbgLikeLoadTablet::Handle(TEvLoad::TEvNbsLoadTabletGetSummary::TPtr& ev,
    const TActorContext& ctx)
{
    auto r = std::make_unique<TEvLoad::TEvNbsLoadTabletGetSummaryResult>();
    r->Record.SetAutomationProtocolVersion(1);
    if (!AllocConfigSerialized.empty()) { *r->Record.MutableAllocation() = AllocConfig; }
    if (Phase == ETabletPhase::Uninitialized) {
        r->Record.SetStatus(NBSLT_NOT_INITIALIZED);
        r->Record.SetErrorReason("not initialized");
        ctx.Send(ev->Sender, r.release(), 0, ev->Cookie);
        return;
    }
    r->Record.SetStatus(NBSLT_OK);
    r->Record.SetDDiskPoolName(AllocConfig.GetDDiskPoolName());
    r->Record.SetPersistentBufferDDiskPoolName(AllocConfig.GetPersistentBufferDDiskPoolName());
    // Use actual allocated count, not just what was requested in AllocConfig.
    r->Record.SetNumDirectBlockGroups(static_cast<ui32>(Dbgs.size()));
    r->Record.SetVChunkSizeBytes(AllocConfig.GetVChunkSizeBytes());
    r->Record.SetTargetNumVChunks(AllocConfig.GetTargetNumVChunks());
    // Count the longest contiguous prefix [0, N) of DBGs that have at least
    // kPrimaryHostsPerDbg PB peers connected. The load actor uses this to
    // limit its address space to DBGs that can actually accept writes.
    ui32 readyDbgs = 0;
    for (ui32 i = 0; i < static_cast<ui32>(DbgPbConnected.size()); ++i) {
        if (DbgPbConnected[i] < kPrimaryHostsPerDbg) {
            break;
        }
        ++readyDbgs;
    }
    r->Record.SetNumReadyDirectBlockGroups(readyDbgs);
    ctx.Send(ev->Sender, r.release(), 0, ev->Cookie);
}

// ---- Pipe + monitoring ---------------------------------------------------

void TNbsDbgLikeLoadTablet::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Get()->ClientId != BscPipeClient) {
        return;
    }
    if (ev->Get()->Status != NKikimrProto::OK) {
        OnBscPipeBroken(ctx);
    }
}

void TNbsDbgLikeLoadTablet::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Get()->ClientId != BscPipeClient) {
        return;
    }
    OnBscPipeBroken(ctx);
}

// ---- Proxy: spawn / poison workers, readiness, routing -------------------

void TNbsDbgLikeLoadTablet::SpawnDbgActors(const TActorContext& ctx) {
    if (Phase != ETabletPhase::Ready) {
        return;
    }
    EnsureCounters(ctx);
    PoisonDbgActors();
    DbgActors.resize(Dbgs.size());
    DbgPbConnected.assign(Dbgs.size(), 0);
    LOG_D("Spawn per-DBG worker actors TabletId# " << TabletID()
        << " Generation# " << Generation()
        << " Dbgs# " << Dbgs.size());
    for (ui32 i = 0; i < Dbgs.size(); ++i) {
        // Routing maps address -> position in DbgActors[], while workers identify
        // themselves by logical DbgIndex and GetSummary indexes DbgPbConnected by it.
        // All three agree only because alloc assigns DbgIndex = position and
        // TTxLoadEverything rolls back to UNINITIALIZED on any gap. That path
        // guarantees contiguousness before we reach here; verify in debug builds.
        Y_DEBUG_ABORT_UNLESS(Dbgs[i].DbgIndex == i);
        DbgActors[i] = ctx.Register(new TNbsDbgLikeActor(
            SelfId(), Dbgs[i].DbgIndex, static_cast<ui32>(Dbgs.size()),
            Generation(), AllocConfig, Dbgs[i], Counters));
    }
}

void TNbsDbgLikeLoadTablet::PoisonDbgActors() {
    for (const auto& actorId : DbgActors) {
        if (actorId) {
            Send(actorId, new TEvents::TEvPoison());
        }
    }
    DbgActors.clear();
}

void TNbsDbgLikeLoadTablet::Handle(TEvLoad::TEvNbsDbgActorReady::TPtr& ev,
    const TActorContext&)
{
    const auto* msg = ev->Get();
    // DbgIndex == position in DbgPbConnected[] (see the invariant asserted in
    // SpawnDbgActors), so GetSummary's positional prefix scan stays correct.
    if (msg->DbgIndex < DbgPbConnected.size()) {
        DbgPbConnected[msg->DbgIndex] = msg->PbConnectedCount;
    }
}

void TNbsDbgLikeLoadTablet::RecomputeRouting(
    const NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet& cfg)
{
    const auto params = ComputeRoutingParams(
        cfg, AllocConfig, static_cast<ui32>(Dbgs.size()));
    ActiveDbgs = params.ActiveDbgs;
    if (params.IoValid) {
        IoSizeBytes = params.IoSizeBytes;
        BytesPerDbg = params.BytesPerDbg;
    }
}

void TNbsDbgLikeLoadTablet::HandleNbsWrite(TEvLoad::TEvNbsWrite::TPtr& ev,
    const TActorContext&)
{
    const auto& msg = ev->Get()->Record;
    const TActorId origin = ev->Sender;
    const ui64 cookie = ev->Cookie;
    LOG_T("Route NbsWrite Cookie# " << cookie << " Addr# " << msg.GetAddress()
        << " Size# " << msg.GetSizeBytes());
    if (Phase != ETabletPhase::Ready || Reconfiguring || IoSizeBytes == 0 || BytesPerDbg == 0) {
        Send(origin, new TEvLoad::TEvNbsWriteResult(NBSIO_TABLET_NOT_READY,
            "Phase not Ready or IoSizeBytes/BytesPerDbg not set"), 0, cookie);
        return;
    }
    const ui32 dbgIndex = static_cast<ui32>(msg.GetAddress() / BytesPerDbg);
    if (dbgIndex >= ActiveDbgs || dbgIndex >= DbgActors.size()
        || !DbgActors[dbgIndex])
    {
        Send(origin, new TEvLoad::TEvNbsWriteResult(NBSIO_INVALID_ADDRESS,
            TStringBuilder() << "dbgIndex " << dbgIndex << " >= ActiveDbgs " << ActiveDbgs),
            0, cookie);
        return;
    }
    // Forward preserves Sender (the requestor) and Cookie, so the worker
    // replies straight back to the requestor over the same IC session.
    TActivationContext::Send(ev->Forward(DbgActors[dbgIndex]));
}

void TNbsDbgLikeLoadTablet::HandleNbsRead(TEvLoad::TEvNbsRead::TPtr& ev,
    const TActorContext&)
{
    const auto& msg = ev->Get()->Record;
    const TActorId origin = ev->Sender;
    const ui64 cookie = ev->Cookie;
    LOG_T("Route NbsRead Cookie# " << cookie << " Addr# " << msg.GetAddress()
        << " Size# " << msg.GetSizeBytes());
    if (Phase != ETabletPhase::Ready || Reconfiguring || IoSizeBytes == 0 || BytesPerDbg == 0) {
        Send(origin, new TEvLoad::TEvNbsReadResult(NBSIO_TABLET_NOT_READY,
            "Phase not Ready or IoSizeBytes/BytesPerDbg not set"), 0, cookie);
        return;
    }
    const ui32 dbgIndex = static_cast<ui32>(msg.GetAddress() / BytesPerDbg);
    if (dbgIndex >= ActiveDbgs || dbgIndex >= DbgActors.size()
        || !DbgActors[dbgIndex])
    {
        Send(origin, new TEvLoad::TEvNbsReadResult(NBSIO_INVALID_ADDRESS,
            TStringBuilder() << "dbgIndex " << dbgIndex << " >= ActiveDbgs " << ActiveDbgs),
            0, cookie);
        return;
    }
    TActivationContext::Send(ev->Forward(DbgActors[dbgIndex]));
}

void TNbsDbgLikeLoadTablet::BeginWorkerDrain(const TActorContext& ctx) {
    ++DrainEpoch;
    DrainPending.clear();
    ConfigurationPending.clear();
    for (const auto& actor : DbgActors) {
        if (actor) {
            DrainPending.insert(actor);
            ctx.Send(actor, new TEvDbgDrain::TEvDrain(DrainEpoch, Phase == ETabletPhase::Deleting));
        }
    }
    if (DrainPending.empty()) {
        FinishWorkerDrain(ctx);
    }
}

void TNbsDbgLikeLoadTablet::HandleWorkerDrained(
    TEvDbgDrain::TEvDrained::TPtr& ev, const TActorContext& ctx)
{
    if ((!Reconfiguring && Phase != ETabletPhase::Deleting)
            || ev->Get()->Epoch != DrainEpoch || !DrainPending.erase(ev->Sender)) {
        return;
    }
    if (DrainPending.empty()) {
        FinishWorkerDrain(ctx);
    }
}

void TNbsDbgLikeLoadTablet::FinishWorkerDrain(const TActorContext& ctx) {
    if (Phase == ETabletPhase::Deleting && !BscDeallocAuthorized) {
        BscDeallocAuthorized = true;
        SendBscAllocate(ctx, true);
    } else if (Reconfiguring && PendingConfiguration) {
        InstallConfiguration(ctx);
    }
}

void TNbsDbgLikeLoadTablet::InstallConfiguration(const TActorContext& ctx) {
    Y_ABORT_UNLESS(PendingConfiguration && DrainPending.empty());
    for (const auto& actor : DbgActors) {
        if (actor) {
            ConfigurationPending.insert(actor);
            auto event = std::make_unique<TEvLoad::TEvConfigureTablet>();
            event->Record = *PendingConfiguration;
            ctx.Send(actor, event.release(), 0, ConfigurationGeneration);
        }
    }
    if (Counters) {
        auto lsns = Counters->GetSubgroup("subsystem", "lsns");
        lsns->GetCounter("MaxLsns", false)->Set(PendingConfiguration->GetMaxInflightLsns());
        lsns->GetCounter("SyncGateThreshold", false)->Set(
            Max<ui32>(1, PendingConfiguration->GetSyncRequestsBatchSize()));
    }
}

void TNbsDbgLikeLoadTablet::HandleConfigureTablet(
    TEvLoad::TEvConfigureTablet::TPtr& ev, const TActorContext& ctx)
{
    const auto& cfg = ev->Get()->Record;
    auto reject = [&](const TString& reason) {
        if (cfg.GetConfigurationId()) {
            auto reply = std::make_unique<TEvLoad::TEvConfigureTabletResult>();
            reply->Record.SetConfigurationId(cfg.GetConfigurationId());
            reply->Record.SetSuccess(false);
            reply->Record.SetError(reason);
            ctx.Send(ev->Sender, reply.release(), 0, ev->Cookie);
        }
    };
    const ui32 count = cfg.GetNumDirectBlockGroupsToUse();
    if (Phase != ETabletPhase::Ready) {
        reject("tablet is not ready");
        return;
    }
    if ((cfg.GetConfigurationId() && (!count || count > DbgActors.size()))
            || DbgActors.empty() || !ComputeRoutingParams(cfg, AllocConfig, Dbgs.size()).IoValid) {
        reject("invalid configuration");
        return;
    }
    for (const auto& actor : DbgActors) {
        if (!actor) {
            reject("DBG actor is unavailable");
            return;
        }
    }
    ReplyConfiguration(false, "configuration superseded", ctx);
    ++ConfigurationGeneration;
    ConfigurationPending.clear();
    if (cfg.GetConfigurationId()) {
        ConfigurationRequester = ev->Sender;
        ConfigurationCookie = ev->Cookie;
        ConfigurationId = cfg.GetConfigurationId();
    }
    PendingConfiguration = cfg;
    Reconfiguring = true;
    EnsureCounters(ctx);
    // Supersession during the same drain replaces only the pending config.
    // Supersession during installation starts a fresh drain of those workers.
    if (DrainPending.empty()) {
        BeginWorkerDrain(ctx);
    }
}

void TNbsDbgLikeLoadTablet::HandleWakeup(TEvents::TEvWakeup::TPtr& ev) {
    if (ev->Get()->Tag == kWakeupBscRetry) {
        // Bind ctx to SelfId() explicitly rather than reading TlsActivationContext.
        // Both produce the same context here (TEvWakeup is delivered to *us*),
        // but the explicit form keeps us safe if this is ever invoked from a
        // callsite where TLS resolves to the executor (e.g. from a tx Complete).
        RetryBscOperation(NActors::TActivationContext::ActorContextFor(SelfId()));
    }
}

void TNbsDbgLikeLoadTablet::RenderHtml(IOutputStream& out) const {
    HTML(out) {
        TAG(TH3) { out << "NBS-DBG-like Load Tablet " << TabletID(); }
        TABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Field"; }
                    TABLEH() { out << "Value"; }
                }
            }
            TABLEBODY() {
                TABLER() { TABLED() { out << "Phase"; } TABLED() {
                    out << Phase;
                } }
                TABLER() { TABLED() { out << "Storage owner (TabletId)"; } TABLED() { out << AllocConfig.GetTabletId(); } }
                TABLER() { TABLED() { out << "NumDirectBlockGroups"; } TABLED() { out << Dbgs.size(); } }
                TABLER() { TABLED() { out << "VChunkSizeBytes"; } TABLED() { out << AllocConfig.GetVChunkSizeBytes(); } }
                TABLER() { TABLED() { out << "BscRetryAttempts"; } TABLED() { out << BscRetryAttempts; } }
            }
        }
        if (!Dbgs.empty()) {
            TAG(TH4) { out << "DBG roster"; }
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Index"; }
                        TABLEH() { out << "DirectBlockGroupId"; }
                        TABLEH() { out << "Hosts (DD/PB)"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& d : Dbgs) {
                        TABLER() {
                            TABLED() { out << d.DbgIndex; }
                            TABLED() { out << d.DirectBlockGroupId; }
                            TABLED() {
                                for (size_t k = 0; k < HostsPerDbg(); ++k) {
                                    if (k) {
                                        out << " | ";
                                    }
                                    out << "DD(" << d.DDiskIds[k].GetNodeId()
                                        << "," << d.DDiskIds[k].GetPDiskId()
                                        << "," << d.DDiskIds[k].GetDDiskSlotId()
                                        << ") PB(" << d.PBIds[k].GetNodeId()
                                        << "," << d.PBIds[k].GetPDiskId()
                                        << "," << d.PBIds[k].GetDDiskSlotId()
                                        << ")";
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TNbsDbgLikeActor — per-DBG worker
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TNbsDbgLikeActor::Bootstrap() {
    Become(&TNbsDbgLikeActor::StateWork);
    LOG_D("Worker Bootstrap DBG# " << MyDbgIndex
        << " Generation# " << Generation()
        << " HostsPerDbg# " << HostsPerDbg());
    KickOffPeerConnect();
}

void TNbsDbgLikeActor::ScheduleDrainTick() {
    if (!DrainTickScheduled && !DrainAcknowledged) {
        DrainTickScheduled = true;
        Schedule(TDuration::MilliSeconds(50), new TEvDbgDrain::TEvTick);
    }
}

void TNbsDbgLikeActor::ScheduleIdleCleanup() {
    if (!Draining && !Stopping && !IdleCleanupScheduled) {
        IdleCleanupScheduled = true;
        Schedule(TDuration::Seconds(1), new TEvDbgDrain::TEvIdleCleanup(++IdleCleanupGeneration));
    }
}

void TNbsDbgLikeActor::InvalidateIdleCleanup() {
    ++IdleCleanupGeneration;
    IdleCleanupScheduled = false;
}

void TNbsDbgLikeActor::HandleIdleCleanup(TEvDbgDrain::TEvIdleCleanup::TPtr& ev) {
    if (ev->Get()->Generation != IdleCleanupGeneration || !IdleCleanupScheduled || Draining || Stopping) {
        return;
    }
    IdleCleanupScheduled = false;
    const auto idle = [&](ui32 vChunk) {
        return Dbg.VChunkActivity[vChunk].IsIdle();
    };
    // Both admission passes see the same activity: pumping flush may send Sync.
    Dbg.Scheduler.AdmitIdleFlush(Dbg.Slots, idle);
    Dbg.Scheduler.AdmitIdleErase(Dbg.Slots, idle);
    PumpFlush(Dbg);
    PumpErase(Dbg);
    // Completions request the next pass; busy or pinned work does not poll.
}

void TNbsDbgLikeActor::HandlePoison() {
    if (Stopping) {
        return;
    }
    Stopping = true;
    Draining = true;
    InvalidateIdleCleanup();
    DrainAcknowledged = false;
    DriveFlushAdmission(Dbg);
    DriveEraseAdmission(Dbg);
    CheckDrained();
    ScheduleDrainTick();
}

void TNbsDbgLikeActor::HandleDrain(TEvDbgDrain::TEvDrain::TPtr& ev) {
    if (ev->Sender != TabletActorId || ev->Get()->Epoch <= DrainEpoch || Stopping) {
        return;
    }
    DrainEpoch = ev->Get()->Epoch;
    Draining = true;
    InvalidateIdleCleanup();
    Stopping = ev->Get()->Stop;
    NotifyDrained = true;
    DrainAcknowledged = false;
    DriveFlushAdmission(Dbg);
    DriveEraseAdmission(Dbg);
    CheckDrained();
    ScheduleDrainTick();
}

void TNbsDbgLikeActor::HandleDrainTick(TEvDbgDrain::TEvTick::TPtr&) {
    DrainTickScheduled = false;
    if (!Draining) {
        return;
    }
    DriveFlushAdmission(Dbg);
    DriveEraseAdmission(Dbg);
    CheckDrained();
    ScheduleDrainTick();
}

void TNbsDbgLikeActor::HandleMaintenance(TEvDbgDrain::TEvContinue::TPtr&) {
    ContinuationQueued = false;
    PumpFlush(Dbg);
    PumpErase(Dbg);
}

void TNbsDbgLikeActor::ScheduleMaintenanceContinuation() {
    if (ContinuationQueued || (!Dbg.Scheduler.HasFlushWork() && !Dbg.Scheduler.HasEraseWork())) {
        return;
    }
    ContinuationQueued = true;
    Send(SelfId(), new TEvDbgDrain::TEvContinue);
}

void TNbsDbgLikeActor::CheckDrained() {
    if (!Draining || DrainAcknowledged || Dbg.WritesInFlight || Dbg.ReadsInFlight
            || !Dbg.Lsns.empty() || !FlushInflight.empty() || !EraseInflight.empty()
            || !ReadInflight.empty()) {
        return;
    }
    if (Stopping) {
        for (ui32 k = 0; k < HostsPerDbg(); ++k) {
            if (PB[k].ConnectInFlight || DD[k].ConnectInFlight) {
                return;
            }
        }
        if (!Disconnecting) {
            Disconnecting = true;
            DisconnectAllPeers();
        }
        for (ui32 k = 0; k < HostsPerDbg(); ++k) {
            if (PB[k].DisconnectInFlight || DD[k].DisconnectInFlight) {
                return;
            }
        }
    }
    DrainAcknowledged = true;
    if (NotifyDrained) {
        Send(TabletActorId, new TEvDbgDrain::TEvDrained(DrainEpoch));
    }
    if (Stopping) {
        FinishStop();
    }
}

void TNbsDbgLikeActor::FinishStop() {
    // Reconcile the shared root up/down gauges: subtract whatever this worker
    // still has in flight so a poisoned worker cannot leak Pending/BytesInFlight
    // across Create/Delete cycles. The per-DBG gauges hold the exact remainder.
    for (ui32 i = 0; i < kOpCount; ++i) {
        auto& root = RootCnt.Op[i];
        auto& local = Dbg.Counters.Op[i];
        if (root.Pending && local.Pending) {
            *root.Pending -= local.Pending->Val();
        }
        if (root.BytesInFlight && local.BytesInFlight) {
            *root.BytesInFlight -= local.BytesInFlight->Val();
        }
    }
    if (Counters && Dbg.Counters.Root) {
        Dbg.Counters.Root->ResetCounters();
    }
    PassAway();
}

ui64 TNbsDbgLikeActor::TotalLsns() const {
    return LsnsTotalAll;
}

void TNbsDbgLikeActor::ReportReadiness() {
    const ui32 pbConnected = static_cast<ui32>(Dbg.PBConnected.count());
    if (pbConnected == LastReportedPbConnected) {
        return;
    }
    LastReportedPbConnected = pbConnected;
    Send(TabletActorId, new TEvLoad::TEvNbsDbgActorReady(MyDbgIndex, pbConnected));
}

void TNbsDbgLikeActor::KickOffPeerConnect() {
    const ui32 hostsPerDbg = HostsPerDbg();
    LOG_D("Worker peer pre-connect DBG# " << MyDbgIndex
        << " Generation# " << Generation()
        << " Peers# " << (2 * hostsPerDbg));
    for (ui32 k = 0; k < hostsPerDbg; ++k) {
        ConnectPeer(k, /*isPb=*/true);
        ConnectPeer(k, /*isPb=*/false);
    }
}

void TNbsDbgLikeActor::ConnectPeer(ui32 k, bool isPb) {
    if (k >= HostsPerDbg()) {
        return;
    }
    auto& st = isPb ? PB[k] : DD[k];
    if (st.Connected || st.ConnectInFlight) {
        return;
    }
    const auto& id = isPb ? DbgInfo.PBIds[k] : DbgInfo.DDiskIds[k];
    TActorId target = isPb
        ? MakeBlobStoragePersistentBufferId(id.GetNodeId(), id.GetPDiskId(), id.GetDDiskSlotId())
        : MakeBlobStorageDDiskId(id.GetNodeId(), id.GetPDiskId(), id.GetDDiskSlotId());
    auto creds = isPb
        ? NDDisk::TQueryCredentials::ToPersistentBuffer(AllocConfig.GetTabletId(), Generation(), std::nullopt, MyDbgIndex)
        : NDDisk::TQueryCredentials::ToDDisk(
            AllocConfig.GetTabletId(), Generation(), kInitialDDiskSessionSeqNo, std::nullopt, MyDbgIndex);
    st.ConnectInFlight = true;
    Send(target, new NDDisk::TEvConnect(creds), 0, PackPeerCookie(k, isPb));
}

void TNbsDbgLikeActor::HandlePeerConnect(NDDisk::TEvConnectResult::TPtr& ev) {
    ui32 k = 0;
    bool isPb = false;
    UnpackPeerCookie(ev->Cookie, k, isPb);
    if (k >= HostsPerDbg()) {
        return;
    }
    auto& st = isPb ? PB[k] : DD[k];
    st.ConnectInFlight = false;
    const auto& rec = ev->Get()->Record;
    LOG_D("Worker HandlePeerConnect DBG# " << MyDbgIndex
        << " " << (isPb ? "PB" : "DD") << k
        << " Status# " << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(rec.GetStatus()));
    if (rec.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        st.Guid = rec.GetDDiskInstanceGuid();
        st.RuntimeActor = ev->Sender;
        st.Token.emplace(rec.GetConnectionToken());
        if (isPb && !Stopping) {
            st.ConnectInFlight = true;
            auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(
                AllocConfig.GetTabletId(), Generation(), st.Guid, MyDbgIndex);
            creds.ConnectionToken = st.Token;
            Send(ev->Sender, new NDDisk::TEvGetPersistentBufferRegistrationToken(creds),
                0, ev->Cookie);
            return;
        }
        PeerConnected(k, isPb);
    } else {
        st.Connected = false;
        st.Guid = 0;
        st.Token.reset();
        if (RootCnt.ConnectErr) {
            RootCnt.ConnectErr->Inc();
        }
        if (isPb) {
            Dbg.PBConnected.reset(k);
        } else {
            Dbg.DDConnected.reset(k);
        }
        ReportReadiness();
        LOG_E("Connect failed DBG# " << MyDbgIndex
            << " " << (isPb ? "PB" : "DD") << k
            << " DirectBlockGroupId# " << DbgInfo.DirectBlockGroupId
            << " Status# " << DDiskStatusText(rec.GetStatus(), rec.GetErrorReason()));
    }
}

void TNbsDbgLikeActor::PeerConnected(ui32 k, bool isPb) {
    auto& st = isPb ? PB[k] : DD[k];
    st.ConnectInFlight = false;
    st.Connected = true;
    if (RootCnt.ConnectOk) {
        RootCnt.ConnectOk->Inc();
    }
    bool allConnected = true;
    for (ui32 i = 0; i < HostsPerDbg(); ++i) {
        if (!DD[i].Connected || !PB[i].Connected) {
            allConnected = false;
            break;
        }
    }
    if (allConnected) {
        LOG_D("Worker AllConnected DBG# " << MyDbgIndex << " — populating DbgState");
        PopulateDbgState();
    }
    ReportReadiness();
}

void TNbsDbgLikeActor::HandlePeerRegistrationToken(NDDisk::TEvGetPersistentBufferRegistrationTokenResult::TPtr& ev) {
    ui32 k = 0;
    bool isPb = false;
    UnpackPeerCookie(ev->Cookie, k, isPb);
    if (!isPb || k >= HostsPerDbg() || !PB[k].ConnectInFlight) {
        return;
    }
    if (ev->Get()->Record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        PB[k].ConnectInFlight = false;
        if (RootCnt.ConnectErr) {
            RootCnt.ConnectErr->Inc();
        }
        const auto& rec = ev->Get()->Record;
        LOG_E("PB registration token failed DBG# " << MyDbgIndex
            << " PB" << k
            << " Status# " << DDiskStatusText(rec.GetStatus(), rec.GetErrorReason()));
        ReportReadiness();
        return;
    }
    auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(
        AllocConfig.GetTabletId(), Generation(), PB[k].Guid, MyDbgIndex);
    creds.ConnectionToken = PB[k].Token;
    Send(ev->Sender, new NDDisk::TEvRegisterPersistentBuffer(creds, ev->Get()->Record.GetToken()),
        0, ev->Cookie);
}

void TNbsDbgLikeActor::HandlePeerRegistration(NDDisk::TEvRegisterPersistentBufferResult::TPtr& ev) {
    ui32 k = 0;
    bool isPb = false;
    UnpackPeerCookie(ev->Cookie, k, isPb);
    if (!isPb || k >= HostsPerDbg() || !PB[k].ConnectInFlight) {
        return;
    }
    // Probe even after a rejected duplicate registration: only a successful list
    // proves that the existing registration is durable and is still being served.
    auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(
        AllocConfig.GetTabletId(), Generation(), PB[k].Guid, MyDbgIndex);
    creds.ConnectionToken = PB[k].Token;
    Send(ev->Sender, new NDDisk::TEvListPersistentBuffer(creds), 0, ev->Cookie);
}

void TNbsDbgLikeActor::HandlePeerRegistrationProbe(NDDisk::TEvListPersistentBufferResult::TPtr& ev) {
    ui32 k = 0;
    bool isPb = false;
    UnpackPeerCookie(ev->Cookie, k, isPb);
    if (!isPb || k >= HostsPerDbg() || !PB[k].ConnectInFlight) {
        return;
    }
    if (ev->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        PeerConnected(k, true);
    } else {
        PB[k].ConnectInFlight = false;
        using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;
        const auto status = ev->Get()->Record.GetStatus();
        if (status == TStatus::BUSY || status == TStatus::OVERLOADED
                || status == TStatus::INCORRECT_REQUEST) {
            Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup(k));
        }
        LOG_E("PB registration probe failed DBG# " << MyDbgIndex
            << " PB" << k
            << " Status# " << DDiskStatusText(status, ev->Get()->Record.GetErrorReason()));
        if (RootCnt.ConnectErr) {
            RootCnt.ConnectErr->Inc();
        }
        ReportReadiness();
    }
}

void TNbsDbgLikeActor::HandlePeerDisconnect(NDDisk::TEvDisconnectResult::TPtr& ev) {
    ui32 k = 0;
    bool isPb = false;
    UnpackPeerCookie(ev->Cookie, k, isPb);
    if (!Disconnecting || k >= HostsPerDbg()) {
        return;
    }
    auto& peer = isPb ? PB[k] : DD[k];
    if (!peer.DisconnectInFlight || ev->Sender != peer.RuntimeActor
            || ev->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
        return;
    }
    peer.DisconnectInFlight = false;
    peer.Connected = false;
    peer.ConnectInFlight = false;
    peer.Token.reset();
    peer.Guid = 0;
    const auto& record = ev->Get()->Record;
    if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        LOG_E("Disconnect failed DBG# " << MyDbgIndex
            << " " << (isPb ? "PB" : "DD") << k
            << " Status# " << DDiskStatusText(record.GetStatus(), record.GetErrorReason()));
    }
    if (isPb) {
        Dbg.PBConnected.reset(k);
        Dbg.PBToken[k].reset();
    } else {
        Dbg.DDConnected.reset(k);
        Dbg.DDToken[k].reset();
    }
    if (RootCnt.DisconnectOk) {
        RootCnt.DisconnectOk->Inc();
    }
}

void TNbsDbgLikeActor::DisconnectAllPeers() {
    for (ui32 k = 0; k < HostsPerDbg(); ++k) {
        for (bool isPb : {true, false}) {
            auto& peer = isPb ? PB[k] : DD[k];
            if (!peer.Token) {
                continue;
            }
            auto credentials = isPb
                ? NDDisk::TQueryCredentials::ToPersistentBuffer(*peer.Token)
                : NDDisk::TQueryCredentials::ToDDisk(*peer.Token);
            auto event = std::make_unique<NDDisk::TEvDisconnect>();
            credentials.SerializeForRequest(event->Record.MutableCredentials());
            peer.DisconnectInFlight = true;
            Send(peer.RuntimeActor, event.release(), 0, PackPeerCookie(k, isPb));
        }
    }
}

void TNbsDbgLikeActor::PopulateDbgState() {
    auto& dst = Dbg;
    // Reserve the expected in-flight LSN capacity up front so the flat hash
    // tables avoid rehashing (and copying the large TWriteInfo slots) on the
    // steady-state path. reserve() only grows capacity, so repeated
    // PopulateDbgState calls on reconnects are harmless.
    {
        const ui64 maxInflight = TabletConfig.GetMaxInflightLsns();
        const size_t perDbg = (NumDbgsTotal == 0)
            ? static_cast<size_t>(maxInflight)
            : static_cast<size_t>(maxInflight / NumDbgsTotal + 1);
        dst.Lsns.reserve(perDbg);
        dst.Slots.Reserve(perDbg);
        dst.Scheduler.Reserve(perDbg);
    }
    LOG_D("Worker PopulateDbgState DBG# " << MyDbgIndex
        << " PBConnected_before# " << dst.PBConnected.count()
        << " DDConnected_before# " << dst.DDConnected.count());
    const auto& src = DbgInfo;
    const ui32 hostsPerDbg = HostsPerDbg();
    dst.DbgIndex = src.DbgIndex;
    dst.DirectBlockGroupId = src.DirectBlockGroupId;
    dst.DDiskIdsPb = src.DDiskIds;
    dst.PBIdsPb = src.PBIds;
    dst.PbIndexById.clear();
    for (ui32 k = 0; k < hostsPerDbg; ++k) {
        const auto& dd = src.DDiskIds[k];
        const auto& pb = src.PBIds[k];
        dst.DDiskActor[k] = MakeBlobStorageDDiskId(
            dd.GetNodeId(), dd.GetPDiskId(), dd.GetDDiskSlotId());
        dst.PBActor[k] = MakeBlobStoragePersistentBufferId(
            pb.GetNodeId(), pb.GetPDiskId(), pb.GetDDiskSlotId());
        dst.PbIndexById.emplace(pb, k);
        if (DD[k].Connected) {
            dst.DDGuid[k] = DD[k].Guid;
            dst.DDToken[k] = DD[k].Token;
            dst.DDConnected.set(k);
        }
        if (PB[k].Connected) {
            dst.PBGuid[k] = PB[k].Guid;
            dst.PBToken[k] = PB[k].Token;
            dst.PBConnected.set(k);
        }
    }
    if (AllocConfig.GetTargetNumVChunks() > 0) {
        dst.FlushedSlots.resize(AllocConfig.GetTargetNumVChunks());
        if (IoSizeBytes != 0 && AllocConfig.GetVChunkSizeBytes() != 0) {
            const ui32 slotsPerVChunk = AllocConfig.GetVChunkSizeBytes() / IoSizeBytes;
            for (auto& slots : dst.FlushedSlots) {
                slots.Reserve(slotsPerVChunk);
            }
        }
    }
}

std::expected<TDecodedAddress, EDecodeAddressError> TNbsDbgLikeActor::DecodeAddress(
    ui64 address, ui32 sizeBytes) const
{
    if (IoSizeBytes == 0 || sizeBytes != IoSizeBytes) {
        return std::unexpected(EDecodeAddressError::InvalidIoSize);
    }

    if (BytesPerDbg == 0 || AllocConfig.GetVChunkSizeBytes() == 0) {
        return std::unexpected(EDecodeAddressError::NotConfigured);
    }

    const ui64 vChunkSizeBytes = AllocConfig.GetVChunkSizeBytes();
    const ui32 targetNumVChunks = AllocConfig.GetTargetNumVChunks();
    const ui32 d = static_cast<ui32>(address / BytesPerDbg);
    if (d >= ActiveDbgs) {
        return std::unexpected(EDecodeAddressError::DbgOutOfRange);
    }

    const ui64 rem = address % BytesPerDbg;
    const ui32 v = static_cast<ui32>(rem / vChunkSizeBytes);
    const ui64 offset = rem % vChunkSizeBytes;
    if (v >= targetNumVChunks) {
        return std::unexpected(EDecodeAddressError::VChunkOutOfRange);
    }

    if (offset % IoSizeBytes != 0) {
        return std::unexpected(EDecodeAddressError::InvalidIoSize);
    }
    if (offset + sizeBytes > vChunkSizeBytes) {
        return std::unexpected(EDecodeAddressError::CrossesVChunkBoundary);
    }

    return TDecodedAddress{
        .DbgIndex = d,
        .VChunkIndex = v,
        .OffsetInVChunk = static_cast<ui32>(offset),
    };
}

ui32 TNbsDbgLikeActor::ChooseCoordinator(const TPerDbgState& dbg) const {
    auto it = std::ranges::min_element(dbg.InFlightTo);
    return static_cast<ui32>(it - dbg.InFlightTo.begin());
}

void TNbsDbgLikeActor::ReplyWriteErr(const TActorId& origin, ui64 cookie,
    ENbsIoResultStatus status, TString reason)
{
    LOG_E("NbsWrite failed Cookie# " << cookie
        << " Status# " << ENbsIoResultStatus_Name(status)
        << " Reason# " << reason);
    if (origin) {
        Send(origin, new TEvLoad::TEvNbsWriteResult(status, std::move(reason)), 0, cookie);
    }
}

void TNbsDbgLikeActor::ReplyReadErr(const TActorId& origin, ui64 cookie,
    ENbsIoResultStatus status, TString reason)
{
    LOG_E("NbsRead failed Cookie# " << cookie
        << " Status# " << ENbsIoResultStatus_Name(status)
        << " Reason# " << reason);
    if (origin) {
        Send(origin, new TEvLoad::TEvNbsReadResult(status, std::move(reason)), 0, cookie);
    }
}

ui32 TNbsDbgLikeActor::LocateInPbIds(const TPerDbgState& dbg, const NKikimrBlobStorage::NDDisk::TDDiskId& id)
{
    auto it = dbg.PbIndexById.find(id);
    return it == dbg.PbIndexById.end() ? kHostsPerDbgMax : it->second;
}

void TNbsDbgLikeActor::RegisterPeerOpCounters(TPeerCounters& pc,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& peerGroup, EOp op)
{
    auto opGroup = peerGroup->GetSubgroup("op", ToString(op));
    const size_t opIndex = static_cast<size_t>(op);
    pc.RequestsSentByOp[opIndex] = opGroup->GetCounter("RequestsSent", true);
    pc.RepliesOkByOp[opIndex] = opGroup->GetCounter("RepliesOk", true);
    pc.RepliesErrByOp[opIndex] = opGroup->GetCounter("RepliesErr", true);
}

void TNbsDbgLikeActor::BumpPeerRequest(TPerDbgState& dbg, ui32 peerIndex, EOp op) {
    if (!dbg.Counters.PerPeerEnabled || peerIndex >= dbg.Counters.Peers.size()) {
        return;
    }
    auto& pc = dbg.Counters.Peers[peerIndex];
    if (pc.RequestsSent) {
        pc.RequestsSent->Inc();
    }
    if (pc.RequestsSentByOp[static_cast<size_t>(op)]) {
        pc.RequestsSentByOp[static_cast<size_t>(op)]->Inc();
    }
}

void TNbsDbgLikeActor::BumpPeerReply(TPerDbgState& dbg, ui32 peerIndex, EOp op, bool ok) {
    if (!dbg.Counters.PerPeerEnabled || peerIndex >= dbg.Counters.Peers.size()) {
        return;
    }
    auto& pc = dbg.Counters.Peers[peerIndex];
    if (ok) {
        if (pc.RepliesOk) {
            pc.RepliesOk->Inc();
        }
        if (pc.RepliesOkByOp[static_cast<size_t>(op)]) {
            pc.RepliesOkByOp[static_cast<size_t>(op)]->Inc();
        }
    } else {
        if (pc.RepliesErr) {
            pc.RepliesErr->Inc();
        }
        if (pc.RepliesErrByOp[static_cast<size_t>(op)]) {
            pc.RepliesErrByOp[static_cast<size_t>(op)]->Inc();
        }
    }
}

void TNbsDbgLikeActor::AccountReadRequest(TPerDbgState& dbg, EOp op, ui32 size) {
    // Root Pending/BytesInFlight are shared up/down gauges maintained in
    // lockstep with the per-DBG (dbg=) gauges; HandlePoison reconciles any
    // ops still in flight when a worker dies so they do not leak across
    // Create/Delete cycles.
    if (auto& c = RootCnt.Op[static_cast<size_t>(op)]; c.Requests) {
        c.Requests->Inc();
        if (c.Pending) {
            c.Pending->Inc();
        }
        if (c.BytesInFlight) {
            *c.BytesInFlight += size;
        }
    }
    if (auto& c = dbg.Counters.Op[static_cast<size_t>(op)]; c.Requests) {
        c.Requests->Inc();
        c.Pending->Inc();
        if (c.BytesInFlight) {
            *c.BytesInFlight += size;
        }
    }
}

void TNbsDbgLikeActor::EnterState(TPerDbgState& dbg, EPBufferState s, int delta) {
    const ui32 idx = static_cast<ui32>(s);
    if (delta < 0) {
        if (dbg.StateCount[idx] > 0) {
            --dbg.StateCount[idx];
        }
    } else {
        dbg.StateCount[idx] += delta;
    }
    if (dbg.Counters.Lsns.BufferStateGauges[idx]) {
        dbg.Counters.Lsns.BufferStateGauges[idx]->Set(dbg.StateCount[idx]);
    }
    // Root buffer-state gauge is a cross-DBG sum and cannot be computed by a
    // single worker; rely on Solomon aggregation across dbg= subgroups.
}

void TNbsDbgLikeActor::UpdateLsnsTotal(TPerDbgState& dbg) {
    if (dbg.Counters.Lsns.Total) {
        dbg.Counters.Lsns.Total->Set(dbg.Lsns.size());
    }
    // Root Total is a cross-DBG sum; left to Solomon aggregation.
}

void TNbsDbgLikeActor::UpdateAvgPbFreeSpacePct(TPerDbgState& dbg) {
    double s = 0;
    for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) s += dbg.LastFreeSpace[k];
    const ui32 used = static_cast<ui32>(100.0 * (1.0 - s / kPrimaryHostsPerDbg));
    dbg.AvgPbUsedPct = used;
    if (dbg.Counters.Lsns.AvgPbUsedPct) {
        dbg.Counters.Lsns.AvgPbUsedPct->Set(used);
    }
    if (dbg.Counters.Lsns.AvgPbFreeSpacePct) {
        dbg.Counters.Lsns.AvgPbFreeSpacePct->Set(used <= 100 ? 100u - used : 0u);
    }
    // Root averages/min/max are cross-DBG and left to Solomon aggregation.
}

void TNbsDbgLikeActor::HandleNbsWrite(TEvLoad::TEvNbsWrite::TPtr& ev, const TActorContext& /*ctx*/)
{
    const auto& msg = ev->Get()->Record;
    const TActorId origin = ev->Sender;
    const ui64 cookie = ev->Cookie;
    LOG_T("HandleNbsWrite Cookie# " << cookie << " Addr# " << msg.GetAddress() << " Size# " << msg.GetSizeBytes());

    auto span = NWilson::TSpan(
        TWilsonNbs::NbsBasic,
        ev->TraceId.Clone(),
        "NbsDbgLike.Write",
        NWilson::EFlags::NONE,
        TActivationContext::ActorSystem());
    span
        .Attribute("addr", static_cast<i64>(msg.GetAddress()))
        .Attribute("size", static_cast<i64>(msg.GetSizeBytes()));

    if (Draining || Stopping) {
        span.EndError("DBG is draining");
        ReplyWriteErr(origin, cookie, NBSIO_TABLET_NOT_READY, "DBG is draining");
        return;
    }
    if (IoSizeBytes == 0) {
        span.EndError("IoSizeBytes not set");
        ReplyWriteErr(origin, cookie, NBSIO_NOT_CONFIGURED, "IoSizeBytes not set");
        return;
    }

    if (ev->Get()->Payload.IsEmpty()) {
        span.EndError("Payload absent");
        ReplyWriteErr(origin, cookie, NBSIO_MISSING_PAYLOAD, "Payload absent");
        return;
    }

    const auto decoded = DecodeAddress(msg.GetAddress(), msg.GetSizeBytes());
    if (!decoded || decoded->DbgIndex != MyDbgIndex) {
        LOG_D("HandleNbsWrite invalid Address# " << msg.GetAddress()
            << " SizeBytes# " << msg.GetSizeBytes()
            << " IoSizeBytes# " << IoSizeBytes
            << " ActiveDbgs# " << ActiveDbgs);
        const TString reason = TStringBuilder() << "decode failed Addr# " << msg.GetAddress()
            << " SizeBytes# " << msg.GetSizeBytes()
            << " IoSizeBytes# " << IoSizeBytes;
        span.EndError(reason);
        ReplyWriteErr(origin, cookie, NBSIO_INVALID_ADDRESS, reason);
        return;
    }
    const ui32 dbgIndex = decoded->DbgIndex;
    const ui32 vChunkIndex = decoded->VChunkIndex;
    const ui32 offset = decoded->OffsetInVChunk;

    const ui32 pbConnectedCount = static_cast<ui32>(Dbg.PBConnected.count());
    if (pbConnectedCount < kPrimaryHostsPerDbg) {
        LOG_D("HandleNbsWrite peers not ready DBG# " << dbgIndex
            << " Cookie# " << cookie
            << " PBConnected# " << pbConnectedCount
            << " need# " << kPrimaryHostsPerDbg
            << "; replying TABLET_NOT_READY");
        const TString reason = TStringBuilder() << "PB peers not ready: " << pbConnectedCount
            << "/" << kPrimaryHostsPerDbg;
        span.EndError(reason);
        ReplyWriteErr(origin, cookie, NBSIO_TABLET_NOT_READY, reason);
        return;
    }

    const ui64 maxInflight = TabletConfig.GetMaxInflightLsns();
    const ui64 denom = Max<ui32>(1, NumDbgsTotal);
    if (maxInflight == 0 || TotalLsns() >= Max<ui64>(1, maxInflight / denom)) {
        if (RootCnt.Lsns.BackpressureHits) {
            RootCnt.Lsns.BackpressureHits->Inc();
        }
        const TString reason = TStringBuilder() << "TotalLsns# " << TotalLsns()
            << " >= cap# " << Max<ui64>(1, maxInflight / denom);
        span.EndError(reason);
        ReplyWriteErr(origin, cookie, NBSIO_BACKPRESSURE, reason);
        return;
    }

    auto& dbg = Dbg;
    // Preserve unique LSNs across DBGs of this tablet generation, including
    // DBGs placed on the same PB slot. PB record identity also includes the
    // DBG index, but striding keeps the load's LSNs disjoint independently of
    // placement. Single-DBG layout is unchanged
    // (stride 1, index 0 -> 1, 2, 3, ...).
    const ui64 lsnStride = Max<ui64>(1, NumDbgsTotal);
    const ui64 lsn = (SequenceGenerator++) * lsnStride + MyDbgIndex + 1;
    const ui32 coord = ChooseCoordinator(dbg);

    TWriteInfo info;
    info.Lsn = lsn;
    info.Size = IoSizeBytes;
    info.VChunkIndex = vChunkIndex;
    info.OffsetInVChunk = offset;
    info.WriteStart = MonotonicNow();
    info.CoordinatorIndex = static_cast<ui8>(coord);
    info.OriginActor = origin;
    info.OriginCookie = cookie;
    info.Span = std::move(span);
    if (TabletConfig.GetDisableReplication()) {
        info.WriteRequested.set(coord);
    } else {
        for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
            info.WriteRequested.set(k);
        }
    }
    EnterState(dbg, EPBufferState::PBufferIncompleteWrite);
    auto [it, inserted] = dbg.Lsns.emplace(lsn, std::move(info));
    Y_ABORT_UNLESS(inserted, "duplicate LSN");

    // below is success path until the end of func

    ++LsnsTotalAll;
    dbg.Slots.Accept({vChunkIndex, offset / IoSizeBytes}, lsn);
    ++dbg.VChunkActivity[vChunkIndex].IncompleteWrites;

    Y_ABORT_UNLESS(dbg.PBToken[coord]);
    auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(*dbg.PBToken[coord]);
    NDDisk::TBlockSelector selector(
        WireVChunkIndex(MyDbgIndex, AllocConfig.GetTargetNumVChunks(), vChunkIndex), offset, IoSizeBytes);
    std::vector<NKikimrBlobStorage::NDDisk::TDDiskId> pbIds;
    if (TabletConfig.GetDisableReplication()) {
        pbIds.push_back(dbg.PBIdsPb[coord]);
    } else {
        pbIds.reserve(kPrimaryHostsPerDbg);
        for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
            pbIds.push_back(dbg.PBIdsPb[k]);
        }
    }
    auto wireEv = std::make_unique<NDDisk::TEvWritePersistentBuffers>(
        creds, selector, lsn, NDDisk::TWriteInstruction(0), pbIds,
        TabletConfig.GetPBufferReplyTimeoutMicroseconds());

    if (TabletConfig.GetEnableChecksums()) {
        std::vector<ui64> checksums(msg.GetChecksums().begin(), msg.GetChecksums().end());
        wireEv->AddPayloadWithChecksum(std::move(ev->Get()->Payload), checksums);
    } else {
        wireEv->AddPayload(std::move(ev->Get()->Payload));
    }

    Send(dbg.PBActor[coord], wireEv.release(), 0, lsn, it->second.Span.GetTraceId().Clone());

    ++dbg.WritesInFlight;
    ++dbg.InFlightTo[coord];

    // Root Pending/BytesInFlight maintained in lockstep with per-DBG gauges
    // (see AccountReadRequest); HandlePoison reconciles in-flight ops.
    if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Write)]; c.Requests) {
        c.Requests->Inc();
        if (c.Pending) {
            c.Pending->Inc();
        }
        if (c.BytesInFlight) {
            *c.BytesInFlight += IoSizeBytes;
        }
    }

    if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Write)]; c.Requests) {
        c.Requests->Inc();
        c.Pending->Inc();
        if (c.BytesInFlight) {
            *c.BytesInFlight += IoSizeBytes;
        }
    }

    // Root NewestLsn is a cross-DBG gauge; left to Solomon aggregation.
    if (dbg.Counters.Lsns.NewestLsn) {
        dbg.Counters.Lsns.NewestLsn->Set(lsn);
    }

    UpdateLsnsTotal(dbg);
    BumpPeerRequest(dbg, coord, EOp::Write);
}

bool TNbsDbgLikeActor::SendPbRead(TPerDbgState& dbg, ui32 dbgIndex, ui64 lsn,
    const TWriteInfo& info, const TActorId& origin, ui64 originCookie,
    NWilson::TSpan& span)
{
    if (!info.WriteConfirmed.any()) {
        return false;
    }
    const ui32 k = PickRandomSetBit(info.WriteConfirmed, Rng.GenRand());
    Y_ABORT_UNLESS(dbg.PBToken[k]);
    auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(*dbg.PBToken[k]);
    NDDisk::TBlockSelector selector(WireVChunkIndex(MyDbgIndex, AllocConfig.GetTargetNumVChunks(), info.VChunkIndex),
        static_cast<ui32>(info.OffsetInVChunk), info.Size);
    auto ev = std::make_unique<NDDisk::TEvReadPersistentBuffer>(
        creds, selector, lsn, Generation_, NDDisk::TReadInstruction(true));
    const ui64 cookie = NextBatchCookie++;
    TReadInflight ri;
    ri.DbgIndex = dbgIndex;
    ri.PeerK = k;
    ri.IsPb = true;
    ri.ReplyActor = PB[k].RuntimeActor;
    ri.Slot = {info.VChunkIndex, static_cast<ui32>(info.OffsetInVChunk / info.Size)};
    ri.Lsn = lsn;
    dbg.Slots.PinPBRead(ri.Slot, lsn);
    ri.Size = info.Size;
    ri.SentAt = MonotonicNow();
    ri.OriginActor = origin;
    ri.OriginCookie = originCookie;
    ri.Span = std::move(span);
    NWilson::TTraceId traceId = ri.Span.GetTraceId();
    ReadInflight.emplace(cookie, std::move(ri));
    Send(dbg.PBActor[k], ev.release(), 0, cookie, std::move(traceId));

    ++dbg.ReadsInFlight;
    AccountReadRequest(dbg, EOp::ReadPB, info.Size);
    BumpPeerRequest(dbg, k, EOp::ReadPB);
    return true;
}

bool TNbsDbgLikeActor::SendDDiskRead(TPerDbgState& dbg, ui32 dbgIndex,
    ui32 vChunkIndex, ui64 offset, ui32 size,
    std::bitset<kHostsPerDbgMax> flushMask,
    const TActorId& origin, ui64 originCookie, NWilson::TSpan& span)
{
    const TPeerBitset& effectiveMask = flushMask.any() ? flushMask : kAllPrimaryHostsMask;
    const ui32 k = PickRandomSetBit(effectiveMask, Rng.GenRand());
    Y_ABORT_UNLESS(dbg.DDToken[k]);
    auto creds = NDDisk::TQueryCredentials::ToDDisk(*dbg.DDToken[k]);
    NDDisk::TBlockSelector selector(
        WireVChunkIndex(MyDbgIndex, AllocConfig.GetTargetNumVChunks(), vChunkIndex), static_cast<ui32>(offset), size);
    auto ev = std::make_unique<NDDisk::TEvRead>(
        creds, selector, NDDisk::TReadInstruction(true));
    const ui64 cookie = NextBatchCookie++;
    TReadInflight ri;
    ri.DbgIndex = dbgIndex;
    ri.PeerK = k;
    ri.IsPb = false;
    ri.ReplyActor = DD[k].RuntimeActor;
    ri.Slot = {vChunkIndex, static_cast<ui32>(offset / size)};
    dbg.Slots.PinDDiskRead(ri.Slot);
    ri.Size = size;
    ri.SentAt = MonotonicNow();
    ri.OriginActor = origin;
    ri.OriginCookie = originCookie;
    ri.Span = std::move(span);
    NWilson::TTraceId traceId = ri.Span.GetTraceId();
    ReadInflight.emplace(cookie, std::move(ri));
    Send(dbg.DDiskActor[k], ev.release(), 0, cookie, std::move(traceId));

    ++dbg.ReadsInFlight;
    AccountReadRequest(dbg, EOp::ReadDDisk, size);
    BumpPeerRequest(dbg, kHostsPerDbgMax + k, EOp::ReadDDisk);
    return true;
}

void TNbsDbgLikeActor::HandleNbsRead(TEvLoad::TEvNbsRead::TPtr& ev,
    const TActorContext& /*ctx*/)
{
    const auto& msg = ev->Get()->Record;
    const TActorId origin = ev->Sender;
    const ui64 cookie = ev->Cookie;
    LOG_T("HandleNbsRead Cookie# " << cookie << " Addr# " << msg.GetAddress() << " Size# " << msg.GetSizeBytes());

    auto span = NWilson::TSpan(
        TWilsonNbs::NbsBasic,
        ev->TraceId.Clone(),
        "NbsDbgLike.Read",
        NWilson::EFlags::NONE,
        TActivationContext::ActorSystem());
    span
        .Attribute("addr", static_cast<i64>(msg.GetAddress()))
        .Attribute("size", static_cast<i64>(msg.GetSizeBytes()));

    if (Draining || Stopping) {
        span.EndError("DBG is draining");
        ReplyReadErr(origin, cookie, NBSIO_TABLET_NOT_READY, "DBG is draining");
        return;
    }
    if (IoSizeBytes == 0) {
        span.EndError("IoSizeBytes not set");
        ReplyReadErr(origin, cookie, NBSIO_NOT_CONFIGURED, "IoSizeBytes not set");
        return;
    }

    if (TabletConfig.GetDisableReplication()) {
        span.EndError("DisableReplication=true");
        ReplyReadErr(origin, cookie, NBSIO_READS_DISABLED, "DisableReplication=true");
        return;
    }

    const auto decoded = DecodeAddress(msg.GetAddress(), msg.GetSizeBytes());
    if (!decoded || decoded->DbgIndex != MyDbgIndex) {
        LOG_D("HandleNbsRead invalid Address# " << msg.GetAddress()
            << " SizeBytes# " << msg.GetSizeBytes());
        const TString reason = TStringBuilder() << "decode failed Addr# " << msg.GetAddress()
            << " SizeBytes# " << msg.GetSizeBytes();
        span.EndError(reason);
        ReplyReadErr(origin, cookie, NBSIO_INVALID_ADDRESS, reason);
        return;
    }
    const ui32 dbgIndex = decoded->DbgIndex;
    const ui32 vChunkIndex = decoded->VChunkIndex;
    const ui32 offset = decoded->OffsetInVChunk;

    const ui32 ddConnectedCount = static_cast<ui32>(Dbg.DDConnected.count());
    if (ddConnectedCount < kPrimaryHostsPerDbg) {
        LOG_D("HandleNbsRead peers not ready DBG# " << dbgIndex
            << " Cookie# " << cookie
            << " DDConnected# " << ddConnectedCount
            << " need# " << kPrimaryHostsPerDbg
            << "; replying TABLET_NOT_READY");
        const TString reason = TStringBuilder() << "DDisk peers not ready: " << ddConnectedCount
            << "/" << kPrimaryHostsPerDbg;
        span.EndError(reason);
        ReplyReadErr(origin, cookie, NBSIO_TABLET_NOT_READY, reason);
        return;
    }

    auto& dbg = Dbg;
    const ui32 slot = offset / IoSizeBytes;

    const ui64 lsn = dbg.Slots.VisibleLsn({vChunkIndex, slot});
    auto lit = dbg.Lsns.find(lsn);
    if (lit != dbg.Lsns.end() && ShouldReadFromPBuffer(lit->second.State)) {
        if (SendPbRead(dbg, dbgIndex, lsn, lit->second, origin, cookie, span)) {
            return;
        }
        span.EndError("visible PB version has no source");
        ReplyReadErr(origin, cookie, NBSIO_READ_DISPATCH_FAILED, "visible PB version has no source");
        return;
    }
    if (!SendDDiskRead(dbg, dbgIndex, vChunkIndex, offset, IoSizeBytes,
                       /*flushMask=*/{}, origin, cookie, span)) {
        const TString reason = TStringBuilder() << "DDisk dispatch failed for cold slot vChunk# "
            << vChunkIndex << " slot# " << slot;
        span.EndError(reason);
        ReplyReadErr(origin, cookie, NBSIO_READ_DISPATCH_FAILED, reason);
    }
}

void TNbsDbgLikeActor::HandleWritePbsResult(
    NDDisk::TEvWritePersistentBuffersResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 lsn = ev->Cookie;
    auto& dbg = Dbg;
    auto it = dbg.Lsns.find(lsn);
    if (it == dbg.Lsns.end()) {
        for (const auto& sub : ev->Get()->Record.GetResult()) {
            if (sub.GetResult().GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
                const auto& pbId = sub.GetPersistentBufferId();
                LOG_E("PB write result for unknown LSN# " << lsn
                    << " DBG# " << MyDbgIndex
                    << " NodeId# " << pbId.GetNodeId()
                    << " PDiskId# " << pbId.GetPDiskId()
                    << " DDiskSlotId# " << pbId.GetDDiskSlotId()
                    << " Status# " << DDiskStatusText(
                        sub.GetResult().GetStatus(), sub.GetResult().GetErrorReason()));
            }
        }
        return;
    }
    if (it->second.WriteFinalized) {
        return;
    }
    auto& info = it->second;
    // Plural-write replies come from the coordinator's private fan-out actor,
    // not the connected PB actor. Bind that incarnation on its first result.
    if (ev->Sender.NodeId() != dbg.PBActor[info.CoordinatorIndex].NodeId()
            || (info.WriteReplyActor && info.WriteReplyActor != ev->Sender)) {
        return;
    }
    const auto& msg = ev->Get()->Record;
    for (const auto& sub : msg.GetResult()) {
        const ui32 k = LocateInPbIds(dbg, sub.GetPersistentBufferId());
        if (k >= HostsPerDbg() || !info.WriteRequested.test(k)) {
            return;
        }
    }
    info.WriteReplyActor = ev->Sender;
    using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;
    for (const auto& sub : msg.GetResult()) {
        const ui32 k = LocateInPbIds(dbg, sub.GetPersistentBufferId());
        if (info.WriteResponded.test(k) && !info.WriteAmbiguous.test(k)) {
            continue;
        }
        const auto status = sub.GetResult().GetStatus();
        const bool ok = status == TStatus::OK;
        // ERROR/session loss can be synthesized while a remote write remains
        // live. Such a result cannot authorize erasing its source or draining.
        const bool ambiguous = status == TStatus::UNKNOWN || status == TStatus::ERROR
            || status == TStatus::SESSION_MISMATCH;
        info.WriteResponded.set(k);
        info.WriteAmbiguous.set(k, ambiguous);
        if (ok) {
            info.WriteConfirmed.set(k);
        }
        if (sub.GetResult().HasFreeSpace()) {
            dbg.LastFreeSpace[k] = sub.GetResult().GetFreeSpace();
            if (dbg.Counters.PerPeerEnabled && dbg.Counters.Peers[k].FreeSpacePct) {
                dbg.Counters.Peers[k].FreeSpacePct->Set(
                    static_cast<i64>(100.0 * (1.0 - sub.GetResult().GetFreeSpace())));
            }
        }
        if (!ok) {
            const auto& pbId = sub.GetPersistentBufferId();
            const TString statusText = DDiskStatusText(status, sub.GetResult().GetErrorReason());
            LOG_E("PB write failed DBG# " << MyDbgIndex
                << " LSN# " << lsn
                << " VChunk# " << info.VChunkIndex
                << " Offset# " << info.OffsetInVChunk
                << " PeerK# " << k
                << " NodeId# " << pbId.GetNodeId()
                << " PDiskId# " << pbId.GetPDiskId()
                << " DDiskSlotId# " << pbId.GetDDiskSlotId()
                << " Status# " << statusText);
            if (info.WriteError.empty()) {
                info.WriteError = TStringBuilder() << "PB" << k
                    << " " << pbId.GetNodeId() << ":" << pbId.GetPDiskId()
                    << ":" << pbId.GetDDiskSlotId() << " " << statusText;
            }
        }
        BumpPeerReply(dbg, k, EOp::Write, ok);
        for (auto* counters : {&RootCnt.Op[static_cast<size_t>(EOp::Write)],
                              &dbg.Counters.Op[static_cast<size_t>(EOp::Write)]}) {
            if (ok && counters->SubReplyOk) {
                counters->SubReplyOk->Inc();
            } else if (!ok && counters->SubReplyErr) {
                counters->SubReplyErr->Inc();
            }
        }
    }
    const ui32 quorum = TabletConfig.GetDisableReplication() ? 1u : kPrimaryHostsPerDbg;
    const ui32 remaining = (info.WriteRequested & ~info.WriteResponded).count();
    const bool overallOk = !info.WriteFailed && info.WriteConfirmed.count() >= quorum;
    const auto now = ctx.Monotonic();
    if (!info.ReplySent && (overallOk || info.WriteConfirmed.count() + remaining < quorum)) {
        info.ReplySent = true;
        info.WriteFailed = !overallOk;
        if (!overallOk) {
            TStringBuilder reason;
            reason << "confirmed# " << info.WriteConfirmed.count() << " need# " << quorum;
            if (info.WriteError) {
                reason << " " << info.WriteError;
            }
            info.WriteError = TString(reason);
            LOG_E("PB write quorum lost DBG# " << MyDbgIndex
                << " LSN# " << lsn
                << " Cookie# " << info.OriginCookie
                << " VChunk# " << info.VChunkIndex
                << " Offset# " << info.OffsetInVChunk
                << " " << info.WriteError);
        }
        if (info.OriginActor) {
            if (overallOk) {
                info.Span.EndOk();
            } else {
                info.Span.EndError(info.WriteError);
            }
            Send(info.OriginActor, new TEvLoad::TEvNbsWriteResult(
                overallOk ? NBSIO_OK : NBSIO_QUORUM_LOST, info.WriteError), 0, info.OriginCookie);
        }
        if (overallOk) {
            dbg.Slots.MakeVisible(
                {info.VChunkIndex, static_cast<ui32>(info.OffsetInVChunk / info.Size)}, lsn);
            const ui64 quorumMs = (now - info.WriteStart).MilliSeconds();
            if (RootCnt.Request.WriteQuorumMs) {
                RootCnt.Request.WriteQuorumMs->Collect(quorumMs);
            }
            if (dbg.Counters.Request.WriteQuorumMs) {
                dbg.Counters.Request.WriteQuorumMs->Collect(quorumMs);
            }
        } else {
            if (RootCnt.Request.Failed) {
                RootCnt.Request.Failed->Inc();
            }
            if (dbg.Counters.Request.Failed) {
                dbg.Counters.Request.Failed->Inc();
            }
        }
    }
    if (remaining || info.WriteAmbiguous.any()) {
        UpdateAvgPbFreeSpacePct(dbg);
        return;
    }
    info.WriteFinalized = true;
    auto& activity = dbg.VChunkActivity[info.VChunkIndex];
    Y_ABORT_UNLESS(activity.IncompleteWrites);
    --activity.IncompleteWrites;
    ScheduleIdleCleanup();
    --dbg.WritesInFlight;
    const ui8 coord = info.CoordinatorIndex;
    if (coord < kPrimaryHostsPerDbg && dbg.InFlightTo[coord] > 0) {
        --dbg.InFlightTo[coord];
    }

    // Root Pending/BytesInFlight maintained in lockstep with per-DBG gauges
    // (see AccountReadRequest); HandlePoison reconciles in-flight ops.
    if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Write)]; c.ReplyOk) {
        if (overallOk) {
            c.ReplyOk->Inc();
        } else {
            c.ReplyErr->Inc();
        }
        if (c.Pending) {
            c.Pending->Dec();
        }
        if (c.BytesInFlight) {
            *c.BytesInFlight -= info.Size;
        }
        if (c.Bytes) {
            *c.Bytes += info.Size;
        }
        if (c.ResponseTimeMs) {
            c.ResponseTimeMs->Collect((now - info.WriteStart).MilliSeconds());
        }
    }

    if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Write)]; c.Pending) {
        if (overallOk) {
            c.ReplyOk->Inc();
        } else {
            c.ReplyErr->Inc();
        }
        c.Pending->Dec();
        if (c.BytesInFlight) {
            *c.BytesInFlight -= info.Size;
        }
        if (c.Bytes) {
            *c.Bytes += info.Size;
        }
        if (c.ResponseTimeMs) {
            c.ResponseTimeMs->Collect((now - info.WriteStart).MilliSeconds());
        }
    }


    EnterState(dbg, EPBufferState::PBufferIncompleteWrite, -1);
    const TSlotIoCoordinator::TSlot slot = SlotOf(info);
    if (info.WriteFailed) {
        info.State = EPBufferState::PBufferFlushed;
        EnterState(dbg, EPBufferState::PBufferFlushed);
        dbg.Slots.MarkFlushed(slot, lsn);
        dbg.Scheduler.BypassEraseGate(dbg.Slots, lsn, slot);
        dbg.Scheduler.ConsiderFlush(dbg.Slots, slot);
        UpdateAvgPbFreeSpacePct(dbg);
        PumpErase(dbg);
        PumpFlush(dbg);
        return;
    }
    if (TabletConfig.GetDisableReplication()) {
        info.State = EPBufferState::PBufferFlushed;
        EnterState(dbg, EPBufferState::PBufferFlushed);
        dbg.Slots.MarkFlushed(slot, lsn);
        dbg.Scheduler.NoteEraseReady(lsn, slot);
        UpdateAvgPbFreeSpacePct(dbg);
        DriveEraseAdmission(dbg);
        PumpFlush(dbg);
        return;
    }
    info.State = EPBufferState::PBufferWritten;
    EnterState(dbg, EPBufferState::PBufferWritten);
    info.FlushDesired = info.WriteConfirmed & kAllPrimaryHostsMask;
    dbg.Scheduler.NoteFlushReady(lsn, slot);
    UpdateAvgPbFreeSpacePct(dbg);
    DriveFlushAdmission(dbg);
}

TSlotIoCoordinator::TSlot TNbsDbgLikeActor::SlotOf(const TWriteInfo& info) const {
    const ui32 index = info.Size == 0 ? 0 : static_cast<ui32>(info.OffsetInVChunk / info.Size);
    return {info.VChunkIndex, index};
}

void TNbsDbgLikeActor::AccountFlushGate(const TPerDbgState& dbg, bool scheduled) {
    if (Draining || scheduled) {
        return;
    }
    const ui32 ready = dbg.Scheduler.UnadmittedFlushCount();
    if (ready == 0 || ready >= SyncGateThreshold()) {
        return;
    }
    if (RootCnt.Lsns.SyncGateFlushBlocked) {
        RootCnt.Lsns.SyncGateFlushBlocked->Inc();
    }
    if (dbg.Counters.Lsns.SyncGateFlushBlocked) {
        dbg.Counters.Lsns.SyncGateFlushBlocked->Inc();
    }
}

void TNbsDbgLikeActor::AccountEraseGate(const TPerDbgState& dbg, bool scheduled) {
    if (Draining || scheduled) {
        return;
    }
    const ui32 ready = dbg.Scheduler.UnadmittedEraseCount();
    if (ready == 0 || ready >= SyncGateThreshold()) {
        return;
    }
    if (RootCnt.Lsns.SyncGateEraseBlocked) {
        RootCnt.Lsns.SyncGateEraseBlocked->Inc();
    }
    if (dbg.Counters.Lsns.SyncGateEraseBlocked) {
        dbg.Counters.Lsns.SyncGateEraseBlocked->Inc();
    }
}

void TNbsDbgLikeActor::DriveFlushAdmission(TPerDbgState& dbg) {
    dbg.Scheduler.AdmitFlush(dbg.Slots, SyncGateThreshold(), Draining);
    const bool scheduled = PumpFlush(dbg);
    AccountFlushGate(dbg, scheduled);
}

void TNbsDbgLikeActor::DriveEraseAdmission(TPerDbgState& dbg) {
    dbg.Scheduler.AdmitErase(dbg.Slots, SyncGateThreshold(), Draining);
    const bool scheduled = PumpErase(dbg);
    AccountEraseGate(dbg, scheduled);
}

void TNbsDbgLikeActor::WakeFlush(TPerDbgState& dbg, TSlotIoCoordinator::TSlot slot) {
    dbg.Scheduler.ConsiderFlush(dbg.Slots, slot);
    PumpFlush(dbg);
}

void TNbsDbgLikeActor::WakeErase(TPerDbgState& dbg, TSlotIoCoordinator::TSlot slot) {
    dbg.Scheduler.ConsiderErase(dbg.Slots, slot);
    PumpErase(dbg);
}

void TNbsDbgLikeActor::ReleaseErasedLsn(TPerDbgState& dbg, ui64 lsn) {
    auto it = dbg.Lsns.find(lsn);
    if (it == dbg.Lsns.end()) {
        return;
    }
    const auto slot = SlotOf(it->second);
    EnterState(dbg, it->second.State, -1);
    dbg.Slots.Retire(slot, lsn);
    dbg.Scheduler.Forget(lsn);
    dbg.Lsns.erase(it);
    --LsnsTotalAll;
    dbg.Scheduler.ConsiderErase(dbg.Slots, slot);
}

bool TNbsDbgLikeActor::PumpFlush(TPerDbgState& dbg) {
    const ui32 batch = Max<ui32>(1, TabletConfig.GetFlushBatchSize());
    using TPending = std::array<std::vector<std::tuple<ui64, NDDisk::TBlockSelector>>, kPrimaryHostsPerDbg>;
    TPending pending;
    std::optional<ui32> firstVChunk;
    absl::flat_hash_map<ui32, TPending> otherVChunks;
    std::array<ui32, kPrimaryHostsPerDbg> count = {};
    ui32 saturated = 0;
    bool scheduled = false;

    while (dbg.Scheduler.HasFlushWork() && saturated < kPrimaryHostsPerDbg) {
        const ui64 lsn = dbg.Scheduler.PeekFlush();
        auto it = dbg.Lsns.find(lsn);
        if (it == dbg.Lsns.end() || it->second.State != EPBufferState::PBufferWritten) {
            dbg.Scheduler.PopFlush();
            continue;
        }
        auto& info = it->second;
        if (!dbg.Slots.CanFlush(SlotOf(info), lsn)) {
            dbg.Scheduler.PopFlush();
            continue;
        }
        // A Sync can name only one vChunk. Keep the common single-vChunk
        // pass in the original vectors and group other vChunks on demand.
        if (!firstVChunk) {
            firstVChunk = info.VChunkIndex;
        }
        auto& chunkPending = info.VChunkIndex == *firstVChunk ? pending : otherVChunks[info.VChunkIndex];
        bool fullyScheduled = true;
        for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
            if (!info.FlushDesired.test(k) || info.FlushRequested.test(k)) {
                continue;
            }
            if (count[k] >= batch) {
                fullyScheduled = false;
                continue;
            }
            info.FlushRequested.set(k);
            if (++count[k] == batch) {
                ++saturated;
            }
            chunkPending[k].push_back({lsn, NDDisk::TBlockSelector(
                WireVChunkIndex(MyDbgIndex, AllocConfig.GetTargetNumVChunks(), info.VChunkIndex),
                static_cast<ui32>(info.OffsetInVChunk), info.Size)});
        }
        if (!fullyScheduled) {
            break;
        }
        dbg.Scheduler.PopFlush();
        EnterState(dbg, EPBufferState::PBufferWritten, -1);
        info.State = EPBufferState::PBufferFlushing;
        EnterState(dbg, EPBufferState::PBufferFlushing);
    }

    const auto sendBatches = [&](TPending& pending) {
        for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
            if (pending[k].empty()) {
                continue;
            }
            scheduled = true;
            Y_ABORT_UNLESS(dbg.DDToken[k]);
            auto creds = NDDisk::TQueryCredentials::ToDDisk(*dbg.DDToken[k]);
            std::tuple<ui32, ui32, ui32> srcId{
                dbg.PBIdsPb[k].GetNodeId(),
                dbg.PBIdsPb[k].GetPDiskId(),
                dbg.PBIdsPb[k].GetDDiskSlotId()};
            auto ev = std::make_unique<NDDisk::TEvSync>(creds);
            TFlushBatch batchInfo;
            batchInfo.DbgIndex = dbg.DbgIndex;
            batchInfo.Sink = static_cast<ui8>(k);
            batchInfo.Lsns.reserve(pending[k].size());
            batchInfo.SentAt = MonotonicNow();
            batchInfo.ReplyActor = DD[k].RuntimeActor;
            for (auto& [lsn, sel] : pending[k]) {
                ev->AddSegmentFromPB(srcId, dbg.PBGuid[k], sel, lsn, Generation_);
                batchInfo.Lsns.push_back(lsn);
                ++dbg.VChunkActivity[dbg.Lsns.at(lsn).VChunkIndex].SyncSegments;
            }
            const ui64 cookie = NextBatchCookie++;
            FlushInflight.emplace(cookie, std::move(batchInfo));
            Send(dbg.DDiskActor[k], ev.release(), 0, cookie);

            // Root Pending maintained in lockstep with per-DBG gauge (see
            // AccountReadRequest); HandlePoison reconciles in-flight ops.
            if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Flush)]; c.Requests) {
                c.Requests->Inc();
                if (c.Pending) {
                    c.Pending->Inc();
                }
                if (c.BatchSize) {
                    c.BatchSize->Collect(pending[k].size());
                }
            }
            if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Flush)]; c.Requests) {
                c.Requests->Inc();
                c.Pending->Inc();
                if (c.BatchSize) {
                    c.BatchSize->Collect(pending[k].size());
                }
            }
            BumpPeerRequest(dbg, kHostsPerDbgMax + k, EOp::Flush);
        }
    };
    sendBatches(pending);
    for (auto& [vChunk, batches] : otherVChunks) {
        sendBatches(batches);
    }
    if (dbg.Scheduler.HasFlushWork()) {
        ScheduleMaintenanceContinuation();
    }
    return scheduled;
}

void TNbsDbgLikeActor::HandleSyncResult(
    NDDisk::TEvSyncResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ev->Cookie;
    auto bIt = FlushInflight.find(cookie);
    if (bIt == FlushInflight.end()) {
        const auto& msg = ev->Get()->Record;
        if (msg.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            LOG_E("DDisk sync result for unknown cookie# " << cookie
                << " DBG# " << MyDbgIndex
                << " Status# " << DDiskStatusText(msg.GetStatus(), msg.GetErrorReason()));
        }
        return;
    }
    if (ev->Sender != bIt->second.ReplyActor) {
        return;
    }
    const auto& result = ev->Get()->Record;
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
        return;
    }
    const bool success = result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
    // Whole-request rejection has no segment results. A successful reply must
    // name every segment; incomplete success cannot release a reservation.
    if ((success && result.SegmentResultsSize() != bIt->second.Lsns.size())
            || (!success && result.SegmentResultsSize() != 0
                && result.SegmentResultsSize() != bIt->second.Lsns.size())) {
        return;
    }
    TFlushBatch batchInfo = std::move(bIt->second);
    FlushInflight.erase(bIt);
    const ui32 k = batchInfo.Sink;
    if (k >= kPrimaryHostsPerDbg) {
        return;
    }
    auto& dbg = Dbg;
    const NActors::TMonotonic now = ctx.Monotonic();
    const NActors::TMonotonic sentAt = batchInfo.SentAt;

    const auto& msg = ev->Get()->Record;
    const auto outerStatus = msg.GetStatus();
    const bool outerOk = outerStatus == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;

    ScheduleIdleCleanup();
    bool notedErase = false;
    std::vector<std::pair<ui64, bool>> perLsn;
    perLsn.reserve(batchInfo.Lsns.size());
    for (ui32 i = 0; i < batchInfo.Lsns.size(); ++i) {
        bool ok;
        if (i < msg.SegmentResultsSize()) {
            ok = msg.GetSegmentResults(i).GetStatus() ==
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
        } else {
            ok = false;
        }
        perLsn.emplace_back(batchInfo.Lsns[i], ok);
    }

    for (auto& [lsn, ok] : perLsn) {
        auto it = dbg.Lsns.find(lsn);
        if (it == dbg.Lsns.end()) {
            continue;
        }
        TWriteInfo& info = it->second;
        auto& activity = dbg.VChunkActivity[info.VChunkIndex];
        Y_ABORT_UNLESS(activity.SyncSegments);
        --activity.SyncSegments;
        if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Flush)]; c.SubReplyOk) {
            if (ok) {
                c.SubReplyOk->Inc();
            } else {
                c.SubReplyErr->Inc();
            }
        }
        if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Flush)]; c.SubReplyOk) {
            if (ok) {
                c.SubReplyOk->Inc();
            } else {
                c.SubReplyErr->Inc();
            }
        }
        if (ok) {
            info.FlushConfirmed.set(k);
            if (info.FlushConfirmed == info.FlushDesired
                && info.State == EPBufferState::PBufferFlushing) {
                const auto slot = SlotOf(info);
                EnterState(dbg, EPBufferState::PBufferFlushing, /*delta=*/-1);
                info.State = EPBufferState::PBufferFlushed;
                EnterState(dbg, EPBufferState::PBufferFlushed, /*delta=*/+1);
                dbg.Slots.MarkFlushed(slot, lsn);
                if (info.VChunkIndex < dbg.FlushedSlots.size() && info.Size != 0) {
                    dbg.FlushedSlots[info.VChunkIndex].Set(info.OffsetInVChunk / info.Size);
                }
                dbg.Scheduler.NoteEraseReady(lsn, slot);
                notedErase = true;
                dbg.Scheduler.ConsiderFlush(dbg.Slots, slot);
            }
        } else {
            info.FlushRequested.reset(k);
            // This destination's Sync failed. The LSN stays flush-admitted and
            // returns to PBufferWritten so the actionable queue can retry it.
            if (info.State == EPBufferState::PBufferFlushing) {
                EnterState(dbg, EPBufferState::PBufferFlushing, /*delta=*/-1);
                info.State = EPBufferState::PBufferWritten;
                EnterState(dbg, EPBufferState::PBufferWritten, /*delta=*/+1);
            }
            dbg.Scheduler.ConsiderFlush(dbg.Slots, SlotOf(info));
            if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Flush)]; c.Retries) {
                c.Retries->Inc();
            }
            if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Flush)]; c.Retries) {
                c.Retries->Inc();
            }
        }
    }

    const ui64 latencyMs = (now - sentAt).MilliSeconds();
    // FlushMs samples the flush request round-trip, once per request (not per
    // LSN in the batch), so its rate tracks flush requests rather than quorums.
    if (RootCnt.Request.FlushMs) {
        RootCnt.Request.FlushMs->Collect(latencyMs);
    }
    if (dbg.Counters.Request.FlushMs) {
        dbg.Counters.Request.FlushMs->Collect(latencyMs);
    }
    // Root Pending maintained in lockstep with per-DBG gauge (see
    // AccountReadRequest); HandlePoison reconciles in-flight ops.
    if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Flush)]; c.ReplyOk) {
        if (c.Pending) {
            c.Pending->Dec();
        }
        if (outerOk) {
            c.ReplyOk->Inc();
        } else {
            c.ReplyErr->Inc();
        }
        if (c.ResponseTimeMs) {
            c.ResponseTimeMs->Collect(latencyMs);
        }
    }
    if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Flush)]; c.Pending) {
        c.Pending->Dec();
        if (outerOk) {
            c.ReplyOk->Inc();
        } else {
            c.ReplyErr->Inc();
        }
        if (c.ResponseTimeMs) {
            c.ResponseTimeMs->Collect(latencyMs);
        }
    }
    if (dbg.Counters.PerPeerEnabled
        && dbg.Counters.Peers[kHostsPerDbgMax + k].ResponseTimeMs)
    {
        dbg.Counters.Peers[kHostsPerDbgMax + k].ResponseTimeMs->Collect(latencyMs);
    }
    BumpPeerReply(dbg, kHostsPerDbgMax + k, EOp::Flush, outerOk);

    ui32 failedSegments = 0;
    TString firstSegment;
    for (ui32 i = 0; i < msg.SegmentResultsSize(); ++i) {
        const auto& segment = msg.GetSegmentResults(i);
        if (segment.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            continue;
        }
        if (!failedSegments) {
            firstSegment = DDiskStatusText(segment.GetStatus(), segment.GetErrorReason());
        }
        ++failedSegments;
    }
    if (!outerOk || failedSegments) {
        const auto& id = dbg.DDiskIdsPb[k];
        LOG_E("DDisk sync failed DBG# " << MyDbgIndex
            << " Sink# " << k
            << " NodeId# " << id.GetNodeId()
            << " PDiskId# " << id.GetPDiskId()
            << " DDiskSlotId# " << id.GetDDiskSlotId()
            << " Cookie# " << cookie
            << " Lsns# " << batchInfo.Lsns.size()
            << " Status# " << DDiskStatusText(outerStatus, msg.GetErrorReason())
            << " FailedSegments# " << failedSegments
            << (firstSegment ? " FirstSegment# " : "")
            << firstSegment);
    }

    PumpFlush(dbg);
    if (notedErase) {
        DriveEraseAdmission(dbg);
    } else {
        PumpErase(dbg);
    }
}

bool TNbsDbgLikeActor::PumpErase(TPerDbgState& dbg) {
    const ui32 batch = Max<ui32>(1, TabletConfig.GetEraseBatchSize());
    std::array<std::vector<ui64>, kHostsPerDbgMax> pending;
    std::array<ui32, kHostsPerDbgMax> count = {};
    const ui32 hostsPerDbg = HostsPerDbg();
    ui32 saturated = 0;
    bool scheduled = false;
    bool retiredWithoutErase = false;

    while (dbg.Scheduler.HasEraseWork() && saturated < hostsPerDbg) {
        const ui64 lsn = dbg.Scheduler.PeekErase();
        auto it = dbg.Lsns.find(lsn);
        if (it == dbg.Lsns.end() || it->second.State != EPBufferState::PBufferFlushed) {
            dbg.Scheduler.PopErase();
            continue;
        }
        TWriteInfo& info = it->second;
        const auto slot = SlotOf(info);
        if (!dbg.Slots.CanErase(slot, lsn)) {
            dbg.Scheduler.PopErase();
            continue;
        }
        info.EraseTarget = info.WriteConfirmed;
        if (!info.EraseTarget.any()) {
            dbg.Scheduler.PopErase();
            ReleaseErasedLsn(dbg, lsn);
            retiredWithoutErase = true;
            continue;
        }
        bool fullyScheduled = true;
        for (ui32 k = 0; k < hostsPerDbg; ++k) {
            if (!info.EraseTarget.test(k) || info.EraseRequested.test(k)) {
                continue;
            }
            if (count[k] >= batch) {
                fullyScheduled = false;
                continue;
            }
            info.EraseRequested.set(k);
            if (++count[k] == batch) {
                ++saturated;
            }
            pending[k].push_back(lsn);
        }
        if (!fullyScheduled) {
            break;
        }
        dbg.Scheduler.PopErase();
        if (info.State == EPBufferState::PBufferFlushed
            && info.EraseTarget.any()
            && (info.EraseRequested & info.EraseTarget) == info.EraseTarget)
        {
            EnterState(dbg, EPBufferState::PBufferFlushed, /*delta=*/-1);
            info.State = EPBufferState::PBufferErasing;
            EnterState(dbg, EPBufferState::PBufferErasing, /*delta=*/+1);
        }
    }

    for (ui32 k = 0; k < hostsPerDbg; ++k) {
        if (pending[k].empty()) {
            continue;
        }
        scheduled = true;
        Y_ABORT_UNLESS(dbg.PBToken[k]);
        auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(*dbg.PBToken[k]);
        auto ev = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(creds);
        TEraseBatch batchInfo;
        batchInfo.DbgIndex = dbg.DbgIndex;
        batchInfo.Sink = static_cast<ui8>(k);
        batchInfo.Lsns.reserve(pending[k].size());
        batchInfo.SentAt = MonotonicNow();
        batchInfo.ReplyActor = PB[k].RuntimeActor;
        for (ui64 lsn : pending[k]) {
            ev->AddErase(lsn, Generation_);
            batchInfo.Lsns.push_back(lsn);
        }
        const ui64 cookie = NextBatchCookie++;
        EraseInflight.emplace(cookie, std::move(batchInfo));
        Send(dbg.PBActor[k], ev.release(), 0, cookie);

        // Root Pending maintained in lockstep with per-DBG gauge (see
        // AccountReadRequest); HandlePoison reconciles in-flight ops.
        if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Erase)]; c.Requests) {
            c.Requests->Inc();
            if (c.Pending) {
                c.Pending->Inc();
            }
            if (c.BatchSize) {
                c.BatchSize->Collect(pending[k].size());
            }
        }
        if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Erase)]; c.Requests) {
            c.Requests->Inc();
            c.Pending->Inc();
            if (c.BatchSize) {
                c.BatchSize->Collect(pending[k].size());
            }
        }
        BumpPeerRequest(dbg, k, EOp::Erase);
    }
    if (retiredWithoutErase) {
        UpdateLsnsTotal(dbg);
    }
    if (dbg.Scheduler.HasEraseWork()) {
        ScheduleMaintenanceContinuation();
    }
    return scheduled;
}

void TNbsDbgLikeActor::HandleEraseResult(
    NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ev->Cookie;
    auto bIt = EraseInflight.find(cookie);
    if (bIt == EraseInflight.end()) {
        const auto& msg = ev->Get()->Record;
        if (msg.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            LOG_E("PB erase result for unknown cookie# " << cookie
                << " DBG# " << MyDbgIndex
                << " Status# " << DDiskStatusText(msg.GetStatus(), msg.GetErrorReason()));
        }
        return;
    }
    if (ev->Sender != bIt->second.ReplyActor
            || ev->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
        return;
    }
    TEraseBatch batchInfo = std::move(bIt->second);
    EraseInflight.erase(bIt);
    const ui32 k = batchInfo.Sink;
    if (k >= HostsPerDbg()) {
        return;
    }
    auto& dbg = Dbg;
    const NActors::TMonotonic now = ctx.Monotonic();

    const auto& msg = ev->Get()->Record;
    const bool outerOk = msg.GetStatus() ==
        NKikimrBlobStorage::NDDisk::TReplyStatus::OK;

    const std::vector<ui64>& lsns = batchInfo.Lsns;

    if (msg.HasFreeSpace()) {
        dbg.LastFreeSpace[k] = msg.GetFreeSpace();
        if (dbg.Counters.PerPeerEnabled && dbg.Counters.Peers[k].FreeSpacePct) {
            dbg.Counters.Peers[k].FreeSpacePct->Set(
                static_cast<i64>(100.0 * (1.0 - msg.GetFreeSpace())));
        }
    }
    UpdateAvgPbFreeSpacePct(dbg);

    for (ui64 lsn : lsns) {
        auto it = dbg.Lsns.find(lsn);
        if (it == dbg.Lsns.end()) {
            continue;
        }
        TWriteInfo& info = it->second;
        if (outerOk) {
            info.EraseConfirmed.set(k);
            if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Erase)]; c.SubReplyOk) {
                c.SubReplyOk->Inc();
            }
            if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Erase)]; c.SubReplyOk) {
                c.SubReplyOk->Inc();
            }
        } else {
            info.EraseRequested.reset(k);
            // This destination's erase failed. Keep the LSN erase-admitted and
            // return it to PBufferFlushed so the actionable queue can retry it.
            if (info.State == EPBufferState::PBufferErasing) {
                EnterState(dbg, EPBufferState::PBufferErasing, /*delta=*/-1);
                info.State = EPBufferState::PBufferFlushed;
                EnterState(dbg, EPBufferState::PBufferFlushed, /*delta=*/+1);
            }
            dbg.Scheduler.ConsiderErase(dbg.Slots, SlotOf(info));
            if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Erase)]; c.SubReplyErr) {
                c.SubReplyErr->Inc();
            }
            if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Erase)]; c.SubReplyErr) {
                c.SubReplyErr->Inc();
            }
            if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Erase)]; c.Retries) {
                c.Retries->Inc();
            }
            if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Erase)]; c.Retries) {
                c.Retries->Inc();
            }
        }
        if (CanDropErasedLsn(info.EraseTarget, info.EraseConfirmed)) {
            const bool failedWrite = info.WriteFailed;
            const ui64 fullMs = (now - info.WriteStart).MilliSeconds();
            ReleaseErasedLsn(dbg, lsn);
            if (!failedWrite && RootCnt.Request.Completed) {
                RootCnt.Request.Completed->Inc();
            }
            if (!failedWrite && dbg.Counters.Request.Completed) {
                dbg.Counters.Request.Completed->Inc();
            }
            if (RootCnt.Request.LatencyMs) {
                RootCnt.Request.LatencyMs->Collect(fullMs);
            }
            if (dbg.Counters.Request.LatencyMs) {
                dbg.Counters.Request.LatencyMs->Collect(fullMs);
            }
        }
    }

    const ui64 latencyMs = (now - batchInfo.SentAt).MilliSeconds();
    // EraseMs samples the erase request round-trip, once per request (not per
    // LSN in the batch), so its rate tracks erase requests rather than quorums.
    if (RootCnt.Request.EraseMs) {
        RootCnt.Request.EraseMs->Collect(latencyMs);
    }
    if (dbg.Counters.Request.EraseMs) {
        dbg.Counters.Request.EraseMs->Collect(latencyMs);
    }
    // Root Pending maintained in lockstep with per-DBG gauge (see
    // AccountReadRequest); HandlePoison reconciles in-flight ops.
    if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Erase)]; c.ReplyOk) {
        if (c.Pending) {
            c.Pending->Dec();
        }
        if (outerOk) {
            c.ReplyOk->Inc();
        } else {
            c.ReplyErr->Inc();
        }
        if (c.ResponseTimeMs) {
            c.ResponseTimeMs->Collect(latencyMs);
        }
    }
    if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Erase)]; c.Pending) {
        c.Pending->Dec();
        if (outerOk) {
            c.ReplyOk->Inc();
        } else {
            c.ReplyErr->Inc();
        }
        if (c.ResponseTimeMs) {
            c.ResponseTimeMs->Collect(latencyMs);
        }
    }
    BumpPeerReply(dbg, k, EOp::Erase, outerOk);
    if (!outerOk) {
        const auto& id = dbg.PBIdsPb[k];
        LOG_E("PB erase failed DBG# " << MyDbgIndex
            << " PB" << k
            << " NodeId# " << id.GetNodeId()
            << " PDiskId# " << id.GetPDiskId()
            << " DDiskSlotId# " << id.GetDDiskSlotId()
            << " Cookie# " << cookie
            << " Lsns# " << lsns.size()
            << " Status# " << DDiskStatusText(msg.GetStatus(), msg.GetErrorReason()));
    }
    UpdateLsnsTotal(dbg);
    PumpErase(dbg);
}

bool TNbsDbgLikeActor::CompleteRead(ui64 cookie, bool ok, TActorId& origin,
    ui64& originCookie, ui32& size, TStringBuf errorReason)
{
    auto rIt = ReadInflight.find(cookie);
    if (rIt == ReadInflight.end()) {
        return false;
    }
    TReadInflight read = std::move(rIt->second);
    ReadInflight.erase(rIt);
    if (read.Span) {
        if (ok) {
            read.Span.EndOk();
        } else {
            read.Span.EndError(TString(errorReason));
        }
    }
    origin = read.OriginActor;
    originCookie = read.OriginCookie;
    size = read.Size;

    auto& dbg = Dbg;
    bool wakeErase = false;
    bool wakeFlush = false;
    if (read.IsPb) {
        wakeErase = dbg.Slots.UnpinPBRead(read.Slot, read.Lsn);
    } else {
        wakeFlush = dbg.Slots.UnpinDDiskRead(read.Slot);
    }
    if (dbg.ReadsInFlight > 0) {
        --dbg.ReadsInFlight;
    }

    const NActors::TMonotonic now = MonotonicNow();
    const EOp op = read.IsPb ? EOp::ReadPB : EOp::ReadDDisk;
    const ui64 latencyMs = (read.SentAt != NActors::TMonotonic::Zero())
        ? (now - read.SentAt).MilliSeconds() : 0;

    // Root Pending/BytesInFlight maintained in lockstep with per-DBG gauges
    // (see AccountReadRequest); HandlePoison reconciles in-flight ops.
    if (auto& c = RootCnt.Op[static_cast<size_t>(op)]; c.ReplyOk) {
        if (ok) {
            c.ReplyOk->Inc();
        } else {
            c.ReplyErr->Inc();
        }
        if (c.Pending) {
            c.Pending->Dec();
        }
        if (c.BytesInFlight) {
            *c.BytesInFlight -= read.Size;
        }
        if (ok && c.Bytes) {
            *c.Bytes += read.Size;
        }
        if (c.ResponseTimeMs && read.SentAt != NActors::TMonotonic::Zero()) {
            c.ResponseTimeMs->Collect(latencyMs);
        }
    }
    if (auto& c = dbg.Counters.Op[static_cast<size_t>(op)]; c.Pending) {
        c.Pending->Dec();
        if (ok) {
            c.ReplyOk->Inc();
        } else {
            c.ReplyErr->Inc();
        }
        if (c.BytesInFlight) {
            *c.BytesInFlight -= read.Size;
        }
        if (ok && c.Bytes) {
            *c.Bytes += read.Size;
        }
        if (c.ResponseTimeMs && read.SentAt != NActors::TMonotonic::Zero()) {
            c.ResponseTimeMs->Collect(latencyMs);
        }
    }

    if (read.IsPb) {
        BumpPeerReply(dbg, read.PeerK, EOp::ReadPB, ok);
    } else {
        BumpPeerReply(dbg, kHostsPerDbgMax + read.PeerK, EOp::ReadDDisk, ok);
    }
    if (wakeErase) {
        WakeErase(dbg, read.Slot);
    } else if (wakeFlush) {
        WakeFlush(dbg, read.Slot);
    }
    return true;
}

void TNbsDbgLikeActor::HandlePbReadResult(
    NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
    const TActorContext& /*ctx*/)
{
    const ui64 cookie = ev->Cookie;
    const auto pending = ReadInflight.find(cookie);
    if (pending == ReadInflight.end() || !pending->second.IsPb
            || pending->second.ReplyActor != ev->Sender) {
        return;
    }
    const auto& msg = ev->Get()->Record;
    if (msg.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
        return;
    }
    const bool ok = msg.GetStatus() ==
        NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
    TActorId origin;
    ui64 originCookie = 0;
    ui32 size = 0;
    TString errorReason;
    if (!ok) {
        errorReason = TStringBuilder() << "PbRead: "
            << DDiskStatusText(msg.GetStatus(), msg.GetErrorReason());
        LOG_E("PB read failed DBG# " << MyDbgIndex
            << " Cookie# " << cookie
            << " VChunk# " << msg.GetVChunkIndex()
            << " Offset# " << msg.GetOffsetInBytes()
            << " Size# " << msg.GetSizeInBytes()
            << " Status# " << DDiskStatusText(msg.GetStatus(), msg.GetErrorReason()));
    }
    if (CompleteRead(cookie, ok, origin, originCookie, size, errorReason) && origin) {
        TString reason;
        if (!ok) {
            reason = std::move(errorReason);
        }
        auto resp = std::make_unique<TEvLoad::TEvNbsReadResult>(
            ok ? NBSIO_OK : NBSIO_IO_ERROR, std::move(reason));
        if (ok && ev->Get()->GetPayloadCount() > 0) {
            const ui32 payloadId = resp->AddPayload(TRope(ev->Get()->GetPayload(0)));
            resp->Record.SetPayloadId(payloadId);
        }
        Send(origin, resp.release(), 0, originCookie);
    }
}

void TNbsDbgLikeActor::HandleDDiskReadResult(
    NDDisk::TEvReadResult::TPtr& ev, const TActorContext& /*ctx*/)
{
    const ui64 cookie = ev->Cookie;
    const auto pending = ReadInflight.find(cookie);
    if (pending == ReadInflight.end() || pending->second.IsPb
            || pending->second.ReplyActor != ev->Sender) {
        return;
    }
    const auto& msg = ev->Get()->Record;
    if (msg.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
        return;
    }
    const bool ok = msg.GetStatus() ==
        NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
    TActorId origin;
    ui64 originCookie = 0;
    ui32 size = 0;
    TString errorReason;
    if (!ok) {
        errorReason = TStringBuilder() << "DDiskRead: "
            << DDiskStatusText(msg.GetStatus(), msg.GetErrorReason());
        LOG_E("DDisk read failed DBG# " << MyDbgIndex
            << " Cookie# " << cookie
            << " Status# " << DDiskStatusText(msg.GetStatus(), msg.GetErrorReason()));
    }
    if (CompleteRead(cookie, ok, origin, originCookie, size, errorReason) && origin) {
        TString reason;
        if (!ok) {
            reason = std::move(errorReason);
        }
        auto resp = std::make_unique<TEvLoad::TEvNbsReadResult>(
            ok ? NBSIO_OK : NBSIO_IO_ERROR, std::move(reason));
        if (ok && ev->Get()->GetPayloadCount() > 0) {
            const ui32 payloadId = resp->AddPayload(TRope(ev->Get()->GetPayload(0)));
            resp->Record.SetPayloadId(payloadId);
        }
        Send(origin, resp.release(), 0, originCookie);
    }
}

void TNbsDbgLikeActor::HandleConfigureTablet(
    TEvLoad::TEvConfigureTablet::TPtr& ev, const TActorContext& ctx)
{
    if (ev->Sender != TabletActorId || Stopping || !Draining || !DrainAcknowledged) {
        return;
    }
    const auto& cfg = ev->Get()->Record;
    LOG_I("Worker HandleConfigureTablet DBG# " << MyDbgIndex
        << " MaxInflightLsns# " << cfg.GetMaxInflightLsns()
        << " FlushBatchSize# " << cfg.GetFlushBatchSize()
        << " EraseBatchSize# " << cfg.GetEraseBatchSize()
        << " SyncRequestsBatchSize# " << cfg.GetSyncRequestsBatchSize()
        << " NumDirectBlockGroupsToUse# " << cfg.GetNumDirectBlockGroupsToUse()
        << " IoSizeBytes# " << cfg.GetIoSizeBytes()
        << " EnableChecksums# " << cfg.GetEnableChecksums());

    InitWorkerCounters();

    Y_ABORT_UNLESS(Dbg.Lsns.empty() && ReadInflight.empty() && FlushInflight.empty() && EraseInflight.empty());
    Dbg.Slots.Clear();
    Dbg.Scheduler.Clear();
    Dbg.VChunkActivity.assign(AllocConfig.GetTargetNumVChunks(), {});
    Draining = false;
    DrainAcknowledged = false;
    NotifyDrained = false;

    TabletConfig = cfg;

    // Use the shared routing helper so the worker's view of ActiveDbgs/
    // IoSizeBytes/BytesPerDbg cannot drift from the proxy tablet's.
    const auto params = ComputeRoutingParams(cfg, AllocConfig, NumDbgsTotal);
    ActiveDbgs = params.ActiveDbgs;

    const ui32 wantIo = cfg.GetIoSizeBytes();
    const ui64 vChunkSizeBytes = AllocConfig.GetVChunkSizeBytes();
    if (!params.IoValid && wantIo != 0) {
        if (wantIo > vChunkSizeBytes) {
            LOG_E("Worker HandleConfigureTablet IoSizeBytes=" << wantIo
                << " > VChunkSizeBytes=" << vChunkSizeBytes
                << "; keeping previous IoSizeBytes=" << IoSizeBytes);
        } else {
            LOG_E("Worker HandleConfigureTablet IoSizeBytes=" << wantIo
                << " does not divide VChunkSizeBytes=" << vChunkSizeBytes
                << "; keeping previous IoSizeBytes=" << IoSizeBytes);
        }
    } else if (params.IoValid && params.IoSizeBytes != IoSizeBytes) {
        IoSizeBytes = params.IoSizeBytes;
        BytesPerDbg = params.BytesPerDbg;
        const ui32 slotsPerVChunk = vChunkSizeBytes / IoSizeBytes;
        if (Dbg.FlushedSlots.size() != AllocConfig.GetTargetNumVChunks()) {
            Dbg.FlushedSlots.resize(AllocConfig.GetTargetNumVChunks());
        }
        for (auto& slots : Dbg.FlushedSlots) {
            slots.Clear();
            slots.Reserve(slotsPerVChunk);
        }
    }

    if (Dbg.Counters.Lsns.SyncGateThreshold) {
        Dbg.Counters.Lsns.SyncGateThreshold->Set(SyncGateThreshold());
    }
    auto reply = std::make_unique<TEvLoad::TEvConfigureTabletResult>();
    reply->Record.SetConfigurationId(cfg.GetConfigurationId());
    reply->Record.SetSuccess(params.IoValid);
    ctx.Send(ev->Sender, reply.release(), 0, ev->Cookie);
}

void TNbsDbgLikeActor::HandleUpdateMonitoring(
    NKikimr::TEvUpdateMonitoring::TPtr&, const TActorContext&)
{
    Schedule(TDuration::MilliSeconds(NKikimr::MonitoringUpdateCycleMs),
        new NKikimr::TEvUpdateMonitoring);
}

void TNbsDbgLikeActor::InitWorkerCounters() {
    if (WorkerCountersInited) {
        return;
    }
    WorkerCountersInited = true;

    if (!Counters) {
        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    }
    RootCnt.Root = Counters;

    const auto latencyBounds = NKikimr::GetCommonLatencyHistBounds(
        NPDisk::DEVICE_TYPE_NVME);

    // Additive lifecycle counters are shared across workers (Inc/Dec net
    // correctly); the DbgsAllocated gauge is owned by the tablet.
    auto life = RootCnt.Root->GetSubgroup("subsystem", "lifecycle_worker");
    RootCnt.ConnectOk = life->GetCounter("ConnectOk", true);
    RootCnt.ConnectErr = life->GetCounter("ConnectErr", true);
    RootCnt.DisconnectOk = life->GetCounter("DisconnectOk", true);

    // Root lsns subgroup: only the additive counters (BackpressureHits,
    // SyncGate*Blocked) are touched by workers; the Set-gauges (Total,
    // BufferState, AvgPb*, NewestLsn, MaxLsns, SyncGateThreshold) are either
    // owned by the tablet or left to Solomon aggregation across dbg= groups.
    auto lsnsRoot = RootCnt.Root->GetSubgroup("subsystem", "lsns");
    RootCnt.Lsns.Init(lsnsRoot, /*isRoot=*/true);

    auto opRoot = RootCnt.Root->GetSubgroup("subsystem", "op");
    for (ui32 i = 0; i < kOpCount; ++i) {
        const auto op = static_cast<EOp>(i);
        auto g = opRoot->GetSubgroup("operation", ToString(op));
        const bool wantBytes = (op == EOp::Write) || (op == EOp::ReadPB) || (op == EOp::ReadDDisk);
        const bool wantBatch = (op == EOp::Flush) || (op == EOp::Erase);
        RootCnt.Op[i].Init(g, latencyBounds, wantBatch, wantBytes);
    }

    auto reqRoot = RootCnt.Root->GetSubgroup("subsystem", "request");
    RootCnt.Request.Init(reqRoot, latencyBounds);

    const bool perPeer = NumDbgsTotal < kMaxPerPeerCounters;
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 hostsPerDbg = HostsPerDbg();
    {
        auto& d = Dbg;
        d.DbgIndex = DbgInfo.DbgIndex;
        d.DirectBlockGroupId = DbgInfo.DirectBlockGroupId;
        d.Counters.Root = RootCnt.Root->GetSubgroup("dbg",
            Sprintf("%" PRIu64 ":%" PRIu64, bscTabletId, d.DirectBlockGroupId));
        auto lsnsG = d.Counters.Root->GetSubgroup("subsystem", "lsns");
        d.Counters.Lsns.Init(lsnsG, /*isRoot=*/false);
        if (d.Counters.Lsns.SyncGateThreshold) {
            d.Counters.Lsns.SyncGateThreshold->Set(SyncGateThreshold());
        }
        auto opG = d.Counters.Root->GetSubgroup("subsystem", "op");
        for (ui32 i2 = 0; i2 < kOpCount; ++i2) {
            const auto op = static_cast<EOp>(i2);
            auto sub = opG->GetSubgroup("operation", ToString(op));
            const bool wantBytes = (op == EOp::Write) || (op == EOp::ReadPB) || (op == EOp::ReadDDisk);
            const bool wantBatch = (op == EOp::Flush) || (op == EOp::Erase);
            d.Counters.Op[i2].Init(sub, latencyBounds, wantBatch, wantBytes);
        }
        auto reqG = d.Counters.Root->GetSubgroup("subsystem", "request");
        d.Counters.Request.Init(reqG, latencyBounds);
        d.Counters.PerPeerEnabled = perPeer;
        if (perPeer) {
            auto peersG = d.Counters.Root->GetSubgroup("subsystem", "peers");
            for (ui32 k = 0; k < hostsPerDbg; ++k) {
                auto pbG = peersG->GetSubgroup("peer", Sprintf("PB%u", k));
                d.Counters.Peers[k].Connected = pbG->GetCounter("Connected", false);
                d.Counters.Peers[k].RequestsSent = pbG->GetCounter("RequestsSent", true);
                d.Counters.Peers[k].RepliesOk = pbG->GetCounter("RepliesOk", true);
                d.Counters.Peers[k].RepliesErr = pbG->GetCounter("RepliesErr", true);
                d.Counters.Peers[k].FreeSpacePct = pbG->GetCounter("FreeSpacePct", false);
                d.Counters.Peers[k].ResponseTimeMs = pbG->GetHistogram(
                    "ResponseTimeMs", NMonitoring::ExplicitHistogram(latencyBounds));
                RegisterPeerOpCounters(d.Counters.Peers[k], pbG, EOp::Write);
                RegisterPeerOpCounters(d.Counters.Peers[k], pbG, EOp::Erase);
                RegisterPeerOpCounters(d.Counters.Peers[k], pbG, EOp::ReadPB);
            }
            for (ui32 k = 0; k < hostsPerDbg; ++k) {
                auto ddG = peersG->GetSubgroup("peer", Sprintf("DD%u", k));
                auto& pc = d.Counters.Peers[kHostsPerDbgMax + k];
                pc.Connected = ddG->GetCounter("Connected", false);
                pc.RequestsSent = ddG->GetCounter("RequestsSent", true);
                pc.RepliesOk = ddG->GetCounter("RepliesOk", true);
                pc.RepliesErr = ddG->GetCounter("RepliesErr", true);
                pc.ResponseTimeMs = ddG->GetHistogram(
                    "ResponseTimeMs", NMonitoring::ExplicitHistogram(latencyBounds));
                RegisterPeerOpCounters(pc, ddG, EOp::Flush);
                RegisterPeerOpCounters(pc, ddG, EOp::ReadDDisk);
            }
        }
    }

    if (!MonitoringScheduled) {
        MonitoringScheduled = true;
        Schedule(TDuration::MilliSeconds(NKikimr::MonitoringUpdateCycleMs),
            new NKikimr::TEvUpdateMonitoring);
    }
}

} // anonymous namespace

NActors::IActor* CreateNbsDbgLikeLoadTablet(
    const NActors::TActorId& tablet, TTabletStorageInfo* info)
{
    return new TNbsDbgLikeLoadTablet(tablet, info);
}

} // namespace NKikimr::NNbsDbgLike

template <>
void Out<NKikimr::ENbsLoadTabletStatus>(IOutputStream& o,
        TTypeTraits<NKikimr::ENbsLoadTabletStatus>::TFuncParam x)
{
    o << NKikimr::ENbsLoadTabletStatus_Name(x);
}
