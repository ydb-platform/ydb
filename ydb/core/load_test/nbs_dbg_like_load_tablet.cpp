#include "nbs_dbg_like_load_tablet.h"

#include "events.h"
#include "nbs_dbg_like_alloc_helper.h"
#include "nbs_dbg_like_load_defs.h"

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

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
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
#include <deque>
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
    mask.set(0, kPrimaryHostsPerDbg);
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
    NActors::TMonotonic WrittenAt;
    NActors::TMonotonic FlushedAt;

    TPeerBitset WriteRequested;
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

// `subsystem=request` (per-LSN end-to-end) counters.
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
    std::bitset<kHostsPerDbgMax> DDConnected;
    std::bitset<kHostsPerDbgMax> PBConnected;

    // TDDiskId -> k in [0..kHostsPerDbgMax).
    absl::flat_hash_map<NKikimrBlobStorage::NDDisk::TDDiskId, ui32, TDDiskIdHash, TDDiskIdEqual> PbIndexById;

    absl::flat_hash_map<ui64, TWriteInfo> Lsns;
    absl::flat_hash_set<ui64> ReadyToErase;

    // FIFO work queues so DoFlush/DoErase touch only actionable LSNs (O(batch))
    // instead of scanning all of Lsns / ReadyToErase on every completion.
    std::deque<ui64> PendingFlush;
    std::deque<ui64> PendingErase;

    // Per-vChunk slot tracking; slot = offsetInVChunk / IoSizeBytes.
    std::vector<absl::flat_hash_map<ui32, ui64>> InflightLsnAtSlot;
    std::vector<TDynBitMap> FlushedSlots;

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
// TEvSyncWithPersistentBuffer / TEvBatchErasePersistentBuffer keyed by a
// fresh u64 cookie so retries from one batch never disturb another.
struct TFlushBatch {
    ui32 DbgIndex = 0;
    ui8  Sink = 0;            // k in [0..kPrimaryHostsPerDbg)
    std::vector<ui64> Lsns;
    NActors::TMonotonic SentAt;
};

struct TEraseBatch {
    ui32 DbgIndex = 0;
    ui8  Sink = 0;            // k in [0..kHostsPerDbgMax)
    std::vector<ui64> Lsns;
    NActors::TMonotonic SentAt;
};

struct TReadInflight {
    ui32 DbgIndex = 0;
    ui32 PeerK = 0;           // PB index for ReadPB; DD index for ReadDDisk.
    bool IsPb = true;
    ui32 Size = 0;
    NActors::TMonotonic SentAt;

    // v2: load-actor origin so the worker can hand the TNbsReadResult back.
    // Zero TActorId means "internal read" (none today; reserved for future).
    NActors::TActorId OriginActor;
    ui64              OriginCookie = 0;
};

struct TDecodedAddress {
    ui32 DbgIndex = 0;
    ui32 VChunkIndex = 0;
    ui32 OffsetInVChunk = 0;
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
            hFunc(NDDisk::TEvDisconnectResult, HandlePeerDisconnect);

            HFunc(TEvLoad::TEvNbsWrite, HandleNbsWrite);
            HFunc(TEvLoad::TEvNbsRead, HandleNbsRead);
            HFunc(TEvLoad::TEvConfigureTablet, HandleConfigureTablet);
            HFunc(NDDisk::TEvWritePersistentBuffersResult, HandleWritePbsResult);
            HFunc(NDDisk::TEvSyncWithPersistentBufferResult, HandleSyncResult);
            HFunc(NDDisk::TEvErasePersistentBufferResult, HandleEraseResult);
            HFunc(NDDisk::TEvReadPersistentBufferResult, HandlePbReadResult);
            HFunc(NDDisk::TEvReadResult, HandleDDiskReadResult);
            HFunc(NKikimr::TEvUpdateMonitoring, HandleUpdateMonitoring);

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

    // ---- Peer connect (the worker's own 10 peers) -------------------------
    void KickOffPeerConnect();
    void ConnectPeer(ui32 k, bool isPb);
    void HandlePeerConnect(NDDisk::TEvConnectResult::TPtr& ev);
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
    void HandleSyncResult(NDDisk::TEvSyncWithPersistentBufferResult::TPtr& ev,
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
    void DoFlush(TPerDbgState& dbg);
    void DoErase(TPerDbgState& dbg);
    void BestEffortEraseAll(TPerDbgState& dbg);
    void AccountReadRequest(TPerDbgState& dbg, EOp op, ui32 size);
    bool SendPbRead(TPerDbgState& dbg, ui32 dbgIndex, ui64 lsn,
        const TWriteInfo& info, const TActorId& origin, ui64 originCookie);
    bool SendDDiskRead(TPerDbgState& dbg, ui32 dbgIndex, ui32 vChunkIndex,
        ui64 offset, ui32 size, std::bitset<kHostsPerDbgMax> flushMask,
        const TActorId& origin, ui64 originCookie);
    bool CompleteRead(ui64 cookie, bool ok, TActorId& origin, ui64& originCookie,
        ui32& size);
    void ReplyWriteErr(const TActorId& origin, ui64 cookie, ui32 status);
    void ReplyReadErr(const TActorId& origin, ui64 cookie, ui32 status);

    static ui32 LocateInPbIds(const TPerDbgState& dbg,
        const NKikimrBlobStorage::NDDisk::TDDiskId& id);
    static void ClearInflightSlotIfMatches(TPerDbgState& dbg,
        const TWriteInfo& info, ui64 lsn);
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
        bool Connected = false;
        bool ConnectInFlight = false;
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

            // Proxy: route the per-request work to the per-DBG worker actors.
            HFunc(TEvLoad::TEvNbsWrite, HandleNbsWrite);
            HFunc(TEvLoad::TEvNbsRead, HandleNbsRead);
            HFunc(TEvLoad::TEvConfigureTablet, HandleConfigureTablet);

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
    // from each step without doubling up. Spec §15.2.
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
        if (Counters) {
            Counters->ResetCounters();
        }
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
        // Note: keep Runs around as historical records.
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        // Poison the per-DBG workers (they own the peer connections, whose
        // credentials are derived from the BSC alloc state we just cleared)
        // and reset all tablet-side state.
        Self->PoisonDbgActors();
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

    // Input validation (spec §23.10 / Phase 2.6).
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
    NTabletPipe::SendData(ctx, BscPipeClient, request.release());
}

void TNbsDbgLikeLoadTablet::OnBscPipeBroken(const TActorContext& ctx) {
    BscPipeClient = TActorId();
    if (Phase != ETabletPhase::Allocating && Phase != ETabletPhase::Deleting) {
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
    } else if (Phase == ETabletPhase::Deleting) {
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
    // Roll back to READY so user can retry Delete.
    Phase = ETabletPhase::Ready;
    EmitPhaseGauge();
    BscRetryAttempts = 0;
}

void TNbsDbgLikeLoadTablet::Handle(
    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const TActorContext& ctx)
{
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
    if (Phase == ETabletPhase::Uninitialized) {
        return reply(NBSLT_OK);
    }
    Y_DEBUG_ABORT_UNLESS(Phase == ETabletPhase::Ready);

    PendingDeleteReplyTo = ev->Sender;
    PendingDeleteCookie  = ev->Cookie;
    Phase = ETabletPhase::Deleting;
    EmitPhaseGauge();
    BscRetryAttempts = 0;
    SendBscAllocate(ctx, /*dealloc=*/true);
}

void TNbsDbgLikeLoadTablet::Handle(TEvLoad::TEvNbsLoadTabletGetSummary::TPtr& ev,
    const TActorContext& ctx)
{
    auto r = std::make_unique<TEvLoad::TEvNbsLoadTabletGetSummaryResult>();
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
    if (Phase != ETabletPhase::Ready || IoSizeBytes == 0 || BytesPerDbg == 0) {
        Send(origin, new TEvLoad::TEvNbsWriteResult(/*status=*/1), 0, cookie);
        return;
    }
    const ui32 dbgIndex = static_cast<ui32>(msg.GetAddress() / BytesPerDbg);
    if (dbgIndex >= ActiveDbgs || dbgIndex >= DbgActors.size()
        || !DbgActors[dbgIndex])
    {
        Send(origin, new TEvLoad::TEvNbsWriteResult(/*status=*/2), 0, cookie);
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
    if (Phase != ETabletPhase::Ready || IoSizeBytes == 0 || BytesPerDbg == 0) {
        Send(origin, new TEvLoad::TEvNbsReadResult(/*status=*/1), 0, cookie);
        return;
    }
    const ui32 dbgIndex = static_cast<ui32>(msg.GetAddress() / BytesPerDbg);
    if (dbgIndex >= ActiveDbgs || dbgIndex >= DbgActors.size()
        || !DbgActors[dbgIndex])
    {
        Send(origin, new TEvLoad::TEvNbsReadResult(/*status=*/2), 0, cookie);
        return;
    }
    TActivationContext::Send(ev->Forward(DbgActors[dbgIndex]));
}

void TNbsDbgLikeLoadTablet::HandleConfigureTablet(
    TEvLoad::TEvConfigureTablet::TPtr& ev, const TActorContext& ctx)
{
    const auto& cfg = ev->Get()->Record;
    LOG_I("Proxy HandleConfigureTablet TabletId# " << TabletID()
        << " NumDirectBlockGroupsToUse# " << cfg.GetNumDirectBlockGroupsToUse()
        << " IoSizeBytes# " << cfg.GetIoSizeBytes()
        << " Dbgs# " << Dbgs.size());

    EnsureCounters(ctx);
    RecomputeRouting(cfg);

    // Forward the run config to every worker; each worker installs its own
    // knobs and initialises its per-DBG counters.
    for (const auto& actorId : DbgActors) {
        if (!actorId) {
            continue;
        }
        auto fwd = std::make_unique<TEvLoad::TEvConfigureTablet>();
        fwd->Record = cfg;
        ctx.Send(actorId, fwd.release());
    }

    // Tablet owns the aggregate (shared) root gauges that are constant across
    // workers; the workers only touch the additive root counters.
    if (Counters) {
        auto life = Counters->GetSubgroup("subsystem", "lifecycle_worker");
        life->GetCounter("DbgsAllocated", false)->Set(Dbgs.size());
        auto lsns = Counters->GetSubgroup("subsystem", "lsns");
        lsns->GetCounter("MaxLsns", false)->Set(cfg.GetMaxInflightLsns());
        lsns->GetCounter("SyncGateThreshold", false)->Set(
            Max<ui32>(1, cfg.GetSyncRequestsBatchSize()));
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
                TABLER() { TABLED() { out << "AllocTabletId"; } TABLED() { out << AllocConfig.GetTabletId(); } }
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

void TNbsDbgLikeActor::HandlePoison() {
    LOG_N("Worker poison DBG# " << MyDbgIndex);
    DisconnectAllPeers();
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
        ? NDDisk::TQueryCredentials::ToPersistentBuffer(AllocConfig.GetTabletId(), Generation(), std::nullopt)
        : NDDisk::TQueryCredentials::ToDDisk(
            AllocConfig.GetTabletId(), Generation(), kInitialDDiskSessionSeqNo, std::nullopt);
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
        st.Connected = true;
        st.Guid = rec.GetDDiskInstanceGuid();
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
    } else {
        st.Connected = false;
        st.Guid = 0;
        if (RootCnt.ConnectErr) {
            RootCnt.ConnectErr->Inc();
        }
        if (isPb) {
            Dbg.PBConnected.reset(k);
        } else {
            Dbg.DDConnected.reset(k);
        }
        ReportReadiness();
        LOG_D("Worker pre-connect failed DBG " << DbgInfo.DirectBlockGroupId
            << " " << (isPb ? "PB" : "DD") << k << ": "
            << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(rec.GetStatus())
            << " " << rec.GetErrorReason());
    }
}

void TNbsDbgLikeActor::HandlePeerDisconnect(NDDisk::TEvDisconnectResult::TPtr& ev) {
    ui32 k = 0;
    bool isPb = false;
    UnpackPeerCookie(ev->Cookie, k, isPb);
    if (k >= HostsPerDbg()) {
        return;
    }
    auto& st = isPb ? PB[k] : DD[k];
    st.Connected = false;
    st.ConnectInFlight = false;
    st.Guid = 0;
    if (RootCnt.DisconnectOk) {
        RootCnt.DisconnectOk->Inc();
    }
    if (isPb) {
        Dbg.PBConnected.reset(k);
    } else {
        Dbg.DDConnected.reset(k);
    }
    ReportReadiness();
}

void TNbsDbgLikeActor::DisconnectAllPeers() {
    const ui32 hostsPerDbg = HostsPerDbg();
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    for (ui32 k = 0; k < hostsPerDbg; ++k) {
        if (PB[k].Connected) {
            auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(
                bscTabletId, generation, PB[k].Guid);
            auto ev = std::make_unique<NDDisk::TEvDisconnect>();
            creds.Serialize(ev->Record.MutableCredentials());
            const auto& id = DbgInfo.PBIds[k];
            Send(MakeBlobStoragePersistentBufferId(id.GetNodeId(), id.GetPDiskId(),
                    id.GetDDiskSlotId()),
                ev.release());
            PB[k].Connected = false;
            PB[k].Guid = 0;
        }
        if (DD[k].Connected) {
            auto creds = NDDisk::TQueryCredentials::ToDDisk(
                bscTabletId, generation, kInitialDDiskSessionSeqNo, DD[k].Guid);
            auto ev = std::make_unique<NDDisk::TEvDisconnect>();
            creds.Serialize(ev->Record.MutableCredentials());
            const auto& id = DbgInfo.DDiskIds[k];
            Send(MakeBlobStorageDDiskId(id.GetNodeId(), id.GetPDiskId(),
                    id.GetDDiskSlotId()),
                ev.release());
            DD[k].Connected = false;
            DD[k].Guid = 0;
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
        dst.ReadyToErase.reserve(perDbg);
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
            dst.DDConnected.set(k);
        }
        if (PB[k].Connected) {
            dst.PBGuid[k] = PB[k].Guid;
            dst.PBConnected.set(k);
        }
    }
    if (AllocConfig.GetTargetNumVChunks() > 0) {
        dst.InflightLsnAtSlot.resize(AllocConfig.GetTargetNumVChunks());
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

void TNbsDbgLikeActor::ReplyWriteErr(const TActorId& origin, ui64 cookie, ui32 status) {
    if (origin) {
        Send(origin, new TEvLoad::TEvNbsWriteResult(status), 0, cookie);
    }
}

void TNbsDbgLikeActor::ReplyReadErr(const TActorId& origin, ui64 cookie, ui32 status) {
    if (origin) {
        Send(origin, new TEvLoad::TEvNbsReadResult(status), 0, cookie);
    }
}

ui32 TNbsDbgLikeActor::LocateInPbIds(const TPerDbgState& dbg, const NKikimrBlobStorage::NDDisk::TDDiskId& id)
{
    auto it = dbg.PbIndexById.find(id);
    return it == dbg.PbIndexById.end() ? kHostsPerDbgMax : it->second;
}

void TNbsDbgLikeActor::ClearInflightSlotIfMatches(TPerDbgState& dbg, const TWriteInfo& info, ui64 lsn)
{
    const ui32 v = info.VChunkIndex;
    if (v >= dbg.InflightLsnAtSlot.size() || info.Size == 0) {
        return;
    }
    const ui32 slot = static_cast<ui32>(info.OffsetInVChunk / info.Size);
    auto& slots = dbg.InflightLsnAtSlot[v];
    auto sit = slots.find(slot);
    if (sit != slots.end() && sit->second == lsn) {
        slots.erase(sit);
    }
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

    if (IoSizeBytes == 0) {
        ReplyWriteErr(origin, cookie, /*status=*/1);
        return;
    }

    if (!msg.HasPayloadId()) {
        ReplyWriteErr(origin, cookie, /*status=*/2);
        return;
    }

    const ui32 payloadId = msg.GetPayloadId();
    if (payloadId >= ev->Get()->GetPayloadCount()) {
        ReplyWriteErr(origin, cookie, /*status=*/2);
        return;
    }

    const auto decoded = DecodeAddress(msg.GetAddress(), msg.GetSizeBytes());
    if (!decoded || decoded->DbgIndex != MyDbgIndex) {
        LOG_D("HandleNbsWrite invalid Address# " << msg.GetAddress()
            << " SizeBytes# " << msg.GetSizeBytes()
            << " IoSizeBytes# " << IoSizeBytes
            << " ActiveDbgs# " << ActiveDbgs);
        ReplyWriteErr(origin, cookie, /*status=*/2);
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
            << "; dropping silently");
        // Mirror v1 worker StateConnect semantics: silently drop the request
        // while the DBG's peers are still mid-handshake. The load actor's
        // drain-timeout safety net reaps any never-replied cookies on Run
        // shutdown; replying ERROR here would force the load actor into a
        // tight retry loop that pins simulated time.
        return;
    }

    const ui64 maxInflight = TabletConfig.GetMaxInflightLsns();
    const ui64 denom = Max<ui32>(1, NumDbgsTotal);
    if (maxInflight == 0 || TotalLsns() >= Max<ui64>(1, maxInflight / denom)) {
        if (RootCnt.Lsns.BackpressureHits) {
            RootCnt.Lsns.BackpressureHits->Inc();
        }
        ReplyWriteErr(origin, cookie, /*status=*/7); // NOTREADY – LSN backpressure
        return;
    }

    auto& dbg = Dbg;
    const ui64 lsn = ++SequenceGenerator;
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
    dbg.InflightLsnAtSlot[vChunkIndex][offset / IoSizeBytes] = lsn;

    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(
        bscTabletId, generation, dbg.PBGuid[coord]);
    NDDisk::TBlockSelector selector(vChunkIndex, offset, IoSizeBytes);
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

    wireEv->AddPayload(TRope(ev->Get()->GetPayload(payloadId)));

    Send(dbg.PBActor[coord], wireEv.release(), 0, lsn);

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
    const TWriteInfo& info, const TActorId& origin, ui64 originCookie)
{
    if (!info.WriteConfirmed.any()) {
        return false;
    }
    const ui32 k = PickRandomSetBit(info.WriteConfirmed, Rng.GenRand());
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(
        bscTabletId, generation, dbg.PBGuid[k]);
    NDDisk::TBlockSelector selector(info.VChunkIndex,
        static_cast<ui32>(info.OffsetInVChunk), info.Size);
    auto ev = std::make_unique<NDDisk::TEvReadPersistentBuffer>(
        creds, selector, lsn, generation, NDDisk::TReadInstruction(true));
    const ui64 cookie = NextBatchCookie++;
    TReadInflight ri;
    ri.DbgIndex = dbgIndex;
    ri.PeerK = k;
    ri.IsPb = true;
    ri.Size = info.Size;
    ri.SentAt = MonotonicNow();
    ri.OriginActor = origin;
    ri.OriginCookie = originCookie;
    ReadInflight.emplace(cookie, std::move(ri));
    Send(dbg.PBActor[k], ev.release(), 0, cookie);

    ++dbg.ReadsInFlight;
    AccountReadRequest(dbg, EOp::ReadPB, info.Size);
    BumpPeerRequest(dbg, k, EOp::ReadPB);
    return true;
}

bool TNbsDbgLikeActor::SendDDiskRead(TPerDbgState& dbg, ui32 dbgIndex,
    ui32 vChunkIndex, ui64 offset, ui32 size,
    std::bitset<kHostsPerDbgMax> flushMask,
    const TActorId& origin, ui64 originCookie)
{
    const TPeerBitset& effectiveMask = flushMask.any() ? flushMask : kAllPrimaryHostsMask;
    const ui32 k = PickRandomSetBit(effectiveMask, Rng.GenRand());
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    auto creds = NDDisk::TQueryCredentials::ToDDisk(
        bscTabletId, generation, kInitialDDiskSessionSeqNo, dbg.DDGuid[k]);
    NDDisk::TBlockSelector selector(vChunkIndex, static_cast<ui32>(offset), size);
    auto ev = std::make_unique<NDDisk::TEvRead>(
        creds, selector, NDDisk::TReadInstruction(true));
    const ui64 cookie = NextBatchCookie++;
    TReadInflight ri;
    ri.DbgIndex = dbgIndex;
    ri.PeerK = k;
    ri.IsPb = false;
    ri.Size = size;
    ri.SentAt = MonotonicNow();
    ri.OriginActor = origin;
    ri.OriginCookie = originCookie;
    ReadInflight.emplace(cookie, std::move(ri));
    Send(dbg.DDiskActor[k], ev.release(), 0, cookie);

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

    if (IoSizeBytes == 0) {
        ReplyReadErr(origin, cookie, /*status=*/1);
        return;
    }

    if (TabletConfig.GetDisableReplication()) {
        ReplyReadErr(origin, cookie, /*status=*/6);
        return;
    }

    const auto decoded = DecodeAddress(msg.GetAddress(), msg.GetSizeBytes());
    if (!decoded || decoded->DbgIndex != MyDbgIndex) {
        LOG_D("HandleNbsRead invalid Address# " << msg.GetAddress()
            << " SizeBytes# " << msg.GetSizeBytes());
        ReplyReadErr(origin, cookie, /*status=*/2);
        return;
    }
    const ui32 dbgIndex = decoded->DbgIndex;
    const ui32 vChunkIndex = decoded->VChunkIndex;
    const ui32 offset = decoded->OffsetInVChunk;

    if (Dbg.DDConnected.count() < kPrimaryHostsPerDbg) {
        // See HandleNbsWrite: drop silently while peers are still mid-handshake
        // so the load actor doesn't tight-loop and pin simulated time.
        LOG_D("HandleNbsRead peers not ready DBG# " << dbgIndex
            << " Cookie# " << cookie << "; dropping silently");
        return;
    }

    auto& dbg = Dbg;
    const ui32 slot = offset / IoSizeBytes;

    auto& inflightSlots = dbg.InflightLsnAtSlot[vChunkIndex];
    auto it = inflightSlots.find(slot);
    if (it != inflightSlots.end()) {
        const ui64 lsn = it->second;
        auto lit = dbg.Lsns.find(lsn);
        if (lit == dbg.Lsns.end()) {
            inflightSlots.erase(it);
        } else {
            const auto& info = lit->second;
            if (ShouldReadFromPBuffer(info.State)) {
                if (SendPbRead(dbg, dbgIndex, lsn, info, origin, cookie)) {
                    return;
                }
            }
            if (SendDDiskRead(dbg, dbgIndex, vChunkIndex, info.OffsetInVChunk,
                              info.Size, info.FlushConfirmed, origin, cookie)) {
                return;
            }
            ReplyReadErr(origin, cookie, /*status=*/4);
            return;
        }
    }
    if (!SendDDiskRead(dbg, dbgIndex, vChunkIndex, offset, IoSizeBytes,
                       /*flushMask=*/{}, origin, cookie)) {
        ReplyReadErr(origin, cookie, /*status=*/5);
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
        return;
    }
    TWriteInfo& info = it->second;
    const NActors::TMonotonic now = ctx.Monotonic();

    const auto& msg = ev->Get()->Record;
    bool overallOk = true;
    std::bitset<kHostsPerDbgMax> respondedThisReply;
    for (ui32 i = 0; i < msg.ResultSize(); ++i) {
        const auto& sub = msg.GetResult(i);
        const auto& pbId = sub.GetPersistentBufferId();
        ui32 k = LocateInPbIds(dbg, pbId);
        const bool ok = (sub.GetResult().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        if (k < HostsPerDbg()) {
            respondedThisReply.set(k);
            if (ok) {
                info.WriteConfirmed.set(k);
            }
            if (sub.GetResult().HasFreeSpace()) {
                dbg.LastFreeSpace[k] = sub.GetResult().GetFreeSpace();
            }
            if (dbg.Counters.PerPeerEnabled && dbg.Counters.Peers[k].FreeSpacePct
                && sub.GetResult().HasFreeSpace()) {
                dbg.Counters.Peers[k].FreeSpacePct->Set(
                    static_cast<i64>(100.0 * (1.0 - sub.GetResult().GetFreeSpace())));
            }
            BumpPeerReply(dbg, k, EOp::Write, ok);
        }
        if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Write)]; c.SubReplyOk) {
            if (ok) {
                c.SubReplyOk->Inc();
            } else {
                c.SubReplyErr->Inc();
            }
        }
        if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Write)]; c.SubReplyOk) {
            if (ok) {
                c.SubReplyOk->Inc();
            } else {
                c.SubReplyErr->Inc();
            }
        }
        if (!ok) {
            overallOk = false;
        }
    }

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

    const ui32 writeQuorum = TabletConfig.GetDisableReplication() ? 1u : kPrimaryHostsPerDbg;
    if (info.WriteConfirmed.count() >= writeQuorum) {
        EnterState(dbg, EPBufferState::PBufferIncompleteWrite, /*delta=*/-1);
        info.WrittenAt = now;
        if (TabletConfig.GetDisableReplication()) {
            // Skip flush: jump directly to PBufferFlushed so DoErase can
            // reclaim PB space without sending TEvSyncWithPersistentBuffer.
            info.State = EPBufferState::PBufferFlushed;
            EnterState(dbg, EPBufferState::PBufferFlushed, /*delta=*/+1);
            if (dbg.ReadyToErase.insert(lsn).second) {
                dbg.PendingErase.push_back(lsn);
            }
        } else {
            info.State = EPBufferState::PBufferWritten;
            EnterState(dbg, EPBufferState::PBufferWritten, /*delta=*/+1);
            dbg.PendingFlush.push_back(lsn);
        }
        const ui64 quorumMs = (now - info.WriteStart).MilliSeconds();
        if (RootCnt.Request.WriteQuorumMs) {
            RootCnt.Request.WriteQuorumMs->Collect(quorumMs);
        }
        if (dbg.Counters.Request.WriteQuorumMs) {
            dbg.Counters.Request.WriteQuorumMs->Collect(quorumMs);
        }
        if (!info.ReplySent && info.OriginActor) {
            Send(info.OriginActor, new TEvLoad::TEvNbsWriteResult(/*status=*/0),
                0, info.OriginCookie);
            info.ReplySent = true;
        }
    } else {
        std::bitset<kHostsPerDbgMax> failedThisReply = respondedThisReply
            & ~info.WriteConfirmed;
        std::bitset<kHostsPerDbgMax> stillPending = info.WriteRequested
            & ~info.WriteConfirmed
            & ~failedThisReply;
        const ui32 needed = writeQuorum > static_cast<ui32>(info.WriteConfirmed.count())
            ? writeQuorum - static_cast<ui32>(info.WriteConfirmed.count()) : 0;
        if (needed > stillPending.count()) {
            EnterState(dbg, info.State, /*delta=*/-1);
            if (!info.ReplySent && info.OriginActor) {
                Send(info.OriginActor, new TEvLoad::TEvNbsWriteResult(/*status=*/1),
                    0, info.OriginCookie);
                info.ReplySent = true;
            }
            ClearInflightSlotIfMatches(dbg, info, lsn);
            dbg.Lsns.erase(it);
            --LsnsTotalAll;
            if (RootCnt.Request.Failed) {
                RootCnt.Request.Failed->Inc();
            }
            if (dbg.Counters.Request.Failed) {
                dbg.Counters.Request.Failed->Inc();
            }
            UpdateLsnsTotal(dbg);
            DoFlush(dbg);
            DoErase(dbg);
            return;
        }
        (void)overallOk;
    }

    UpdateAvgPbFreeSpacePct(dbg);
    DoFlush(dbg);
    DoErase(dbg);
}

void TNbsDbgLikeActor::DoFlush(TPerDbgState& dbg) {
    const ui32 syncGate = Max<ui32>(1, SyncGateThreshold() / Max(1u, NumDbgsTotal));
    if (dbg.StateCount[static_cast<ui32>(EPBufferState::PBufferWritten)] < syncGate) {
        if (RootCnt.Lsns.SyncGateFlushBlocked) {
            RootCnt.Lsns.SyncGateFlushBlocked->Inc();
        }
        if (dbg.Counters.Lsns.SyncGateFlushBlocked) {
            dbg.Counters.Lsns.SyncGateFlushBlocked->Inc();
        }
        return;
    }
    const ui32 batch = Max<ui32>(1, TabletConfig.GetFlushBatchSize());
    std::array<std::vector<std::tuple<ui64, NDDisk::TBlockSelector>>, kPrimaryHostsPerDbg> pending;
    std::array<ui32, kPrimaryHostsPerDbg> count = {};
    ui32 saturated = 0;

    // Consume only actionable LSNs from the FIFO work queue instead of scanning
    // all of dbg.Lsns. An LSN stays at the front until every confirmed host has
    // been flush-requested, then it transitions to PBufferFlushing and is popped.
    while (!dbg.PendingFlush.empty() && saturated < kPrimaryHostsPerDbg) {
        const ui64 lsn = dbg.PendingFlush.front();
        auto it = dbg.Lsns.find(lsn);
        if (it == dbg.Lsns.end()
            || it->second.State != EPBufferState::PBufferWritten) {
            dbg.PendingFlush.pop_front();
            continue;
        }
        TWriteInfo& info = it->second;
        bool fullyScheduled = true;
        for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
            if (!info.WriteConfirmed.test(k)) {
                continue;
            }
            if (info.FlushRequested.test(k)) {
                continue;
            }
            if (count[k] >= batch) {
                fullyScheduled = false;
                continue;
            }
            info.FlushDesired.set(k);
            info.FlushRequested.set(k);
            if (++count[k] == batch) {
                ++saturated;
            }
            pending[k].push_back({lsn, NDDisk::TBlockSelector(
                info.VChunkIndex,
                static_cast<ui32>(info.OffsetInVChunk), info.Size)});
        }
        if (!fullyScheduled) {
            // A host batch filled mid-LSN; resume this LSN on the next call.
            break;
        }
        EnterState(dbg, EPBufferState::PBufferWritten, /*delta=*/-1);
        info.State = EPBufferState::PBufferFlushing;
        EnterState(dbg, EPBufferState::PBufferFlushing, /*delta=*/+1);
        dbg.PendingFlush.pop_front();
    }

    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
        if (pending[k].empty()) {
            continue;
        }
        auto creds = NDDisk::TQueryCredentials::ToDDisk(
            bscTabletId, generation, kInitialDDiskSessionSeqNo, dbg.DDGuid[k]);
        std::tuple<ui32, ui32, ui32> srcId{
            dbg.PBIdsPb[k].GetNodeId(),
            dbg.PBIdsPb[k].GetPDiskId(),
            dbg.PBIdsPb[k].GetDDiskSlotId()};
        auto ev = std::make_unique<NDDisk::TEvSyncWithPersistentBuffer>(
            creds, srcId, dbg.PBGuid[k]);
        TFlushBatch batchInfo;
        batchInfo.DbgIndex = dbg.DbgIndex;
        batchInfo.Sink = static_cast<ui8>(k);
        batchInfo.Lsns.reserve(pending[k].size());
        batchInfo.SentAt = MonotonicNow();
        for (auto& [lsn, sel] : pending[k]) {
            ev->AddSegment(sel, lsn, generation);
            batchInfo.Lsns.push_back(lsn);
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
}

void TNbsDbgLikeActor::HandleSyncResult(
    NDDisk::TEvSyncWithPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ev->Cookie;
    auto bIt = FlushInflight.find(cookie);
    if (bIt == FlushInflight.end()) {
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

    std::vector<std::pair<ui64, bool>> perLsn;
    perLsn.reserve(batchInfo.Lsns.size());
    for (ui32 i = 0; i < batchInfo.Lsns.size(); ++i) {
        bool ok;
        if (i < msg.SegmentResultsSize()) {
            ok = msg.GetSegmentResults(i).GetStatus() ==
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
        } else {
            ok = outerOk;
        }
        perLsn.emplace_back(batchInfo.Lsns[i], ok);
    }

    for (auto& [lsn, ok] : perLsn) {
        auto it = dbg.Lsns.find(lsn);
        if (it == dbg.Lsns.end()) {
            continue;
        }
        TWriteInfo& info = it->second;
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
                EnterState(dbg, EPBufferState::PBufferFlushing, /*delta=*/-1);
                info.State = EPBufferState::PBufferFlushed;
                info.FlushedAt = now;
                EnterState(dbg, EPBufferState::PBufferFlushed, /*delta=*/+1);
                if (info.VChunkIndex < dbg.FlushedSlots.size() && info.Size != 0) {
                    dbg.FlushedSlots[info.VChunkIndex].Set(info.OffsetInVChunk / info.Size);
                }
                if (dbg.ReadyToErase.insert(lsn).second) {
                    dbg.PendingErase.push_back(lsn);
                }
                const ui64 flushMs = ((info.WrittenAt != NActors::TMonotonic::Zero())
                    ? (now - info.WrittenAt).MilliSeconds()
                    : (now - info.WriteStart).MilliSeconds());
                if (RootCnt.Request.FlushMs) {
                    RootCnt.Request.FlushMs->Collect(flushMs);
                }
                if (dbg.Counters.Request.FlushMs) {
                    dbg.Counters.Request.FlushMs->Collect(flushMs);
                }
            }
        } else {
            info.FlushRequested.reset(k);
            // Reopen this host's flush and requeue so DoFlush retries it. The
            // LSN was moved to PBufferFlushing and popped from PendingFlush when
            // fully requested; move it back to PBufferWritten so DoFlush (which
            // only processes PBufferWritten) picks it up again. The guard makes
            // repeated per-host failures idempotent.
            if (info.State == EPBufferState::PBufferFlushing) {
                EnterState(dbg, EPBufferState::PBufferFlushing, /*delta=*/-1);
                info.State = EPBufferState::PBufferWritten;
                EnterState(dbg, EPBufferState::PBufferWritten, /*delta=*/+1);
            }
            dbg.PendingFlush.push_back(lsn);
            if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Flush)]; c.Retries) {
                c.Retries->Inc();
            }
            if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Flush)]; c.Retries) {
                c.Retries->Inc();
            }
        }
    }

    const ui64 latencyMs = (now - sentAt).MilliSeconds();
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

    DoFlush(dbg);
    DoErase(dbg);
}

void TNbsDbgLikeActor::DoErase(TPerDbgState& dbg) {
    const ui32 syncGate = Max<ui32>(1, SyncGateThreshold() / Max(1u, NumDbgsTotal));
    if (dbg.ReadyToErase.size() < syncGate) {
        if (RootCnt.Lsns.SyncGateEraseBlocked) {
            RootCnt.Lsns.SyncGateEraseBlocked->Inc();
        }
        if (dbg.Counters.Lsns.SyncGateEraseBlocked) {
            dbg.Counters.Lsns.SyncGateEraseBlocked->Inc();
        }
        return;
    }
    const ui32 batch = Max<ui32>(1, TabletConfig.GetEraseBatchSize());
    std::array<std::vector<ui64>, kHostsPerDbgMax> pending;
    std::array<ui32, kHostsPerDbgMax> count = {};

    const ui32 hostsPerDbg = HostsPerDbg();
    ui32 saturated = 0;

    // Consume only actionable LSNs from the FIFO work queue instead of scanning
    // all of ReadyToErase. An LSN stays at the front until every erase-target
    // host has been erase-requested, then it transitions to PBufferErasing and
    // is popped. It is dropped from ReadyToErase later, once erases confirm.
    while (!dbg.PendingErase.empty() && saturated < hostsPerDbg) {
        const ui64 lsn = dbg.PendingErase.front();
        auto it = dbg.Lsns.find(lsn);
        if (it == dbg.Lsns.end()) {
            dbg.PendingErase.pop_front();
            continue;
        }
        TWriteInfo& info = it->second;
        if (!info.EraseTarget.any()) {
            info.EraseTarget |= info.WriteConfirmed;
        }
        bool fullyScheduled = true;
        for (ui32 k = 0; k < hostsPerDbg; ++k) {
            if (!info.EraseTarget.test(k)) {
                continue;
            }
            if (info.EraseRequested.test(k)) {
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
            // A host batch filled mid-LSN; resume this LSN on the next call.
            break;
        }
        if (info.State == EPBufferState::PBufferFlushed
            && info.EraseTarget.any()
            && (info.EraseRequested & info.EraseTarget) == info.EraseTarget)
        {
            EnterState(dbg, EPBufferState::PBufferFlushed, /*delta=*/-1);
            info.State = EPBufferState::PBufferErasing;
            EnterState(dbg, EPBufferState::PBufferErasing, /*delta=*/+1);
        }
        dbg.PendingErase.pop_front();
    }

    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    for (ui32 k = 0; k < hostsPerDbg; ++k) {
        if (pending[k].empty()) {
            continue;
        }
        auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(
            bscTabletId, generation, dbg.PBGuid[k]);
        auto ev = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(creds);
        TEraseBatch batchInfo;
        batchInfo.DbgIndex = dbg.DbgIndex;
        batchInfo.Sink = static_cast<ui8>(k);
        batchInfo.Lsns.reserve(pending[k].size());
        batchInfo.SentAt = MonotonicNow();
        for (ui64 lsn : pending[k]) {
            ev->AddErase(lsn, generation);
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
}

void TNbsDbgLikeActor::HandleEraseResult(
    NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ev->Cookie;
    auto bIt = EraseInflight.find(cookie);
    if (bIt == EraseInflight.end()) {
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
            // Reopen erase work for this host; re-queue so DoErase retries it
            // (the LSN was popped from PendingErase once fully requested).
            dbg.PendingErase.push_back(lsn);
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
            EnterState(dbg, info.State, /*delta=*/-1);
            const NActors::TMonotonic ws = info.WriteStart;
            const NActors::TMonotonic fa = info.FlushedAt;
            const ui64 fullMs = (now - ws).MilliSeconds();
            const ui64 eraseMs = (fa != NActors::TMonotonic::Zero()
                ? (now - fa).MilliSeconds() : 0);
            ClearInflightSlotIfMatches(dbg, info, lsn);
            dbg.Lsns.erase(it);
            --LsnsTotalAll;
            dbg.ReadyToErase.erase(lsn);
            if (RootCnt.Request.Completed) {
                RootCnt.Request.Completed->Inc();
            }
            if (dbg.Counters.Request.Completed) {
                dbg.Counters.Request.Completed->Inc();
            }
            if (RootCnt.Request.LatencyMs) {
                RootCnt.Request.LatencyMs->Collect(fullMs);
            }
            if (dbg.Counters.Request.LatencyMs) {
                dbg.Counters.Request.LatencyMs->Collect(fullMs);
            }
            if (eraseMs && RootCnt.Request.EraseMs) {
                RootCnt.Request.EraseMs->Collect(eraseMs);
            }
            if (eraseMs && dbg.Counters.Request.EraseMs) {
                dbg.Counters.Request.EraseMs->Collect(eraseMs);
            }
        }
    }

    const ui64 latencyMs = (now - batchInfo.SentAt).MilliSeconds();
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
    UpdateLsnsTotal(dbg);

    DoFlush(dbg);
    DoErase(dbg);
}

void TNbsDbgLikeActor::BestEffortEraseAll(TPerDbgState& dbg) {
    std::array<std::vector<ui64>, kHostsPerDbgMax> pending;
    const ui32 hostsPerDbg = HostsPerDbg();
    for (auto& [lsn, info] : dbg.Lsns) {
        if (!info.EraseTarget.any()) {
            info.EraseTarget |= info.WriteConfirmed;
        }
        for (ui32 k = 0; k < hostsPerDbg; ++k) {
            if (!info.EraseTarget.test(k)) {
                continue;
            }
            if (info.EraseRequested.test(k)) {
                continue;
            }
            info.EraseRequested.set(k);
            pending[k].push_back(lsn);
        }
    }
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    for (ui32 k = 0; k < hostsPerDbg; ++k) {
        if (pending[k].empty()) {
            continue;
        }
        auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(
            bscTabletId, generation, dbg.PBGuid[k]);
        auto ev = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(creds);
        TEraseBatch batchInfo;
        batchInfo.DbgIndex = dbg.DbgIndex;
        batchInfo.Sink = static_cast<ui8>(k);
        batchInfo.Lsns = pending[k];
        batchInfo.SentAt = MonotonicNow();
        for (ui64 lsn : pending[k]) ev->AddErase(lsn, generation);
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
        }
        if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Erase)]; c.Requests) {
            c.Requests->Inc(); c.Pending->Inc();
        }
        BumpPeerRequest(dbg, k, EOp::Erase);
    }
}

bool TNbsDbgLikeActor::CompleteRead(ui64 cookie, bool ok, TActorId& origin,
    ui64& originCookie, ui32& size)
{
    auto rIt = ReadInflight.find(cookie);
    if (rIt == ReadInflight.end()) {
        return false;
    }
    TReadInflight read = rIt->second;
    ReadInflight.erase(rIt);
    origin = read.OriginActor;
    originCookie = read.OriginCookie;
    size = read.Size;

    auto& dbg = Dbg;
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
    return true;
}

void TNbsDbgLikeActor::HandlePbReadResult(
    NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
    const TActorContext& /*ctx*/)
{
    const ui64 cookie = ev->Cookie;
    const auto& msg = ev->Get()->Record;
    const bool ok = msg.GetStatus() ==
        NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
    TActorId origin;
    ui64 originCookie = 0;
    ui32 size = 0;
    if (CompleteRead(cookie, ok, origin, originCookie, size) && origin) {
        auto resp = std::make_unique<TEvLoad::TEvNbsReadResult>(ok ? 0u : 1u);
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
    const auto& msg = ev->Get()->Record;
    const bool ok = msg.GetStatus() ==
        NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
    TActorId origin;
    ui64 originCookie = 0;
    ui32 size = 0;
    if (CompleteRead(cookie, ok, origin, originCookie, size) && origin) {
        auto resp = std::make_unique<TEvLoad::TEvNbsReadResult>(ok ? 0u : 1u);
        if (ok && ev->Get()->GetPayloadCount() > 0) {
            const ui32 payloadId = resp->AddPayload(TRope(ev->Get()->GetPayload(0)));
            resp->Record.SetPayloadId(payloadId);
        }
        Send(origin, resp.release(), 0, originCookie);
    }
}
void TNbsDbgLikeActor::HandleConfigureTablet(
    TEvLoad::TEvConfigureTablet::TPtr& ev, const TActorContext& /*ctx*/)
{
    const auto& cfg = ev->Get()->Record;
    LOG_I("Worker HandleConfigureTablet DBG# " << MyDbgIndex
        << " MaxInflightLsns# " << cfg.GetMaxInflightLsns()
        << " FlushBatchSize# " << cfg.GetFlushBatchSize()
        << " EraseBatchSize# " << cfg.GetEraseBatchSize()
        << " SyncRequestsBatchSize# " << cfg.GetSyncRequestsBatchSize()
        << " NumDirectBlockGroupsToUse# " << cfg.GetNumDirectBlockGroupsToUse()
        << " IoSizeBytes# " << cfg.GetIoSizeBytes());

    InitWorkerCounters();

    // Drain anything left over from a previous run before installing new knobs.
    // No Phase guard is needed (unlike the old single-tablet code): workers are
    // spawned only while the tablet is Ready and poisoned when it leaves Ready.
    BestEffortEraseAll(Dbg);

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
        if (Dbg.InflightLsnAtSlot.size() != AllocConfig.GetTargetNumVChunks()) {
            Dbg.InflightLsnAtSlot.resize(AllocConfig.GetTargetNumVChunks());
        }
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
