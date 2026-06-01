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

// Cookie packs (lsn, dbgIndex). 16 bits for dbgIndex is plenty (NumDirectBlockGroups
// is bounded by partition-side 32 in production; we leave headroom).
constexpr ui32 kDbgIndexBits = 16;
constexpr ui32 kDbgIndexMask = (1u << kDbgIndexBits) - 1;
inline ui64 PackCookie(ui64 lsn, ui32 dbgIndex) { return (lsn << kDbgIndexBits) | (dbgIndex & kDbgIndexMask); }
inline ui32 DbgIndexOfCookie(ui64 c) { return static_cast<ui32>(c & kDbgIndexMask); }
inline ui64 LsnOfCookie(ui64 c) { return c >> kDbgIndexBits; }

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
    ::NMonitoring::TDynamicCounters::TCounterPtr OldestLsn;
    ::NMonitoring::TDynamicCounters::TCounterPtr OldestLsnAgeMs;
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
        OldestLsn        = g->GetCounter("OldestLsn", false);
        OldestLsnAgeMs   = g->GetCounter("OldestLsnAgeMs", false);
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
    THashMap<NKikimrBlobStorage::NDDisk::TDDiskId, ui32, TDDiskIdHash, TDDiskIdEqual> PbIndexById;

    std::map<ui64, TWriteInfo> Lsns;
    std::set<ui64> ReadyToErase;

    // Per-vChunk slot tracking; slot = offsetInVChunk / IoSizeBytes.
    std::vector<THashMap<ui32, ui64>> InflightLsnAtSlot;
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
        DisconnectAllPeers();
        if (BscPipeClient) {
            NTabletPipe::CloseClient(SelfId(), BscPipeClient);
            BscPipeClient = TActorId();
        }
        if (RootCnt.Root) {
            RootCnt.Root->ResetCounters();
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
            KickOffPeerConnect(ctx);
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
    void HandlePeerConnect(NDDisk::TEvConnectResult::TPtr& ev);
    void HandlePeerDisconnect(NDDisk::TEvDisconnectResult::TPtr& ev);
    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev);

    ui32 HostsPerDbg() const {
        const ui32 hosts = GetHostsPerDbg(AllocConfig);
        return Max(hosts, kPrimaryHostsPerDbg);
    }

    // Returns the generation stamped at boot. Stays valid after the executor
    // has been detached (HandlePoison / HandleTabletDead null Executor() before
    // calling OnDetach/OnTabletDead/PassAway), which is required so peer
    // disconnects continue to fence by the correct generation.
    ui32 Generation() const {
        if (Generation_) {
            return Generation_;
        }
        return Executor() ? Executor()->Generation() : 0;
    }

    // Phase 3.2: pre-establish 10*N peer connections at boot.
    void KickOffPeerConnect(const TActorContext& ctx);
    void ConnectPeer(const TActorContext& ctx, ui32 dbgIdx, ui32 k, bool isPb);
    static ui64 PackPeerCookie(ui32 dbgIdx, ui32 k, bool isPb) {
        return (static_cast<ui64>(dbgIdx) << 4) | (static_cast<ui64>(k) << 1)
            | (isPb ? 1u : 0u);
    }
    static void UnpackPeerCookie(ui64 cookie, ui32& dbgIdx, ui32& k, bool& isPb) {
        dbgIdx = static_cast<ui32>(cookie >> 4);
        k = static_cast<ui32>((cookie >> 1) & 0x7);
        isPb = (cookie & 1u) != 0;
    }

    void EnsureCounters(const TActorContext& ctx);
    void EmitPhaseGauge();

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

    void InitWorkerCounters(const TActorContext& ctx);
    void PopulateDbgState(ui32 dbgIdx);
    void DisconnectAllPeers();
    std::expected<TDecodedAddress, EDecodeAddressError> DecodeAddress(
        ui64 address, ui32 sizeBytes) const;
    ui32 ChooseCoordinator(const TPerDbgState& dbg) const;
    ui64 TotalLsns() const;
    ui32 SyncGateThreshold() const {
        return Max<ui32>(1, TabletConfig.GetSyncRequestsBatchSize());
    }
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

    static NActors::TMonotonic MonotonicNow() {
        return NActors::TActivationContext::Monotonic();
    }
    static ui32 LocateInPbIds(const TPerDbgState& dbg,
        const NKikimrBlobStorage::NDDisk::TDDiskId& id);
    static void ClearInflightSlotIfMatches(TPerDbgState& dbg,
        const TWriteInfo& info, ui64 lsn);
    static void RegisterPeerOpCounters(TPeerCounters& pc,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& peerGroup, EOp op);
    static void BumpPeerRequest(TPerDbgState& dbg, ui32 peerIndex, EOp op);
    static void BumpPeerReply(TPerDbgState& dbg, ui32 peerIndex, EOp op, bool ok);

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

    // Cached at OnActivateExecutor so generation-fenced messages (peer
    // disconnects in particular) still carry the right value on the poison /
    // TabletDead paths, where TTabletExecutedFlat nulls Executor() before our
    // OnDetach/OnTabletDead/PassAway run. See Generation() below.
    ui32 Generation_ = 0;

    bool CleanedUp_ = false;

    TString AllocConfigSerialized;  // empty when uninitialized
    TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig AllocConfig;
    std::vector<TDirectBlockGroup> Dbgs;

    // Phase 3.2: per-peer connection state, parallel to Dbgs[].
    struct TPeerConnState {
        ui64 Guid = 0;
        bool Connected = false;
        bool ConnectInFlight = false;
    };

    struct TDbgConnState {
        std::array<TPeerConnState, kHostsPerDbgMax> DD;
        std::array<TPeerConnState, kHostsPerDbgMax> PB;
        bool AllConnected(ui32 hostsPerDbg) const {
            for (size_t k = 0; k < hostsPerDbg; ++k) {
                if (!DD[k].Connected || !PB[k].Connected) {
                    return false;
                }
            }
            return true;
        }
    };
    std::vector<TDbgConnState> Conn;

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

    NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet TabletConfig;
    std::vector<TPerDbgState> DbgStates;   // parallel to Dbgs[]; populated lazily
    ui64 LsnsTotalAll = 0;                 // cached sum of DbgStates[i].Lsns.size()
    ui32 ActiveDbgs = 0;                   // M_eff installed by TConfigureTablet
    ui32 IoSizeBytes = 0;
    ui64 BytesPerDbg = 0;
    bool WorkerCountersInited = false;
    bool MonitoringScheduled = false;

    ui64 SequenceGenerator = 0;
    ui32 WriteInFlight = 0;
    ui32 ReadInFlight = 0;

    ui64 NextBatchCookie = 1;
    std::map<ui64, TFlushBatch> FlushInflight;
    std::map<ui64, TEraseBatch> EraseInflight;
    std::map<ui64, TReadInflight> ReadInflight;

    TFastRng64 Rng{0};
    TRootCounters RootCnt;

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

        // If we lost any rows or the config is malformed, force UNINITIALIZED;
        // operator must re-Create.
        if (malformedConfig || !DroppedDbgIndices.empty()) {
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
        // Phase 3.2: pre-establish connections for the freshly-allocated DBGs.
        Self->KickOffPeerConnect(ctx);
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
        // Disconnect peers and reset all state — the BSC alloc
        // state we're about to clear is the source of truth for those
        // connection guids.
        Self->DisconnectAllPeers();
        Self->DbgStates.clear();
        Self->ActiveDbgs = 0;
        Self->IoSizeBytes = 0;
        Self->BytesPerDbg = 0;
        Self->TabletConfig.Clear();
        Self->FlushInflight.clear();
        Self->EraseInflight.clear();
        Self->ReadInflight.clear();
        Self->WriteInFlight = 0;
        Self->ReadInFlight = 0;
        Self->SequenceGenerator = 0;
        Self->NextBatchCookie = 1;
        Self->Dbgs.clear();
        Self->Conn.clear();
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
    for (ui32 i = 0; i < static_cast<ui32>(DbgStates.size()); ++i) {
        if (DbgStates[i].PBConnected.count() < kPrimaryHostsPerDbg) {
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

// ---- Peer pre-connect (spec §23.4 / Phase 3.2) ---------------------------

void TNbsDbgLikeLoadTablet::KickOffPeerConnect(const TActorContext& ctx) {
    if (Phase != ETabletPhase::Ready) {
        return;
    }
    const ui32 hostsPerDbg = HostsPerDbg();
    LOG_D("Kick off peer pre-connect TabletId# " << TabletID()
        << " Generation# " << Generation()
        << " Phase# " << Phase
        << " Dbgs# " << Dbgs.size()
        << " Peers# " << (Dbgs.size() * 2 * hostsPerDbg));
    Conn.resize(Dbgs.size());
    for (ui32 i = 0; i < Dbgs.size(); ++i) {
        for (ui32 k = 0; k < hostsPerDbg; ++k) {
            ConnectPeer(ctx, i, k, /*isPb=*/true);
            ConnectPeer(ctx, i, k, /*isPb=*/false);
        }
    }
}

void TNbsDbgLikeLoadTablet::ConnectPeer(const TActorContext& ctx,
    ui32 dbgIdx, ui32 k, bool isPb)
{
    if (dbgIdx >= Dbgs.size() || k >= HostsPerDbg()) {
        return;
    }
    if (Conn.size() < Dbgs.size()) {
        Conn.resize(Dbgs.size());
    }
    auto& st = isPb ? Conn[dbgIdx].PB[k] : Conn[dbgIdx].DD[k];
    if (st.Connected || st.ConnectInFlight) {
        return;
    }
    const auto& id = isPb ? Dbgs[dbgIdx].PBIds[k] : Dbgs[dbgIdx].DDiskIds[k];
    TActorId target = isPb
        ? MakeBlobStoragePersistentBufferId(id.GetNodeId(), id.GetPDiskId(), id.GetDDiskSlotId())
        : MakeBlobStorageDDiskId(id.GetNodeId(), id.GetPDiskId(), id.GetDDiskSlotId());
    NDDisk::TQueryCredentials creds(AllocConfig.GetTabletId(), Generation(),
        std::nullopt);
    st.ConnectInFlight = true;
    ctx.Send(target, new NDDisk::TEvConnect(creds), 0,
        PackPeerCookie(dbgIdx, k, isPb));
}

void TNbsDbgLikeLoadTablet::HandlePeerConnect(NDDisk::TEvConnectResult::TPtr& ev) {
    ui32 dbgIdx = 0, k = 0;
    bool isPb = false;
    UnpackPeerCookie(ev->Cookie, dbgIdx, k, isPb);
    if (dbgIdx >= Conn.size() || k >= HostsPerDbg()) {
        return;
    }
    auto& st = isPb ? Conn[dbgIdx].PB[k] : Conn[dbgIdx].DD[k];
    st.ConnectInFlight = false;
    const auto& rec = ev->Get()->Record;
    LOG_D("HandlePeerConnect dbg# " << dbgIdx
        << " " << (isPb ? "PB" : "DD") << k
        << " Status# " << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(rec.GetStatus()));
    if (rec.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        st.Connected = true;
        st.Guid = rec.GetDDiskInstanceGuid();
        if (RootCnt.ConnectOk) {
            RootCnt.ConnectOk->Inc();
        }
        // Spec §23.6.1: as soon as all 10 peers of a DBG are connected,
        // populate per-DBG state (DD/PB actors, GUIDs,
        // PbIndexById, slot trackers). Subsequent re-connects refresh the
        // GUIDs in place.
        if (Conn[dbgIdx].AllConnected(HostsPerDbg())) {
            LOG_D("AllConnected for dbg# " << dbgIdx << " — populating DbgState");
            PopulateDbgState(dbgIdx);
        }
    } else {
        st.Connected = false;
        st.Guid = 0;
        if (RootCnt.ConnectErr) {
            RootCnt.ConnectErr->Inc();
        }
        if (dbgIdx < DbgStates.size()) {
            if (isPb) {
                DbgStates[dbgIdx].PBConnected.reset(k);
            } else {
                DbgStates[dbgIdx].DDConnected.reset(k);
            }
        }
        LOG_D("Pre-connect failed for DBG " << Dbgs[dbgIdx].DirectBlockGroupId
            << " " << (isPb ? "PB" : "DD") << k << ": "
            << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(rec.GetStatus())
            << " " << rec.GetErrorReason());
    }
}

void TNbsDbgLikeLoadTablet::HandlePeerDisconnect(NDDisk::TEvDisconnectResult::TPtr& ev) {
    ui32 dbgIdx = 0, k = 0;
    bool isPb = false;
    UnpackPeerCookie(ev->Cookie, dbgIdx, k, isPb);
    if (dbgIdx >= Conn.size() || k >= HostsPerDbg()) {
        return;
    }
    auto& st = isPb ? Conn[dbgIdx].PB[k] : Conn[dbgIdx].DD[k];
    st.Connected = false;
    st.ConnectInFlight = false;
    st.Guid = 0;
    if (RootCnt.DisconnectOk) {
        RootCnt.DisconnectOk->Inc();
    }
    if (dbgIdx < DbgStates.size()) {
        if (isPb) {
            DbgStates[dbgIdx].PBConnected.reset(k);
        } else {
            DbgStates[dbgIdx].DDConnected.reset(k);
        }
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

void TNbsDbgLikeLoadTablet::DisconnectAllPeers() {
    if (Conn.empty()) {
        return;
    }
    const ui32 hostsPerDbg = HostsPerDbg();
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    for (ui32 i = 0; i < Conn.size() && i < Dbgs.size(); ++i) {
        for (ui32 k = 0; k < hostsPerDbg; ++k) {
            if (Conn[i].PB[k].Connected) {
                NDDisk::TQueryCredentials creds(bscTabletId, generation, Conn[i].PB[k].Guid);
                auto ev = std::make_unique<NDDisk::TEvDisconnect>();
                creds.Serialize(ev->Record.MutableCredentials());
                const auto& id = Dbgs[i].PBIds[k];
                Send(MakeBlobStoragePersistentBufferId(id.GetNodeId(), id.GetPDiskId(),
                        id.GetDDiskSlotId()),
                    ev.release());
                Conn[i].PB[k].Connected = false;
                Conn[i].PB[k].Guid = 0;
            }
            if (Conn[i].DD[k].Connected) {
                NDDisk::TQueryCredentials creds(bscTabletId, generation, Conn[i].DD[k].Guid);
                auto ev = std::make_unique<NDDisk::TEvDisconnect>();
                creds.Serialize(ev->Record.MutableCredentials());
                const auto& id = Dbgs[i].DDiskIds[k];
                Send(MakeBlobStorageDDiskId(id.GetNodeId(), id.GetPDiskId(),
                        id.GetDDiskSlotId()),
                    ev.release());
                Conn[i].DD[k].Connected = false;
                Conn[i].DD[k].Guid = 0;
            }
        }
    }
}

void TNbsDbgLikeLoadTablet::PopulateDbgState(ui32 dbgIdx) {
    if (dbgIdx >= Dbgs.size()) {
        return;
    }
    if (DbgStates.size() < Dbgs.size()) {
        DbgStates.resize(Dbgs.size());
    }
    auto& dst = DbgStates[dbgIdx];
    LOG_D("PopulateDbgState dbg# " << dbgIdx
        << " PBConnected_before# " << dst.PBConnected.count()
        << " DDConnected_before# " << dst.DDConnected.count());
    const auto& src = Dbgs[dbgIdx];
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
        if (dbgIdx < Conn.size()) {
            if (Conn[dbgIdx].DD[k].Connected) {
                dst.DDGuid[k] = Conn[dbgIdx].DD[k].Guid;
                dst.DDConnected.set(k);
            }
            if (Conn[dbgIdx].PB[k].Connected) {
                dst.PBGuid[k] = Conn[dbgIdx].PB[k].Guid;
                dst.PBConnected.set(k);
            }
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

std::expected<TDecodedAddress, EDecodeAddressError> TNbsDbgLikeLoadTablet::DecodeAddress(
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

ui32 TNbsDbgLikeLoadTablet::ChooseCoordinator(const TPerDbgState& dbg) const {
    auto it = std::ranges::min_element(dbg.InFlightTo);
    return static_cast<ui32>(it - dbg.InFlightTo.begin());
}

ui64 TNbsDbgLikeLoadTablet::TotalLsns() const {
    return LsnsTotalAll;
}

void TNbsDbgLikeLoadTablet::ReplyWriteErr(const TActorId& origin, ui64 cookie, ui32 status) {
    if (origin) {
        Send(origin, new TEvLoad::TEvNbsWriteResult(status), 0, cookie);
    }
}

void TNbsDbgLikeLoadTablet::ReplyReadErr(const TActorId& origin, ui64 cookie, ui32 status) {
    if (origin) {
        Send(origin, new TEvLoad::TEvNbsReadResult(status), 0, cookie);
    }
}

ui32 TNbsDbgLikeLoadTablet::LocateInPbIds(const TPerDbgState& dbg, const NKikimrBlobStorage::NDDisk::TDDiskId& id)
{
    auto it = dbg.PbIndexById.find(id);
    return it == dbg.PbIndexById.end() ? kHostsPerDbgMax : it->second;
}

void TNbsDbgLikeLoadTablet::ClearInflightSlotIfMatches(TPerDbgState& dbg, const TWriteInfo& info, ui64 lsn)
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

void TNbsDbgLikeLoadTablet::RegisterPeerOpCounters(TPeerCounters& pc,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& peerGroup, EOp op)
{
    auto opGroup = peerGroup->GetSubgroup("op", ToString(op));
    const size_t opIndex = static_cast<size_t>(op);
    pc.RequestsSentByOp[opIndex] = opGroup->GetCounter("RequestsSent", true);
    pc.RepliesOkByOp[opIndex] = opGroup->GetCounter("RepliesOk", true);
    pc.RepliesErrByOp[opIndex] = opGroup->GetCounter("RepliesErr", true);
}

void TNbsDbgLikeLoadTablet::BumpPeerRequest(TPerDbgState& dbg, ui32 peerIndex, EOp op) {
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

void TNbsDbgLikeLoadTablet::BumpPeerReply(TPerDbgState& dbg, ui32 peerIndex, EOp op, bool ok) {
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

void TNbsDbgLikeLoadTablet::AccountReadRequest(TPerDbgState& dbg, EOp op, ui32 size) {
    if (auto& c = RootCnt.Op[static_cast<size_t>(op)]; c.Requests) {
        c.Requests->Inc();
        c.Pending->Inc();
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

void TNbsDbgLikeLoadTablet::EnterState(TPerDbgState& dbg, EPBufferState s, int delta) {
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
    if (RootCnt.Lsns.BufferStateGauges[idx]) {
        ui64 sum = 0;
        for (auto& d : DbgStates) sum += d.StateCount[idx];
        RootCnt.Lsns.BufferStateGauges[idx]->Set(sum);
    }
}

void TNbsDbgLikeLoadTablet::UpdateLsnsTotal(TPerDbgState& dbg) {
    if (dbg.Counters.Lsns.Total) {
        dbg.Counters.Lsns.Total->Set(dbg.Lsns.size());
    }
    if (RootCnt.Lsns.Total) {
        RootCnt.Lsns.Total->Set(LsnsTotalAll);
    }
}

void TNbsDbgLikeLoadTablet::UpdateAvgPbFreeSpacePct(TPerDbgState& dbg) {
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
    if (RootCnt.Lsns.AvgPbUsedPct) {
        ui64 sum = 0;
        ui32 mn = 100;
        ui32 mx = 0;
        for (auto& d : DbgStates) {
            sum += d.AvgPbUsedPct;
            mn = Min(mn, d.AvgPbUsedPct);
            mx = Max(mx, d.AvgPbUsedPct);
        }
        const ui32 avgUsed = DbgStates.empty() ? 0u : static_cast<ui32>(sum / DbgStates.size());
        RootCnt.Lsns.AvgPbUsedPct->Set(avgUsed);
        if (RootCnt.Lsns.AvgPbFreeSpacePct) {
            RootCnt.Lsns.AvgPbFreeSpacePct->Set(avgUsed <= 100 ? 100u - avgUsed : 0u);
        }
        if (RootCnt.Lsns.AvgPbFreeSpacePctMin) {
            RootCnt.Lsns.AvgPbFreeSpacePctMin->Set(mn);
        }
        if (RootCnt.Lsns.AvgPbFreeSpacePctMax) {
            RootCnt.Lsns.AvgPbFreeSpacePctMax->Set(mx);
        }
    }
}

void TNbsDbgLikeLoadTablet::HandleNbsWrite(TEvLoad::TEvNbsWrite::TPtr& ev, const TActorContext& /*ctx*/)
{
    const auto& msg = ev->Get()->Record;
    const TActorId origin = ev->Sender;
    const ui64 cookie = ev->Cookie;
    LOG_T("HandleNbsWrite Cookie# " << cookie << " Addr# " << msg.GetAddress() << " Size# " << msg.GetSizeBytes());

    if (Phase != ETabletPhase::Ready) {
        ReplyWriteErr(origin, cookie, /*status=*/1);
        return;
    }

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
    if (!decoded) {
        LOG_D("HandleNbsWrite invalid Address# " << msg.GetAddress()
            << " SizeBytes# " << msg.GetSizeBytes()
            << " Error# " << decoded.error()
            << " IoSizeBytes# " << IoSizeBytes
            << " ActiveDbgs# " << ActiveDbgs);
        ReplyWriteErr(origin, cookie, /*status=*/2);
        return;
    }
    const ui32 dbgIndex = decoded->DbgIndex;
    const ui32 vChunkIndex = decoded->VChunkIndex;
    const ui32 offset = decoded->OffsetInVChunk;

    const ui32 pbConnectedCount = dbgIndex < DbgStates.size()
        ? static_cast<ui32>(DbgStates[dbgIndex].PBConnected.count()) : 0u;
    if (dbgIndex >= DbgStates.size()
        || pbConnectedCount < kPrimaryHostsPerDbg)
    {
        LOG_D("HandleNbsWrite peers not ready DBG# " << dbgIndex
            << " Cookie# " << cookie
            << " DbgStatesSize# " << DbgStates.size()
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

    if (TotalLsns() >= TabletConfig.GetMaxInflightLsns()) {
        if (RootCnt.Lsns.BackpressureHits) {
            RootCnt.Lsns.BackpressureHits->Inc();
        }
        ReplyWriteErr(origin, cookie, /*status=*/7); // NOTREADY – LSN backpressure
        return;
    }

    auto& dbg = DbgStates[dbgIndex];
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
    NDDisk::TQueryCredentials creds(bscTabletId, generation, dbg.PBGuid[coord]);
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

    Send(dbg.PBActor[coord], wireEv.release(), 0, PackCookie(lsn, dbgIndex));

    ++WriteInFlight;
    ++dbg.WritesInFlight;
    ++dbg.InFlightTo[coord];

    if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Write)]; c.Requests) {
        c.Requests->Inc();
        c.Pending->Inc();
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

    if (RootCnt.Lsns.NewestLsn) {
        RootCnt.Lsns.NewestLsn->Set(lsn);
    }

    if (dbg.Counters.Lsns.NewestLsn) {
        dbg.Counters.Lsns.NewestLsn->Set(lsn);
    }

    UpdateLsnsTotal(dbg);
    BumpPeerRequest(dbg, coord, EOp::Write);
}

bool TNbsDbgLikeLoadTablet::SendPbRead(TPerDbgState& dbg, ui32 dbgIndex, ui64 lsn,
    const TWriteInfo& info, const TActorId& origin, ui64 originCookie)
{
    if (!info.WriteConfirmed.any()) {
        return false;
    }
    const ui32 k = PickRandomSetBit(info.WriteConfirmed, Rng.GenRand());
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    NDDisk::TQueryCredentials creds(bscTabletId, generation, dbg.PBGuid[k],
        /*fromPersistentBuffer=*/true);
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

    ++ReadInFlight;
    ++dbg.ReadsInFlight;
    AccountReadRequest(dbg, EOp::ReadPB, info.Size);
    BumpPeerRequest(dbg, k, EOp::ReadPB);
    return true;
}

bool TNbsDbgLikeLoadTablet::SendDDiskRead(TPerDbgState& dbg, ui32 dbgIndex,
    ui32 vChunkIndex, ui64 offset, ui32 size,
    std::bitset<kHostsPerDbgMax> flushMask,
    const TActorId& origin, ui64 originCookie)
{
    const TPeerBitset& effectiveMask = flushMask.any() ? flushMask : kAllPrimaryHostsMask;
    const ui32 k = PickRandomSetBit(effectiveMask, Rng.GenRand());
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    NDDisk::TQueryCredentials creds(bscTabletId, generation, dbg.DDGuid[k]);
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

    ++ReadInFlight;
    ++dbg.ReadsInFlight;
    AccountReadRequest(dbg, EOp::ReadDDisk, size);
    BumpPeerRequest(dbg, kHostsPerDbgMax + k, EOp::ReadDDisk);
    return true;
}

void TNbsDbgLikeLoadTablet::HandleNbsRead(TEvLoad::TEvNbsRead::TPtr& ev,
    const TActorContext& /*ctx*/)
{
    const auto& msg = ev->Get()->Record;
    const TActorId origin = ev->Sender;
    const ui64 cookie = ev->Cookie;
    LOG_T("HandleNbsRead Cookie# " << cookie << " Addr# " << msg.GetAddress() << " Size# " << msg.GetSizeBytes());

    if (Phase != ETabletPhase::Ready) {
        ReplyReadErr(origin, cookie, /*status=*/1);
        return;
    }

    if (IoSizeBytes == 0) {
        ReplyReadErr(origin, cookie, /*status=*/1);
        return;
    }

    if (TabletConfig.GetDisableReplication()) {
        ReplyReadErr(origin, cookie, /*status=*/6);
        return;
    }

    const auto decoded = DecodeAddress(msg.GetAddress(), msg.GetSizeBytes());
    if (!decoded) {
        LOG_D("HandleNbsRead invalid Address# " << msg.GetAddress()
            << " SizeBytes# " << msg.GetSizeBytes()
            << " Error# " << decoded.error());
        ReplyReadErr(origin, cookie, /*status=*/2);
        return;
    }
    const ui32 dbgIndex = decoded->DbgIndex;
    const ui32 vChunkIndex = decoded->VChunkIndex;
    const ui32 offset = decoded->OffsetInVChunk;

    if (dbgIndex >= DbgStates.size()
        || DbgStates[dbgIndex].DDConnected.count() < kPrimaryHostsPerDbg)
    {
        // See HandleNbsWrite: drop silently while peers are still mid-handshake
        // so the load actor doesn't tight-loop and pin simulated time.
        LOG_D("HandleNbsRead peers not ready DBG# " << dbgIndex
            << " Cookie# " << cookie << "; dropping silently");
        return;
    }

    auto& dbg = DbgStates[dbgIndex];
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

void TNbsDbgLikeLoadTablet::HandleWritePbsResult(
    NDDisk::TEvWritePersistentBuffersResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ev->Cookie;
    const ui32 dbgIndex = DbgIndexOfCookie(cookie);
    const ui64 lsn = LsnOfCookie(cookie);
    if (dbgIndex >= DbgStates.size()) {
        return;
    }
    auto& dbg = DbgStates[dbgIndex];
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

    --WriteInFlight;
    --dbg.WritesInFlight;
    const ui8 coord = info.CoordinatorIndex;
    if (coord < kPrimaryHostsPerDbg && dbg.InFlightTo[coord] > 0) {
        --dbg.InFlightTo[coord];
    }

    if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Write)]; c.Pending) {
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
            dbg.ReadyToErase.insert(lsn);
        } else {
            info.State = EPBufferState::PBufferWritten;
            EnterState(dbg, EPBufferState::PBufferWritten, /*delta=*/+1);
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

void TNbsDbgLikeLoadTablet::DoFlush(TPerDbgState& dbg) {
    const ui32 syncGate = SyncGateThreshold();
    if (Phase == ETabletPhase::Ready
        && dbg.StateCount[static_cast<ui32>(EPBufferState::PBufferWritten)] < syncGate) {
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

    for (auto& [lsn, info] : dbg.Lsns) {
        if (info.State != EPBufferState::PBufferWritten) {
            continue;
        }
        for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
            if (!info.WriteConfirmed.test(k)) {
                continue;
            }
            if (info.FlushRequested.test(k)) {
                continue;
            }
            if (count[k] >= batch) {
                continue;
            }
            info.FlushDesired.set(k);
            info.FlushRequested.set(k);
            ++count[k];
            pending[k].push_back({lsn, NDDisk::TBlockSelector(
                info.VChunkIndex,
                static_cast<ui32>(info.OffsetInVChunk), info.Size)});
        }
    }

    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    for (ui32 k = 0; k < kPrimaryHostsPerDbg; ++k) {
        if (pending[k].empty()) {
            continue;
        }
        NDDisk::TQueryCredentials creds(bscTabletId, generation, dbg.DDGuid[k]);
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
            auto& info = dbg.Lsns[lsn];
            if (info.State == EPBufferState::PBufferWritten) {
                EnterState(dbg, EPBufferState::PBufferWritten, /*delta=*/-1);
                info.State = EPBufferState::PBufferFlushing;
                EnterState(dbg, EPBufferState::PBufferFlushing, /*delta=*/+1);
            }
        }
        const ui64 cookie = NextBatchCookie++;
        FlushInflight.emplace(cookie, std::move(batchInfo));
        Send(dbg.DDiskActor[k], ev.release(), 0, cookie);

        if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Flush)]; c.Requests) {
            c.Requests->Inc();
            c.Pending->Inc();
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

void TNbsDbgLikeLoadTablet::HandleSyncResult(
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
    const ui32 dbgIndex = batchInfo.DbgIndex;
    const ui32 k = batchInfo.Sink;
    if (dbgIndex >= DbgStates.size() || k >= kPrimaryHostsPerDbg) {
        return;
    }
    auto& dbg = DbgStates[dbgIndex];
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
                dbg.ReadyToErase.insert(lsn);
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
            if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Flush)]; c.Retries) {
                c.Retries->Inc();
            }
            if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Flush)]; c.Retries) {
                c.Retries->Inc();
            }
        }
    }

    const ui64 latencyMs = (now - sentAt).MilliSeconds();
    if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Flush)]; c.Pending) {
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

void TNbsDbgLikeLoadTablet::DoErase(TPerDbgState& dbg) {
    const ui32 syncGate = SyncGateThreshold();
    if (Phase == ETabletPhase::Ready && dbg.ReadyToErase.size() < syncGate) {
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
    for (ui64 lsn : dbg.ReadyToErase) {
        auto it = dbg.Lsns.find(lsn);
        if (it == dbg.Lsns.end()) {
            continue;
        }
        TWriteInfo& info = it->second;
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
            if (count[k] >= batch) {
                continue;
            }
            info.EraseRequested.set(k);
            ++count[k];
            pending[k].push_back(lsn);
        }
        if (info.State == EPBufferState::PBufferFlushed
            && info.EraseTarget.any()
            && (info.EraseRequested & info.EraseTarget) == info.EraseTarget)
        {
            EnterState(dbg, EPBufferState::PBufferFlushed, /*delta=*/-1);
            info.State = EPBufferState::PBufferErasing;
            EnterState(dbg, EPBufferState::PBufferErasing, /*delta=*/+1);
        }
    }

    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 generation = Generation();
    for (ui32 k = 0; k < hostsPerDbg; ++k) {
        if (pending[k].empty()) {
            continue;
        }
        NDDisk::TQueryCredentials creds(bscTabletId, generation, dbg.PBGuid[k]);
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

        if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Erase)]; c.Requests) {
            c.Requests->Inc();
            c.Pending->Inc();
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

void TNbsDbgLikeLoadTablet::HandleEraseResult(
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
    const ui32 dbgIndex = batchInfo.DbgIndex;
    const ui32 k = batchInfo.Sink;
    if (dbgIndex >= DbgStates.size() || k >= HostsPerDbg()) {
        return;
    }
    auto& dbg = DbgStates[dbgIndex];
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
    if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Erase)]; c.Pending) {
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

void TNbsDbgLikeLoadTablet::BestEffortEraseAll(TPerDbgState& dbg) {
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
        NDDisk::TQueryCredentials creds(bscTabletId, generation, dbg.PBGuid[k]);
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
        if (auto& c = RootCnt.Op[static_cast<size_t>(EOp::Erase)]; c.Requests) {
            c.Requests->Inc(); c.Pending->Inc();
        }
        if (auto& c = dbg.Counters.Op[static_cast<size_t>(EOp::Erase)]; c.Requests) {
            c.Requests->Inc(); c.Pending->Inc();
        }
        BumpPeerRequest(dbg, k, EOp::Erase);
    }
}

bool TNbsDbgLikeLoadTablet::CompleteRead(ui64 cookie, bool ok, TActorId& origin,
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

    --ReadInFlight;
    if (read.DbgIndex >= DbgStates.size()) {
        return true;
    }
    auto& dbg = DbgStates[read.DbgIndex];
    if (dbg.ReadsInFlight > 0) {
        --dbg.ReadsInFlight;
    }

    const NActors::TMonotonic now = MonotonicNow();
    const EOp op = read.IsPb ? EOp::ReadPB : EOp::ReadDDisk;
    const ui64 latencyMs = (read.SentAt != NActors::TMonotonic::Zero())
        ? (now - read.SentAt).MilliSeconds() : 0;

    if (auto& c = RootCnt.Op[static_cast<size_t>(op)]; c.Pending) {
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

void TNbsDbgLikeLoadTablet::HandlePbReadResult(
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

void TNbsDbgLikeLoadTablet::HandleDDiskReadResult(
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
void TNbsDbgLikeLoadTablet::HandleConfigureTablet(
    TEvLoad::TEvConfigureTablet::TPtr& ev, const TActorContext& ctx)
{
    const auto& cfg = ev->Get()->Record;
    LOG_I("HandleConfigureTablet TabletId# " << TabletID()
        << " MaxInflightLsns# " << cfg.GetMaxInflightLsns()
        << " FlushBatchSize# " << cfg.GetFlushBatchSize()
        << " EraseBatchSize# " << cfg.GetEraseBatchSize()
        << " SyncRequestsBatchSize# " << cfg.GetSyncRequestsBatchSize()
        << " NumDirectBlockGroupsToUse# " << cfg.GetNumDirectBlockGroupsToUse()
        << " IoSizeBytes# " << cfg.GetIoSizeBytes());

    InitWorkerCounters(ctx);

    if (Phase == ETabletPhase::Ready) {
        for (auto& d : DbgStates) {
            BestEffortEraseAll(d);
        }
    }

    TabletConfig = cfg;

    const ui32 want = cfg.GetNumDirectBlockGroupsToUse();
    if (want == 0) {
        ActiveDbgs = static_cast<ui32>(Dbgs.size());
    } else if (want <= static_cast<ui32>(Dbgs.size())) {
        ActiveDbgs = want;
    } else {
        ActiveDbgs = static_cast<ui32>(Dbgs.size());
        LOG_E("HandleConfigureTablet TabletId# " << TabletID()
            << " NumDirectBlockGroupsToUse=" << want
            << " > Dbgs.size()=" << Dbgs.size()
            << "; clamping to all DBGs");
    }

    const ui32 wantIo = cfg.GetIoSizeBytes();
    const ui64 vChunkSizeBytes = AllocConfig.GetVChunkSizeBytes();
    if (wantIo != 0 && wantIo != IoSizeBytes) {
        if (wantIo > vChunkSizeBytes) {
            LOG_E("HandleConfigureTablet IoSizeBytes=" << wantIo
                << " > VChunkSizeBytes=" << vChunkSizeBytes
                << "; keeping previous IoSizeBytes=" << IoSizeBytes);
        } else if (vChunkSizeBytes % wantIo != 0) {
            LOG_E("HandleConfigureTablet IoSizeBytes=" << wantIo
                << " does not divide VChunkSizeBytes=" << vChunkSizeBytes
                << "; keeping previous IoSizeBytes=" << IoSizeBytes);
        } else {
            IoSizeBytes = wantIo;
            BytesPerDbg = static_cast<ui64>(AllocConfig.GetTargetNumVChunks())
                * vChunkSizeBytes;
            const ui32 slotsPerVChunk = vChunkSizeBytes / IoSizeBytes;
            for (auto& dbg : DbgStates) {
                if (dbg.InflightLsnAtSlot.size() != AllocConfig.GetTargetNumVChunks()) {
                    dbg.InflightLsnAtSlot.resize(AllocConfig.GetTargetNumVChunks());
                }
                if (dbg.FlushedSlots.size() != AllocConfig.GetTargetNumVChunks()) {
                    dbg.FlushedSlots.resize(AllocConfig.GetTargetNumVChunks());
                }
                for (auto& slots : dbg.FlushedSlots) {
                    slots.Clear();
                    slots.Reserve(slotsPerVChunk);
                }
            }
        }
    }

    if (RootCnt.Lsns.MaxLsns) {
        RootCnt.Lsns.MaxLsns->Set(TabletConfig.GetMaxInflightLsns());
    }
    if (RootCnt.Lsns.SyncGateThreshold) {
        RootCnt.Lsns.SyncGateThreshold->Set(SyncGateThreshold());
    }
    for (auto& d : DbgStates) {
        if (d.Counters.Lsns.SyncGateThreshold) {
            d.Counters.Lsns.SyncGateThreshold->Set(SyncGateThreshold());
        }
    }
}

void TNbsDbgLikeLoadTablet::HandleUpdateMonitoring(
    NKikimr::TEvUpdateMonitoring::TPtr&, const TActorContext& ctx)
{
    Schedule(TDuration::MilliSeconds(NKikimr::MonitoringUpdateCycleMs),
        new NKikimr::TEvUpdateMonitoring);
    const NActors::TMonotonic now = ctx.Monotonic();
    ui64 globalOldest = 0;
    ui64 globalOldestAge = 0;
    for (auto& d : DbgStates) {
        if (d.Lsns.empty()) {
            if (d.Counters.Lsns.OldestLsn) {
                d.Counters.Lsns.OldestLsn->Set(0);
            }
            if (d.Counters.Lsns.OldestLsnAgeMs) {
                d.Counters.Lsns.OldestLsnAgeMs->Set(0);
            }
            continue;
        }
        const auto& [lsn, info] = *d.Lsns.begin();
        const ui64 age = (now - info.WriteStart).MilliSeconds();
        if (d.Counters.Lsns.OldestLsn) {
            d.Counters.Lsns.OldestLsn->Set(lsn);
        }
        if (d.Counters.Lsns.OldestLsnAgeMs) {
            d.Counters.Lsns.OldestLsnAgeMs->Set(age);
        }
        if (globalOldest == 0 || lsn < globalOldest) {
            globalOldest = lsn;
        }
        if (age > globalOldestAge) {
            globalOldestAge = age;
        }
    }
    if (RootCnt.Lsns.OldestLsn) {
        RootCnt.Lsns.OldestLsn->Set(globalOldest);
    }
    if (RootCnt.Lsns.OldestLsnAgeMs) {
        RootCnt.Lsns.OldestLsnAgeMs->Set(globalOldestAge);
    }
}

void TNbsDbgLikeLoadTablet::InitWorkerCounters(const TActorContext& ctx) {
    EnsureCounters(ctx);
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

    auto life = RootCnt.Root->GetSubgroup("subsystem", "lifecycle_worker");
    RootCnt.ConnectOk = life->GetCounter("ConnectOk", true);
    RootCnt.ConnectErr = life->GetCounter("ConnectErr", true);
    RootCnt.DisconnectOk = life->GetCounter("DisconnectOk", true);
    RootCnt.DbgsAllocated = life->GetCounter("DbgsAllocated", false);
    RootCnt.DbgsAllocated->Set(Dbgs.size());

    auto lsnsRoot = RootCnt.Root->GetSubgroup("subsystem", "lsns");
    RootCnt.Lsns.Init(lsnsRoot, /*isRoot=*/true);
    if (RootCnt.Lsns.MaxLsns) {
        RootCnt.Lsns.MaxLsns->Set(TabletConfig.GetMaxInflightLsns());
    }
    if (RootCnt.Lsns.SyncGateThreshold) {
        RootCnt.Lsns.SyncGateThreshold->Set(SyncGateThreshold());
    }

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

    const bool perPeer = Dbgs.size() < kMaxPerPeerCounters;
    if (DbgStates.size() < Dbgs.size()) {
        DbgStates.resize(Dbgs.size());
    }
    const ui64 bscTabletId = AllocConfig.GetTabletId();
    const ui32 hostsPerDbg = HostsPerDbg();
    for (size_t i = 0; i < Dbgs.size(); ++i) {
        auto& d = DbgStates[i];
        d.DbgIndex = Dbgs[i].DbgIndex;
        d.DirectBlockGroupId = Dbgs[i].DirectBlockGroupId;
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
