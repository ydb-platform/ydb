#include "ddisk_actor.h"
#include "ddisk_mon.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/subsystems/inmemory_metrics.h>
#include <ydb/library/pdisk_io/device_type.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <algorithm>
#include <map>
#include <tuple>
#include <type_traits>

namespace NKikimr::NDDisk {
namespace {

ui64 CounterVal(const NMonitoring::TDynamicCounters::TCounterPtr& counter) {
    return counter ? Max<i64>(0, counter->Val()) : 0;
}

// Keep only the first page while scanning unordered actor-owned containers.
template <typename TKey, typename TValue>
void AddRow(std::map<TKey, TValue>* rows, const TKey& key, TValue value, bool* more,
        ui32 limit = TDDiskMonQuery::MaxRows) {
    rows->emplace(key, std::move(value));
    if (rows->size() > limit) {
        rows->erase(std::prev(rows->end()));
        *more = true;
    }
}

template <typename TKey, typename TValue>
std::vector<TValue> TakeRows(std::map<TKey, TValue> rows) {
    std::vector<TValue> result;
    result.reserve(rows.size());
    for (auto& [key, value] : rows) {
        Y_UNUSED(key);
        result.push_back(std::move(value));
    }
    return result;
}

bool ParseQuery(const TCgiParameters& params, TDDiskMonQuery* query) {
    if (params.Has("tab")) {
        const auto& tab = params.Get("tab");
        if (tab != "overview" && tab != "tablets" && tab != "space"
                && tab != "operations" && tab != "diagnostics" && tab != "analytics") {
            return false;
        }
        query->Tab = tab;
    }
    if (params.Has("sort")) {
        query->StatsSort = params.Get("sort");
        if (query->StatsSort != "throughput" && query->StatsSort != "iops" && query->StatsSort != "chunks") {
            return false;
        }
    }
    if (query->Tab == "analytics") {
        query->Tab = "tablets"; // Keep existing bookmarks working.
    }
    if (params.Has("other")) {
        query->StatsOther = params.Get("other");
        if (query->StatsOther == "io") {
            query->StatsOther = query->StatsSort == "iops" ? "iops" : "throughput";
        }
        if (query->StatsOther != "iops" && query->StatsOther != "throughput" && query->StatsOther != "space") {
            return false;
        }
    }
    auto parse = [&](const char* name, auto* result) {
        if (!params.Has(name)) {
            return true;
        }
        typename std::decay_t<decltype(*result)>::value_type value;
        if (!TryFromString(params.Get(name), value)) {
            return false;
        }
        *result = value;
        return true;
    };
    if (params.Has("searchTabletId") && !params.Get("searchTabletId").empty()
            && !parse("searchTabletId", &query->SearchTabletId)) {
        return false;
    }
    std::optional<ui32> dbgIndex;
    std::optional<ui32> refreshRate;
    if (!parse("highlightTabletId", &query->StatsSelectedTabletId)
            || !parse("tabletId", &query->TabletId)
            || !parse("dbgIndex", &dbgIndex)
            || !parse("refreshRate", &refreshRate)
            || !parse("afterTabletId", &query->AfterTabletId)
            || !parse("afterVChunk", &query->AfterVChunk)
            || !parse("vChunk", &query->VChunk)) {
        return false;
    }
    if ((dbgIndex && *dbgIndex > 255) || (query->VChunk && (!query->TabletId || dbgIndex))) {
        return false;
    }
    query->DirectBlockGroupIndex = dbgIndex;
    query->RefreshRate = refreshRate.value_or(0);
    if (query->TabletId) {
        query->Tab = "tablets";
    }
    return true;
}

// Owns only a snapshot, never pointers into either storage actor. A missing PB
// produces a partial page; monitoring must also work while the slot is stopping.
class TDDiskMonRequestActor : public TActorBootstrapped<TDDiskMonRequestActor> {
    const TActorId Recipient;
    const ui64 Cookie;
    const int SubRequestId;
    const TActorId PersistentBuffer;
    const TActorId StatsActor;
    bool WaitingStats = false;
    bool WaitingOperations = false;
    TDDiskMonInfo Info;
    TDDiskMonQuery Query;
    std::optional<TPersistentBufferMonInfo> BufferInfo;
    TString BufferError;
    std::array<bool, 2> WaitingMemory = {};

    std::array<TDDiskMonMemory*, 5> Histories() {
        return {&Info.Memory,
            &Info.SpaceHistory[0], &Info.SpaceHistory[1], &Info.SpaceHistory[2], &Info.SpaceHistory[3]};
    }

    bool WaitingHistory() const {
        return std::any_of(WaitingMemory.begin(), WaitingMemory.end(), [](bool value) { return value; });
    }
    bool WaitingBuffer = true;
    bool SnapshotRequested = false;

    void Reply(const TPersistentBufferMonInfo* pb, TStringBuf error) {
        if (Query.Tab == "tablets" && !Query.TabletId && StatsActor && !SnapshotRequested) {
            SnapshotRequested = true;
            WaitingStats = true;
            Schedule(TDuration::Seconds(3), new TEvents::TEvWakeup);
            auto request = std::make_unique<TEvGetTabletStatsSnapshot>();
            request->Query.SearchTabletId = Query.SearchTabletId;
            request->Query.StatsSelectedTabletId = Query.StatsSelectedTabletId;
            request->Query.AfterTabletId = Query.AfterTabletId;
            request->Query.StatsOther = Query.StatsOther;
            Send(StatsActor, request.release(), IEventHandle::FlagTrackDelivery, 10);
            return;
        }
        for (auto& history : Info.SpaceHistory) {
            history.Error = Info.SpaceHistory[0].Error;
        }
        Send(Recipient, new NMon::TEvHttpInfoRes(RenderDDiskMonPage(Info, pb, Query, error),
            SubRequestId), 0, Cookie);
        PassAway();
    }

    void RequestData() {
        WaitingBuffer = false;
        if ((Query.Tab == "overview" || Query.TabletId) && StatsActor) {
            WaitingStats = true;
            auto request = std::make_unique<TEvGetTabletStats>();
            request->Limit = 10;
            request->TabletId = Query.TabletId;
            if (!Query.TabletId) {
                request->RankBy = Query.StatsSort;
            }
            Send(StatsActor, request.release(), IEventHandle::FlagTrackDelivery, 10);
            return;
        }
        if (Query.Tab == "operations") {
            auto* registry = GetInMemoryMetrics();
            if (!registry || !Info.OperationLineId) {
                Info.OperationHistoryError = "Operation history is unavailable";
            } else {
                WaitingOperations = registry->RequestLineSnapshot(SelfId(), Info.OperationLineId, 4);
                if (!WaitingOperations) {
                    Info.OperationHistoryError = "History request was not accepted";
                }
            }
            if (!WaitingOperations) {
                Reply(nullptr, {});
            }
            return;
        }
        if (Query.Tab != "space") {
            Reply(BufferInfo ? &*BufferInfo : nullptr, BufferError);
            return;
        }
        auto* registry = GetInMemoryMetrics();
        const auto memories = Histories();
        for (size_t i = 0; i < WaitingMemory.size(); ++i) {
            auto* memory = memories[i];
            if (!memory) {
                continue;
            }
            if (!registry) {
                memory->Error = "In-memory metrics are unavailable";
            } else if (memory->LineId) {
                WaitingMemory[i] = registry->RequestLineSnapshot(SelfId(), memory->LineId, i + 2);
                if (!WaitingMemory[i]) {
                    memory->Error = "History request was not accepted";
                }
            }
        }
        if (!WaitingHistory()) {
            Reply(BufferInfo ? &*BufferInfo : nullptr, BufferError);
        }
    }

    void Handle(TEvTabletStatsSnapshot::TPtr ev) {
        if (!WaitingStats || ev->Sender != StatsActor || ev->Cookie != 10) {
            return;
        }
        WaitingStats = false;
        static_cast<TTabletStatsSnapshot&>(Info) = std::move(ev->Get()->Info);
        Info.StatsColorSlots = Info.ParticipantSlots;
        Reply(BufferInfo ? &*BufferInfo : nullptr, BufferError);
    }

    void Handle(TEvTabletStats::TPtr ev) {
        if (!WaitingStats || ev->Sender != StatsActor || ev->Cookie != 10) {
            return;
        }
        Info.StatsAvailable = ev->Get()->Available;
        Info.StatsChunks = ev->Get()->TotalChunks;
        Info.StatsIops = ev->Get()->TotalIops;
        Info.StatsBytesPerSecond = ev->Get()->TotalBytesPerSecond;
        for (const auto& row : ev->Get()->Tablets) {
            TDDiskMonTabletStats value;
            value.TabletId = row.TabletId;
            value.Chunks = row.DataMappedChunks;
            value.SampledAt = row.SampledAt;
            value.Interval = row.Interval;
            for (size_t i = 0; i < value.Rates.size(); ++i) {
                value.Rates[i] = {row.Rates[i].Iops, row.Rates[i].BytesPerSecond};
            }
            Info.TabletStats.push_back(value);
        }
        Reply(BufferInfo ? &*BufferInfo : nullptr, BufferError);
    }

    void Handle(TEvPersistentBufferInfo::TPtr ev) {
        if (!WaitingBuffer || ev->Sender != PersistentBuffer || ev->Cookie != 1) {
            return;
        }
        BufferInfo = std::move(ev->Get()->MonInfo);
        if (!BufferInfo) {
            BufferError = "PB monitoring data unavailable";
        }
        RequestData();
    }

    void Handle(TEvInMemoryMetricsSnapshot::TPtr ev) {
        if (WaitingOperations && ev->Cookie == 4) {
            std::vector<TDDiskMonCounterSample> samples;
            ev->Get()->Snapshot.Read([&](const TSnapshotView& view) {
                view.ForEachLine([&](const TLineSnapshot& line) {
                    if (line.LineId != Info.OperationLineId) {
                        return;
                    }
                    // Include a predecessor to calculate the first visible rate.
                    const auto begin = Info.CollectedAt - TDuration::Minutes(5) - TDuration::Seconds(2);
                    for (const auto& record : TOperationMetricsFrontend::ReadRecords(line, begin, Info.CollectedAt)) {
                        const auto values = ReadOperationMetricValues(record.Value, std::make_index_sequence<11>{});
                        TDDiskMonCounterSample sample;
                        sample.Timestamp = record.Timestamp;
                        sample.SampledAt = TMonotonic::MicroSeconds(values[10]);
                        for (size_t i = 0; i < sample.Counters.size(); ++i) {
                            sample.Counters[i] = {values[2 * i], values[2 * i + 1]};
                        }
                        samples.push_back(sample);
                    }
                });
            });
            Info.RateHistory = CalculateDDiskMonRateHistory(samples);
            WaitingOperations = false;
            Reply(nullptr, {});
            return;
        }
        if (ev->Cookie < 2 || ev->Cookie >= 2 + WaitingMemory.size() || !WaitingMemory[ev->Cookie - 2]) {
            return;
        }
        auto& memory = *Histories()[ev->Cookie - 2];
        ev->Get()->Snapshot.Read([&](const TSnapshotView& view) {
            view.ForEachLine([&](const TLineSnapshot& line) {
                if (line.LineId != memory.LineId) {
                    return;
                }
                // Keep the last sample in each second. Group fields share a timestamp.
                const auto append = [](TDDiskMonMemory* history, TInstant timestamp, ui64 value) {
                    if (!history->Samples.empty() && history->Samples.back().Timestamp.Seconds() == timestamp.Seconds()) {
                        history->Samples.back() = {timestamp, value};
                    } else {
                        history->Samples.push_back({timestamp, value});
                    }
                };
                const auto begin = Info.CollectedAt - TDuration::Minutes(5);
                if (ev->Cookie == 2) {
                    for (const auto& record : line.ReadRecordsAsInRange<ui64>(begin, Info.CollectedAt)) {
                        append(&memory, record.Timestamp, record.Value);
                    }
                } else {
                    for (const auto& record : TSpaceMetricsFrontend::ReadRecords(line, begin, Info.CollectedAt)) {
                        append(&Info.SpaceHistory[0], record.Timestamp, record.Value.Get<TSpaceMetrics::TData>());
                        append(&Info.SpaceHistory[1], record.Timestamp, record.Value.Get<TSpaceMetrics::TChecksums>());
                        append(&Info.SpaceHistory[2], record.Timestamp, record.Value.Get<TSpaceMetrics::TPersistentBuffer>());
                        append(&Info.SpaceHistory[3], record.Timestamp, record.Value.Get<TSpaceMetrics::TReserve>());
                    }
                }
            });
        });
        WaitingMemory[ev->Cookie - 2] = false;
        if (!WaitingHistory()) {
            Reply(BufferInfo ? &*BufferInfo : nullptr, BufferError);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr ev) {
        if (WaitingStats && ev->Cookie == 10) {
            Reply(BufferInfo ? &*BufferInfo : nullptr, BufferError);
            return;
        }
        if (WaitingBuffer && ev->Cookie == 1) {
            BufferError = "PersistentBuffer is unavailable";
            RequestData();
        }
    }

    void Timeout() {
        if (WaitingOperations) {
            Info.OperationHistoryError = "History request timed out";
        }
        if (WaitingBuffer) {
            BufferError = "PersistentBuffer did not respond within 3 seconds";
            for (auto* history : Histories()) {
                if (history) {
                    history->Error = "History was not collected";
                }
            }
        }
        const auto histories = Histories();
        for (size_t i = 0; i < histories.size(); ++i) {
            if (histories[i] && WaitingMemory[i ? 1 : 0]) {
                histories[i]->Error = "History request timed out";
            }
        }
        Reply(BufferInfo ? &*BufferInfo : nullptr, BufferError);
    }

public:
    TDDiskMonRequestActor(TActorId recipient, ui64 cookie, int subRequestId,
            TActorId persistentBuffer, TActorId statsActor, TDDiskMonInfo info, TDDiskMonQuery query)
        : Recipient(recipient)
        , Cookie(cookie)
        , SubRequestId(subRequestId)
        , PersistentBuffer(persistentBuffer)
        , StatsActor(statsActor)
        , Info(std::move(info))
        , Query(std::move(query))
    {}

    void Bootstrap() {
        if ((Query.TabletId && StatsActor) || Query.Tab == "operations") {
            Become(&TThis::StateWork);
            Schedule(TDuration::Seconds(3), new TEvents::TEvWakeup);
            RequestData();
            return;
        }
        if (Query.TabletId || (Query.Tab != "overview" && Query.Tab != "space" && Query.Tab != "tablets")) {
            Reply(nullptr, {});
            return;
        }
        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(3), new TEvents::TEvWakeup);
        if (!PersistentBuffer) {
            BufferError = "PersistentBuffer has not started or has already stopped";
            RequestData();
            return;
        }
        auto request = std::make_unique<TEvGetPersistentBufferInfo>();
        request->MonQuery.emplace().SummaryOnly = true;
        Send(PersistentBuffer, request.release(), IEventHandle::FlagTrackDelivery, 1);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvInMemoryMetricsSnapshot, Handle)
        hFunc(TEvPersistentBufferInfo, Handle)
        hFunc(TEvTabletStats, Handle)
        hFunc(TEvTabletStatsSnapshot, Handle)
        hFunc(TEvents::TEvUndelivered, Handle)
        cFunc(TEvents::TSystem::Wakeup, Timeout)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )
};

} // namespace

void TDDiskActor::RegisterMonPage() {
    auto* mon = AppData()->Mon;
    if (!mon) {
        return;
    }
    auto* actorsPage = mon->RegisterIndexPage("actors", "Actors");
    auto* ddisksPage = actorsPage->RegisterIndexPage("ddisks", "DDisks");
    const TString path = Sprintf("ddisk_p%09" PRIu32 "_s%09" PRIu32,
        BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
    mon->RegisterActorPage(ddisksPage, path, TStringBuilder() << "DDisk " << DDiskId, false,
        TActivationContext::ActorSystem(), SelfId());
}

void TDDiskActor::Handle(NMon::TEvHttpInfo::TPtr ev) {
    TDDiskMonQuery query;
    if (!ParseQuery(ev->Get()->Request.GetParams(), &query)) {
        Send(ev->Sender, new NMon::TEvHttpInfoRes(
            "<div class=\"alert alert-danger\">Invalid monitoring query. "
            "Use unsigned numeric IDs and a DBG index in 0..255.</div>", ev->Get()->SubRequestId),
            0, ev->Cookie);
        return;
    }

    TDDiskMonInfo info;
    info.OperationLineId = OperationMetric.GetLineId();
    if (!MemoryMetric) {
        info.Memory.Error = "Memory history is unavailable";
    }
    info.Memory.LineId = MemoryMetric.GetLineId();
    for (auto& history : info.SpaceHistory) {
        history.LineId = SpaceMetric.GetLineId();
        if (!SpaceMetric) {
            history.Error = "Space history is unavailable";
        }
    }
    info.Memory.Limit = Config.EnableChecksums ? Config.IntegrityChecksumCacheBytes : 0;
    info.CollectedAt = TActivationContext::Now();
    info.StartedAt = StartedAt;
    info.Id = DDiskId;
    info.ActorId = SelfId().ToString();
    info.NodeId = BaseInfo.PDiskActorID.NodeId();
    info.PDiskId = BaseInfo.PDiskId;
    info.SlotId = BaseInfo.VDiskSlotId;
    info.Pool = BaseInfo.StoragePoolName;
    info.State = Stopping ? "Stopping" : IsBroken() ? "Broken" : HandlingQueries ? "Ready" : "Recovering";
    info.BrokenReason = BrokenReason;
    info.Backend = "unknown";
    if (DiskFormat) {
        info.ChunkSize = DiskFormat->ChunkSize;
        info.Backend = "PDisk";
#if defined(__linux__)
        if (UringRouter) {
            info.Backend = "io_uring";
        }
#endif
    }
    if (query.Tab == "tablets" && !query.TabletId && TabletStatsActor && !Stopping) {
        info.DataChunks = MonMappedDataChunks;
        info.ReservedChunks = ChunkReserve.size();
        info.IntegrityChunks = IntegrityManager
            ? IntegrityManager->GetIntegrityChunkCount() : CommittedIntegrityChunks.size();
        Register(new TDDiskMonRequestActor(ev->Sender, ev->Cookie, ev->Get()->SubRequestId,
            PersistentBufferActorId, TabletStatsActor, std::move(info), std::move(query)));
        return;
    }
    info.ReservedChunks = ChunkReserve.size();
    info.AllocationsInFlight = DataChunkAllocationsInFlight.size();
    info.FormattingChunks = FormattingChunks.size();
    info.PendingRelease = PendingChunkRelease.size();
    info.TabletsWithChunks = Tablets.size();
    info.PendingQueries = PendingQueries.size();
    info.RouterInFlight = GetDirectIoInflight();
    info.SharedIoInFlight = CounterVal(Counters.DirectIO.RunningCount);
    info.IoStalled = IoStalled;
    if (IntegrityManager) {
        info.IntegrityChunks = IntegrityManager->GetIntegrityChunkCount();
    } else {
        info.IntegrityChunks = CommittedIntegrityChunks.size();
    }

    std::map<ui64, TDDiskMonTablet> tablets;
    std::map<ui64, TDDiskMonChunk> chunks;
    for (const auto& [tabletId, state] : Tablets) {
        const auto& refs = state.ChunkRefs;
        TDDiskMonTablet tablet;
        tablet.TabletId = tabletId;
        for (const auto& [vchunk, ref] : refs) {
            tablet.DataChunks += ref.ChunkIdx != 0;
            tablet.UnmappedChunks += ref.ChunkIdx == 0;
            tablet.PendingAllocation += ref.PendingEventsForChunk.size();
            tablet.PendingIntegrity += ref.PendingSerializedWrites.size();
            if (query.TabletId == tabletId && query.Tab == "tablets"
                    && (!query.AfterVChunk || vchunk > *query.AfterVChunk)) {
                TDDiskMonChunk chunk;
                chunk.VChunk = vchunk;
                chunk.PhysicalChunk = ref.ChunkIdx;
                chunk.InFlight = ref.InFlightDataIo;
                chunk.PendingAllocation = ref.PendingEventsForChunk.size();
                chunk.PendingIntegrity = ref.PendingSerializedWrites.size();
                if (IntegrityManager) {
                    if (const auto* extent = IntegrityManager->FindExtentRef({tabletId, vchunk})) {
                        chunk.IntegrityChunk = extent->IntegrityChunkIdx;
                        chunk.IntegritySlot = extent->ExtentSlot;
                    }
                }
                AddRow(&chunks, vchunk, std::move(chunk), &info.MoreChunks);
            }
        }
        info.DataChunks += tablet.DataChunks;
        info.PendingAllocation += tablet.PendingAllocation;
        info.PendingIntegrity += tablet.PendingIntegrity;
        if (query.TabletId ? query.TabletId == tabletId
                : !query.AfterTabletId || tabletId > *query.AfterTabletId) {
            AddRow(&tablets, tabletId, std::move(tablet), &info.MoreTablets);
        }
    }
    info.Chunks = TakeRows(std::move(chunks));

    std::map<std::pair<ui64, ui32>, TDDiskMonConnection> connections;
    for (const auto& connection : Connections) {
        if (!connection.Active) {
            continue;
        }
        ++info.ConnectionCount;
        if (!Tablets.contains(connection.TabletId)
                && (query.TabletId ? query.TabletId == connection.TabletId
                    : !query.AfterTabletId || connection.TabletId > *query.AfterTabletId)) {
            TDDiskMonTablet tablet;
            tablet.TabletId = connection.TabletId;
            AddRow(&tablets, connection.TabletId, std::move(tablet), &info.MoreTablets);
        }
        if (query.Tab == "tablets" && (!query.TabletId || query.TabletId == connection.TabletId)) {
            AddRow(&connections, std::make_pair(connection.TabletId, connection.DirectBlockGroupIndex),
                TDDiskMonConnection{
                    .TabletId = connection.TabletId,
                    .DirectBlockGroupIndex = connection.DirectBlockGroupIndex,
                    .Generation = connection.Generation,
                    .Sequence = connection.DDiskSessionSeqNo,
                    .NodeId = connection.NodeId,
                    .InterconnectSession = connection.InterconnectSessionId.ToString(),
                }, &info.MoreConnections, query.TabletId
                    ? TDDiskMonQuery::MaxTabletSessions : TDDiskMonQuery::MaxRows);
        }
    }
    info.Tablets = TakeRows(std::move(tablets));
    info.Connections = TakeRows(std::move(connections));

    if (query.Tab == "operations") {
        std::map<ui64, TDDiskMonSync> syncs;
        for (const auto& [syncId, sync] : SyncsInFlight) {
            if (query.TabletId && query.TabletId != sync.Creds.TabletId) {
                continue;
            }
            AddRow(&syncs, syncId, TDDiskMonSync{
                .Id = syncId,
                .TabletId = sync.Creds.TabletId,
                .DirectBlockGroupIndex = sync.Creds.DirectBlockGroupIndex,
                .VChunk = sync.VChunkIndex,
                .SourceRanges = sync.Requests.size(),
                .PendingSourceRanges = sync.RequestsInFlight,
                .Error = sync.ErrorReason,
            }, &info.MoreSyncs);
        }
        info.Syncs = TakeRows(std::move(syncs));
    }
    auto op = [](TString name, const auto& counters) {
        TDDiskMonOperation result;
        result.Name = std::move(name);
        result.Requests = CounterVal(counters.Requests);
        result.InFlight = CounterVal(counters.RequestsInFlight);
        result.Bytes = CounterVal(counters.Bytes);
        result.BytesInFlight = CounterVal(counters.BytesInFlight);
        return result;
    };
#define MON_OPERATION(NAME) \
    { \
        auto value = op(#NAME, Counters.Interface.NAME); \
        value.ReplyOk = CounterVal(Counters.Interface.NAME.ReplyOk); \
        value.ReplyErr = CounterVal(Counters.Interface.NAME.ReplyErr); \
        info.Operations.push_back(std::move(value)); \
    }
    LIST_COUNTERS_INTERFACE_OPS(MON_OPERATION)
#undef MON_OPERATION
    const auto now = TActivationContext::Monotonic();
    if (!Stopping && MonRateWindow && now >= MonRateSampledAt && now - MonRateSampledAt <= 2 * MonRatePeriod) {
        info.RateWindowSeconds = MonRateWindow.SecondsFloat();
        for (auto& operation : info.Operations) {
            if (operation.Name == "Read") {
                operation.Rate = MonRates[0];
            } else if (operation.Name == "Write") {
                operation.Rate = MonRates[1];
            } else if (operation.Name == "Sync") {
                operation.Rate = MonRates[2];
            }
        }
    }
    info.DirectIo.push_back(op("Read", Counters.DirectIO.Read));
    info.DirectIo.push_back(op("Write", Counters.DirectIO.Write));

    auto field = [](std::vector<TDDiskMonField>* fields, TString name, const auto& value) {
        fields->push_back({std::move(name), ToString(value)});
    };
    field(&info.Identity, "ActorId", info.ActorId);
    field(&info.Identity, "Instance GUID", DDiskInstanceGuid);
    field(&info.Identity, "PDisk actor", BaseInfo.PDiskActorID);
    field(&info.Identity, "VDiskIdShort", BaseInfo.VDiskIdShort.ToString());
    field(&info.Identity, "Storage pool", BaseInfo.StoragePoolName);
    field(&info.Identity, "Device type", NPDisk::DeviceTypeStr(BaseInfo.DeviceType, false));
    if (Info) {
        field(&info.Identity, "BSC pool group ID", Info->GroupID.GetRawId());
        field(&info.Identity, "Order number", Info->GetOrderNumber(BaseInfo.VDiskIdShort));
    }
    if (DiskFormat) {
        field(&info.Identity, "Sector size (bytes)", DiskFormat->SectorSize);
    }
    field(&info.Recovery, "PDisk initialized", PDiskParams ? "true" : "false");
    field(&info.Recovery, "Log replay complete", LogReplayComplete ? "true" : "false");
    field(&info.Recovery, "Startup orphan reservations remaining", StartupOrphanChunks.size());
    field(&info.Recovery, "NextLsn", NextLsn);
    field(&info.Recovery, "ChunkMapSnapshotLsn",
        ChunkMapSnapshotLsn == Max<ui64>() ? TString("none") : ToString(ChunkMapSnapshotLsn));
    field(&info.Recovery, "FirstLsnToKeep",
        HandlingQueries && DiskFormat ? ToString(GetFirstLsnToKeep()) : TString("unknown"));
#define MON_RECOVERY(NAME) field(&info.Recovery, #NAME, CounterVal(Counters.RecoveryLog.NAME));
    MON_RECOVERY(ReadLogChunks)
    MON_RECOVERY(LogRecordsProcessed)
    MON_RECOVERY(LogRecordsApplied)
    MON_RECOVERY(LogRecordsWritten)
    MON_RECOVERY(NumChunkMapSnapshots)
    MON_RECOVERY(NumChunkMapIncrements)
    MON_RECOVERY(CutLogMessages)
#undef MON_RECOVERY
    field(&info.Integrity, "EnableChecksums", Config.EnableChecksums ? "true" : "false");
    field(&info.Integrity, "CheckChecksumBeforeWrite", Config.CheckChecksumBeforeWrite ? "true" : "false");
    field(&info.Integrity, "CheckChecksumWhenRead", Config.CheckChecksumWhenRead ? "true" : "false");
    field(&info.Integrity, "Checksum cache limit (bytes)", Config.IntegrityChecksumCacheBytes);
    if (IntegrityManager) {
        field(&info.Integrity, "Cached integrity block states", IntegrityManager->CachedBlockStates());
        field(&info.Integrity, "Cache capacity (block states)", IntegrityManager->MaxCachedBlockStates());
    }
#define MON_CHECKSUM(NAME) field(&info.Integrity, #NAME " (DDisk + PB)", CounterVal(Counters.Checksums.NAME));
    MON_CHECKSUM(WritesWithoutChecksums)
    MON_CHECKSUM(ChecksumMismatch)
    MON_CHECKSUM(IntegrityPairReads)
    MON_CHECKSUM(IntegrityPairWrites)
    MON_CHECKSUM(IntegrityCorruption)
    MON_CHECKSUM(IntegrityLostWriteDetected)
#undef MON_CHECKSUM
    field(&info.Lifecycle, "Stopping", Stopping ? "true" : "false");
    field(&info.Lifecycle, "Waiting for PB", Stopping && !PersistentBufferGone ? "true" : "false");
    field(&info.Lifecycle, "Own I/O drained", OwnDrainComplete ? "true" : "false");
    field(&info.Lifecycle, "Reservation in flight", ReserveInFlight ? "true" : "false");
    field(&info.Lifecycle, "Log callbacks", LogCallbacks.size());
    field(&info.Lifecycle, "Chunk commits in flight", ChunkMapIncrementsInFlight.size());
    field(&info.Lifecycle, "Pending chunk allocations", ChunkAllocateQueue.size());
    field(&info.Lifecycle, "Delayed I/O retries", DelayedRetries.size());
    field(&info.Lifecycle, "Pending checksum reads", PendingChecksumReads.size());
    field(&info.Lifecycle, "Pending client writes", PendingClientWrites.size());
    field(&info.Lifecycle, "Pending sync segments", PendingSyncSegments.size());
    field(&info.Lifecycle, "Read callbacks (PDisk fallback)", ReadCallbacks.size());
    field(&info.Lifecycle, "Write callbacks (PDisk fallback)", WriteCallbacks.size());
    field(&info.Lifecycle, "ShortReads (DDisk + PB)", CounterVal(Counters.DirectIO.ShortReads));
    field(&info.Lifecycle, "ShortWrites (DDisk + PB)", CounterVal(Counters.DirectIO.ShortWrites));
    field(&info.Lifecycle, "Unaligned write payloads", CounterVal(Counters.Interface.UnalignedWritePayloads));

    Register(new TDDiskMonRequestActor(ev->Sender, ev->Cookie, ev->Get()->SubRequestId,
        Stopping && PersistentBufferGone ? TActorId{} : PersistentBufferActorId,
        Stopping ? TActorId{} : TabletStatsActor, std::move(info), std::move(query)));
}

} // namespace NKikimr::NDDisk
