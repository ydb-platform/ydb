#include "controller_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/mon_auth.h>
#include <ydb/core/tx/replication/controller/protos/schema_barrier.pb.h>

#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NKikimr::NReplication::NController {
namespace {

constexpr size_t MaxRows = 100;
constexpr size_t MaxCellSize = 4096;

TString Text(const TString& value) {
    if (value.size() > MaxCellSize) {
        return EncodeHtmlPcdata(TStringBuf(value).Head(MaxCellSize)) + " [truncated]";
    }
    return EncodeHtmlPcdata(value);
}

template <typename T>
TString Text(const T& value) {
    return Text(ToString(value));
}

TString Text(bool value) {
    return value ? "yes" : "no";
}

TString Lag(const std::optional<TDuration>& value) {
    return value ? value->ToString() : "unknown";
}

// Every cell is escaped here, including paths, issues and protobuf diagnostics.
class TTable {
public:
    TTable(IOutputStream& out, TStringBuf title, std::initializer_list<TStringBuf> columns,
            size_t offset = 0, bool* hasMore = nullptr)
        : Out(out)
        , Columns(columns.size())
        , Offset(offset)
        , HasMore(hasMore)
    {
        Out << "<section class='rc-panel'><h3>" << title << "</h3>"
            << "<div class='rc-scroll'><table><thead><tr>";
        for (auto column : columns) {
            Out << "<th>" << column << "</th>";
        }
        Out << "</tr></thead><tbody>";
    }

    template <typename... T>
    void Row(const T&... values) {
        if (Rows++ < Offset || Rows > Offset + MaxRows) {
            return;
        }
        Out << "<tr class='rc-data'>";
        ((Out << "<td>" << Text(values) << "</td>"), ...);
        Out << "</tr>";
    }

    ~TTable() {
        if (Rows <= Offset) {
            Out << "<tr><td colspan='" << Columns << "' class='rc-muted'>"
                << (Rows ? "No entries on this page" : "No entries") << "</td></tr>";
        }
        Out << "</tbody></table></div>";
        if (Rows > Offset && (Rows > MaxRows || Offset)) {
            Out << "<p class='rc-notice'>Rows " << Offset + 1 << "&ndash;" << Min(Rows, Offset + MaxRows)
                << " of " << Rows << ". Use the page links below or select a replication to narrow the view.</p>";
        }
        if (HasMore && Rows > Offset + MaxRows) {
            *HasMore = true;
        }
        Out << "</section>";
    }

private:
    IOutputStream& Out;
    const size_t Columns;
    const size_t Offset;
    bool* const HasMore;
    size_t Rows = 0;
};

template <typename TMap>
auto SortedKeys(const TMap& map) {
    TVector<typename TMap::key_type> keys;
    keys.reserve(map.size());
    for (const auto& [key, _] : map) {
        keys.push_back(key);
    }
    Sort(keys);
    return keys;
}

void Metric(IOutputStream& out, TStringBuf name, size_t value) {
    out << "<div class='rc-metric'><span>" << name << "</span><strong>" << value << "</strong></div>";
}

const char* const Styles = R"HTML(
<style>
#rc-monitor { --rc-border: #dce4ef; color: #23354d; background: #f4f7fb;
    padding: 24px; border-radius: 14px; font: 14px/1.5 system-ui, sans-serif; }
#rc-monitor * { box-sizing: border-box; }
#rc-monitor .rc-header { display: flex; flex-wrap: wrap; justify-content: space-between;
    align-items: center; gap: 16px; margin-bottom: 20px; }
#rc-monitor h2 { margin: 0 0 6px; font-size: 26px; font-weight: 700; }
#rc-monitor h3 { margin: 0; padding: 16px 20px; font-size: 16px; font-weight: 650;
    border-bottom: 1px solid var(--rc-border); }
#rc-monitor .rc-muted { color: #677a92; }
#rc-monitor .rc-nav, #rc-monitor .rc-tools { display: flex; flex-wrap: wrap; gap: 10px;
    align-items: center; margin: 14px 0; }
#rc-monitor a.rc-button { display: inline-block; padding: 8px 14px; border-radius: 8px;
    border: 1px solid var(--rc-border); background: white; color: #2451a6; text-decoration: none; }
#rc-monitor a.rc-active { background: #2451a6; color: white; border-color: #2451a6; }
#rc-monitor input, #rc-monitor button { padding: 8px 12px; border: 1px solid var(--rc-border);
    border-radius: 8px; background: white; color: #23354d; }
#rc-monitor input[type=number] { width: 150px; }
#rc-monitor .rc-metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
    gap: 12px; margin: 20px 0; }
#rc-monitor .rc-metric { background: white; border: 1px solid var(--rc-border);
    border-top: 3px solid #4285d4; border-radius: 10px; padding: 15px 18px; }
#rc-monitor .rc-metric span { display: block; color: #677a92; font-size: 12px; }
#rc-monitor .rc-metric strong { display: block; font-size: 28px; margin-top: 4px; }
#rc-monitor .rc-panel { background: white; border: 1px solid var(--rc-border);
    border-radius: 10px; margin: 18px 0; overflow: hidden; box-shadow: 0 2px 6px #23354d06; }
#rc-monitor .rc-scroll { overflow-x: auto; max-height: 560px; }
#rc-monitor table { width: 100%; border-collapse: collapse; font-size: 13px; }
#rc-monitor th { text-align: left; color: #52667e; background: #f8fafd; position: sticky;
    top: 0; z-index: 1; white-space: nowrap; }
#rc-monitor td, #rc-monitor th { padding: 10px 14px; border-bottom: 1px solid #edf1f6; }
#rc-monitor td { min-width: 70px; max-width: 480px; overflow-wrap: break-word; white-space: pre-wrap; }
#rc-monitor tbody tr:hover { background: #f3f7ff; }
#rc-monitor .rc-notice { padding: 12px 16px; margin: 0; color: #71551a; background: #fff8e6; }
#rc-monitor .rc-error { padding: 12px 16px; border-left: 3px solid #cb4353; background: #fff0f2; }
</style>
)HTML";

const char* const Script = R"HTML(
<script>
(function () {
    var root = document.getElementById('rc-monitor');
    var search = document.getElementById('rc-search');
    if (search) search.addEventListener('input', function () {
        var query = search.value.toLowerCase();
        root.querySelectorAll('tr.rc-data').forEach(function (row) {
            row.hidden = row.textContent.toLowerCase().indexOf(query) === -1;
        });
    });
    root.querySelectorAll('tr.rc-data td').forEach(function (cell) {
        var value = cell.textContent;
        if (value === 'Error') { cell.style.color = '#b52a42'; cell.style.fontWeight = '700'; }
        if (value === 'Ready') { cell.style.color = '#18784e'; cell.style.fontWeight = '700'; }
    });
    if (root.dataset.refresh === '15') setTimeout(function () { window.location.reload(); }, 15000);
})();
</script>
)HTML";

TString Url(ui64 tabletId, TStringBuf view, ui64 replicationId, bool refresh,
        TStringBuf table = {}, TStringBuf from = {}, ui32 page = 0) {
    // All arguments are constants, validated numeric values, or generated cursors.
    TStringBuilder url;
    url << "?TabletID=" << tabletId << "&amp;view=" << view;
    if (replicationId) {
        url << "&amp;rid=" << replicationId;
    }
    if (refresh) {
        url << "&amp;refresh=15";
    }
    if (table) {
        url << "&amp;table=" << table;
    }
    if (from) {
        url << "&amp;from=" << from;
    }
    if (page) {
        url << "&amp;page=" << page;
    }
    return url;
}

void Header(IOutputStream& out, ui64 tabletId, ui32 nodeId, ui32 generation,
        TStringBuf state, bool storage, ui64 replicationId, bool refresh, ui32 page = 0) {
    out << Styles << "<div id='rc-monitor' data-refresh='" << (refresh ? "15" : "0") << "'>"
        << "<div class='rc-header'><div><h2>Replication controller</h2>"
        << "<span class='rc-muted'>Tablet " << tabletId << " &middot; Node " << nodeId
        << " &middot; Generation " << generation << " &middot; " << state << "</span></div>"
        << "<div class='rc-tools'>";
    if (!storage) {
        out << "<a class='rc-button' href='" << Url(tabletId, "memory", replicationId, !refresh, {}, {}, page)
            << "'>Auto refresh: " << (refresh ? "15s" : "off") << "</a>";
    }
    out << "<button type='button' onclick='window.location.reload()'>Refresh now</button></div></div>"
        << "<nav class='rc-nav'><a class='rc-button " << (storage ? "" : "rc-active")
        << "' href='" << Url(tabletId, "memory", replicationId, refresh) << "'>In memory</a>"
        << "<a class='rc-button " << (storage ? "rc-active" : "") << "' href='"
        << Url(tabletId, "storage", replicationId, false) << "'>Persisted tables</a></nav>";
    if (!storage) {
        out << "<form class='rc-tools' method='get'><input type='hidden' name='TabletID' value='"
            << tabletId << "'><input type='hidden' name='view' value='memory'>"
            << "<label>Replication ID <input type='number' name='rid' min='0' value='"
            << replicationId << "'></label><button type='submit'>Apply</button>"
            << "<a href='" << Url(tabletId, "memory", 0, false) << "'>All replications</a></form>";
    }
    out << "<div class='rc-tools'><label>Filter displayed rows <input id='rc-search' type='search'"
        << " placeholder='Path, worker, state...'></label></div>";
}

template <typename TProto>
TString ProtoText(const TString& value) {
    if (value.empty()) {
        return {};
    }
    TProto proto;
    if (!proto.ParseFromString(value)) {
        return "Cannot decode persisted protobuf";
    }
    return proto.ShortDebugString();
}

// Only explicitly selected non-secret columns can reach the storage renderer.
template <typename TColumn>
auto StorageValue(const typename TColumn::Type& value) {
    using S = TControllerSchema;
    if constexpr (std::is_same_v<TColumn, S::Targets::SchemaBarrierChange>) {
        return ProtoText<NKikimrReplication::TSchemaChange>(value);
    } else if constexpr (std::is_same_v<TColumn, S::Targets::SchemaBarrierFlushTxIds>) {
        return ProtoText<NKikimrReplicationController::TSchemaBarrierFlushTxIds>(value);
    } else if constexpr (std::is_same_v<TColumn, S::Targets::IndexBuild>) {
        return ProtoText<NKikimrReplication::TIndexBuildState>(value);
    } else if constexpr (std::is_same_v<TColumn, S::IndexBuildWorkers::Progress>) {
        return ProtoText<NKikimrReplication::TIndexBuildProgress>(value);
    } else {
        return value;
    }
}

} // anonymous namespace

bool TController::RenderMonitoring(TStringStream& out, ui64 replicationId, ui32 page) const {
    const auto matches = [replicationId](ui64 id) { return !replicationId || id == replicationId; };
    bool hasMore = false;
    const auto makeTable = [&](TStringBuf title, std::initializer_list<TStringBuf> columns) {
        return TTable(out, title, columns, size_t(page) * MaxRows, &hasMore);
    };
    size_t targets = 0;
    size_t errors = 0;
    size_t running = 0;
    for (const auto& [_, replication] : Replications) {
        targets += replication->GetTargets().size();
        errors += replication->GetState() == TReplication::EState::Error;
    }
    for (const auto& [_, worker] : Workers) {
        running += worker.HasSession();
    }
    out << "<p class='rc-muted'>Live actor state. Summary counts cover the entire controller; "
        << "tables respect the replication filter. Snapshot: " << TActivationContext::Now().ToString() << "</p>"
        << "<div class='rc-metrics'>";
    Metric(out, "Replications", Replications.size());
    Metric(out, "Targets", targets);
    Metric(out, "Workers with session", running);
    Metric(out, "Workers total", Workers.size());
    Metric(out, "Sessions", Sessions.size());
    Metric(out, "Replication errors", errors);
    Metric(out, "Boot / stop / remove queued", BootQueue.size() + StopQueue.size() + RemoveQueue.size());
    Metric(out, "Schema barriers", SchemaBarriers.size());
    out << "</div>";
    if (replicationId && !Find(replicationId)) {
        out << "<p class='rc-error'>Replication " << replicationId << " is not present in memory.</p>";
    }

    {
        auto table = makeTable("Replications", {"ID", "Path ID", "Database", "State", "Desired state", "Lag",
            "Targets", "Next target ID", "Committed version", "Deferred alter", "DB resolve attempts left", "Issue"});
        for (auto id : SortedKeys(Replications)) {
            if (!matches(id)) {
                continue;
            }
            const auto& replication = *Replications.at(id);
            auto committed = CommittedVersions.find(id);
            auto unresolved = UnresolvedDatabaseReplications.find(id);
            table.Row(id, replication.GetPathId(), replication.GetDatabase(), replication.GetState(),
                replication.GetDesiredState(), Lag(replication.GetLag()), replication.GetTargets().size(),
                replication.GetNextTargetId(), committed == CommittedVersions.end() ? "unknown" : ToString(committed->second),
                DeferredAlters.contains(id), unresolved == UnresolvedDatabaseReplications.end() ? "resolved" : ToString(ui32(unresolved->second)),
                replication.GetIssue());
        }
    }
    {
        auto table = makeTable("Targets", {"Replication", "Target", "Kind", "Source", "Destination", "Destination state",
            "Path ID", "Pending path ID", "Stream", "Stream state", "Consumer", "Schema changes", "Lag", "Workers",
            "Worker set complete", "Issue"});
        for (auto rid : SortedKeys(Replications)) {
            if (!matches(rid)) {
                continue;
            }
            auto targets = Replications.at(rid)->GetTargets();
            Sort(targets, [](const auto* a, const auto* b) { return a->GetId() < b->GetId(); });
            for (const auto* target : targets) {
                const auto schemaChanges = target->GetStreamSchemaChanges();
                table.Row(rid, target->GetId(), target->GetKind(), target->GetSrcPath(), target->GetDstPath(),
                    target->GetDstState(), target->GetDstPathId(), target->GetPendingDstPathId(), target->GetStreamName(),
                    target->GetStreamState(), target->GetStreamConsumerName(),
                    schemaChanges ? (*schemaChanges ? "enabled" : "disabled") : "unknown", Lag(target->GetLag()),
                    target->GetWorkers().size(), CompleteWorkerSets.contains(std::make_pair(rid, target->GetId())), target->GetIssue());
            }
        }
    }
    {
        auto table = makeTable("Workers", {"Replication", "Target", "Worker / partition", "Node", "Has command", "Data ended",
            "Heartbeat", "Pending heartbeat", "Quorum participant", "Boot queued", "Remove queued"});
        for (const auto& id : SortedKeys(Workers)) {
            if (!matches(id.ReplicationId())) {
                continue;
            }
            const auto& worker = Workers.at(id);
            auto pending = PendingHeartbeats.find(id);
            table.Row(id.ReplicationId(), id.TargetId(), id.WorkerId(), worker.HasSession() ? ToString(worker.GetSession()) : "unassigned",
                worker.HasCommand(), worker.IsDataEnded(), worker.HasHeartbeat() ? ToString(worker.GetHeartbeat()) : "unknown",
                pending == PendingHeartbeats.end() ? "none" : ToString(pending->second), IsHeartbeatParticipant(id),
                BootQueue.contains(id), RemoveQueue.contains(id));
        }
    }
    {
        auto table = makeTable("Sessions (all nodes)", {"Node", "Ready", "Attached workers"});
        for (auto node : SortedKeys(Sessions)) {
            const auto& session = Sessions.at(node);
            table.Row(node, session.IsReady(), session.GetWorkers().size());
        }
    }
    {
        auto table = makeTable("Pipeline (controller-wide)", {"Operation", "Value"});
        table.Row("Committing transaction ID (0 = idle)", CommittingTxId);
        table.Row("Allocated transaction IDs", AllocatedTxIds.size());
        table.Row("Allocation in flight", AllocateTxIdInFlight);
        table.Row("Assignment in flight", AssignTxIdInFlight);
        table.Row("Heartbeat processing in flight", ProcessHeartbeatsInFlight);
        table.Row("Index build transaction in flight", IndexBuildTxInFlight);
        table.Row("Workers with heartbeat", WorkersWithHeartbeat.size());
        table.Row("Heartbeat participants", CountHeartbeatParticipants());
        table.Row("Pending heartbeats", PendingHeartbeats.size());
        table.Row("Queue processing scheduled", ProcessQueuesScheduled);
        table.Row("Create stream: queued", RequestedCreateStream.size());
        table.Row("Create stream: in flight", InflightCreateStream.size());
        table.Row("Drop stream: queued", RequestedDropStream.size());
        table.Row("Drop stream: in flight", InflightDropStream.size());
        table.Row("Pending index progress", PendingIndexBuildProgress.size());
        table.Row("Pending index transaction requests", PendingIndexTxIds.size());
        table.Row("Index commits in flight", IndexCommits.size());
        table.Row("Completed index commits", CompletedIndexCommits.size());
        table.Row("Schema destination alterers", SchemaChangeDstAlterers.size());
        table.Row("Schema target flushes", SchemaTargetFlushes.size());
        table.Row("Active schema target flush", ActiveSchemaTargetFlush
            ? TStringBuilder() << ActiveSchemaTargetFlush->first << "/" << ActiveSchemaTargetFlush->second : TString("none"));
    }
    {
        auto table = makeTable("Worker queues", {"Queue", "Replication", "Target", "Worker", "Node"});
        // Queue membership can outlive a live worker entry.
        auto boot = TVector<TWorkerId>(BootQueue.begin(), BootQueue.end());
        Sort(boot);
        for (const auto& id : boot) {
            if (matches(id.ReplicationId())) {
                table.Row("Boot", id.ReplicationId(), id.TargetId(), id.WorkerId(), "any");
            }
        }
        auto stop = TVector<std::pair<TWorkerId, ui32>>(StopQueue.begin(), StopQueue.end());
        Sort(stop);
        for (const auto& [id, node] : stop) {
            if (matches(id.ReplicationId())) {
                table.Row("Stop", id.ReplicationId(), id.TargetId(), id.WorkerId(), node);
            }
        }
        auto remove = TVector<TWorkerId>(RemoveQueue.begin(), RemoveQueue.end());
        Sort(remove);
        for (const auto& id : remove) {
            if (matches(id.ReplicationId())) {
                table.Row("Remove", id.ReplicationId(), id.TargetId(), id.WorkerId(), "any");
            }
        }
    }
    {
        auto table = makeTable("Transactions (controller-wide)", {"Version", "Assigned write transaction ID", "Waiting nodes"});
        for (const auto& [version, txId] : AssignedTxIds) {
            table.Row(version, txId, 0);
        }
        for (const auto& [version, nodes] : PendingTxId) {
            table.Row(version, "pending", nodes.size());
        }
    }
    {
        auto table = makeTable("Schema barriers", {"Replication", "Target", "Phase", "Schema version", "Record version", "Expected",
            "Reported", "Index metadata", "Applied", "Completed", "Destination alter tx", "Target flush progress", "Blocks commit"});
        for (const auto& [key, barrier] : SchemaBarriers) {
            if (!matches(key.first)) {
                continue;
            }
            TString phase;
            switch (barrier.Phase) {
            case ESchemaBarrierPhase::Collecting: phase = "Collecting"; break;
            case ESchemaBarrierPhase::Altering: phase = "Altering"; break;
            case ESchemaBarrierPhase::Applied: phase = "Applied"; break;
            case ESchemaBarrierPhase::Error: phase = "Error"; break;
            case ESchemaBarrierPhase::FlushingTarget: phase = "FlushingTarget"; break;
            case ESchemaBarrierPhase::Verifying: phase = "Verifying"; break;
            }
            table.Row(key.first, key.second, phase, barrier.Schema.GetSourceSchemaVersion(),
                barrier.Schema.GetVersion().ShortDebugString(), barrier.ExpectedWorkers.size(), barrier.ReportedWorkers.size(),
                barrier.IndexMetadataWorkers.size(), barrier.AppliedWorkers.size(), barrier.CompletedWorkers.size(), barrier.DstAlterTxId,
                TStringBuilder() << barrier.NextTargetFlushTxId << "/" << barrier.TargetFlushTxIds.size(), BlocksGlobalCommit(barrier));
        }
    }
    {
        auto table = makeTable("Schema barrier workers", {"Replication", "Target", "Worker", "Reported", "Index metadata", "Applied", "Completed", "Offset"});
        for (const auto& [key, barrier] : SchemaBarriers) {
            if (!matches(key.first)) {
                continue;
            }
            auto workers = TVector<TWorkerId>(barrier.ExpectedWorkers.begin(), barrier.ExpectedWorkers.end());
            Sort(workers);
            for (const auto& id : workers) {
                auto offset = barrier.WorkerOffsets.find(id);
                table.Row(key.first, key.second, id.WorkerId(), barrier.ReportedWorkers.contains(id),
                    barrier.IndexMetadataWorkers.contains(id), barrier.AppliedWorkers.contains(id), barrier.CompletedWorkers.contains(id),
                    offset == barrier.WorkerOffsets.end() ? "unknown" : ToString(offset->second));
            }
        }
    }
    {
        auto table = makeTable("Index builds", {"Replication", "Target", "Phase", "Join version", "Ready tx ID", "Snapshot tx ID", "Assignments"});
        for (const auto& [key, build] : IndexBuilds) {
            if (matches(key.first)) {
                table.Row(key.first, key.second, NKikimrReplication::TIndexBuildState::EPhase_Name(build.GetPhase()),
                    build.GetJoinVersion().ShortDebugString(), build.GetReadyTxId(), build.GetSnapshotTxId(), build.AssignmentsSize());
            }
        }
    }
    {
        auto table = makeTable("Index build workers", {"Replication", "Target", "Worker", "Checkpoint"});
        for (const auto& id : SortedKeys(IndexBuildProgress)) {
            if (matches(id.ReplicationId())) {
                table.Row(id.ReplicationId(), id.TargetId(), id.WorkerId(), IndexBuildProgress.at(id).ShortDebugString());
            }
        }
    }
    return hasMore;
}

class TController::TTxMonitoring: public TTxBase {
public:
    TTxMonitoring(TController* self, NMon::TEvRemoteHttpInfo::TPtr ev)
        : TTxBase("TxMonitoring", self)
        , Request(ev)
        , TableName(Request->Get()->Cgi().Has("table") ? Request->Get()->Cgi().Get("table") : TString("Replications"))
        , Cursor(Request->Get()->Cgi().Get("from"))
    {
    }

    TTxType GetTxType() const override {
        // Monitoring describes controller state without changing it.
        return TXTYPE_DESCRIBE_REPLICATION;
    }

    template <typename TSchema, typename... TColumns>
    bool ReadTable(NIceDb::TNiceDb& db, std::initializer_list<TStringBuf> columns) {
        typename TSchema::TKey::KeyValuesType from{};
        if (Cursor) {
            TVector<TString> parts;
            Split(Cursor, ",", parts);
            size_t index = 0;
            bool valid = parts.size() == std::tuple_size_v<decltype(from)>;
            std::apply([&](auto&... key) {
                ((valid = valid && TryFromString(parts[index++], key)), ...);
            }, from);
            if (!valid) {
                Error = "Invalid table cursor. Select a table above to reset it.";
                return true;
            }
        }
        auto rows = std::apply([&](auto... key) {
            return db.Table<TSchema>().GreaterOrEqual(key...).template Select<TColumns...>();
        }, from);
        if (!rows.IsReady()) {
            return false;
        }
        TTable table(Body, TableName, columns);
        size_t count = 0;
        while (!rows.EndOfSet()) {
            if (count++ == MaxRows) {
                typename TSchema::TKey::KeyValuesType next(rows.GetKey());
                std::apply([&](auto... key) {
                    ((NextCursor += (NextCursor.empty() ? "" : ",") + ToString(key)), ...);
                }, next);
                break;
            }
            table.Row(StorageValue<TColumns>(rows.template GetValueOrDefault<TColumns>())...);
            if (!rows.Next()) {
                return false;
            }
        }
        return true;
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        // A page fault restarts this read-only transaction. Never retain partial output.
        Body.Clear();
        NextCursor.clear();
        Error.clear();
        NIceDb::TNiceDb db(txc.DB);
        using S = Schema;
        if (TableName == "Replications") {
            return ReadTable<S::Replications, S::Replications::Id, S::Replications::PathOwnerId,
                S::Replications::PathLocalId, S::Replications::Database, S::Replications::State,
                S::Replications::DesiredState, S::Replications::Issue, S::Replications::NextTargetId,
                S::Replications::DeferredAlter, S::Replications::CommittedStep, S::Replications::CommittedTxId>(db,
                {"ID", "Path owner", "Path local ID", "Database", "State", "Desired state", "Issue", "Next target ID",
                    "Deferred alter", "Committed step", "Committed tx ID"});
        } else if (TableName == "Targets") {
            return ReadTable<S::Targets, S::Targets::ReplicationId, S::Targets::Id, S::Targets::Kind,
                S::Targets::SrcPath, S::Targets::DstPath, S::Targets::DstState, S::Targets::DstPathOwnerId,
                S::Targets::DstPathLocalId, S::Targets::PendingDstPathOwnerId, S::Targets::PendingDstPathLocalId,
                S::Targets::Issue, S::Targets::WorkerSetComplete, S::Targets::SchemaBarrierPhase,
                S::Targets::SchemaBarrierChange, S::Targets::DstAlterTxId, S::Targets::SchemaBarrierFlushTxIds,
                S::Targets::IndexBuild>(db, {"Replication", "Target", "Kind", "Source", "Destination", "Destination state",
                    "Path owner", "Path local ID", "Pending path owner", "Pending path local ID", "Issue", "Worker set complete",
                    "Barrier phase (1 collecting, 2 altering, 3 applied, 4 error, 5 flushing, 6 verifying)", "Schema change",
                    "Destination alter tx", "Flush tx IDs", "Index build"});
        } else if (TableName == "Workers") {
            return ReadTable<S::Workers, S::Workers::ReplicationId, S::Workers::TargetId, S::Workers::WorkerId,
                S::Workers::HeartbeatVersionStep, S::Workers::HeartbeatVersionTxId>(db,
                {"Replication", "Target", "Worker", "Heartbeat step", "Heartbeat tx ID"});
        } else if (TableName == "SrcStreams") {
            return ReadTable<S::SrcStreams, S::SrcStreams::ReplicationId, S::SrcStreams::TargetId, S::SrcStreams::Name,
                S::SrcStreams::State, S::SrcStreams::ConsumerName, S::SrcStreams::SchemaChanges>(db,
                {"Replication", "Target", "Name", "State", "Consumer", "Schema changes"});
        } else if (TableName == "TxIds") {
            return ReadTable<S::TxIds, S::TxIds::VersionStep, S::TxIds::VersionTxId, S::TxIds::WriteTxId>(db,
                {"Version step", "Version tx ID", "Write tx ID"});
        } else if (TableName == "SysParams") {
            return ReadTable<S::SysParams, S::SysParams::Id, S::SysParams::IntValue>(db,
                {"Parameter ID (1 = next replication ID)", "Integer value"});
        } else if (TableName == "SchemaBarrierWorkers") {
            return ReadTable<S::SchemaBarrierWorkers, S::SchemaBarrierWorkers::ReplicationId,
                S::SchemaBarrierWorkers::TargetId, S::SchemaBarrierWorkers::WorkerId, S::SchemaBarrierWorkers::Reported,
                S::SchemaBarrierWorkers::IndexMetadata, S::SchemaBarrierWorkers::Applied, S::SchemaBarrierWorkers::Completed,
                S::SchemaBarrierWorkers::Offset>(db, {"Replication", "Target", "Worker", "Reported", "Index metadata", "Applied", "Completed", "Offset"});
        } else if (TableName == "IndexBuildWorkers") {
            return ReadTable<S::IndexBuildWorkers, S::IndexBuildWorkers::ReplicationId, S::IndexBuildWorkers::TargetId,
                S::IndexBuildWorkers::WorkerId, S::IndexBuildWorkers::Progress>(db,
                {"Replication", "Target", "Worker", "Progress"});
        }
        Error = "Unknown persisted table. Select a table above.";
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TStringStream out;
        Header(out, Self->TabletID(), Self->SelfId().NodeId(), Self->Executor()->Generation(),
            "Persisted snapshot", true, 0, false);
        out << "<p class='rc-muted'>Read-only snapshot of local tablet tables, up to " << MaxRows
            << " rows per page. Runtime sessions and queues are not persisted. Configurations, credentials "
            << "and arbitrary binary system parameters are omitted.</p><nav class='rc-nav'>";
        for (TStringBuf name : {"Replications", "Targets", "SrcStreams", "Workers", "TxIds", "SysParams",
                "SchemaBarrierWorkers", "IndexBuildWorkers"}) {
            out << "<a class='rc-button " << (name == TableName ? "rc-active" : "") << "' href='"
                << Url(Self->TabletID(), "storage", 0, false, name) << "'>" << name << "</a>";
        }
        out << "</nav>";
        if (Error) {
            out << "<p class='rc-error'>" << Text(Error) << "</p>";
        } else {
            out << Body.Str();
        }
        // TableName is only reused in links after the allowlist above has accepted it.
        if (!Error) {
            out << "<nav class='rc-nav'><a class='rc-button' href='"
                << Url(Self->TabletID(), "storage", 0, false, TableName) << "'>First page</a>";
            if (NextCursor) {
                out << "<a class='rc-button' href='"
                    << Url(Self->TabletID(), "storage", 0, false, TableName, NextCursor) << "'>Next page</a>";
            }
            out << "</nav>";
        }
        out << "</div>" << Script;
        ctx.Send(Request->Sender, new NMon::TEvRemoteHttpInfoRes(out.Str()), 0, Request->Cookie);
    }

private:
    const NMon::TEvRemoteHttpInfo::TPtr Request;
    const TString TableName;
    const TString Cursor;
    TStringStream Body;
    TString NextCursor;
    TString Error;
};

bool TController::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) {
    if (!Executor() || !Executor()->GetStats().IsActive) {
        return false;
    }
    if (!ev) {
        return true;
    }
    if (!IsTabletDevUiAccessAllowed(AppData(ctx), ev->Get()->PathInfo(), ev->Get()->GetUserToken(), true)) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPFORBIDDEN), 0, ev->Cookie);
        return true;
    }
    const auto cgi = ev->Get()->Cgi();
    if (cgi.Get("view") == "storage") {
        Execute(new TTxMonitoring(this, ev), ctx);
        return true;
    }
    ui64 replicationId = 0;
    ui32 page = 0;
    const bool validId = !cgi.Has("rid") || TryFromString(cgi.Get("rid"), replicationId);
    const bool validPage = !cgi.Has("page") || TryFromString(cgi.Get("page"), page);
    const bool refresh = cgi.Get("refresh") == "15";
    TStringStream out;
    Header(out, TabletID(), SelfId().NodeId(), Executor()->Generation(),
        CurrentStateFunc() == &TThis::StateWork ? "Working"
            : CurrentStateFunc() == &TThis::StateDatabaseResolve ? "Resolving databases" : "Initializing",
        false, replicationId, refresh, page);
    if (!validId) {
        out << "<p class='rc-error'>Invalid replication ID. Enter an unsigned integer.</p>";
    } else if (!validPage) {
        out << "<p class='rc-error'>Invalid page number. Enter an unsigned integer.</p>";
    } else {
        const bool hasMore = RenderMonitoring(out, replicationId, page);
        out << "<nav class='rc-nav'><span>Page " << ui64(page) + 1 << " (up to " << MaxRows
            << " rows per table)</span><a class='rc-button' href='"
            << Url(TabletID(), "memory", replicationId, refresh) << "'>First page</a>";
        if (page) {
            out << "<a class='rc-button' href='" << Url(TabletID(), "memory", replicationId, refresh, {}, {}, page - 1)
                << "'>Previous page</a>";
        }
        if (hasMore) {
            out << "<a class='rc-button' href='" << Url(TabletID(), "memory", replicationId, refresh, {}, {}, page + 1)
                << "'>Next page</a>";
        }
        out << "</nav>";
    }
    out << "</div>" << Script;
    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(out.Str()), 0, ev->Cookie);
    return true;
}

} // namespace NKikimr::NReplication::NController
