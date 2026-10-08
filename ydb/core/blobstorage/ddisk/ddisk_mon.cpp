#include "ddisk_mon.h"

#include <library/cpp/json/json_writer.h>

#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <algorithm>
#include <initializer_list>
#include <map>
#include <utility>

namespace NKikimr::NDDisk {
namespace {

TString Html(TStringBuf value) {
    TString result;
    for (char c : value) {
        switch (c) {
        case '&':
            result += "&amp;";
            break;
        case '<':
            result += "&lt;";
            break;
        case '>':
            result += "&gt;";
            break;
        case '"':
            result += "&quot;";
            break;
        case '\'':
            result += "&#39;";
            break;
        default:
            result += c;
        }
    }
    return result;
}

TString Bytes(ui64 value) {
    if (value >= (1ull << 40)) {
        return Sprintf("%.2f TiB", double(value) / (1ull << 40));
    }
    if (value >= (1ull << 30)) {
        return Sprintf("%.2f GiB", double(value) / (1ull << 30));
    }
    if (value >= (1ull << 20)) {
        return Sprintf("%.2f MiB", double(value) / (1ull << 20));
    }
    if (value >= (1ull << 10)) {
        return Sprintf("%.2f KiB", double(value) / (1ull << 10));
    }
    return ToString(value) + " B";
}

TString Capacity(ui64 chunks, ui64 chunkSize) {
    return chunkSize ? Bytes(chunks * chunkSize) : "unknown";
}

TString ByteRate(double value) {
    for (const auto& [size, unit] : {std::pair<double, TStringBuf>{1ull << 40, "TiB/s"},
            {1ull << 30, "GiB/s"}, {1ull << 20, "MiB/s"}, {1ull << 10, "KiB/s"}}) {
        if (value >= size) {
            return Sprintf("%.2f ", value / size) + TString(unit);
        }
    }
    return value ? Sprintf("%.2f B/s", value) : TString("0 B/s");
}

TString Uptime(const TDDiskMonInfo& info) {
    if (!info.StartedAt || info.CollectedAt < info.StartedAt) {
        return "unknown";
    }
    const ui64 seconds = (info.CollectedAt - info.StartedAt).Seconds();
    TStringStream out;
    if (seconds >= 86400) {
        out << seconds / 86400 << "d ";
    }
    if (seconds >= 3600) {
        out << seconds / 3600 % 24 << "h ";
    }
    if (seconds >= 60) {
        out << seconds / 60 % 60 << "m ";
    }
    out << seconds % 60 << "s";
    return out.Str();
}

TString StateBadge(TStringBuf state) {
    const TStringBuf tone = state == "Ready" ? "ready" : state == "Broken" ? "broken"
        : state == "Recovering" || state == "Stopping" ? "pending" : "unknown";
    return "<span class=\"ddisk-state ddisk-state-" + TString(tone) + "\">" + Html(state) + "</span>";
}

TString Link(TStringBuf query, TStringBuf label, ui32 refreshRate = 0) {
    TString target(query);
    if (refreshRate) {
        target += "&refreshRate=" + ToString(refreshRate);
    }
    return "<a href=\"?" + Html(target) + "\">" + Html(label) + "</a>";
}

void Cell(TStringStream* out, TStringBuf value) {
    *out << "<td>" << Html(value) << "</td>";
}

TString GroupDigits(TString value) {
    const auto dot = value.find('.');
    size_t position = dot == TString::npos ? value.size() : dot;
    while (position > 3) {
        position -= 3;
        value.insert(position, " ");
    }
    return value;
}

template <typename T>
void Number(TStringStream* out, T value) {
    Cell(out, GroupDigits(ToString(value)));
}

void Field(TStringStream* out, TStringBuf name, TStringBuf value) {
    *out << "<tr>";
    Cell(out, name);
    Cell(out, value);
    *out << "</tr>";
}

void Fields(TStringStream* out, TStringBuf title, const std::vector<TDDiskMonField>& fields) {
    *out << "<h3>" << Html(title) << "</h3><table class=\"table table-condensed\"><tbody>";
    if (fields.empty()) {
        Field(out, "State", "unknown");
    }
    for (const auto& field : fields) {
        Field(out, field.Name, field.Value);
    }
    *out << "</tbody></table>";
}

TStringBuf DiagnosticValue(const std::vector<TDDiskMonField>& fields, TStringBuf name) {
    for (const auto& field : fields) {
        if (field.Name == name) {
            return field.Value;
        }
    }
    return "unknown";
}

void DiagnosticFields(TStringStream* out, TStringBuf title, const std::vector<TDDiskMonField>& fields,
        std::initializer_list<std::pair<TStringBuf, TStringBuf>> names) {
    *out << "<h4>" << Html(title) << "</h4><table class=\"table table-condensed\"><tbody>";
    for (const auto& [key, label] : names) {
        auto value = DiagnosticValue(fields, key);
        if (value == "true") {
            value = key == "Reservation in flight" ? "Yes" : "Enabled";
        } else if (value == "false") {
            value = key == "Reservation in flight" ? "No" : "Disabled";
        }
        Field(out, label, value);
    }
    *out << "</tbody></table>";
}

void Diagnostics(TStringStream* out, const TDDiskMonInfo& info) {
    *out << "<style>.ddisk-diagnostics{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:42px}"
        ".ddisk-diagnostics .ddisk-state{display:inline-block;padding:2px 8px;border-radius:4px;}"
        ".ddisk-diagnostics .ddisk-state-ready{background:#eaf5ee;color:#23733c;}"
        ".ddisk-diagnostics .ddisk-state-broken{background:#fbeaea;color:#a33;}"
        ".ddisk-diagnostics .ddisk-state-pending{background:#fff4dc;color:#856017;}"
        ".ddisk-diagnostics .ddisk-state-unknown{background:#eee;color:#666;}"
        ".ddisk-diagnostics section{margin-bottom:30px}.ddisk-diagnostics h3{margin-top:0}"
        ".ddisk-diagnostics h4{font-size:13px;color:#777;margin-top:18px}"
        ".ddisk-diagnostics table{table-layout:fixed}.ddisk-diagnostics td{overflow-wrap:anywhere}"
        ".ddisk-diagnostics td:last-child{text-align:right;font-variant-numeric:tabular-nums;width:35%}"
        ".ddisk-diagnostics .ddisk-identity td:last-child{width:57%}"
        "@media(max-width:767px){.ddisk-diagnostics{grid-template-columns:1fr;gap:0}}</style>"
        "<div class=\"ddisk-diagnostics\"><div><section><h3>Health</h3>"
        "<table class=\"table table-condensed\"><tbody>";
    auto state = [&](TStringBuf name, TStringBuf value) {
        *out << "<tr>";
        Cell(out, name);
        *out << "<td>" << StateBadge(value) << "</td></tr>";
    };
    state("DDisk", info.State);
    Field(out, "DDisk failure reason", info.BrokenReason.empty() ? TStringBuf("None") : TStringBuf(info.BrokenReason));
    Field(out, "DDisk I/O stalled", info.IoStalled ? "Yes" : "No");
    *out << "</tbody></table></section><section><h3>Integrity</h3>";
    DiagnosticFields(out, "DDisk checks", info.Integrity, {
        {"EnableChecksums", "Checksums"}, {"CheckChecksumBeforeWrite", "Verify before write"},
        {"CheckChecksumWhenRead", "Verify on read"}});
    *out << "</section><section><h3>I/O events</h3>";
    DiagnosticFields(out, "Since start", info.Lifecycle, {
        {"Unaligned write payloads", "Unaligned write payloads"}});
    *out << "</section><section><h3>Shutdown</h3><table class=\"table table-condensed\"><tbody>";
    const auto stopping = DiagnosticValue(info.Lifecycle, "Stopping");
    auto yesNo = [](TStringBuf value) -> TStringBuf {
        return value == "true" ? "Yes" : value == "false" ? "No" : "unknown";
    };
    Field(out, "Stopping", yesNo(stopping));
    Field(out, "Waiting for PB to stop", stopping == "false" ? "-"
        : yesNo(DiagnosticValue(info.Lifecycle, "Waiting for PB")));
    Field(out, "DDisk I/O drained", stopping == "false" ? "-"
        : yesNo(DiagnosticValue(info.Lifecycle, "Own I/O drained")));
    *out << "</tbody></table>";
    DiagnosticFields(out, "Outstanding work / now", info.Lifecycle, {
        {"Reservation in flight", "Chunk reservation in flight"},
        {"Log callbacks", "Log callbacks"}, {"Chunk commits in flight", "Chunk map commits"},
        {"Pending chunk allocations", "Chunk allocations"}, {"Delayed I/O retries", "I/O retries"},
        {"Pending checksum reads", "Checksum reads"}, {"Pending client writes", "Client writes"},
        {"Pending sync segments", "Sync segments"}, {"Read callbacks (PDisk fallback)", "PDisk read callbacks"},
        {"Write callbacks (PDisk fallback)", "PDisk write callbacks"}});
    *out << "</section></div><div><section class=\"ddisk-identity\">";
    Fields(out, "Identity", info.Identity);
    *out << "</section><section><h3>Recovery</h3><table class=\"table table-condensed\"><tbody>";
    auto recovery = [&](TStringBuf key, TStringBuf label) {
        const auto value = DiagnosticValue(info.Recovery, key);
        Field(out, label, value == "true" ? "Complete" : value == "false" ? "Pending" : "unknown");
    };
    recovery("PDisk initialized", "PDisk initialization");
    recovery("Log replay complete", "DDisk log replay");
    Field(out, "Orphan reservations remaining", DiagnosticValue(info.Recovery, "Startup orphan reservations remaining"));
    *out << "</tbody></table></section><section><h3>Log</h3>";
    DiagnosticFields(out, "Positions / LSN", info.Recovery, {
        {"NextLsn", "Next record"}, {"ChunkMapSnapshotLsn", "Chunk map snapshot"},
        {"FirstLsnToKeep", "First record to retain"}});
    DiagnosticFields(out, "Activity / since start", info.Recovery, {
        {"ReadLogChunks", "Log chunks read"}, {"LogRecordsProcessed", "Records processed"},
        {"LogRecordsApplied", "Records applied"}, {"LogRecordsWritten", "Records written"},
        {"NumChunkMapSnapshots", "Chunk map snapshots"}, {"NumChunkMapIncrements", "Chunk map increments"},
        {"CutLogMessages", "Cut log messages"}});
    *out << "</section></div></div>";
}

void Table(TStringStream* out, std::initializer_list<TStringBuf> headers) {
    *out << "<div class=\"table-responsive\"><table class=\"table table-condensed\"><thead><tr>";
    for (const auto header : headers) {
        *out << "<th>" << Html(header) << "</th>";
    }
    *out << "</tr></thead><tbody>";
}

void EndTable(TStringStream* out) {
    *out << "</tbody></table></div>";
}

enum class EOperationScope {
    DDisk,
    DirectIo
};

struct TOperationSeries {
    TStringBuf Name;
    TStringBuf Label;
    TStringBuf Color;
};

constexpr TOperationSeries OperationSeries[] = {
    {"Read", "Read", "#377bba"},
    {"Write", "Write", "#9e79bd"},
    {"Sync", "Sync", "#5caaa5"},
    {"Read", "Read", "#377bba"},
    {"Write", "Write", "#9e79bd"},
};

void Operations(TStringStream* out, TStringBuf title, const std::vector<TDDiskMonOperation>& operations,
    EOperationScope scope, bool compact = false) {
    if (!title.empty()) {
        *out << "<h3>" << Html(title) << "</h3>";
    }
    if (compact) {
        *out << "<style>.ddisk-operation-table td:not(:first-child),.ddisk-operation-table th:not(:first-child)"
            "{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;}"
            ".ddisk-operation-swatch{display:inline-block;width:9px;height:9px;border-radius:2px;margin-right:7px;}"
            "</style><div class=\"ddisk-operation-table\">";
        if (scope == EOperationScope::DirectIo) {
            Table(out, {"Operation", "Requests total", "Transferred total", "In flight now"});
        } else {
            Table(out, {"Operation", "Last IOPS", "Last throughput", "Requests total", "OK total", "Errors total", "Transferred total", "In flight now"});
        }
        const size_t first = scope == EOperationScope::DDisk ? 0 : 3;
        const size_t last = scope == EOperationScope::DDisk ? 3 : std::size(OperationSeries);
        for (size_t index = first; index < last; ++index) {
            const auto& series = OperationSeries[index];
            const auto it = std::find_if(operations.begin(), operations.end(), [&series](const auto& op) {
                return op.Name == series.Name;
            });
            if (it == operations.end()) {
                continue;
            }
            const auto& op = *it;
            *out << "<tr><td><span class=\"ddisk-operation-swatch\" aria-hidden=\"true\" style=\"background:"
                << series.Color << "\"></span>" << Html(series.Label) << "</td>";
            if (scope != EOperationScope::DirectIo) {
                Cell(out, op.Rate ? GroupDigits(Sprintf("%.1f", op.Rate->Iops)) : TString("unknown"));
                Cell(out, op.Rate ? ByteRate(op.Rate->BytesPerSecond) : TString("unknown"));
            }
            Number(out, op.Requests);
            if (scope != EOperationScope::DirectIo) {
                Number(out, op.ReplyOk);
                Number(out, op.ReplyErr);
            }
            Cell(out, Bytes(op.Bytes));
            *out << "<td>" << GroupDigits(ToString(op.InFlight)) << " <span class=\"text-muted\">(" << Bytes(op.BytesInFlight)
                << ")</span></td></tr>";
        }
        EndTable(out);
        *out << "</div>";
        return;
    }
    if (scope == EOperationScope::DirectIo) {
        Table(out, {"Operation", "Requests", "In flight", "Bytes", "Bytes in flight"});
    } else {
        Table(out, {"Operation", "Requests", "In flight", "OK", "Error", "Bytes", "Bytes in flight"});
    }
    for (const auto& op : operations) {
        const bool isDDisk = op.Name == "Read" || op.Name == "Write" || op.Name == "Sync";
        if (scope == EOperationScope::DDisk && !isDDisk) {
            continue;
        }
        *out << "<tr>";
        Cell(out, op.Name);
        Number(out, op.Requests);
        Number(out, op.InFlight);
        if (scope != EOperationScope::DirectIo) {
            Number(out, op.ReplyOk);
            Number(out, op.ReplyErr);
        }
        Cell(out, Bytes(op.Bytes));
        Cell(out, Bytes(op.BytesInFlight));
        *out << "</tr>";
    }
    EndTable(out);
}

struct TSpaceAllocation {
    ui64 Quota;
    ui64 Allocated;
    ui64 Available;
    ui64 Shortfall;
    ui64 Classified;
};

std::optional<TSpaceAllocation> SpaceAllocation(const TDDiskMonInfo& info,
        const TPersistentBufferMonInfo* pb, ui64 dataChunks) {
    if (!pb || !pb->PDiskSpace || !pb->PDiskSpace->TotalChunks || !info.ChunkSize
            || pb->ChunkSize != info.ChunkSize) {
        return std::nullopt;
    }
    const auto& sample = *pb->PDiskSpace;
    const ui64 classified = dataChunks + info.IntegrityChunks + pb->AllocatedChunks + info.ReservedChunks;
    // Independent snapshots may disagree. Retain all known allocations.
    const ui64 allocated = std::max(sample.UsedChunks, classified);
    const ui64 remaining = sample.TotalChunks > allocated ? sample.TotalChunks - allocated : 0;
    const ui64 available = std::min(remaining, sample.FreeChunks);
    return TSpaceAllocation{sample.TotalChunks, allocated, available, remaining - available, classified};
}

bool SpaceShare(TStringStream* out, const TDDiskMonInfo& info, const TPersistentBufferMonInfo* pb) {
    const auto allocation = SpaceAllocation(info, pb, info.DataChunks);
    if (!allocation) {
        *out << "<p class=\"text-muted\">Fair share unavailable: waiting for a fresh PDisk space sample.</p>";
        return false;
    }
    const auto& sample = *pb->PDiskSpace;
    const auto [quota, allocated, available, shortfall, classified] = *allocation;
    const ui64 scale = std::max(quota, allocated);
    struct TSegment {
        TStringBuf Key;
        TStringBuf Name;
        ui64 Chunks;
    };
    const TSegment segments[] = {
        {"data", "Data chunks", info.DataChunks},
        {"integrity", "Checksums", info.IntegrityChunks},
        {"pb", "PersistentBuffer", pb->AllocatedChunks},
        {"reserve", "Empty reserve", info.ReservedChunks},
        {"other", "Other allocated", allocated - classified},
        {"available", "Unallocated share", available},
        {"shortfall", "Reserve shortfall", shortfall},
    };
    *out << "<style>.ddisk-space-summary{display:flex;flex-wrap:wrap;justify-content:space-between;gap:8px;margin-bottom:12px;}</style>"
        "<div class=\"ddisk-space-summary\"><span>DDisk + PB fair share <strong>"
        << Capacity(sample.TotalChunks, info.ChunkSize) << "</strong></span><span>Allocated <strong>"
        << Capacity(allocated, info.ChunkSize) << " / " << Sprintf("%.1f%%", 100.0 * allocated / sample.TotalChunks)
        << "</strong></span></div>";
    NJson::TJsonValue data(NJson::JSON_MAP);
    data["title"] = "DDisk + PB space";
    data["entityLabel"] = "Resource";
    data["unit"] = "bytes";
    data["legend"] = true;
    data["capacity"] = scale * info.ChunkSize;
    data["segments"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    const std::array<TStringBuf, 7> colors = {"#377bba", "#9e79bd", "#5caaa5", "#d4d7dc", "#939ba5", "#f0f2f5", "#f5d6d6"};
    for (size_t i = 0; i < std::size(segments); ++i) {
        const auto& segment = segments[i];
        if (!segment.Chunks && (segment.Key == "shortfall" || segment.Key == "other")) {
            continue;
        }
        NJson::TJsonValue value(NJson::JSON_MAP);
        value["key"] = TString(segment.Key);
        value["label"] = TString(segment.Name);
        value["value"] = segment.Chunks * info.ChunkSize;
        value["color"] = TString(colors[i]);
        if (segment.Key == "shortfall") {
            value["pattern"] = "striped";
        }
        data["segments"].AppendValue(std::move(value));
    }
    *out << "<div data-allocation=\"" << Html(NJson::WriteJson(data, false)) << "\"></div>";
    if (allocated > sample.TotalChunks) {
        *out << "<p class=\"text-warning\">Above fair share by "
            << Capacity(allocated - sample.TotalChunks, info.ChunkSize) << ". Bar scaled to allocated space.</p>";
    }
    return true;
}

void SpaceHistory(TStringStream* out, const TDDiskMonInfo& info);

void MemoryHistory(TStringStream* out, const TDDiskMonInfo& info);

void Space(TStringStream* out, const TDDiskMonInfo& info, const TPersistentBufferMonInfo* pb) {
    *out << "<h3>Space</h3>";
    SpaceHistory(out, info);
    SpaceShare(out, info, pb);
    *out << "<style>"
        ".ddisk-space-details{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:30px;margin-top:24px;}"
        ".ddisk-space-details>section{min-width:0;}"
        ".ddisk-space-details .ddisk-memory-history{grid-template-columns:minmax(0,1fr);}"
        ".ddisk-space-details h3{margin-top:0;}"
        ".ddisk-space-title{display:flex;flex-wrap:wrap;align-items:baseline;justify-content:space-between;gap:8px;}"
        ".ddisk-space-title>span,.ddisk-space-unit{color:#777;font-size:12px;}"
        ".ddisk-space-details td:not(:first-child),.ddisk-space-details th:not(:first-child){text-align:right;font-variant-numeric:tabular-nums;}"
        ".ddisk-space-details td:not(:first-child){white-space:nowrap;}"
        ".ddisk-space-resource td:first-child:before{content:'';display:inline-block;width:9px;height:9px;border-radius:2px;margin-right:8px;}"
        ".ddisk-space-resource-data td:first-child:before{background:#377bba;}"
        ".ddisk-space-resource-integrity td:first-child:before{background:#9e79bd;}"
        ".ddisk-space-resource-pb td:first-child:before{background:#5caaa5;}"
        ".ddisk-space-resource-reserve td:first-child:before{background:#d4d7dc;}"
        ".ddisk-space-total{font-weight:600;}"
        ".ddisk-space-processes{display:flex;flex-wrap:wrap;gap:8px 22px;color:#777;margin-top:14px;}"
        ".ddisk-space-processes strong{color:#333;margin-left:5px;}"
        "@media(max-width:767px){.ddisk-space-details{grid-template-columns:1fr;}}"
        "</style><div class=\"ddisk-space-details\"><section>"
        "<div class=\"ddisk-space-title\"><h3>Allocated chunks</h3><span>Chunk size <strong>"
        << (info.ChunkSize ? Bytes(info.ChunkSize) : TString("unknown")) << "</strong></span></div>";
    Table(out, {"Resource", "Chunks", "Allocation"});
    const auto row = [&](TStringBuf key, TStringBuf name, ui64 count, ui64 size) {
        *out << "<tr class=\"ddisk-space-resource ddisk-space-resource-" << key << "\">";
        Cell(out, name);
        Number(out, count);
        Cell(out, Capacity(count, size));
        *out << "</tr>";
    };
    row("data", "Data", info.DataChunks, info.ChunkSize);
    row("integrity", "Checksums", info.IntegrityChunks, info.ChunkSize);
    if (pb) {
        row("pb", "PersistentBuffer", pb->AllocatedChunks, pb->ChunkSize);
    } else {
        *out << "<tr>";
        Cell(out, "PersistentBuffer");
        Cell(out, "unknown");
        Cell(out, "unknown");
        *out << "</tr>";
    }
    row("reserve", "Empty reserve", info.ReservedChunks, info.ChunkSize);
    *out << "<tr class=\"ddisk-space-total\">";
    Cell(out, "Classified total");
    Cell(out, pb ? ToString(info.DataChunks + info.IntegrityChunks + info.ReservedChunks + pb->AllocatedChunks)
        : TString("unknown"));
    Cell(out, pb && info.ChunkSize && pb->ChunkSize
        ? Bytes((info.DataChunks + info.IntegrityChunks + info.ReservedChunks) * info.ChunkSize + pb->AllocatedChunks * pb->ChunkSize)
        : TString("unknown"));
    *out << "</tr>";
    EndTable(out);
    *out << "<div class=\"ddisk-space-processes\"><span>Allocating <strong>" << info.AllocationsInFlight
        << "</strong></span><span>Formatting <strong>" << info.FormattingChunks
        << "</strong></span><span>Releasing <strong>" << info.PendingRelease
        << "</strong></span></div></section><section>";
    MemoryHistory(out, info);
    *out << "</section></div>";
}

// Data stays in the monitoring snapshot. Shared components own only presentation.
void ChartData(TStringStream* out, const NJson::TJsonValue& data) {
    *out << "<div class=\"ddisk-metric-chart\" data-chart=\""
        << Html(NJson::WriteJson(data, false)) << "\"></div>";
}

NJson::TJsonValue Chart(TStringBuf title, TStringBuf unit, TInstant now, bool stacked = false) {
    NJson::TJsonValue data(NJson::JSON_MAP);
    data["title"] = TString(title);
    data["begin"] = (now - TDuration::Minutes(5)).MilliSeconds();
    data["end"] = now.MilliSeconds();
    data["emptyText"] = "Collecting history";
    data["settings"]["unit"] = TString(unit);
    data["settings"]["type"] = stacked ? "area" : "line";
    data["settings"]["fill"] = true;
    data["settings"]["height"] = 220;
    data["settings"]["precision"] = 2;
    data["series"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    return data;
}

NJson::TJsonValue Series(TStringBuf name, TStringBuf color, bool step = false) {
    NJson::TJsonValue series(NJson::JSON_MAP);
    series["key"] = TString(name);
    series["display"] = TString(name);
    series["color"] = TString(color);
    series["step"] = step;
    series["width"] = 1.5;
    series["points"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    return series;
}

void Point(NJson::TJsonValue* series, TInstant time, std::optional<double> value) {
    NJson::TJsonValue point(NJson::JSON_MAP);
    point["time"] = time.MilliSeconds();
    if (value) {
        point["value"] = *value;
        point["raw"] = ToString(*value);
    } else {
        point["value"] = NJson::TJsonValue(NJson::JSON_NULL);
        point["raw"] = NJson::TJsonValue(NJson::JSON_NULL);
    }
    (*series)["points"].AppendValue(std::move(point));
}

void SpaceHistory(TStringStream* out, const TDDiskMonInfo& info) {
    *out << "<section class=\"ddisk-space-history\"><h4>Allocated space history</h4>";
    std::map<ui64, std::array<std::optional<ui64>, 4>> samples;
    for (size_t i = 0; i < info.SpaceHistory.size(); ++i) {
        const auto& history = info.SpaceHistory[i];
        if (!history.Error.empty()) {
            *out << "<p class=\"text-muted\">" << Html(history.Error) << "</p></section>";
            return;
        }
        for (const auto& sample : history.Samples) {
            if (sample.Timestamp <= info.CollectedAt && info.CollectedAt - sample.Timestamp < TDuration::Minutes(5)) {
                samples[sample.Timestamp.Seconds()][i] = sample.Bytes;
            }
        }
    }
    auto data = Chart("Allocated space history", "bytes", info.CollectedAt, true);
    data["emptyText"] = "Collecting space history";
    data["tooltipTotal"] = "Allocated";
    const std::array<TStringBuf, 4> colors = {"#377bba", "#9e79bd", "#5caaa5", "#d4d7dc"};
    const std::array<TStringBuf, 4> names = {"Data chunks", "Checksums", "PersistentBuffer", "Empty reserve"};
    for (size_t i = 0; i < names.size(); ++i) {
        auto series = Series(names[i], colors[i], true);
        std::optional<ui64> previous;
        for (const auto& [second, values] : samples) {
            if (previous && second > *previous + 1) {
                Point(&series, TInstant::Seconds(*previous + 1), {});
            }
            const bool complete = std::all_of(values.begin(), values.end(), [](const auto& v) { return v.has_value(); });
            Point(&series, TInstant::Seconds(second), complete ? std::optional<double>(*values[i]) : std::nullopt);
            previous = second;
        }
        // These are one-second samples, not an indefinitely valid on-change value.
        if (previous && TInstant::Seconds(*previous + 1) < info.CollectedAt) {
            Point(&series, TInstant::Seconds(*previous + 1), {});
        }
        data["series"].AppendValue(std::move(series));
    }
    ChartData(out, data);
    *out << "</section>";
}

void MemoryChart(TStringStream* out, TStringBuf title, TStringBuf tooltipLabel, TStringBuf color,
        const TDDiskMonMemory& memory, TInstant now) {
    *out << "<section class=\"ddisk-memory-chart\"><h4>" << Html(title) << "</h4>";
    if (!memory.Error.empty()) {
        *out << "<p class=\"text-muted\">" << Html(memory.Error) << "</p></section>";
        return;
    }
    if (!memory.Samples.empty()) {
        *out << "<div class=\"ddisk-memory-values\"><span>Latest <strong>" << Bytes(memory.Samples.back().Bytes)
            << "</strong></span><span>Limit <strong>" << Bytes(memory.Limit) << "</strong></span></div>";
    }
    auto data = Chart(title, "bytes", now);
    auto series = Series(tooltipLabel, color);
    std::optional<TInstant> previous;
    for (const auto& sample : memory.Samples) {
        if (sample.Timestamp < now - TDuration::Minutes(5) || sample.Timestamp > now) {
            continue;
        }
        if (previous && sample.Timestamp.Seconds() > previous->Seconds() + 1) {
            Point(&series, *previous + TDuration::Seconds(1), {});
        }
        Point(&series, sample.Timestamp, sample.Bytes);
        previous = sample.Timestamp;
    }
    data["series"].AppendValue(std::move(series));
    ChartData(out, data);
    *out << "</section>";
}

void HistoryChartStyles(TStringStream* out) {
    *out << "<style>.ddisk-memory-history{display:grid;grid-template-columns:1fr 1fr;gap:30px;}"
        ".ddisk-memory-history>section{min-width:0;}"
        ".ddisk-memory-values{display:flex;justify-content:space-between;color:#777;font-size:12px;}"
        ".ddisk-memory-values strong{color:#333;}"
        "@media(max-width:767px){.ddisk-memory-history{grid-template-columns:1fr;}}"
        "</style>";
}

void MetricComponents(TStringStream* out) {
    *out << R"HTML(<link rel="stylesheet" href="../../static/metric-chart/chart.css">
<script type="module">
import {createMetricChart,createMetricChartCursorGroup,formatMetricValue} from '../../static/metric-chart/chart.js';
import {createAllocationBar} from '../../static/metric-chart/allocation.js';
const charts=[],cursorGroup=createMetricChartCursorGroup();
for(const host of document.querySelectorAll('[data-chart]')){
    const data=JSON.parse(host.dataset.chart);
    const chart=createMetricChart(host,{
        settings:data.settings,legend:false,tooltipOrder:'series',tooltipTotal:data.tooltipTotal||false,
        cursorGroup,plotLeft:100,
    });
    chart.setData(data);charts.push(chart);
}
const bars=[],allocations=[];
const rows=document.querySelectorAll('[data-tablet-row]');
function highlight(key){
    bars.forEach(bar=>bar.highlight(key));
    rows.forEach(row=>row.classList.toggle('share-highlight',row.dataset.tabletRow===key));
}
for(const host of document.querySelectorAll('[data-allocation]')){
    const data=JSON.parse(host.dataset.allocation);
    const bar=createAllocationBar(host,{
        mode:'prepared',summary:false,legend:data.legend,segmentLabels:data.segmentLabels,
        title:data.title,entityLabel:data.entityLabel,emptyText:data.emptyText,
        formatValue:value=>formatMetricValue(value,{unit:data.unit,precision:2}),
        onHover:host.dataset.shareBar?segment=>highlight(segment?.key):undefined,
    });
    bar.setData(data);allocations.push(bar);if(host.dataset.shareBar)bars.push(bar);
}
for(const row of rows){
    row.addEventListener('pointerenter',()=>highlight(row.dataset.tabletRow));
    row.addEventListener('pointerleave',()=>highlight(null));
    row.addEventListener('focusin',()=>highlight(row.dataset.tabletRow));
    row.addEventListener('focusout',()=>highlight(null));
}
window.addEventListener('pagehide',()=>{charts.forEach(chart=>chart.destroy());allocations.forEach(bar=>bar.destroy());},{once:true});
</script>)HTML";
}

void MemoryHistory(TStringStream* out, const TDDiskMonInfo& info) {
    HistoryChartStyles(out);
    *out << "<div class=\"ddisk-memory-history\">";
    MemoryChart(out, "DDisk checksum cache (estimated)", "Checksum cache", "#9e79bd", info.Memory, info.CollectedAt);
    *out << "</div>";
}

void OperationRateChart(TStringStream* out, const TDDiskMonInfo& info, EOperationScope scope, bool throughput) {
    const size_t first = scope == EOperationScope::DDisk ? 0 : 3;
    const size_t last = scope == EOperationScope::DDisk ? 3 : std::size(OperationSeries);
    *out << "<section><h4>" << (throughput ? "Throughput" : "Requests") << "</h4>";
    if (!info.OperationHistoryError.empty()) {
        *out << "<p class=\"text-muted\">" << Html(info.OperationHistoryError) << "</p></section>";
        return;
    }
    const TString title = TString(scope == EOperationScope::DDisk ? "DDisk " : "Shared DirectIO ")
        + (throughput ? "bytes per second" : "requests per second");
    auto data = Chart(title, throughput ? "bytesPerSecond" : "iops", info.CollectedAt);
    for (size_t op = first; op < last; ++op) {
        auto series = Series(OperationSeries[op].Label, OperationSeries[op].Color);
        std::optional<TInstant> previous;
        for (const auto& sample : info.RateHistory) {
            if (sample.Timestamp < info.CollectedAt - TDuration::Minutes(5) || sample.Timestamp > info.CollectedAt) {
                continue;
            }
            if (previous && sample.Timestamp.Seconds() > previous->Seconds() + 1) {
                Point(&series, *previous + TDuration::Seconds(1), {});
            }
            const auto& rate = sample.Rates[op];
            Point(&series, sample.Timestamp, rate
                ? std::optional<double>(throughput ? rate->BytesPerSecond : rate->Iops) : std::nullopt);
            previous = sample.Timestamp;
        }
        data["series"].AppendValue(std::move(series));
    }
    ChartData(out, data);
    *out << "</section>";
}

void OperationHistory(TStringStream* out, const TDDiskMonInfo& info, EOperationScope scope) {
    *out << "<h3>" << (scope == EOperationScope::DDisk ? "Logical DDisk operations"
        : "Shared DirectIO counters / DDisk + PersistentBuffer")
        << "</h3><div class=\"ddisk-memory-history\">";
    OperationRateChart(out, info, scope, false);
    OperationRateChart(out, info, scope, true);
    *out << "</div>";
}

void WaitingRequests(TStringStream* out, const TDDiskMonInfo& info) {
    const auto metric = [out](TStringBuf label, std::optional<ui64> value) {
        *out << "<tr>";
        Cell(out, label);
        *out << "<td>";
        if (!value) {
            *out << "<span class=\"text-muted\">unknown</span>";
        } else if (!*value) {
            *out << "<span class=\"text-muted\">0</span>";
        } else {
            *out << "<strong>" << *value << "</strong>";
        }
        *out << "</td></tr>";
    };
    *out << "<style>.ddisk-waiting-requests td:last-child{text-align:right;font-variant-numeric:tabular-nums;}"
        ".ddisk-waiting-columns{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:30px;}"
        "@media(max-width:700px){.ddisk-waiting-columns{grid-template-columns:1fr;gap:0;}}"
        "</style><div class=\"ddisk-waiting-requests\"><h3>Requests waiting for</h3>"
        "<div class=\"ddisk-waiting-columns\"><section><table class=\"table table-condensed\"><tbody>";
    metric("DDisk initialization / recovery", info.PendingQueries);
    metric("Data chunk allocation", info.PendingAllocation);
    *out << "</tbody></table></section><section><table class=\"table table-condensed\"><tbody>";
    metric("Previous write to the same chunk", info.PendingIntegrity);
    *out << "</tbody></table></section></div></div>";
}

void TabletField(TStringStream* out, TStringBuf name, TStringBuf value) {
    *out << "<div><dt>" << Html(name) << "</dt><dd>" << Html(value) << "</dd></div>";
}

void Tablet(TStringStream* out, const TDDiskMonInfo& info,
    const TDDiskMonQuery& query) {
    const ui64 tabletId = *query.TabletId;
    const TString listQuery = "tab=tablets" + (query.AfterTabletId
        ? "&afterTabletId=" + ToString(*query.AfterTabletId) : TString());
    const TString tabletQuery = listQuery + "&tabletId=" + ToString(tabletId);
    const auto pageLink = [&query](TStringBuf target, TStringBuf label, TStringBuf fragment = {}) {
        TString url(target);
        if (query.RefreshRate) {
            url += "&refreshRate=" + ToString(query.RefreshRate);
        }
        url += fragment;
        return Link(url, label);
    };
    struct TGroup {
        const TDDiskMonConnection* Session = nullptr;
    };
    std::map<ui32, TGroup> groups;
    for (const auto& session : info.Connections) {
        if (session.TabletId == tabletId) {
            groups[session.DirectBlockGroupIndex].Session = &session;
        }
    }
    *out << R"HTML(<style>
.ddisk-tablet h2{overflow-wrap:anywhere;}
.ddisk-tablet-tabs{display:flex;border-bottom:1px solid #ddd;margin:20px 0;}
.ddisk-tablet-tabs a{padding:10px 16px;border:1px solid transparent;border-radius:4px 4px 0 0;margin-bottom:-1px;}
.ddisk-tablet-tabs a[aria-selected=true]{color:#333;background:#fff;border-color:#ddd;border-bottom-color:#fff;}
.ddisk-tablet-panel[hidden]{display:none;}
.ddisk-tablet-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:24px;}
.ddisk-tablet-summary{padding-bottom:18px;border-bottom:1px solid #ddd;}
.ddisk-tablet-fields{margin:0;font-variant-numeric:tabular-nums;}
.ddisk-tablet-fields>div{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:12px;margin:5px 0;}
.ddisk-tablet-fields dt{font-weight:normal;color:#666;}
.ddisk-tablet-fields dd{margin:0;overflow-wrap:anywhere;}
.ddisk-connection-type{display:inline-block;background:#eef2f5;border-radius:3px;padding:2px 7px;margin-right:6px;}
@media(max-width:600px){.ddisk-tablet-grid{grid-template-columns:minmax(0,1fr);gap:12px;}}
</style><div class="ddisk-tablet" id="ddisk-tablet-detail">)HTML";
    const auto disk = std::find_if(info.Tablets.begin(), info.Tablets.end(), [tabletId](const auto& row) {
        return row.TabletId == tabletId;
    });
    *out << "<p>&larr; " << pageLink(listQuery, "Tablets") << "</p><p class=\"text-muted\">DDisk "
        << Html(info.Id) << " / " << tabletId << "</p><h2>Tablet " << tabletId << "</h2><p>Node "
        << info.NodeId << " / PDisk " << info.PDiskId << " / Slot " << info.SlotId << "</p>"
        << "<nav class=\"ddisk-tablet-tabs\" role=\"tablist\" aria-label=\"Tablet sections\">"
        "<a id=\"ddisk-overview-tab\" href=\"#overview\" role=\"tab\" aria-controls=\"ddisk-overview-panel\">Overview</a>"
        "<a id=\"ddisk-chunks-tab\" href=\"#chunks\" role=\"tab\" aria-controls=\"ddisk-chunks-panel\">Chunks</a>"
        "<a id=\"ddisk-connections-tab\" href=\"#connections\" role=\"tab\" aria-controls=\"ddisk-connections-panel\">DBG connections</a>"
        "</nav><section id=\"ddisk-overview-panel\" class=\"ddisk-tablet-panel\" role=\"tabpanel\" aria-labelledby=\"ddisk-overview-tab\">"
        "<div class=\"ddisk-tablet-grid\"><section><h3>Data</h3><dl class=\"ddisk-tablet-fields\">";
    TabletField(out, "Allocated", disk != info.Tablets.end() ? Capacity(disk->DataChunks, info.ChunkSize)
        : info.MoreTablets ? TString("unknown") : TString("0 B"));
    TabletField(out, "Mapped chunks", disk != info.Tablets.end() ? ToString(disk->DataChunks)
        : info.MoreTablets ? TString("unknown") : TString("0"));
    if (info.StatsAvailable) {
        const double chunks = disk != info.Tablets.end() ? disk->DataChunks : 0;
        TabletField(out, "Share of tablet space", Sprintf("%.1f%%", info.StatsChunks ? chunks * 100 / info.StatsChunks : 0));
    }
    *out << "</dl></section><section><h3>DDisk operations</h3>";
    if (info.StatsAvailable && !info.TabletStats.empty()) {
        const auto& stats = info.TabletStats.front();
        Table(out, {"Operation", "IOPS", "Throughput"});
        const std::array<TStringBuf, 3> names = {"Read", "Write", "Sync"};
        double iops = 0, bytes = 0;
        for (size_t i = 0; i < names.size(); ++i) {
            *out << "<tr>";
            Cell(out, names[i]);
            Cell(out, Sprintf("%.1f", stats.Rates[i].Iops));
            Cell(out, ByteRate(stats.Rates[i].BytesPerSecond));
            *out << "</tr>";
            iops += stats.Rates[i].Iops;
            bytes += stats.Rates[i].BytesPerSecond;
        }
        EndTable(out);
        *out << "<dl class=\"ddisk-tablet-fields\">";
        TabletField(out, "Share of IOPS", Sprintf("%.1f%%", info.StatsIops ? iops * 100 / info.StatsIops : 0));
        TabletField(out, "Share of throughput", Sprintf("%.1f%%", info.StatsBytesPerSecond ? bytes * 100 / info.StatsBytesPerSecond : 0));
        *out << "</dl>";
    } else {
        *out << "<p class=\"text-muted\">Tablet operation statistics unavailable.</p>";
    }
    *out << "</section></div></section><section id=\"ddisk-chunks-panel\" class=\"ddisk-tablet-panel\" role=\"tabpanel\" aria-labelledby=\"ddisk-chunks-tab\"><h3>Chunks</h3><div class=\"table-responsive\">";
    Table(out, {"Virtual chunk", "Physical chunk", "Allocated", "Checksum chunk", "Checksum slot", "Requests in flight", "Waiting for allocation", "Waiting for integrity"});
    auto chunks = info.Chunks;
    std::sort(chunks.begin(), chunks.end(), [](const auto& a, const auto& b) { return a.VChunk < b.VChunk; });
    ui32 chunkCount = 0;
    ui64 lastChunk = 0;
    bool moreChunks = info.MoreChunks;
    for (const auto& chunk : chunks) {
        if (query.AfterVChunk && chunk.VChunk <= *query.AfterVChunk) {
            continue;
        }
        if (chunkCount == TDDiskMonQuery::MaxRows) {
            moreChunks = true;
            break;
        }
        ++chunkCount;
        lastChunk = chunk.VChunk;
        *out << "<tr>";
        Number(out, chunk.VChunk);
        Cell(out, chunk.PhysicalChunk ? ToString(chunk.PhysicalChunk) : TString("unmapped"));
        Cell(out, chunk.PhysicalChunk ? Capacity(1, info.ChunkSize) : TString("0 B"));
        Cell(out, chunk.IntegrityChunk ? ToString(*chunk.IntegrityChunk) : TString("none"));
        Cell(out, chunk.IntegritySlot ? ToString(*chunk.IntegritySlot) : TString("none"));
        Number(out, chunk.InFlight);
        Number(out, chunk.PendingAllocation);
        Number(out, chunk.PendingIntegrity);
        *out << "</tr>";
    }
    EndTable(out);
    *out << "</div>";
    if (!chunkCount) {
        *out << "<p>No chunks in this page.</p>";
    }
    if (moreChunks && chunkCount) {
        *out << "<p>" << pageLink(tabletQuery + "&afterVChunk=" + ToString(lastChunk), "Next mappings", "#chunks") << "</p>";
    }
    *out << "</section><section id=\"ddisk-connections-panel\" class=\"ddisk-tablet-panel\" role=\"tabpanel\" aria-labelledby=\"ddisk-connections-tab\"><h3>DBG connections</h3>";
    *out << "<div class=\"table-responsive\">";
    Table(out, {"DBG index", "Node", "Generation", "Sequence", "Interconnect session"});
    for (const auto& [index, group] : groups) {
        const auto& session = *group.Session;
        *out << "<tr>";
        Number(out, index);
        Number(out, session.NodeId);
        Number(out, session.Generation);
        Number(out, session.Sequence);
        Cell(out, session.InterconnectSession);
        *out << "</tr>";
    }
    EndTable(out);
    *out << "</div>";
    if (groups.empty()) {
        *out << "<p>No DBG connections available.</p>";
    }
    if (info.MoreConnections) {
        *out << "<p class=\"text-muted\">Session list truncated.</p>";
    }
    *out << R"HTML(</section><script>(function(){
var root=document.getElementById('ddisk-tablet-detail');
var tabs=Array.from(root.querySelectorAll('[role=tab]'));
function selectTab(){
    var selected=['#connections','#chunks'].includes(window.location.hash)?window.location.hash:'#overview';
    tabs.forEach(function(tab){
        var active=tab.getAttribute('href')===selected;
        tab.setAttribute('aria-selected',String(active));
        tab.tabIndex=active?0:-1;
        document.getElementById(tab.getAttribute('aria-controls')).hidden=!active;
    });
}
selectTab();
window.addEventListener('hashchange',selectTab);
root.querySelector('[role=tablist]').addEventListener('keydown',function(event){
    var index=tabs.indexOf(event.target);
    if(index<0)return;
    if(event.key==='ArrowRight')index=(index+1)%tabs.length;
    else if(event.key==='ArrowLeft')index=(index+tabs.length-1)%tabs.length;
    else if(event.key==='Home')index=0;
    else if(event.key==='End')index=tabs.length-1;
    else return;
    event.preventDefault();
    window.location.hash=tabs[index].getAttribute('href');
    selectTab();
    tabs[index].focus();
});
})();</script></div>)HTML";
}

TDDiskMonRate TabletRate(const TDDiskMonTabletStats& row) {
    TDDiskMonRate result;
    for (const auto& rate : row.Rates) {
        result.Iops += rate.Iops;
        result.BytesPerSecond += rate.BytesPerSecond;
    }
    return result;
}

TStringBuf TabletColor(ui32 slot) {
    // Distinct palette slots, shared by all bars and the table. Later colors
    // were selected by their distance in CIELAB, rather than by a TabletId hash.
    static constexpr std::array<TStringBuf, 90> Palette = {
        "#4e79a7", "#f28e2b", "#59a14f", "#b07aa1", "#e15759", "#76b7b2",
        "#b6992d", "#9c755f", "#2626ed", "#ed26cc", "#26ed26", "#1d2587",
        "#cced26", "#970c69", "#26edbc", "#968ef6", "#dcea9a", "#a65af2",
        "#97460c", "#f68eed", "#5af280", "#762d3f", "#ed2679", "#f6c28e",
        "#2d7658", "#ed2626", "#26bced", "#8b0c97", "#70762d", "#85b526",
        "#edcc26", "#f68e9f", "#26eded", "#2668ed", "#0c970c", "#2d3976",
        "#8eb0f6", "#4a26b5", "#970c18", "#cc26ed", "#106ecb", "#6a2d76",
        "#e0896c", "#9aeac2", "#ddaba6", "#1d7e87", "#f25ab2", "#b0f68e",
        "#8a55be", "#99f25a", "#be5581", "#26b591", "#cb4f10", "#87631d",
        "#9f453c", "#970c46", "#b6be55", "#10cb7d", "#9ac8ea", "#be55b6",
        "#f2b25a", "#c2a6dd", "#b37ece", "#10cb3f", "#76452d", "#f2f25a",
        "#2647ed", "#be9255", "#7d10cb", "#ed2658", "#63871d", "#ea9ad6",
        "#ddd4a6", "#10bbcb", "#8eedf6", "#cb109c", "#7ece8b", "#2d7639",
        "#edab26", "#269aed", "#5ecb10", "#2d5e76", "#e55af2", "#d67d3d",
        "#7e84ce", "#0c978b", "#dda6c2", "#55be55", "#2691b5", "#f25a80",
    };
    return Palette.at(slot);
}

void TabletResources(TStringStream* out, const TDDiskMonInfo& info, const TPersistentBufferMonInfo* pb, const TDDiskMonQuery& query) {
    const auto color = [&](ui64 id) {
        const auto it = info.StatsColorSlots.find(id);
        return it == info.StatsColorSlots.end() ? TStringBuf("#d5d8de") : TabletColor(it->second);
    };
    const auto percent = [](double value, double total) {
        if (total <= 0) {
            return TString("-");
        }
        const double share = std::max(0.0, value / total * 100);
        return share > 0 && share < .1 ? TString("<0.1%") : Sprintf("%.1f%%", share);
    };
    const ui64 dataChunks = std::max(info.StatsChunks, info.DataChunks);
    const auto allocation = SpaceAllocation(info, pb, dataChunks);
    const TString base = "tab=tablets";
    const auto link = [&query](TStringBuf target, TStringBuf label) {
        return Link(target, label, query.RefreshRate);
    };
    *out << R"(<style>
.ddisk-tablet-resources{position:relative;margin-top:22px;}
.ddisk-share-head{display:flex;align-items:center;gap:16px;flex-wrap:wrap;margin:20px 0 10px;}
.ddisk-share-head strong{font-size:16px;}.ddisk-share-total{margin-left:auto;}
.ddisk-tablet-resources .tablet-stats td:not(:first-child),.ddisk-tablet-resources .tablet-stats th:not(:first-child){text-align:right;font-variant-numeric:tabular-nums;}
.ddisk-tablet-resources .tablet-stats{margin-top:18px;}.ddisk-share-swatch{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:7px;}
.ddisk-tablet-resources tr.share-highlight{background:#f1f4f8;}
.ddisk-tablet-search{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:24px;}
.ddisk-tablet-search input[type=text]{max-width:310px;width:100%;}.ddisk-tablet-pages{display:flex;gap:20px;justify-content:flex-end;}
</style><section class="ddisk-tablet-resources" aria-label="Tablet resource shares">)";
    const auto bar = [&](TStringBuf kind, TStringBuf title, const auto& shares, double total) {
        const bool space = kind == "space";
        const bool iops = kind == "iops";
        const auto value = [&](const auto& row) {
            const auto rate = TabletRate(row);
            return space ? double(row.Chunks) * info.ChunkSize : iops ? rate.Iops : rate.BytesPerSecond;
        };
        if (space) {
            if (!allocation) {
                *out << "<div class=\"ddisk-share-head\"><strong>Space / fair share</strong></div>"
                    "<p class=\"text-muted\">Fair share unavailable: waiting for a fresh PDisk space sample.</p>";
                return;
            }
            total = allocation->Quota * info.ChunkSize;
        }
        const TString formatted = space ? Bytes(ui64(total)) : iops ? Sprintf("%.1f ops/s", total) : ByteRate(total);
        *out << "<div class=\"ddisk-share-head\"><strong>" << title << "</strong>"
            << "<span class=\"ddisk-share-total\">" << formatted << "</span></div>";
        NJson::TJsonValue data(NJson::JSON_MAP);
        data["title"] = TString(title);
        data["entityLabel"] = space ? "Consumer" : "Tablet";
        data["unit"] = space ? "bytes" : iops ? "iops" : "bytesPerSecond";
        data["legend"] = false;
        data["segmentLabels"] = true;
        data["emptyText"] = space ? "No allocated data chunks" : "No I/O activity";
        data["capacity"] = space ? double(std::max(allocation->Quota, allocation->Allocated)) * info.ChunkSize : total;
        data["segments"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        const auto segment = [&](TStringBuf key, TStringBuf label, double amount, TStringBuf segmentColor, TString target) {
            NJson::TJsonValue item(NJson::JSON_MAP);
            item["key"] = TString(key);
            item["label"] = TString(label);
            item["value"] = amount;
            item["color"] = TString(segmentColor);
            if (!target.empty() && query.RefreshRate) {
                target += "&refreshRate=" + ToString(query.RefreshRate);
            }
            if (!target.empty()) {
                item["href"] = "?" + target;
            }
            data["segments"].AppendValue(std::move(item));
        };
        double selected = 0;
        for (const auto& row : shares) {
            const double amount = value(row);
            selected += amount;
            if (amount > 0 && total > 0) {
                const auto id = ToString(row.TabletId);
                segment(id, id, amount, color(row.TabletId), base + "&searchTabletId=" + id);
            }
        }
        const double rest = std::max(0.0, (space ? double(dataChunks) * info.ChunkSize : total) - selected);
        if (rest > total * 1e-12 && total > 0 && (space || info.StatsTablets > shares.size())) {
            const ui64 otherCount = info.StatsTablets > shares.size() ? info.StatsTablets - shares.size() : 0;
            const TString label = otherCount ? "Other / " + ToString(otherCount) + " tablets" : TString("Other data");
            TString target = otherCount ? base + "&other=" + TString(kind) : TString();
            const auto selectedId = query.SearchTabletId ? query.SearchTabletId : query.StatsSelectedTabletId;
            if (!target.empty() && selectedId) {
                target += "&highlightTabletId=" + ToString(*selectedId);
            }
            segment("others", label, rest, "#d5d8de", target);
            data["segments"].GetArraySafe().back()["segmentLabel"] = otherCount ? "Other / " + ToString(otherCount) : TString("Other data");
        }
        if (space) {
            segment("integrity", "Checksums", info.IntegrityChunks * info.ChunkSize, "#9e79bd", {});
            segment("pb", "PersistentBuffer", pb->AllocatedChunks * info.ChunkSize, "#5caaa5", {});
            segment("reserve", "Empty reserve", info.ReservedChunks * info.ChunkSize, "#d4d7dc", {});
            if (allocation->Allocated > allocation->Classified) {
                segment("unattributed", "Other allocated", (allocation->Allocated - allocation->Classified) * info.ChunkSize, "#939ba5", {});
            }
            segment("available", "Unallocated share", allocation->Available * info.ChunkSize, "#f0f2f5", {});
            if (allocation->Shortfall) {
                segment("shortfall", "Reserve shortfall", allocation->Shortfall * info.ChunkSize, "#f5d6d6", {});
                data["segments"].GetArraySafe().back()["pattern"] = "striped";
            }
        }
        *out << "<div data-share-bar=\"" << kind << "\" data-allocation=\""
            << Html(NJson::WriteJson(data, false)) << "\"></div>";
    };
    bar("iops", "IOPS", info.StatsShares, info.StatsIops);
    bar("throughput", "Throughput", info.StatsShares, info.StatsBytesPerSecond);
    bar("space", "Space / fair share", info.StatsShares, double(info.StatsChunks));
    *out << "<form class=\"ddisk-tablet-search\" method=\"get\">"
        "<input type=\"hidden\" name=\"tab\" value=\"tablets\">";
    if (query.RefreshRate) {
        *out << "<input type=\"hidden\" name=\"refreshRate\" value=\"" << query.RefreshRate << "\">";
    }
    *out << "<input class=\"form-control\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]*\" name=\"searchTabletId\""
        " aria-label=\"Search Tablet ID across this DDisk\" placeholder=\"Tablet ID\" value=\""
        << (query.SearchTabletId ? ToString(*query.SearchTabletId) : TString())
        << "\"><button class=\"btn btn-default\" type=\"submit\">Search</button>";
    if (query.SearchTabletId || !query.StatsOther.empty() || query.AfterTabletId) {
        *out << link(base, "All tablets");
    }
    *out << "<span class=\"text-muted\">" << info.StatsFilteredTablets << " tablets / 100 per page</span></form>";
    if (!query.StatsOther.empty() && !query.SearchTabletId) {
        *out << "<p>Other / " << (query.StatsOther == "space" ? "Allocated space" : query.StatsOther == "iops" ? "IOPS" : "Throughput") << "</p>";
    }
    *out << "<div class=\"table-responsive\"><table class=\"table table-condensed tablet-stats\"><thead><tr>"
        "<th>Tablet</th><th>IOPS (share)</th><th>Throughput (share)</th><th>Chunks (allocated bytes)</th><th>Share of fair quota</th>"
        "</tr></thead><tbody>";
    for (const auto& row : info.TabletStats) {
        const auto rate = TabletRate(row);
        *out << "<tr data-tablet-row=\"" << row.TabletId << "\" data-tablet-id=\"" << row.TabletId << "\"><td>"
            << "<span class=\"ddisk-share-swatch\" style=\"background:" << color(row.TabletId) << "\"></span>"
            << link("tab=tablets&tabletId=" + ToString(row.TabletId), ToString(row.TabletId)) << "</td>";
        Cell(out, GroupDigits(Sprintf("%.1f", rate.Iops)) + " (" + percent(rate.Iops, info.StatsIops) + ")");
        Cell(out, ByteRate(rate.BytesPerSecond) + " (" + percent(rate.BytesPerSecond, info.StatsBytesPerSecond) + ")");
        Cell(out, GroupDigits(ToString(row.Chunks)) + " (" + Bytes(row.Chunks * info.ChunkSize) + ")");
        Cell(out, allocation ? percent(double(row.Chunks), double(allocation->Quota)) : TString("-"));
        *out << "</tr>";
    }
    if (info.TabletStats.empty()) {
        *out << "<tr><td colspan=\"5\">" << (query.SearchTabletId ? "No data for this tablet on this DDisk" : "No tablets in this page") << "</td></tr>";
    }
    *out << "</tbody></table></div><div class=\"ddisk-tablet-pages\">";
    TString pageBase = base;
    if (query.StatsSelectedTabletId) {
        pageBase += "&highlightTabletId=" + ToString(*query.StatsSelectedTabletId);
    }
    if (!query.StatsOther.empty()) {
        pageBase += "&other=" + query.StatsOther;
    }
    if (query.AfterTabletId) {
        *out << link(pageBase, "First page");
    }
    if (info.StatsNextTabletId) {
        *out << link(pageBase + "&afterTabletId=" + ToString(*info.StatsNextTabletId), "Next tablets");
    }
    *out << "</div></section>";
}

void TopTablets(TStringStream* out, const TDDiskMonInfo& info, const TDDiskMonQuery& query) {
    *out << "<div class=\"ddisk-operation-heading\"><h3>Top 10 tablets</h3><div>";
    for (const auto& [key, title] : std::initializer_list<std::pair<TStringBuf, TStringBuf>>{
            {"iops", "IOPS"}, {"throughput", "Throughput"}, {"chunks", "Space"}}) {
        *out << "<a class=\"btn btn-default" << (query.StatsSort == key ? " active" : "")
            << "\" href=\"?tab=overview&amp;sort=" << key;
        if (query.RefreshRate) {
            *out << "&amp;refreshRate=" << query.RefreshRate;
        }
        *out << "\">" << title << "</a> ";
    }
    *out << "</div></div><div class=\"ddisk-operation-rates table-responsive\">";
    Table(out, {"Tablet", "IOPS", "Throughput", "Allocated space"});
    for (const auto& row : info.TabletStats) {
        const auto rate = TabletRate(row);
        *out << "<tr><td><a href=\"?tab=tablets&amp;tabletId=" << row.TabletId << "\">" << row.TabletId << "</a></td>";
        Cell(out, Sprintf("%.1f", rate.Iops));
        Cell(out, ByteRate(rate.BytesPerSecond));
        Cell(out, Bytes(row.Chunks * info.ChunkSize));
        *out << "</tr>";
    }
    if (info.TabletStats.empty()) {
        *out << "<tr><td colspan=\"4\">" << (info.StatsAvailable ? "No tablets" : "Tablet statistics unavailable") << "</td></tr>";
    }
    EndTable(out);
    *out << "</div>";
}

void Tablets(TStringStream* out, const TDDiskMonInfo& info, const TDDiskMonQuery& query) {
    const auto pageLink = [&query](TStringBuf target, TStringBuf label) {
        return Link(target, label, query.RefreshRate);
    };
    struct TRow {
        const TDDiskMonTablet* Disk = nullptr;
    };
    std::map<ui64, TRow> rows;
    for (const auto& t : info.Tablets) {
        if (!query.AfterTabletId || t.TabletId > *query.AfterTabletId) {
            rows[t.TabletId].Disk = &t;
        }
    }
    *out << "<h3>Tablets</h3>";
    Table(out, {"Tablet ID", "Mapped chunks", "Data allocation"});
    ui32 shown = 0;
    ui64 last = 0;
    for (const auto& [id, row] : rows) {
        if (shown == TDDiskMonQuery::MaxRows) {
            break;
        }
        ++shown;
        last = id;
        TString target = "tab=tablets&tabletId=" + ToString(id);
        if (query.AfterTabletId) {
            target += "&afterTabletId=" + ToString(*query.AfterTabletId);
        }
        *out << "<tr><td>" << pageLink(target, ToString(id)) << "</td>";
        // A missing entry in a truncated component page is unknown, not necessarily zero.
        if (row.Disk) {
            Number(out, row.Disk->DataChunks);
            Cell(out, Capacity(row.Disk->DataChunks, info.ChunkSize));
        } else {
            Cell(out, info.MoreTablets ? "unknown" : "0");
            Cell(out, info.MoreTablets ? "unknown" : "0 B");
        }
        *out << "</tr>";
    }
    EndTable(out);
    if (!shown) {
        *out << "<p>No tablets in this page.</p>";
    }
    const bool more = rows.size() > shown || info.MoreTablets;
    if (shown && more) {
        *out << "<p>" << pageLink("tab=tablets&afterTabletId=" + ToString(last), "Next tablets") << "</p>";
    }
    *out << "<p class=\"text-muted\">Shown " << shown << " tablets; at most " << TDDiskMonQuery::MaxRows << " per page.</p>";
}

} // namespace

TString RenderDDiskMonPage(const TDDiskMonInfo& info, const TPersistentBufferMonInfo* pb,
    const TDDiskMonQuery& query, TStringBuf pbError) {
    TStringStream out;
    const auto pageLink = [&query](TStringBuf target, TStringBuf label) {
        return Link(target, label, query.RefreshRate);
    };
    if (query.RefreshRate) {
        const ui64 timeout = std::min<ui64>(ui64(query.RefreshRate) * 1000, 2147483647);
        out << "<script>setTimeout(function(){window.location.reload();}," << timeout << ");</script>";
    }
    if (query.TabletId) {
        Tablet(&out, info, query);
        return out.Str();
    }
    const TString tab = query.Tab == "analytics" ? "tablets" : query.Tab == "tablets" || query.Tab == "space" || query.Tab == "operations" || query.Tab == "diagnostics" ? query.Tab : "overview";
    out << "<p>Node " << info.NodeId << " / PDisk " << info.PDiskId << " / Slot " << info.SlotId << "</p><ul class=\"nav nav-tabs\">";
    for (const auto& [key, title] : std::initializer_list<std::pair<TStringBuf, TStringBuf>>{{"overview", "Overview"}, {"tablets", "Tablets"}, {"space", "Space"}, {"operations", "Operations"}, {"diagnostics", "Diagnostics"}}) {
        out << "<li" << (tab == key ? " class=\"active\"" : "") << ">" << pageLink("tab=" + TString(key), title) << "</li>";
    }
    out << "</ul>";
    if (!pb && (tab == "overview" || tab == "space")) {
        out << "<p class=\"text-warning\">Shared space unavailable: " << Html(pbError.empty() ? TStringBuf("unknown") : pbError) << "</p>";
    }
    if (tab == "overview") {
        out << "<style>"
            ".ddisk-status{display:flex;align-items:center;flex-wrap:wrap;gap:12px 24px;margin:16px 0;}"
            ".ddisk-status-item{display:inline-flex;align-items:center;gap:7px;white-space:nowrap;}"
            ".ddisk-state,.ddisk-backend{display:inline-flex;align-items:center;padding:2px 8px;border-radius:4px;font-weight:600;}"
            ".ddisk-state:before{content:'';width:6px;height:6px;border-radius:50%;background:currentColor;margin-right:6px;}"
            ".ddisk-state-ready{background:#eaf5ee;color:#23733c;}"
            ".ddisk-state-broken{background:#fce9e9;color:#ac2929;}"
            ".ddisk-state-pending{background:#fff2d8;color:#805400;}"
            ".ddisk-state-unknown{background:#eee;color:#666;}"
            ".ddisk-backend{background:#eef2f6;color:#43566b;}"
            ".ddisk-uptime{margin-left:auto;color:#666;}"
            ".ddisk-uptime-value{color:#333;font-variant-numeric:tabular-nums;}"
            "</style><div class=\"ddisk-status\">"
            "<span class=\"ddisk-status-item\">DDisk " << StateBadge(info.State) << "</span>"
            "<span class=\"ddisk-status-item\">Backend <span class=\"ddisk-backend\">" << Html(info.Backend) << "</span></span>"
            "<span class=\"ddisk-status-item ddisk-uptime\">Uptime <span class=\"ddisk-uptime-value\">"
            << Uptime(info) << "</span></span></div>";
        if (!info.BrokenReason.empty()) {
            out << "<p class=\"text-danger\">DDisk: " << Html(info.BrokenReason) << "</p>";
        }
        if (info.PendingQueries || info.IoStalled) {
            out << "<p>Pending queries: " << info.PendingQueries << " / DDisk I/O stalled: "
                << (info.IoStalled ? "true" : "false") << "</p>";
        }
        out << "<div class=\"row\"><div class=\"col-md-6\"><h3>Space</h3>";
        if (!SpaceShare(&out, info, pb)) {
            out << "<table class=\"table table-condensed\"><tbody>";
            Field(&out, "Data mapped chunks", Capacity(info.DataChunks, info.ChunkSize));
            Field(&out, "Integrity", Capacity(info.IntegrityChunks, info.ChunkSize));
            Field(&out, "PersistentBuffer", pb ? Capacity(pb->AllocatedChunks, pb->ChunkSize) : TString("unknown"));
            Field(&out, "Free DDisk reserve", Capacity(info.ReservedChunks, info.ChunkSize));
            out << "</tbody></table>";
        }
        out << "</div><div class=\"col-md-6\"><style>"
            ".ddisk-operation-heading{display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap;gap:8px;}"
            ".ddisk-operation-heading span{color:#777;font-size:12px;}"
            ".ddisk-operation-rates td:not(:first-child),.ddisk-operation-rates th:not(:first-child)"
            "{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums;}"
            "</style><div class=\"ddisk-operation-heading\"><h3>DDisk operations</h3><span>"
            << (info.RateWindowSeconds ? Sprintf("%.1fs average", *info.RateWindowSeconds) : TString("Collecting rates"))
            << "</span></div><div class=\"ddisk-operation-rates\">";
        Table(&out, {"Operation", "IOPS", "Throughput"});
        for (TStringBuf name : {TStringBuf("Read"), TStringBuf("Write"), TStringBuf("Sync")}) {
            const auto it = std::find_if(info.Operations.begin(), info.Operations.end(), [name](const auto& op) {
                return op.Name == name;
            });
            out << "<tr>";
            Cell(&out, name);
            Cell(&out, it != info.Operations.end() && it->Rate ? Sprintf("%.1f", it->Rate->Iops) : TString("unknown"));
            Cell(&out, it != info.Operations.end() && it->Rate ? ByteRate(it->Rate->BytesPerSecond) : TString("unknown"));
            out << "</tr>";
        }
        EndTable(&out);
        out << "</div></div></div>";
        TopTablets(&out, info, query);
    } else if (tab == "tablets") {
        if (info.StatsAvailable) {
            TabletResources(&out, info, pb, query);
        } else {
            Tablets(&out, info, query);
        }
    } else if (tab == "space") {
        Space(&out, info, pb);
    } else if (tab == "operations") {
        HistoryChartStyles(&out);
        OperationHistory(&out, info, EOperationScope::DDisk);
        Operations(&out, {}, info.Operations, EOperationScope::DDisk, true);
        OperationHistory(&out, info, EOperationScope::DirectIo);
        Operations(&out, {}, info.DirectIo, EOperationScope::DirectIo, true);
        WaitingRequests(&out, info);
    } else {
        Diagnostics(&out, info);
    }
    MetricComponents(&out);
    return out.Str();
}
} // namespace NKikimr::NDDisk
