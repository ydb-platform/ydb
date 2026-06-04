#include "ddisk_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/util/stlog.h>

#include <ydb/library/pdisk_io/device_type.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NDDisk {

namespace {

TString FormatDuration(TDuration v) {
    TStringBuilder str;
    if (v.Days() > 1) {
        str << v.Days() << 'd';
    } else if (v.Hours() > 1) {
        str << v.Hours() << 'h';
    } else if (v.Minutes() > 1) {
        str << v.Minutes() << 'm';
    } else if (v.Seconds() > 1) {
        str << v.Seconds() << 's';
    } else {
        str << "just now";
    }
    return str;
}

TString FormatBytes(ui64 s) {
    TStringBuilder str;
    if (s >= (1ull << 40)) {
        str << Sprintf("%.2f", s / double(1ull << 40)) << " TiB";
    } else if (s >= (1ull << 30)) {
        str << Sprintf("%.2f", s / double(1ull << 30)) << " GiB";
    } else if (s >= (1ull << 20)) {
        str << Sprintf("%.2f", s / double(1ull << 20)) << " MiB";
    } else if (s >= (1ull << 10)) {
        str << Sprintf("%.2f", s / double(1ull << 10)) << " KiB";
    } else {
        str << s << " B";
    }
    return str;
}

i64 CounterVal(const NMonitoring::TDynamicCounters::TCounterPtr& c) {
    return c ? c->Val() : 0;
}

} // namespace

void TDDiskActor::RegisterMonPage() {
    auto* mon = AppData()->Mon;
    if (!mon) {
        return;
    }
    NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
    NMonitoring::TIndexMonPage* ddisksMonPage = actorsMonPage->RegisterIndexPage("ddisks", "DDisks");
    TString path = Sprintf("ddisk_p%09" PRIu32 "_s%09" PRIu32,
        BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
    TString name = TStringBuilder() << "DDisk " << DDiskId;
    mon->RegisterActorPage(ddisksMonPage, path, name, false,
        TActivationContext::ActorSystem(), SelfId());
}

void TDDiskActor::Handle(NMon::TEvHttpInfo::TPtr ev) {
    const TCgiParameters& params = ev->Get()->Request.GetParams();
    ui32 refreshRate = 0;
    if (params.Has("refreshRate")) {
        ui32 value = 0;
        if (TryFromString(params.Get("refreshRate"), value)) {
            refreshRate = value;
        }
    }

    STLOG(PRI_DEBUG, BS_DDISK, BSDD45, "TDDiskActor::Handle(TEvHttpInfo)",
        (DDiskId, DDiskId), (Sender, ev->Sender));

    const bool diskReady = HandlingQueries && DiskFormat;

    TStringStream str;
    HTML(str) {
        if (refreshRate > 0) {
            constexpr ui64 maxJsTimeoutMs = 2147483647ull;
            const ui64 refreshTimeoutMs = static_cast<ui64>(refreshRate) * 1000ull;
            str << "<script>setTimeout(function(){window.location.reload(1);}, "
                << (refreshTimeoutMs > maxJsTimeoutMs ? maxJsTimeoutMs : refreshTimeoutMs) << ");</script>";
        }

        if (!diskReady) {
            str << "<div style=\"color:#b94a48;\"><b>DDisk is initializing...</b> "
                << "PDisk init has not yet completed.</div>";
        }

        // --- Section 1: Overview ----------------------------------------------------
        str << "<h3>Overview</h3>";
        TABLE_CLASS("table") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "DDiskId"; } TABLED() { str << DDiskId; } }
                TABLER() { TABLED() { str << "ActorId"; } TABLED() { str << SelfId(); } }
                TABLER() { TABLED() { str << "DDiskInstanceGuid"; } TABLED() { str << DDiskInstanceGuid; } }
                TABLER() { TABLED() { str << "Uptime"; } TABLED() { str << FormatDuration(TInstant::Now() - StartedAt); } }
                TABLER() { TABLED() { str << "HandlingQueries"; } TABLED() { str << (HandlingQueries ? "true" : "false"); } }
                TABLER() { TABLED() { str << "PendingQueries"; } TABLED() { str << PendingQueries.size(); } }
            }
        }

        // --- Section 2: Identity / Topology -----------------------------------------
        str << "<h3>Identity / Topology</h3>";
        TABLE_CLASS("table") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "PDiskId"; } TABLED() { str << BaseInfo.PDiskId; } }
                TABLER() { TABLED() { str << "VDiskSlotId"; } TABLED() { str << BaseInfo.VDiskSlotId; } }
                TABLER() { TABLED() { str << "PDiskActorID"; } TABLED() { str << BaseInfo.PDiskActorID; } }
                TABLER() { TABLED() { str << "VDiskIdShort"; } TABLED() { str << BaseInfo.VDiskIdShort.ToString(); } }
                TABLER() { TABLED() { str << "StoragePoolName"; } TABLED() { str << BaseInfo.StoragePoolName; } }
                TABLER() { TABLED() { str << "DeviceType"; } TABLED() { str << NPDisk::DeviceTypeStr(BaseInfo.DeviceType, false); } }
                if (Info) {
                    TABLER() { TABLED() { str << "GroupID"; } TABLED() { str << Info->GroupID.GetRawId(); } }
                    TABLER() { TABLED() { str << "OrderNumber"; } TABLED() { str << Info->GetOrderNumber(BaseInfo.VDiskIdShort); } }
                }
                if (DiskFormat) {
                    TABLER() { TABLED() { str << "ChunkSize"; } TABLED() { str << FormatBytes(DiskFormat->ChunkSize); } }
                    TABLER() { TABLED() { str << "SectorSize"; } TABLED() { str << DiskFormat->SectorSize << " B"; } }
                } else {
                    TABLER() { TABLED() { str << "ChunkSize"; } TABLED() { str << "n/a"; } }
                    TABLER() { TABLED() { str << "SectorSize"; } TABLED() { str << "n/a"; } }
                }
            }
        }

        // --- Section 3: State / Recovery log progress -------------------------------
        str << "<h3>State / Recovery log progress</h3>";
        TABLE_CLASS("table") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "NextLsn"; } TABLED() { str << NextLsn; } }
                TABLER() {
                    TABLED() { str << "ChunkMapSnapshotLsn"; }
                    TABLED() {
                        if (ChunkMapSnapshotLsn == Max<ui64>()) {
                            str << "none";
                        } else {
                            str << ChunkMapSnapshotLsn;
                        }
                    }
                }
                TABLER() {
                    TABLED() { str << "FirstLsnToKeep"; }
                    TABLED() {
                        if (diskReady) {
                            str << GetFirstLsnToKeep();
                        } else {
                            str << "n/a";
                        }
                    }
                }
                TABLER() {
                    TABLED() { str << "NormalizedOccupancy"; }
                    TABLED() {
                        if (NormalizedOccupancy < 0) {
                            str << "unknown";
                        } else {
                            str << Sprintf("%.4f", NormalizedOccupancy);
                        }
                    }
                }
                TABLER() { TABLED() { str << "ReadLogChunks"; } TABLED() { str << CounterVal(Counters.RecoveryLog.ReadLogChunks); } }
                TABLER() { TABLED() { str << "LogRecordsProcessed"; } TABLED() { str << CounterVal(Counters.RecoveryLog.LogRecordsProcessed); } }
                TABLER() { TABLED() { str << "LogRecordsApplied"; } TABLED() { str << CounterVal(Counters.RecoveryLog.LogRecordsApplied); } }
                TABLER() { TABLED() { str << "LogRecordsWritten"; } TABLED() { str << CounterVal(Counters.RecoveryLog.LogRecordsWritten); } }
                TABLER() { TABLED() { str << "NumChunkMapSnapshots"; } TABLED() { str << CounterVal(Counters.RecoveryLog.NumChunkMapSnapshots); } }
                TABLER() { TABLED() { str << "NumChunkMapIncrements"; } TABLED() { str << CounterVal(Counters.RecoveryLog.NumChunkMapIncrements); } }
                TABLER() { TABLED() { str << "CutLogMessages"; } TABLED() { str << CounterVal(Counters.RecoveryLog.CutLogMessages); } }
            }
        }

        // --- Section 4: Chunks ------------------------------------------------------
        ui64 dataChunksInUse = 0;
        for (const auto& [tabletId, perTablet] : ChunkRefs) {
            Y_UNUSED(tabletId);
            dataChunksInUse += perTablet.size();
        }
        const ui64 reservedFree = ChunkReserve.size();
        const ui64 commitsInFlight = ChunkMapIncrementsInFlight.size();
        const ui64 chunkBytes = DiskFormat ? DiskFormat->ChunkSize : 0;
        str << "<h3>Chunks</h3>";
        TABLE_CLASS("table") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "Reserved (free pool)"; } TABLED() { str << reservedFree; } }
                TABLER() { TABLED() { str << "Reserve refill in flight"; } TABLED() { str << (ReserveInFlight ? "true" : "false"); } }
                TABLER() { TABLED() { str << "Committed (data)"; } TABLED() { str << dataChunksInUse; } }
                TABLER() { TABLED() { str << "Commits in flight"; } TABLED() { str << commitsInFlight; } }
                TABLER() { TABLED() { str << "Tablets using disk"; } TABLED() { str << ChunkRefs.size(); } }
                TABLER() { TABLED() { str << "Pending chunk allocations"; } TABLED() { str << ChunkAllocateQueue.size(); } }
                TABLER() {
                    TABLED() { str << "Committed data bytes"; }
                    TABLED() {
                        if (chunkBytes > 0) {
                            str << FormatBytes(dataChunksInUse * chunkBytes);
                        } else {
                            str << "n/a";
                        }
                    }
                }
            }
        }

        // --- Section 5: Active connections ------------------------------------------
        str << "<h3>Active connections (" << Connections.size() << ")</h3>";
        TABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "TabletId"; }
                    TABLEH() { str << "Generation"; }
                    TABLEH() { str << "NodeId"; }
                    TABLEH() { str << "InterconnectSessionId"; }
                }
            }
            TABLEBODY() {
                for (const auto& [tabletId, info] : Connections) {
                    Y_UNUSED(tabletId);
                    TABLER() {
                        TABLED() { str << info.TabletId; }
                        TABLED() { str << info.Generation; }
                        TABLED() { str << info.NodeId; }
                        TABLED() { str << info.InterconnectSessionId; }
                    }
                }
            }
        }

        // --- Section 6: In-flight I/O -----------------------------------------------
        str << "<h3>In-flight I/O</h3>";
        TABLE_CLASS("table") {
            TABLEBODY() {
                TABLER() { TABLED() { str << "DirectIoQueue"; } TABLED() { str << DirectIoQueue.size(); } }
                TABLER() { TABLED() { str << "WriteCallbacks"; } TABLED() { str << WriteCallbacks.size(); } }
                TABLER() { TABLED() { str << "ReadCallbacks"; } TABLED() { str << ReadCallbacks.size(); } }
                TABLER() { TABLED() { str << "SyncsInFlight"; } TABLED() { str << SyncsInFlight.size(); } }
                TABLER() { TABLED() { str << "LogCallbacks"; } TABLED() { str << LogCallbacks.size(); } }
                TABLER() { TABLED() { str << "DirectIO QueueSize counter"; } TABLED() { str << CounterVal(Counters.DirectIO.QueueSize); } }
                TABLER() { TABLED() { str << "DirectIO RunningCount counter"; } TABLED() { str << CounterVal(Counters.DirectIO.RunningCount); } }
                TABLER() { TABLED() { str << "ShortReads"; } TABLED() { str << CounterVal(Counters.DirectIO.ShortReads); } }
                TABLER() { TABLED() { str << "ShortWrites"; } TABLED() { str << CounterVal(Counters.DirectIO.ShortWrites); } }
                TABLER() { TABLED() { str << "RegularUringCount"; } TABLED() { str << CounterVal(Counters.DirectIO.RegularUringCount); } }
                TABLER() { TABLED() { str << "FallbackUringCount"; } TABLED() { str << CounterVal(Counters.DirectIO.FallbackUringCount); } }
                TABLER() { TABLED() { str << "FallbackPDiskCount"; } TABLED() { str << CounterVal(Counters.DirectIO.FallbackPDiskCount); } }
            }
        }

        // --- Section 7: Interface counters ------------------------------------------
        str << "<h3>Interface counters</h3>";
        TABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "Operation"; }
                    TABLEH() { str << "Requests"; }
                    TABLEH() { str << "ReplyOk"; }
                    TABLEH() { str << "ReplyErr"; }
                    TABLEH() { str << "Bytes"; }
                    TABLEH() { str << "BytesInFlight"; }
                }
            }
            TABLEBODY() {
#define DDISK_RENDER_INTERFACE_OP(NAME)                                                            \
                TABLER() {                                                                         \
                    TABLED() { str << #NAME; }                                                     \
                    TABLED() { str << CounterVal(Counters.Interface.NAME.Requests); }              \
                    TABLED() { str << CounterVal(Counters.Interface.NAME.ReplyOk); }               \
                    TABLED() { str << CounterVal(Counters.Interface.NAME.ReplyErr); }              \
                    TABLED() { str << CounterVal(Counters.Interface.NAME.Bytes); }                 \
                    TABLED() { str << CounterVal(Counters.Interface.NAME.BytesInFlight); }         \
                }
                LIST_COUNTERS_INTERFACE_OPS(DDISK_RENDER_INTERFACE_OP)
#undef DDISK_RENDER_INTERFACE_OP
            }
        }

        // --- Section 8: DirectIO counters -------------------------------------------
        str << "<h3>DirectIO counters</h3>";
        TABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "Operation"; }
                    TABLEH() { str << "Requests"; }
                    TABLEH() { str << "Bytes"; }
                    TABLEH() { str << "BytesInFlight"; }
                }
            }
            TABLEBODY() {
                TABLER() {
                    TABLED() { str << "Write"; }
                    TABLED() { str << CounterVal(Counters.DirectIO.Write.Requests); }
                    TABLED() { str << CounterVal(Counters.DirectIO.Write.Bytes); }
                    TABLED() { str << CounterVal(Counters.DirectIO.Write.BytesInFlight); }
                }
                TABLER() {
                    TABLED() { str << "Read"; }
                    TABLED() { str << CounterVal(Counters.DirectIO.Read.Requests); }
                    TABLED() { str << CounterVal(Counters.DirectIO.Read.Bytes); }
                    TABLED() { str << CounterVal(Counters.DirectIO.Read.BytesInFlight); }
                }
            }
        }
    }

    Send(ev->Sender,
        new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId),
        0, ev->Cookie);
}

} // namespace NKikimr::NDDisk
