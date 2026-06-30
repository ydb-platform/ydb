#include "mon_render.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/subst.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString HtmlEscape(TStringBuf in)
{
    TString escaped(in);
    SubstGlobal(escaped, "&", "&amp;");
    SubstGlobal(escaped, "<", "&lt;");
    SubstGlobal(escaped, ">", "&gt;");
    SubstGlobal(escaped, "\"", "&quot;");
    return escaped;
}

const char* PageParam(EMonPage page)
{
    switch (page) {
        case EMonPage::Overview:
            return "overview";
        case EMonPage::Dbg:
            return "dbg";
    }
    return "overview";
}

const char* PageTitle(EMonPage page)
{
    switch (page) {
        case EMonPage::Overview:
            return "Overview";
        case EMonPage::Dbg:
            return "DBGs";
    }
    return "";
}

const char* OperationName(EOperation op)
{
    switch (op) {
        case EOperation::ReadFromPBuffer:
            return "ReadFromPBuffer";
        case EOperation::ReadFromDDisk:
            return "ReadFromDDisk";
        case EOperation::WriteToPBuffer:
            return "WriteToPBuffer";
        case EOperation::WriteToManyPBuffers:
            return "WriteToManyPBuffers";
        case EOperation::WriteToDDisk:
            return "WriteToDDisk";
        case EOperation::Flush:
            return "Flush";
        case EOperation::FlushCrossNode:
            return "FlushCrossNode";
        case EOperation::Erase:
            return "Erase";
        case EOperation::BarrierErase:
            return "BarrierErase";
        case EOperation::Count_:
            return "?";
    }
    return "?";
}

const char* HostStateName(EHostState state)
{
    switch (state) {
        case EHostState::Online:
            return "Online";
        case EHostState::TemporaryOffline:
            return "TemporaryOffline";
        case EHostState::Offline:
            return "Offline";
    }
    return "?";
}

const char* HealthName(EHostHealthView health)
{
    switch (health) {
        case EHostHealthView::Online:
            return "Online";
        case EHostHealthView::Sufferer:
            return "Sufferer";
        case EHostHealthView::TemporaryOffline:
            return "TemporaryOffline";
        case EHostHealthView::Offline:
            return "Offline";
    }
    return "?";
}

// "6 Online" or "4 Online / 2 Sufferer"; counts indexed by EHostHealthView.
TString HealthRollup(const std::array<size_t, 4>& counts)
{
    TStringBuilder sb;
    for (size_t i = 0; i < counts.size(); ++i) {
        if (counts[i] == 0) {
            continue;
        }
        if (!sb.empty()) {
            sb << " / ";
        }
        sb << counts[i] << " " << HealthName(static_cast<EHostHealthView>(i));
    }
    return sb.empty() ? TString("-") : TString(sb);
}

////////////////////////////////////////////////////////////////////////////////

void RenderHeader(IOutputStream& str, const TTabletInfo& tabletInfo)
{
    HTML (str) {
        TAG (TH3) {
            str << "partition_direct tablet " << tabletInfo.TabletId;
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "TabletId";
                    }
                    TABLED () {
                        str << tabletInfo.TabletId;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Generation";
                    }
                    TABLED () {
                        str << tabletInfo.Generation;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "DiskId";
                    }
                    TABLED () {
                        str << HtmlEscape(tabletInfo.DiskId);
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "State";
                    }
                    TABLED () {
                        str << HtmlEscape(tabletInfo.State);
                    }
                }
            }
        }
    }
}

void RenderMenu(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    EMonPage current)
{
    static const EMonPage pages[] = {
        EMonPage::Overview,
        EMonPage::Dbg,
    };
    str << "<div style='margin:0.5em 0 1em;'>";
    for (EMonPage page: pages) {
        const char* btnClass =
            (page == current) ? "btn btn-primary" : "btn btn-default";
        str << "<a class='" << btnClass
            << "' style='margin-right:0.4em;'"
               " href='?TabletID="
            << tabletInfo.TabletId << "&page=" << PageParam(page) << "'>"
            << PageTitle(page) << "</a>";
    }
    str << "</div>";
}

void RenderOverview(IOutputStream& str, const TFastPathServiceInfo& info)
{
    HTML (str) {
        TAG (TH3) {
            str << "Overview";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "DirectBlockGroups";
                    }
                    TABLED () {
                        str << info.DbgCount;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "VChunks (total)";
                    }
                    TABLED () {
                        str << info.TotalVChunks;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "LSN counter";
                    }
                    TABLED () {
                        str << info.LsnCounter;
                    }
                }
            }
        }
    }
}

// DBG tab, list view: one row per DBG with a health rollup, clickable into the
// per-DBG detail.
void RenderDbgList(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    const TVector<TDbgSnapshot>& dbgs)
{
    HTML (str) {
        TAG (TH3) {
            str << "Direct Block Groups";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "DBG";
                    }
                    TABLEH () {
                        str << "Hosts";
                    }
                    TABLEH () {
                        str << "VChunks";
                    }
                    TABLEH () {
                        str << "Host health";
                    }
                    TABLEH () {
                        str << "Inflight";
                    }
                    TABLEH () {
                        str << "Errors";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& dbg: dbgs) {
                    std::array<size_t, 4> healthCounts{};
                    size_t inflight = 0;
                    size_t errors = 0;
                    for (const auto& host: dbg.Hosts) {
                        ++healthCounts[static_cast<size_t>(host.Health)];
                        errors += host.Errors.ErrorCount;
                        for (size_t op = 0; op < OperationCount; ++op) {
                            inflight += host.InflightByOp[op];
                        }
                    }
                    TABLER () {
                        TABLED () {
                            str << "<a href='?TabletID=" << tabletInfo.TabletId
                                << "&page=dbg&dbg=" << dbg.Index << "'>#"
                                << dbg.Index << "</a>";
                        }
                        TABLED () {
                            str << dbg.Hosts.size();
                        }
                        TABLED () {
                            str << dbg.VChunkCount;
                        }
                        TABLED () {
                            str << HealthRollup(healthCounts);
                        }
                        TABLED () {
                            str << inflight;
                        }
                        TABLED () {
                            str << errors;
                        }
                    }
                }
            }
        }
    }
}

// DBG tab, detail view: the full per-host table for one DBG.
void RenderDbgDetail(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    const TDbgSnapshot& dbg)
{
    str << "<div style='margin-bottom:0.5em;'><a href='?TabletID="
        << tabletInfo.TabletId << "&page=dbg'>&larr; back to DBGs</a></div>";
    HTML (str) {
        TAG (TH3) {
            str << "DBG #" << dbg.Index;
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Host";
                    }
                    TABLEH () {
                        str << "State";
                    }
                    TABLEH () {
                        str << "Health";
                    }
                    TABLEH () {
                        str << "PBuffer used";
                    }
                    TABLEH () {
                        str << "Errors";
                    }
                    for (size_t op = 0; op < OperationCount; ++op) {
                        TABLEH () {
                            str << OperationName(static_cast<EOperation>(op));
                        }
                    }
                }
            }
            TABLEBODY () {
                for (const auto& host: dbg.Hosts) {
                    TABLER () {
                        TABLED () {
                            str << (int)host.Index;
                        }
                        TABLED () {
                            str << HostStateName(host.State);
                        }
                        TABLED () {
                            str << HealthName(host.Health);
                        }
                        TABLED () {
                            str << host.PBufferUsedSize;
                        }
                        TABLED () {
                            str << host.Errors.ErrorCount;
                        }
                        for (size_t op = 0; op < OperationCount; ++op) {
                            TABLED () {
                                str << host.InflightByOp[op];
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderDbg(IOutputStream& str, const TMonPageData& data)
{
    if (!data.SelectedDbg) {
        RenderDbgList(str, data.TabletInfo, data.Dbgs);
        return;
    }
    for (const auto& dbg: data.Dbgs) {
        if (dbg.Index == *data.SelectedDbg) {
            RenderDbgDetail(str, data.TabletInfo, dbg);
            return;
        }
    }
    HTML (str) {
        DIV_CLASS ("alert alert-warning") {
            str << "DBG #" << *data.SelectedDbg << " not found.";
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString RenderMonPage(const TMonPageData& data)
{
    TStringStream str;

    RenderHeader(str, data.TabletInfo);
    RenderMenu(str, data.TabletInfo, data.Page);

    if (data.RuntimeError) {
        HTML (str) {
            DIV_CLASS ("alert alert-warning") {
                str << HtmlEscape(*data.RuntimeError);
            }
        }
        return str.Str();
    }

    switch (data.Page) {
        case EMonPage::Overview:
            if (data.FastPathServiceInfo) {
                RenderOverview(str, *data.FastPathServiceInfo);
            }
            break;
        case EMonPage::Dbg:
            RenderDbg(str, data);
            break;
    }
    return str.Str();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
