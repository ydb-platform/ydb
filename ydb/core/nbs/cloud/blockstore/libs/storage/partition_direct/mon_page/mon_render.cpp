#include "mon_render.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/map.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
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
        case EMonPage::LocalDb:
            return "localdb";
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
        case EMonPage::LocalDb:
            return "Local DB";
    }
    return "";
}

void RenderDDiskIdCell(
    IOutputStream& str,
    const TString& ddiskId,
    const TString& pageUrl)
{
    if (pageUrl.empty()) {
        str << HtmlEscape(ddiskId);
        return;
    }
    str << "<a href='" << pageUrl << "'>" << HtmlEscape(ddiskId) << "</a>";
}

// "6 Online" or "4 Online / 2 Sufferer".
TString HealthRollup(const TMap<EHostHealth, size_t>& counts)
{
    TStringBuilder sb;
    for (const auto& [health, count]: counts) {
        if (!sb.empty()) {
            sb << " / ";
        }
        sb << count << " " << ToString(health);
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
        EMonPage::LocalDb,
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
                        str << "Consecutive errors";
                    }
                    TABLEH () {
                        str << "Consecutive success";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& dbg: dbgs) {
                    TMap<EHostHealth, size_t> healthCounts;
                    size_t inflight = 0;
                    size_t consecutiveErrors = 0;
                    size_t consecutiveSuccesses = 0;
                    for (const auto& host: dbg.Hosts) {
                        ++healthCounts[host.Health];
                        consecutiveErrors += host.Errors.ConsecutiveErrorCount;
                        consecutiveSuccesses +=
                            host.Errors.ConsecutiveSuccessCount;
                        for (size_t operation = 0; operation < OperationCount;
                             ++operation)
                        {
                            inflight += host.InflightByOperation[operation];
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
                            str << consecutiveErrors;
                        }
                        TABLED () {
                            str << consecutiveSuccesses;
                        }
                    }
                }
            }
        }
    }
}

void RenderDbgDetail(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    const TDbgSnapshot& dbg)
{
    str << "<div style='margin-bottom:0.5em;'><a href='?TabletID="
        << tabletInfo.TabletId << "&page=dbg'>&larr; back to DBGs</a></div>";
    // POST, not a link: link prefetching must not add hosts.
    //
    // The same parameters go into both the action URL and the hidden fields
    // because the request has two readers, each looking at one place only:
    // the mon proxy picks the target tablet from the POST body, while the
    // tablet's Cgi() reads the URL query.
    str << "<form method='post' action='?TabletID=" << tabletInfo.TabletId
        << "&page=dbg&dbg=" << dbg.Index
        << "&action=addhost' style='margin-bottom:0.5em;'>"
           "<input type='hidden' name='TabletID' value='"
        << tabletInfo.TabletId
        << "'/>"
           "<input type='hidden' name='page' value='dbg'/>"
           "<input type='hidden' name='dbg' value='"
        << dbg.Index
        << "'/>"
           "<input type='hidden' name='action' value='addhost'/>"
           "<button type='submit' class='btn btn-default'>Add host</button>"
           "</form>";
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
                        str << "Consecutive errors";
                    }
                    TABLEH () {
                        str << "Consecutive success";
                    }
                    for (size_t operation = 0; operation < OperationCount;
                         ++operation)
                    {
                        TABLEH () {
                            str << ToString(static_cast<EOperation>(operation));
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
                            str << ToString(host.State);
                        }
                        TABLED () {
                            str << ToString(host.Health);
                        }
                        TABLED () {
                            str << host.PBufferUsedSize;
                        }
                        TABLED () {
                            str << host.Errors.ConsecutiveErrorCount;
                        }
                        TABLED () {
                            str << host.Errors.ConsecutiveSuccessCount;
                        }
                        for (size_t operation = 0; operation < OperationCount;
                             ++operation)
                        {
                            TABLED () {
                                str << host.InflightByOperation[operation];
                            }
                        }
                    }
                }
            }
        }
        TAG (TH4) {
            str << "Connections";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Host";
                    }
                    TABLEH () {
                        str << "DDisk id";
                    }
                    TABLEH () {
                        str << "PBuffer id";
                    }
                    TABLEH () {
                        str << "DDisk session";
                    }
                    TABLEH () {
                        str << "PBuffer connected";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& connection: dbg.Connections) {
                    TABLER () {
                        TABLED () {
                            str << (int)connection.HostIndex;
                        }
                        TABLED () {
                            RenderDDiskIdCell(
                                str,
                                connection.DDiskId,
                                connection.DDiskPageUrl);
                        }
                        TABLED () {
                            RenderDDiskIdCell(
                                str,
                                connection.PBufferId,
                                connection.PBufferPageUrl);
                        }
                        TABLED () {
                            str << connection.DDiskSession;
                        }
                        TABLED () {
                            str << (connection.PBufferConnected ? "yes" : "no");
                        }
                    }
                }
            }
        }
    }
}

void RenderProtoDump(
    IOutputStream& str,
    const char* name,
    const std::optional<TString>& dump)
{
    if (!dump) {
        str << "<div style='margin-bottom:0.5em;'>" << name << " (none)</div>";
        return;
    }
    // display:list-item brings back the fold triangle that the page CSS
    // hides; the pointer marks the line as clickable.
    str << "<details style='margin-bottom:0.5em;'>"
           "<summary style='display:list-item; cursor:pointer;'>"
        << name << "</summary><pre>" << HtmlEscape(*dump) << "</pre></details>";
}

void RenderLocalDb(IOutputStream& str, const TLocalDbContents& db)
{
    HTML (str) {
        TAG (TH3) {
            str << "Local DB";
        }
        RenderProtoDump(str, "VolumeConfig", db.VolumeConfig);
        RenderProtoDump(
            str,
            "DirectBlockGroupsConnections",
            db.DirectBlockGroupsConnections);
        RenderProtoDump(str, "AddHostInProgress", db.AddHostInProgress);
        TAG (TH4) {
            str << "VChunkConfigs (persisted overrides)";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "VChunkIndex";
                    }
                    TABLEH () {
                        str << "Config";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& config: db.VChunkConfigs) {
                    TABLER () {
                        TABLED () {
                            str << config.GetVChunkIndex();
                        }
                        TABLED () {
                            str << "<pre>" << HtmlEscape(config.DebugPrint())
                                << "</pre>";
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
        case EMonPage::LocalDb:
            if (data.LocalDb) {
                RenderLocalDb(str, *data.LocalDb);
            }
            break;
    }
    return str.Str();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
