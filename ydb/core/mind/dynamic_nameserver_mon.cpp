#include "dynamic_nameserver_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/location.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/set.h>

namespace NKikimr {
namespace NNodeBroker {

namespace {

TString ToString(TInstant t)
{
    if (t == TInstant::Max())
        return "NEVER";
    return t.FormatLocalTime("%d %b %Y %H:%M:%S %Z");
}

void OutputStaticContent(IOutputStream &str)
{
    str << R"__(
            <style>
            .right-align {
              text-align: right;
            }

            .center-align {
              text-align: center;
            }

            table {
              margin-top: 20px;
            }

            table.nodes {
              border-bottom-style: solid;
              border-top-style: solid;
              border-width: 1px;
              border-color: darkgrey;
            }

            caption {
              font-size: 120%;
              color: black;
              padding: 0px 0px 0px 10px;
            }

            thead {
              border-bottom-style: solid;
              border-width: 1px;
              border-color: darkgrey;
            }

            th {
              font-weight: 700;
              padding-left: 10px;
              padding-right: 10px;
            }

            td {
              padding-left: 5px;
              padding-right: 5px;
            }

            .gray {
              color: gray;
            }
            </style>
        )__";
}

void OutputNodeInfo(ui32 nodeId,
                    const TTableNameserverSetup::TNodeInfo &info,
                    IOutputStream &str,
                    TInstant expire = TInstant::Zero(),
                    const TString &cl = "")
{
    str << "<tr class='" << cl << "'>" << Endl
        << "  <td>" << nodeId << "</td>" << Endl
        << "  <td>" << info.Host << ":" << info.Port << "</td>" << Endl
        << "  <td>" << info.ResolveHost << "</td>" << Endl
        << "  <td>" << info.Address << "</td>" << Endl
        << "  <td>" << info.Location.GetDataCenterId() << "</td>" << Endl
        << "  <td>" << info.Location.ToString() << "</td>" << Endl;
    if (expire)
        str << "<td>" << ToString(expire) << "</td>" << Endl;
    str << "</tr>" << Endl;
}

void OutputStaticNodes(const TTableNameserverSetup &setup,
                       IOutputStream &str)
{
    str << "<div><table class='nodes'>" << Endl
        << "  <caption>Static nodes</caption>" << Endl
        << "  <thead>" << Endl
        << "    <tr>" << Endl
        << "      <th>Node ID</th>" << Endl
        << "      <th>Host</th>" << Endl
        << "      <th>Resolve Host</th>" << Endl
        << "      <th>Address</th>" << Endl
        << "      <th>Data Center</th>" << Endl
        << "      <th>Location</th>" << Endl
        << "    </tr>" << Endl
        << "  </thead>" << Endl
        << "  <tbody class='center-align'>" << Endl;
    for (auto &pr : setup.StaticNodeTable)
        OutputNodeInfo(pr.first, pr.second, str);
    str << "  </tbody>" << Endl
        << "</table></div>" << Endl;
}

void OutputDynamicNodes(const TString &domain,
                        TDynamicConfigPtr config,
                        IOutputStream &str)
{
    str << "<div><table class='nodes'>" << Endl
        << "  <caption>Dynamic nodes in " << domain
        << " (epoch #" << config->Epoch.Id << "." << config->Epoch.Version
        << " expiring " << ToString(config->Epoch.End) << ")</caption>" << Endl
        << "  <thead>" << Endl
        << "    <tr>" << Endl
        << "      <th>Node ID</th>" << Endl
        << "      <th>Host</th>" << Endl
        << "      <th>Resolve Host</th>" << Endl
        << "      <th>Address</th>" << Endl
        << "      <th>Data Center</th>" << Endl
        << "      <th>Room</th>" << Endl
        << "      <th>Rack</th>" << Endl
        << "      <th>Body</th>" << Endl
        << "      <th>Expire</th>" << Endl
        << "    </tr>" << Endl
        << "  </thead>" << Endl
        << "  <tbody class='center-align'>" << Endl;

    TSet<ui32> ids;
    for (auto &pr : config->DynamicNodes)
        ids.insert(pr.first);
    for (auto id : ids) {
        auto &node = config->DynamicNodes.at(id);
        OutputNodeInfo(id, node, str, node.Expire);
    }

    ids.clear();
    for (auto &pr : config->ExpiredNodes)
        ids.insert(pr.first);
    for (auto id : ids) {
        auto &node = config->ExpiredNodes.at(id);
        OutputNodeInfo(id, node, str, node.Expire, "gray");
    }

    str << "  </tbody>" << Endl
        << "</table></div>" << Endl;
}

} // anonymous namespace

void TDynamicNameserver::Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx)
{
    TStringStream str;
    HTML(str) {
        OutputStaticContent(str);

        auto config = AppData(ctx)->DynamicNameserviceConfig;
        if (config)
            str << "<div><table class='config'>" << Endl
                << "  <caption>Config</caption>" << Endl
                << "  <tbody>" << Endl
                << "    <tr>" << Endl
                << "      <td class='right-align'>Max static node ID:</td>" << Endl
                << "      <td>" << config->MaxStaticNodeId << "</td>" << Endl
                << "    </tr>" << Endl
                << "    <tr>" << Endl
                << "      <td class='right-align'>Min dynamic node ID:</td>" << Endl
                << "      <td>" << config->MinDynamicNodeId << "</td>" << Endl
                << "    </tr>" << Endl
                << "    <tr>" << Endl
                << "      <td class='right-align'>Max dynamic node ID:</td>" << Endl
                << "      <td>" << config->MaxDynamicNodeId << "</td>" << Endl
                << "    </tr>" << Endl
                << "  </tbody>" << Endl
                << "</table></div>" << Endl;

        OutputStaticNodes(*StaticConfig, str);

        if (const auto& domain = AppData(ctx)->DomainsInfo->Domain) {
            OutputDynamicNodes(domain->Name, DynamicConfigs[domain->DomainUid], str);
        }
    }
    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
}

} // NNodeBroker
} // NKikimr
