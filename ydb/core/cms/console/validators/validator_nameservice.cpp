#include "validator_nameservice.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/protos/console_config.pb.h>

#include <ydb/library/actors/interconnect/interconnect.h>

#include <util/string/builder.h>

namespace NKikimr::NConsole {

TNameserviceConfigValidator::TNameserviceConfigValidator()
    : IConfigValidator("nameservice", NKikimrConsole::TConfigItem::NameserviceConfigItem)
{
}

TString TNameserviceConfigValidator::GetDescription() const
{
    return "Check Nameservice config for consistency and suspicious modifications";
}

bool TNameserviceConfigValidator::CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                                              const NKikimrConfig::TAppConfig &newConfig,
                                              TVector<Ydb::Issue::IssueMessage> &issues) const
{
    if (!oldConfig.HasNameserviceConfig() && !newConfig.HasNameserviceConfig())
        return true;

    if (!newConfig.HasNameserviceConfig()) {
        AddError(issues, "missing nameservice config");
        return false;
    }

    THashMap<ui32, TTableNameserverSetup::TNodeInfo> idMap;
    THashMap<std::pair<TString, ui16>, ui32> hostMap;
    THashMap<std::pair<TString, ui16>, ui32> ihostMap;
    THashMap<std::pair<TString, ui16>, ui32> addrMap;

    for (auto &node : newConfig.GetNameserviceConfig().GetNode()) {
        ui32 id = node.GetNodeId();
        TString host = node.GetHost();
        TString ihost = node.GetInterconnectHost();
        TString addr = node.GetAddress();
        ui16 port = node.GetPort();

        if (idMap.contains(id)) {
            AddError(issues,
                     TStringBuilder() << "duplicating node id " << id);
            return false;
        }

        auto hostPort = std::make_pair(host, port);
        if (hostMap.contains(hostPort)) {
            AddError(issues,
                     TStringBuilder() << "duplicating " << host << ":" << port
                     << " for nodes " << hostMap.at(hostPort) << " and " << id);
            return false;
        }

        auto ihostPort = std::make_pair(ihost, port);
        if (ihostMap.contains(ihostPort)) {
            AddError(issues,
                     TStringBuilder() << "duplicating " << ihost << ":" << port
                     << " for nodes " << ihostMap.at(ihostPort) << " and " << id);
            return false;
        }

        auto addrPort = std::make_pair(addr, port);
        // do not validate for duplicates if addr is empty
        if (addr && addrMap.contains(addrPort)) {
            AddError(issues,
                     TStringBuilder() << "duplicating " << addr << ":" << port
                     << " for nodes " << addrMap.at(addrPort) << " and " << id);
            return false;
        }

        if (node.GetWalleLocation().GetDataCenter().size() > 4) {
            AddError(issues,
                     TStringBuilder() << "node " << id << " has data center in"
                     << " Wall-E location longer than 4 symbols");
            return false;
        }

        idMap.emplace(id, TTableNameserverSetup::TNodeInfo(addr, host, ihost, port, {}));
        hostMap.emplace(hostPort, id);
        ihostMap.emplace(ihostPort, id);
        if (addr) {
            addrMap.emplace(addrPort, id);
        }
    }

    if (idMap.empty()) {
        AddError(issues, "config should have at least 1 node");
        return false;
    }

    if (oldConfig.HasNameserviceConfig()) {
        if (oldConfig.GetNameserviceConfig().GetClusterUUID()
            != newConfig.GetNameserviceConfig().GetClusterUUID()) {
            AddError(issues, "cannot modify cluster UUID");
            return false;
        }

        size_t prevTotal = oldConfig.GetNameserviceConfig().NodeSize();
        size_t removed = prevTotal;
        for (auto &node : oldConfig.GetNameserviceConfig().GetNode()) {
            ui32 id = node.GetNodeId();
            TString host = node.GetHost();
            TString ihost = node.GetInterconnectHost();
            TString addr = node.GetAddress();
            ui16 port = node.GetPort();

            auto hostPort = std::make_pair(host, port);
            if (hostMap.contains(hostPort) && hostMap.at(hostPort) != id) {
                AddError(issues,
                         TStringBuilder() << "modified id for " << host << ":" << port
                         << " from " << id << " to " << hostMap.at(hostPort));
                return false;
            }

            auto ihostPort = std::make_pair(ihost, port);
            if (ihostMap.contains(ihostPort) && ihostMap.at(ihostPort) != id) {
                AddError(issues,
                         TStringBuilder() << "modified id for " << ihost << ":" << port
                         << " from " << id << " to " << ihostMap.at(ihostPort));
                return false;
            }

            auto addrPort = std::make_pair(addr, port);
            if (addr && addrMap.contains(addrPort) && addrMap.at(addrPort) != id) {
                AddError(issues,
                         TStringBuilder() << "modified id for " << addr << ":" << port
                         << " from " << id << " to " << addrMap.at(addrPort));
                return false;
            }

            if (idMap.contains(id)) {
                auto &info = idMap.at(id);
                if (info.Host != host) {
                    AddWarning(issues,
                               TStringBuilder() << "modified host for node " << id
                               << " from " << host << " to " << info.Host);
                }

                if (info.ResolveHost != ihost) {
                    AddWarning(issues,
                               TStringBuilder() << "modified resolve host for node "
                               << id << " from " << ihost << " to " << info.ResolveHost);
                }

                if (info.Port != port) {
                    AddWarning(issues,
                               TStringBuilder() << "modified port for node "
                               << id << " from " << port << " to " << info.Port);
                }

                --removed;
            }
        }

        if (removed > Max<size_t>(prevTotal / 2, 1)) {
            AddWarning(issues,
                       TStringBuilder() << "suspiciously large number of removed nodes ("
                       << removed << " of " << prevTotal << ")");
        }
    }

    return true;
}

} // namespace NKikimr::NConsole
