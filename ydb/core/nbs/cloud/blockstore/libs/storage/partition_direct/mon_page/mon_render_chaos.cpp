#include "mon_render_chaos.h"

#include "mon_model.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/generic/set.h>
#include <util/generic/strbuf.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

using EChaosMode = TChaosConfig::TChaosNodeConfig::EChaosMode;

bool IsNodeDisabled(const TChaosConfig& config, ui32 nodeId, ui32 dbgIndex)
{
    const auto* nodeConfig =
        config.NodeConfigs.FindPtr(TChaosConfig::TDbgAndNodeId{
            .NodeId = nodeId,
            .DbgIndex = dbgIndex,
        });
    return nodeConfig && nodeConfig->Mode == EChaosMode::Disabled;
}

void RenderToggle(
    const TTabletInfo& tabletInfo,
    ui32 nodeId,
    TStringBuf dbg,
    EChaosMode currentMode,
    IOutputStream& str)
{
    const EChaosMode newMode = currentMode == EChaosMode::Disabled
                                   ? EChaosMode::Enabled
                                   : EChaosMode::Disabled;
    const TStringBuf action =
        newMode == EChaosMode::Disabled ? "disable" : "enable";
    const TStringBuf stateClass = currentMode == EChaosMode::Disabled
                                      ? "chaos-toggle-off"
                                      : "chaos-toggle-on";

    TStringBuilder label;
    label << (newMode == EChaosMode::Disabled ? "Disable" : "Enable")
          << " node " << nodeId;
    if (dbg == "all") {
        label << " in all DBGs";
    } else {
        label << " in DBG #" << dbg;
    }

    // The tablet reads URL parameters, while the monitoring proxy routes POST
    // requests using the same parameters from the request body.
    str << "<form method='post' action='?TabletID=" << tabletInfo.TabletId
        << "&page=chaos&action=" << action << "&node=" << nodeId
        << "&dbg=" << dbg << "' class='chaos-toggle-form'>"
        << "<input type='hidden' name='TabletID' value='" << tabletInfo.TabletId
        << "'/>"
        << "<input type='hidden' name='page' value='chaos'/>"
        << "<input type='hidden' name='action' value='" << action << "'/>"
        << "<input type='hidden' name='node' value='" << nodeId << "'/>"
        << "<input type='hidden' name='dbg' value='" << dbg << "'/>"
        << "<button type='submit' class='chaos-toggle " << stateClass
        << "' title='" << label << "' aria-label='" << label
        << "'></button></form>";
}

void RenderNodeToggle(const TMonPageData& data, ui32 nodeId, IOutputStream& str)
{
    const bool allDisabled = AllOf(
        data.Dbgs,
        [&](const TDbgSnapshot& dbg) {
            return IsNodeDisabled(
                data.Chaos,
                nodeId,
                static_cast<ui32>(dbg.Index));
        });
    RenderToggle(
        data.TabletInfo,
        nodeId,
        "all",
        allDisabled ? EChaosMode::Disabled : EChaosMode::Enabled,
        str);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RenderChaos(IOutputStream& str, const TMonPageData& data)
{
    HTML (str) {
        TAG (TH3) {
            str << "Chaos";
        }

        if (data.Dbgs.empty()) {
            DIV_CLASS ("alert alert-info") {
                str << "No Direct Block Groups.";
            }
            return;
        }

        TSet<ui32> nodeIds;
        for (const auto& dbg: data.Dbgs) {
            for (const auto& connection: dbg.Connections) {
                if (connection.DDiskId.NodeId != 0) {
                    nodeIds.insert(connection.DDiskId.NodeId);
                }
            }
        }
        for (const auto& [id, config]: data.Chaos.NodeConfigs) {
            Y_UNUSED(config);
            nodeIds.insert(id.NodeId);
        }

        TABLE_CLASS ("table table-condensed table-bordered") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Node";
                    }
                    for (const auto& dbg: data.Dbgs) {
                        TABLEH () {
                            str << "DBG #" << dbg.Index;
                        }
                    }
                }
            }
            TABLEBODY () {
                for (ui32 nodeId: nodeIds) {
                    TABLER () {
                        TABLED () {
                            str << "Node " << nodeId << " ";
                            RenderNodeToggle(data, nodeId, str);
                        }
                        for (const auto& dbg: data.Dbgs) {
                            TABLED_CLASS ("chaos-cell") {
                                const bool disabled = IsNodeDisabled(
                                    data.Chaos,
                                    nodeId,
                                    static_cast<ui32>(dbg.Index));
                                RenderToggle(
                                    data.TabletInfo,
                                    nodeId,
                                    ToString(dbg.Index),
                                    disabled ? EChaosMode::Disabled
                                             : EChaosMode::Enabled,
                                    str);
                            }
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
