#include "json_handlers.h"
#include "viewer_acl.h"
#include "viewer_autocomplete.h"
#include "viewer_bscontrollerinfo.h"
#include "viewer_capabilities.h"
#include "viewer_check_access.h"
#include "viewer_cluster.h"
#include "viewer_compute.h"
#include "viewer_config.h"
#include "viewer_counters.h"
#include "viewer_describe_consumer.h"
#include "viewer_describe.h"
#include "viewer_describe_topic.h"
#include "viewer_feature_flags.h"
#include "viewer_graph.h"
#include "viewer_healthcheck.h"
#include "viewer_hiveinfo.h"
#include "viewer_hivestats.h"
#include "viewer_hotkeys.h"
#include "viewer_labeled_counters.h"
#include "viewer_netinfo.h"
#include "viewer_nodelist.h"
#include "viewer_nodes.h"
#include "viewer_pqconsumerinfo.h"
#include "viewer_query.h"
#include "viewer_render.h"
#include "viewer_storage.h"
#include "viewer_storage_usage.h"
#include "viewer_tabletcounters.h"
#include "viewer_tenantinfo.h"
#include "viewer_tenants.h"
#include "viewer_topicinfo.h"
#include "viewer_whoami.h"

namespace NKikimr::NViewer {

TBSGroupState GetBSGroupOverallStateWithoutLatency(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex) {

    TBSGroupState groupState;
    groupState.Overall = NKikimrViewer::EFlag::Grey;

    const auto& vDiskIds = info.GetVDiskIds();
    std::unordered_map<ui32, ui32> failedRings;
    std::unordered_map<ui32, ui32> failedDomains;
    TVector<NKikimrViewer::EFlag> vDiskFlags;
    vDiskFlags.reserve(vDiskIds.size());
    for (auto iv = vDiskIds.begin(); iv != vDiskIds.end(); ++iv) {
        const NKikimrBlobStorage::TVDiskID& vDiskId = *iv;
        NKikimrViewer::EFlag flag = NKikimrViewer::EFlag::Grey;
        auto ie = vDisksIndex.find(vDiskId);
        if (ie != vDisksIndex.end()) {
            auto pDiskId = std::make_pair(ie->second.GetNodeId(), ie->second.GetPDiskId());
            auto ip = pDisksIndex.find(pDiskId);
            if (ip != pDisksIndex.end()) {
                const NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo(ip->second);
                flag = Max(flag, GetPDiskOverallFlag(pDiskInfo));
            } else {
                flag = NKikimrViewer::EFlag::Red;
            }
            const NKikimrWhiteboard::TVDiskStateInfo& vDiskInfo(ie->second);
            flag = Max(flag, GetVDiskOverallFlag(vDiskInfo));
            if (vDiskInfo.GetDiskSpace() > NKikimrWhiteboard::EFlag::Green) {
                groupState.SpaceProblems++;
            }
        } else {
            flag = NKikimrViewer::EFlag::Red;
        }
        vDiskFlags.push_back(flag);
        if (flag == NKikimrViewer::EFlag::Red || flag == NKikimrViewer::EFlag::Blue) {
            groupState.MissingDisks++;
            ++failedRings[vDiskId.GetRing()];
            ++failedDomains[vDiskId.GetDomain()];
        }
        groupState.Overall = Max(groupState.Overall, flag);
    }

    groupState.Overall = Min(groupState.Overall, NKikimrViewer::EFlag::Yellow); // without failed rings we only allow to raise group status up to Blue/Yellow
    TString erasure = info.GetErasureSpecies();
    if (erasure == TErasureType::ErasureSpeciesName(TErasureType::ErasureNone)) {
        if (!failedDomains.empty()) {
            groupState.Overall = NKikimrViewer::EFlag::Red;
        }
    } else if (erasure == TErasureType::ErasureSpeciesName(TErasureType::ErasureMirror3dc)) {
        if (failedRings.size() > 2) {
            groupState.Overall = NKikimrViewer::EFlag::Red;
        } else if (failedRings.size() == 2) { // TODO: check for 1 ring - 1 domain rule
            groupState.Overall = NKikimrViewer::EFlag::Orange;
        } else if (failedRings.size() > 0) {
            groupState.Overall = Min(groupState.Overall, NKikimrViewer::EFlag::Yellow);
        }
    } else if (erasure == TErasureType::ErasureSpeciesName(TErasureType::Erasure4Plus2Block)) {
        if (failedDomains.size() > 2) {
            groupState.Overall = NKikimrViewer::EFlag::Red;
        } else if (failedDomains.size() > 1) {
            groupState.Overall = NKikimrViewer::EFlag::Orange;
        } else if (failedDomains.size() > 0) {
            groupState.Overall = Min(groupState.Overall, NKikimrViewer::EFlag::Yellow);
        }
    }
    return groupState;
}

NKikimrViewer::EFlag GetBSGroupOverallFlagWithoutLatency(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex) {
    return GetBSGroupOverallStateWithoutLatency(info, vDisksIndex, pDisksIndex).Overall;
}

TBSGroupState GetBSGroupOverallState(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex) {
    TBSGroupState state = GetBSGroupOverallStateWithoutLatency(info, vDisksIndex, pDisksIndex);
    if (info.HasLatency()) {
        state.Overall = Max(state.Overall, Min(NKikimrViewer::EFlag::Yellow, GetViewerFlag(info.GetLatency())));
    }
    return state;
}

NKikimrViewer::EFlag GetBSGroupOverallFlag(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex) {
    return GetBSGroupOverallState(info, vDisksIndex, pDisksIndex).Overall;
}

void InitViewerCapabilitiesJsonHandler(TJsonHandlers& jsonHandlers) {
    TSimpleYamlBuilder yaml({
        .Method = "get",
        .Tag = "viewer",
        .Summary = "Viewer capabilities",
        .Description = "Viewer capabilities",
    });
    jsonHandlers.AddHandler("/viewer/capabilities", new TJsonHandler<TViewerCapabilities>(yaml));
}

void InitViewerNodelistJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/nodelist", new TJsonHandler<TJsonNodeList>(TJsonNodeList::GetSwagger()));
}

void InitViewerNodeInfoJsonHandler(TJsonHandlers& jsonHandlers);
void InitViewerSysInfoJsonHandler(TJsonHandlers& jsonHandlers);
void InitViewerVDiskInfoJsonHandler(TJsonHandlers& jsonHandlers);
void InitViewerPDiskInfoJsonHandler(TJsonHandlers& jsonHandlers);
void InitViewerTabletInfoJsonHandler(TJsonHandlers& jsonHandlers);

void InitViewerDescribeJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/describe", new TJsonHandler<TJsonDescribe>(TJsonDescribe::GetSwagger()));
}

void InitViewerDescribeTopicJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/describe_topic", new TJsonHandler<TJsonDescribeTopic>(TJsonDescribeTopic::GetSwagger()));
}

void InitViewerDescribeConsumerJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/describe_consumer", new TJsonHandler<TJsonDescribeConsumer>(TJsonDescribeConsumer::GetSwagger()));
}

void InitViewerHotkeysJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/hotkeys", new TJsonHandler<TJsonHotkeys>(TJsonHotkeys::GetSwagger()));
}

void InitViewerHiveInfoJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/hiveinfo", new TJsonHandler<TJsonHiveInfo>(TJsonHiveInfo::GetSwagger()));
}

void InitViewerBSGroupInfoJsonHandler(TJsonHandlers& jsonHandlers);

void InitViewerBSControllerInfoJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/bscontrollerinfo", new TJsonHandler<TJsonBSControllerInfo>(TJsonBSControllerInfo::GetSwagger()));
}

void InitViewerConfigJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/config", new TJsonHandler<TJsonConfig>(TJsonConfig::GetSwagger()));
}

void InitViewerCountersJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/counters", new TJsonHandler<TJsonCounters>(TJsonCounters::GetSwagger()));
}

void InitViewerTopicInfoJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/topicinfo", new TJsonHandler<TJsonTopicInfo>(TJsonTopicInfo::GetSwagger()));
}

void InitViewerPQConsumerInfoJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/pqconsumerinfo", new TJsonHandler<TJsonPQConsumerInfo>(TJsonPQConsumerInfo::GetSwagger()));
}

void InitViewerTabletCountersJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/tabletcounters", new TJsonHandler<TJsonTabletCounters>(TJsonTabletCounters::GetSwagger()));
}

void InitViewerStorageJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/storage", new TJsonHandler<TJsonStorage>(TJsonStorage::GetSwagger()));
}

void InitViewerStorageUsageJsonHandler(TJsonHandlers &handlers) {
    handlers.AddHandler("/viewer/storage_usage", new TJsonHandler<TJsonStorageUsage>(TJsonStorageUsage::GetSwagger()));
}

void InitViewerClusterJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/cluster", new TJsonHandler<TJsonCluster>(TJsonCluster::GetSwagger()));
}

void InitViewerLabeledCountersJsonHandler(TJsonHandlers &handlers) {
    handlers.AddHandler("/viewer/labeledcounters", new TJsonHandler<TJsonLabeledCounters>(TJsonLabeledCounters::GetSwagger()));
}

void InitViewerTenantsJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/tenants", new TJsonHandler<TJsonTenants>(TJsonTenants::GetSwagger()));
}

void InitViewerHiveStatsJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/hivestats", new TJsonHandler<TJsonHiveStats>(TJsonHiveStats::GetSwagger()));
}

void InitViewerTenantInfoJsonHandler(TJsonHandlers &handlers) {
    handlers.AddHandler("/viewer/tenantinfo", new TJsonHandler<TJsonTenantInfo>(TJsonTenantInfo::GetSwagger()), 2);
}

void InitViewerWhoAmIJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/whoami", new TJsonHandler<TJsonWhoAmI>(TJsonWhoAmI::GetSwagger()));
}

void InitViewerQueryJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/query", new TJsonHandler<TJsonQuery>(TJsonQuery::GetSwagger()), 2);
}

void InitViewerNetInfoJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/netinfo", new TJsonHandler<TJsonNetInfo>(TJsonNetInfo::GetSwagger()));
}

void InitViewerComputeJsonHandler(TJsonHandlers &handlers) {
    handlers.AddHandler("/viewer/compute", new TJsonHandler<TJsonCompute>(TJsonCompute::GetSwagger()));
}

void InitViewerHealthCheckJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/healthcheck", new TJsonHandler<TJsonHealthCheck>(TJsonHealthCheck::GetSwagger()));
}

void InitViewerNodesJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/nodes", new TJsonHandler<TJsonNodes>(TJsonNodes::GetSwagger()));
}

void InitViewerACLJsonHandler(TJsonHandlers &jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/acl", new TJsonHandler<TJsonACL>(TJsonACL::GetSwagger()));
}

void InitViewerGraphJsonHandler(TJsonHandlers &handlers) {
    handlers.AddHandler("/viewer/graph", new TJsonHandler<TJsonGraph>(TJsonGraph::GetSwagger()));
}

void InitViewerRenderJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/render", new TJsonHandler<TJsonRender>(TJsonRender::GetSwagger()));
}

void InitViewerAutocompleteJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/autocomplete", new TJsonHandler<TJsonAutocomplete>(TJsonAutocomplete::GetSwagger()));
}

void InitViewerCheckAccessJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/check_access", new TJsonHandler<TCheckAccess>(TCheckAccess::GetSwagger()));
}

void InitViewerFeatureFlagsJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/feature_flags", new TJsonHandler<TJsonFeatureFlags>(TJsonFeatureFlags::GetSwagger()));
}

void InitViewerJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitViewerCapabilitiesJsonHandler(jsonHandlers);
    InitViewerNodelistJsonHandler(jsonHandlers);
    InitViewerNodeInfoJsonHandler(jsonHandlers);
    InitViewerSysInfoJsonHandler(jsonHandlers);
    InitViewerVDiskInfoJsonHandler(jsonHandlers);
    InitViewerPDiskInfoJsonHandler(jsonHandlers);
    InitViewerTabletInfoJsonHandler(jsonHandlers);
    InitViewerDescribeJsonHandler(jsonHandlers);
    InitViewerDescribeTopicJsonHandler(jsonHandlers);
    InitViewerDescribeConsumerJsonHandler(jsonHandlers);
    InitViewerHotkeysJsonHandler(jsonHandlers);
    InitViewerHiveInfoJsonHandler(jsonHandlers);
    InitViewerBSGroupInfoJsonHandler(jsonHandlers);
    InitViewerBSControllerInfoJsonHandler(jsonHandlers);
    InitViewerConfigJsonHandler(jsonHandlers);
    InitViewerCountersJsonHandler(jsonHandlers);
    InitViewerTopicInfoJsonHandler(jsonHandlers);
    InitViewerPQConsumerInfoJsonHandler(jsonHandlers);
    InitViewerTabletCountersJsonHandler(jsonHandlers);
    InitViewerStorageJsonHandler(jsonHandlers);
    InitViewerStorageUsageJsonHandler(jsonHandlers);
    InitViewerClusterJsonHandler(jsonHandlers);
    InitViewerLabeledCountersJsonHandler(jsonHandlers);
    InitViewerTenantsJsonHandler(jsonHandlers);
    InitViewerHiveStatsJsonHandler(jsonHandlers);
    InitViewerTenantInfoJsonHandler(jsonHandlers);
    InitViewerWhoAmIJsonHandler(jsonHandlers);
    InitViewerQueryJsonHandler(jsonHandlers);
    InitViewerNetInfoJsonHandler(jsonHandlers);
    InitViewerComputeJsonHandler(jsonHandlers);
    InitViewerHealthCheckJsonHandler(jsonHandlers);
    InitViewerNodesJsonHandler(jsonHandlers);
    InitViewerACLJsonHandler(jsonHandlers);
    InitViewerGraphJsonHandler(jsonHandlers);
    InitViewerRenderJsonHandler(jsonHandlers);
    InitViewerAutocompleteJsonHandler(jsonHandlers);
    InitViewerCheckAccessJsonHandler(jsonHandlers);
    InitViewerFeatureFlagsJsonHandler(jsonHandlers);
}

}
