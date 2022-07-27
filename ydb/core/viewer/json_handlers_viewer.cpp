#include "json_handlers.h"

#include "json_nodelist.h"
#include "json_nodeinfo.h"
#include "json_vdiskinfo.h"
#include "json_pdiskinfo.h"
#include "json_describe.h"
#include "json_hotkeys.h"
#include "json_sysinfo.h"
#include "json_tabletinfo.h"
#include "json_hiveinfo.h"
#include "json_bsgroupinfo.h"
#include "json_bscontrollerinfo.h"
#include "json_config.h"
#include "json_counters.h"
#include "json_topicinfo.h"
#include "json_pqconsumerinfo.h"
#include "json_tabletcounters.h"
#include "json_storage.h"
#include "json_metainfo.h"
#include "json_browse.h"
#include "json_cluster.h"
#include "json_content.h"
#include "json_labeledcounters.h"
#include "json_tenants.h"
#include "json_hivestats.h"
#include "json_tenantinfo.h"
#include "json_whoami.h"
#include "json_query.h"
#include "json_netinfo.h"
#include "json_compute.h"
#include "counters_hosts.h"
#include "json_healthcheck.h"
#include "json_nodes.h"
#include "json_acl.h"


namespace NKikimr::NViewer {

template <>
void TViewerJsonHadlers::Init() {
    JsonHandlers["viewer/json/nodelist"] = new TJsonHandler<TJsonNodeList>;
    JsonHandlers["viewer/json/nodeinfo"] = new TJsonHandler<TJsonNodeInfo>;
    JsonHandlers["viewer/json/vdiskinfo"] = new TJsonHandler<TJsonVDiskInfo>;
    JsonHandlers["viewer/json/pdiskinfo"] = new TJsonHandler<TJsonPDiskInfo>;
    JsonHandlers["viewer/json/describe"] = new TJsonHandler<TJsonDescribe>;
    JsonHandlers["viewer/json/hotkeys"] = new TJsonHandler<TJsonHotkeys>;
    JsonHandlers["viewer/json/sysinfo"] = new TJsonHandler<TJsonSysInfo>;
    JsonHandlers["viewer/json/tabletinfo"] = new TJsonHandler<TJsonTabletInfo>;
    JsonHandlers["viewer/json/hiveinfo"] = new TJsonHandler<TJsonHiveInfo>;
    JsonHandlers["viewer/json/bsgroupinfo"] = new TJsonHandler<TJsonBSGroupInfo>;
    JsonHandlers["viewer/json/bscontrollerinfo"] = new TJsonHandler<TJsonBSControllerInfo>;
    JsonHandlers["viewer/json/config"] = new TJsonHandler<TJsonConfig>;
    JsonHandlers["viewer/json/counters"] = new TJsonHandler<TJsonCounters>;
    JsonHandlers["viewer/json/topicinfo"] = new TJsonHandler<TJsonTopicInfo>;
    JsonHandlers["viewer/json/pqconsumerinfo"] = new TJsonHandler<TJsonPQConsumerInfo>();
    JsonHandlers["viewer/json/tabletcounters"] = new TJsonHandler<TJsonTabletCounters>;
    JsonHandlers["viewer/json/storage"] = new TJsonHandler<TJsonStorage>;
    JsonHandlers["viewer/json/metainfo"] = new TJsonHandler<TJsonMetaInfo>;
    JsonHandlers["viewer/json/browse"] = new TJsonHandler<TJsonBrowse>;
    JsonHandlers["viewer/json/cluster"] = new TJsonHandler<TJsonCluster>;
    JsonHandlers["viewer/json/content"] = new TJsonHandler<TJsonContent>;
    JsonHandlers["viewer/json/labeledcounters"] = new TJsonHandler<TJsonLabeledCounters>;
    JsonHandlers["viewer/json/tenants"] = new TJsonHandler<TJsonTenants>;
    JsonHandlers["viewer/json/hivestats"] = new TJsonHandler<TJsonHiveStats>;
    JsonHandlers["viewer/json/tenantinfo"] = new TJsonHandler<TJsonTenantInfo>;
    JsonHandlers["viewer/json/whoami"] = new TJsonHandler<TJsonWhoAmI>;
    JsonHandlers["viewer/json/query"] = new TJsonHandler<TJsonQuery>;
    JsonHandlers["viewer/json/netinfo"] = new TJsonHandler<TJsonNetInfo>;
    JsonHandlers["viewer/json/compute"] = new TJsonHandler<TJsonCompute>;
    JsonHandlers["viewer/json/healthcheck"] = new TJsonHandler<TJsonHealthCheck>;
    JsonHandlers["viewer/json/nodes"] = new TJsonHandler<TJsonNodes>;
    JsonHandlers["viewer/json/acl"] = new TJsonHandler<TJsonACL>;
}

}
