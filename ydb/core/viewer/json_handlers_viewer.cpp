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
void TViewerJsonHandlers::Init() {
    Router.RegisterGetHandler("/json/nodelist", std::make_shared<TJsonHandler<TJsonNodeList>>());
    Router.RegisterGetHandler("/json/nodeinfo", std::make_shared<TJsonHandler<TJsonNodeInfo>>());
    Router.RegisterGetHandler("/json/vdiskinfo", std::make_shared<TJsonHandler<TJsonVDiskInfo>>());
    Router.RegisterGetHandler("/json/pdiskinfo", std::make_shared<TJsonHandler<TJsonPDiskInfo>>());
    Router.RegisterGetHandler("/json/describe", std::make_shared<TJsonHandler<TJsonDescribe>>());
    Router.RegisterGetHandler("/json/hotkeys", std::make_shared<TJsonHandler<TJsonHotkeys>>());
    Router.RegisterGetHandler("/json/sysinfo", std::make_shared<TJsonHandler<TJsonSysInfo>>());
    Router.RegisterGetHandler("/json/tabletinfo", std::make_shared<TJsonHandler<TJsonTabletInfo>>());
    Router.RegisterGetHandler("/json/hiveinfo", std::make_shared<TJsonHandler<TJsonHiveInfo>>());
    Router.RegisterGetHandler("/json/bsgroupinfo", std::make_shared<TJsonHandler<TJsonBSGroupInfo>>());
    Router.RegisterGetHandler("/json/bscontrollerinfo", std::make_shared<TJsonHandler<TJsonBSControllerInfo>>());
    Router.RegisterGetHandler("/json/config", std::make_shared<TJsonHandler<TJsonConfig>>());
    Router.RegisterGetHandler("/json/counters", std::make_shared<TJsonHandler<TJsonCounters>>());
    Router.RegisterGetHandler("/json/topicinfo", std::make_shared<TJsonHandler<TJsonTopicInfo>>());
    Router.RegisterGetHandler("/json/pqconsumerinfo", std::make_shared<TJsonHandler<TJsonPQConsumerInfo>>());
    Router.RegisterGetHandler("/json/tabletcounters", std::make_shared<TJsonHandler<TJsonTabletCounters>>());
    Router.RegisterGetHandler("/json/storage", std::make_shared<TJsonHandler<TJsonStorage>>());
    Router.RegisterGetHandler("/json/metainfo", std::make_shared<TJsonHandler<TJsonMetaInfo>>());
    Router.RegisterGetHandler("/json/browse", std::make_shared<TJsonHandler<TJsonBrowse>>());
    Router.RegisterGetHandler("/json/cluster", std::make_shared<TJsonHandler<TJsonCluster>>());
    Router.RegisterGetHandler("/json/content", std::make_shared<TJsonHandler<TJsonContent>>());
    Router.RegisterGetHandler("/json/labeledcounters", std::make_shared<TJsonHandler<TJsonLabeledCounters>>());
    Router.RegisterGetHandler("/json/tenants", std::make_shared<TJsonHandler<TJsonTenants>>());
    Router.RegisterGetHandler("/json/hivestats", std::make_shared<TJsonHandler<TJsonHiveStats>>());
    Router.RegisterGetHandler("/json/tenantinfo", std::make_shared<TJsonHandler<TJsonTenantInfo>>());
    Router.RegisterGetHandler("/json/whoami", std::make_shared<TJsonHandler<TJsonWhoAmI>>());
    Router.RegisterGetHandler("/json/query", std::make_shared<TJsonHandler<TJsonQuery>>());
    Router.RegisterGetHandler("/json/netinfo", std::make_shared<TJsonHandler<TJsonNetInfo>>());
    Router.RegisterGetHandler("/json/compute", std::make_shared<TJsonHandler<TJsonCompute>>());
    Router.RegisterGetHandler("/json/healthcheck", std::make_shared<TJsonHandler<TJsonHealthCheck>>());
    Router.RegisterGetHandler("/json/nodes", std::make_shared<TJsonHandler<TJsonNodes>>());
    Router.RegisterGetHandler("/json/acl", std::make_shared<TJsonHandler<TJsonACL>>());
}

}
