#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include "json_handlers.h"

#include "json_nodelist.h"
#include "json_nodeinfo.h"
#include "json_vdiskinfo.h"
#include "json_pdiskinfo.h"
#include "json_describe.h"
#include "json_local_rpc.h"
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
#include "json_storage_usage.h"
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
#include "json_healthcheck.h"
#include "json_nodes.h"
#include "json_acl.h"
#include "json_graph.h"
#include "json_render.h"
#include "json_autocomplete.h"

namespace NKikimr::NViewer {

void InitViewerJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/json/nodelist", new TJsonHandler<TJsonNodeList>);
    jsonHandlers.AddHandler("/viewer/json/nodeinfo", new TJsonHandler<TJsonNodeInfo>);
    jsonHandlers.AddHandler("/viewer/json/sysinfo", new TJsonHandler<TJsonSysInfo>);
    jsonHandlers.AddHandler("/viewer/json/vdiskinfo", new TJsonHandler<TJsonVDiskInfo>);
    jsonHandlers.AddHandler("/viewer/json/pdiskinfo", new TJsonHandler<TJsonPDiskInfo>);
    jsonHandlers.AddHandler("/viewer/json/tabletinfo", new TJsonHandler<TJsonTabletInfo>);
    jsonHandlers.AddHandler("/viewer/json/describe", new TJsonHandler<TJsonDescribe>);
    jsonHandlers.AddHandler("/viewer/json/describe_topic", new TJsonHandler<TJsonDescribeTopic>);
    jsonHandlers.AddHandler("/viewer/json/describe_consumer", new TJsonHandler<TJsonDescribeConsumer>);
    jsonHandlers.AddHandler("/viewer/json/hotkeys", new TJsonHandler<TJsonHotkeys>);
    jsonHandlers.AddHandler("/viewer/json/hiveinfo", new TJsonHandler<TJsonHiveInfo>);
    jsonHandlers.AddHandler("/viewer/json/bsgroupinfo", new TJsonHandler<TJsonBSGroupInfo>);
    jsonHandlers.AddHandler("/viewer/json/bscontrollerinfo", new TJsonHandler<TJsonBSControllerInfo>);
    jsonHandlers.AddHandler("/viewer/json/config", new TJsonHandler<TJsonConfig>);
    jsonHandlers.AddHandler("/viewer/json/counters", new TJsonHandler<TJsonCounters>);
    jsonHandlers.AddHandler("/viewer/json/topicinfo", new TJsonHandler<TJsonTopicInfo>);
    jsonHandlers.AddHandler("/viewer/json/pqconsumerinfo", new TJsonHandler<TJsonPQConsumerInfo>);
    jsonHandlers.AddHandler("/viewer/json/tabletcounters", new TJsonHandler<TJsonTabletCounters>);
    jsonHandlers.AddHandler("/viewer/json/storage", new TJsonHandler<TJsonStorage>);
    jsonHandlers.AddHandler("/viewer/json/storage_usage", new TJsonHandler<TJsonStorageUsage>);
    jsonHandlers.AddHandler("/viewer/json/metainfo", new TJsonHandler<TJsonMetaInfo>);
    jsonHandlers.AddHandler("/viewer/json/browse", new TJsonHandler<TJsonBrowse>);
    jsonHandlers.AddHandler("/viewer/json/cluster", new TJsonHandler<TJsonCluster>);
    jsonHandlers.AddHandler("/viewer/json/content", new TJsonHandler<TJsonContent>);
    jsonHandlers.AddHandler("/viewer/json/labeledcounters", new TJsonHandler<TJsonLabeledCounters>);
    jsonHandlers.AddHandler("/viewer/json/tenants", new TJsonHandler<TJsonTenants>);
    jsonHandlers.AddHandler("/viewer/json/hivestats", new TJsonHandler<TJsonHiveStats>);
    jsonHandlers.AddHandler("/viewer/json/tenantinfo", new TJsonHandler<TJsonTenantInfo>);
    jsonHandlers.AddHandler("/viewer/json/whoami", new TJsonHandler<TJsonWhoAmI>);
    jsonHandlers.AddHandler("/viewer/json/query", new TJsonHandler<TJsonQuery>);
    jsonHandlers.AddHandler("/viewer/json/netinfo", new TJsonHandler<TJsonNetInfo>);
    jsonHandlers.AddHandler("/viewer/json/compute", new TJsonHandler<TJsonCompute>);
    jsonHandlers.AddHandler("/viewer/json/healthcheck", new TJsonHandler<TJsonHealthCheck>);
    jsonHandlers.AddHandler("/viewer/json/nodes", new TJsonHandler<TJsonNodes>);
    jsonHandlers.AddHandler("/viewer/json/acl", new TJsonHandler<TJsonACL>);
    jsonHandlers.AddHandler("/viewer/json/graph", new TJsonHandler<TJsonGraph>);
    jsonHandlers.AddHandler("/viewer/json/render", new TJsonHandler<TJsonRender>);
    jsonHandlers.AddHandler("/viewer/json/autocomplete", new TJsonHandler<TJsonAutocomplete>);
}

}
