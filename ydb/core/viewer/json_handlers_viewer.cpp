#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include "json_handlers.h"

#include "json_nodelist.h"
#include "json_nodeinfo.h"
#include "json_vdiskinfo.h"
#include "json_pdiskinfo.h"
#include "json_describe.h"
#include "json_describe_topic.h"
#include "json_describe_consumer.h"
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
#include "check_access.h"

namespace NKikimr::NViewer {

void InitViewerCapabilitiesJsonHandler(TJsonHandlers& jsonHandlers);

void InitViewerJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitViewerCapabilitiesJsonHandler(jsonHandlers);
    jsonHandlers.AddHandler("/viewer/nodelist", new TJsonHandler<TJsonNodeList>);
    jsonHandlers.AddHandler("/viewer/nodeinfo", new TJsonHandler<TJsonNodeInfo>);
    jsonHandlers.AddHandler("/viewer/sysinfo", new TJsonHandler<TJsonSysInfo>);
    jsonHandlers.AddHandler("/viewer/vdiskinfo", new TJsonHandler<TJsonVDiskInfo>);
    jsonHandlers.AddHandler("/viewer/pdiskinfo", new TJsonHandler<TJsonPDiskInfo>);
    jsonHandlers.AddHandler("/viewer/tabletinfo", new TJsonHandler<TJsonTabletInfo>);
    jsonHandlers.AddHandler("/viewer/describe", new TJsonHandler<TJsonDescribe>);
    jsonHandlers.AddHandler("/viewer/describe_topic", new TJsonHandler<TJsonDescribeTopic>);
    jsonHandlers.AddHandler("/viewer/describe_consumer", new TJsonHandler<TJsonDescribeConsumer>);
    jsonHandlers.AddHandler("/viewer/hotkeys", new TJsonHandler<TJsonHotkeys>);
    jsonHandlers.AddHandler("/viewer/hiveinfo", new TJsonHandler<TJsonHiveInfo>);
    jsonHandlers.AddHandler("/viewer/bsgroupinfo", new TJsonHandler<TJsonBSGroupInfo>);
    jsonHandlers.AddHandler("/viewer/bscontrollerinfo", new TJsonHandler<TJsonBSControllerInfo>);
    jsonHandlers.AddHandler("/viewer/config", new TJsonHandler<TJsonConfig>);
    jsonHandlers.AddHandler("/viewer/counters", new TJsonHandler<TJsonCounters>);
    jsonHandlers.AddHandler("/viewer/topicinfo", new TJsonHandler<TJsonTopicInfo>);
    jsonHandlers.AddHandler("/viewer/pqconsumerinfo", new TJsonHandler<TJsonPQConsumerInfo>);
    jsonHandlers.AddHandler("/viewer/tabletcounters", new TJsonHandler<TJsonTabletCounters>);
    jsonHandlers.AddHandler("/viewer/storage", new TJsonHandler<TJsonStorage>);
    jsonHandlers.AddHandler("/viewer/storage_usage", new TJsonHandler<TJsonStorageUsage>);
    jsonHandlers.AddHandler("/viewer/metainfo", new TJsonHandler<TJsonMetaInfo>);
    jsonHandlers.AddHandler("/viewer/browse", new TJsonHandler<TJsonBrowse>);
    jsonHandlers.AddHandler("/viewer/cluster", new TJsonHandler<TJsonCluster>);
    jsonHandlers.AddHandler("/viewer/content", new TJsonHandler<TJsonContent>);
    jsonHandlers.AddHandler("/viewer/labeledcounters", new TJsonHandler<TJsonLabeledCounters>);
    jsonHandlers.AddHandler("/viewer/tenants", new TJsonHandler<TJsonTenants>);
    jsonHandlers.AddHandler("/viewer/hivestats", new TJsonHandler<TJsonHiveStats>);
    jsonHandlers.AddHandler("/viewer/tenantinfo", new TJsonHandler<TJsonTenantInfo>);
    jsonHandlers.AddHandler("/viewer/whoami", new TJsonHandler<TJsonWhoAmI>);
    jsonHandlers.AddHandler("/viewer/query", new TJsonHandler<TJsonQuery>);
    jsonHandlers.AddHandler("/viewer/netinfo", new TJsonHandler<TJsonNetInfo>);
    jsonHandlers.AddHandler("/viewer/compute", new TJsonHandler<TJsonCompute>);
    jsonHandlers.AddHandler("/viewer/healthcheck", new TJsonHandler<TJsonHealthCheck>);
    jsonHandlers.AddHandler("/viewer/nodes", new TJsonHandler<TJsonNodes>);
    jsonHandlers.AddHandler("/viewer/acl", new TJsonHandler<TJsonACL>);
    jsonHandlers.AddHandler("/viewer/graph", new TJsonHandler<TJsonGraph>);
    jsonHandlers.AddHandler("/viewer/render", new TJsonHandler<TJsonRender>);
    jsonHandlers.AddHandler("/viewer/autocomplete", new TJsonHandler<TJsonAutocomplete>);
    jsonHandlers.AddHandler("/viewer/check_access", new TJsonHandler<TCheckAccess>);
}

}
