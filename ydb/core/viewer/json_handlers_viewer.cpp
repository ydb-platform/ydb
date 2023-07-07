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

namespace NKikimr::NViewer {

template <>
void TViewerJsonHandlers::Init() {
    JsonHandlers["/json/nodelist"] = new TJsonHandler<TJsonNodeList>;
    JsonHandlers["/json/nodeinfo"] = new TJsonHandler<TJsonNodeInfo>;
    JsonHandlers["/json/vdiskinfo"] = new TJsonHandler<TJsonVDiskInfo>;
    JsonHandlers["/json/pdiskinfo"] = new TJsonHandler<TJsonPDiskInfo>;
    JsonHandlers["/json/describe"] = new TJsonHandler<TJsonDescribe>;
    JsonHandlers["/json/describe_topic"] = new TJsonHandler<TJsonDescribeTopic>;
    JsonHandlers["/json/describe_consumer"] = new TJsonHandler<TJsonDescribeConsumer>;
    JsonHandlers["/json/hotkeys"] = new TJsonHandler<TJsonHotkeys>;
    JsonHandlers["/json/sysinfo"] = new TJsonHandler<TJsonSysInfo>;
    JsonHandlers["/json/tabletinfo"] = new TJsonHandler<TJsonTabletInfo>;
    JsonHandlers["/json/hiveinfo"] = new TJsonHandler<TJsonHiveInfo>;
    JsonHandlers["/json/bsgroupinfo"] = new TJsonHandler<TJsonBSGroupInfo>;
    JsonHandlers["/json/bscontrollerinfo"] = new TJsonHandler<TJsonBSControllerInfo>;
    JsonHandlers["/json/config"] = new TJsonHandler<TJsonConfig>;
    JsonHandlers["/json/counters"] = new TJsonHandler<TJsonCounters>;
    JsonHandlers["/json/topicinfo"] = new TJsonHandler<TJsonTopicInfo>;
    JsonHandlers["/json/pqconsumerinfo"] = new TJsonHandler<TJsonPQConsumerInfo>();
    JsonHandlers["/json/tabletcounters"] = new TJsonHandler<TJsonTabletCounters>;
    JsonHandlers["/json/storage"] = new TJsonHandler<TJsonStorage>;
    JsonHandlers["/json/metainfo"] = new TJsonHandler<TJsonMetaInfo>;
    JsonHandlers["/json/browse"] = new TJsonHandler<TJsonBrowse>;
    JsonHandlers["/json/cluster"] = new TJsonHandler<TJsonCluster>;
    JsonHandlers["/json/content"] = new TJsonHandler<TJsonContent>;
    JsonHandlers["/json/labeledcounters"] = new TJsonHandler<TJsonLabeledCounters>;
    JsonHandlers["/json/tenants"] = new TJsonHandler<TJsonTenants>;
    JsonHandlers["/json/hivestats"] = new TJsonHandler<TJsonHiveStats>;
    JsonHandlers["/json/tenantinfo"] = new TJsonHandler<TJsonTenantInfo>;
    JsonHandlers["/json/whoami"] = new TJsonHandler<TJsonWhoAmI>;
    JsonHandlers["/json/query"] = new TJsonHandler<TJsonQuery>;
    JsonHandlers["/json/netinfo"] = new TJsonHandler<TJsonNetInfo>;
    JsonHandlers["/json/compute"] = new TJsonHandler<TJsonCompute>;
    JsonHandlers["/json/healthcheck"] = new TJsonHandler<TJsonHealthCheck>;
    JsonHandlers["/json/nodes"] = new TJsonHandler<TJsonNodes>;
    JsonHandlers["/json/acl"] = new TJsonHandler<TJsonACL>;
}}
