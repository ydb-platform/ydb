#include "attrs.h"


namespace NYql {

namespace NCommonAttrs {
    TString ACTOR_NODEID_ATTR = "yql_actor_node_id";
    TString ROLE_ATTR = "yql_role";
    TString HOSTNAME_ATTR = "yql_hostname";
    TString GRPCPORT_ATTR = "yql_grpc_port";
    TString REVISION_ATTR = "yql_revision";
    TString INTERCONNECTPORT_ATTR = "yql_interconnect_port";
    TString EPOCH_ATTR = "yql_epoch";
    TString PINGTIME_ATTR = "yql_ping_time";

    TString OPERATIONID_ATTR = "yql_operation_id";
    TString OPERATIONSIZE_ATTR = "yql_operation_size";
    TString JOBID_ATTR = "yql_job_id";

    TString CLUSTERNAME_ATTR = "yql_cluster_name";

    TString UPLOAD_EXECUTABLE_ATTR = "yql_upload_executable";
}

}
