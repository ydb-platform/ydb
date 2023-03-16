#include "utils.h"

#include <ydb/core/fq/libs/db_schema/db_schema.h>

namespace NFq {

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvNodesHealthCheckRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    TRequestCounters requestCounters{nullptr, Counters.GetCommonCounters(RTC_NODES_HEALTH_CHECK)};
    requestCounters.IncInFly();

    const TEvControlPlaneStorage::TEvNodesHealthCheckRequest& event = *ev->Get();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const auto& request = event.Request;
    const TString tenant = request.tenant();
    const auto& node = request.node();
    const ui32 nodeId = node.node_id();
    const TString instanceId = node.instance_id();
    const TString hostName = node.hostname();
    const ui64 activeWorkers = node.active_workers();
    const ui64 memoryLimit = node.memory_limit();
    const ui64 memoryAllocated = node.memory_allocated();
    const ui32 icPort = node.interconnect_port();
    const TString dataCenter = node.data_center();
    const TString nodeAddress = node.node_address();
    const auto ttl = TDuration::Seconds(5);
    const auto deadline = startTime + ttl * 3;

    CPS_LOG_T("NodesHealthCheckRequest: {" << request.DebugString() << "}");

    NYql::TIssues issues = ValidateNodesHealthCheck(tenant, instanceId, hostName);
    if (issues) {
        CPS_LOG_W("NodesHealthCheckRequest: {" << request.DebugString() << "} validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvNodesHealthCheckResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(NodesHealthCheckRequest, "", 0, "", "", delta, false);
        return;
    }

    std::shared_ptr<Fq::Private::NodesHealthCheckResult> response = std::make_shared<Fq::Private::NodesHealthCheckResult>();
    {
        auto* node = response->add_nodes();
        node->set_node_id(nodeId);
        node->set_instance_id(instanceId);
        node->set_hostname(hostName);
        node->set_active_workers(activeWorkers);
        node->set_memory_limit(memoryLimit);
        node->set_memory_allocated(memoryAllocated);
        node->set_node_address(nodeAddress);
        node->set_data_center(dataCenter);
    }

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "NodesHealthCheck(read)");
    readQueryBuilder.AddTimestamp("now", TInstant::Now());
    readQueryBuilder.AddString("tenant", tenant);
    readQueryBuilder.AddText(
        "SELECT `" NODE_ID_COLUMN_NAME "`, `" INSTANCE_ID_COLUMN_NAME "`, `" HOST_NAME_COLUMN_NAME "`, `" ACTIVE_WORKERS_COLUMN_NAME"`, `" MEMORY_LIMIT_COLUMN_NAME"`, "
        "`" MEMORY_ALLOCATED_COLUMN_NAME"`, `" INTERCONNECT_PORT_COLUMN_NAME "`, `" NODE_ADDRESS_COLUMN_NAME"`, `" DATA_CENTER_COLUMN_NAME "` FROM `" NODES_TABLE_NAME "`\n"
        "WHERE `" TENANT_COLUMN_NAME"` = $tenant AND `" EXPIRE_AT_COLUMN_NAME "` >= $now;\n"
    );

    auto prepareParams = [=, tablePathPrefix=YdbConnection->TablePathPrefix](const TVector<TResultSet>& resultSets) {
        for (const auto& resultSet : resultSets) {
            TResultSetParser parser(resultSet);
            while (parser.TryNextRow()) {
                auto nid = *parser.ColumnParser(NODE_ID_COLUMN_NAME).GetOptionalUint32();
                if (nid != nodeId) {
                    auto* node = response->add_nodes();
                    node->set_node_id(nid);
                    node->set_instance_id(*parser.ColumnParser(INSTANCE_ID_COLUMN_NAME).GetOptionalString());
                    node->set_hostname(*parser.ColumnParser(HOST_NAME_COLUMN_NAME).GetOptionalString());
                    node->set_active_workers(*parser.ColumnParser(ACTIVE_WORKERS_COLUMN_NAME).GetOptionalUint64());
                    node->set_memory_limit(*parser.ColumnParser(MEMORY_LIMIT_COLUMN_NAME).GetOptionalUint64());
                    node->set_memory_allocated(*parser.ColumnParser(MEMORY_ALLOCATED_COLUMN_NAME).GetOptionalUint64());
                    node->set_interconnect_port(parser.ColumnParser(INTERCONNECT_PORT_COLUMN_NAME).GetOptionalUint32().GetOrElse(0));
                    node->set_node_address(*parser.ColumnParser(NODE_ADDRESS_COLUMN_NAME).GetOptionalString());
                    node->set_data_center(*parser.ColumnParser(DATA_CENTER_COLUMN_NAME).GetOptionalString());
                }
            }
        }

        TSqlQueryBuilder writeQueryBuilder(tablePathPrefix, "NodesHealthCheck(write)");
        writeQueryBuilder.AddString("tenant", tenant);
        writeQueryBuilder.AddUint32("node_id", nodeId);
        writeQueryBuilder.AddString("instance_id", instanceId);
        writeQueryBuilder.AddString("hostname", hostName);
        writeQueryBuilder.AddTimestamp("deadline", deadline);
        writeQueryBuilder.AddUint64("active_workers", activeWorkers);
        writeQueryBuilder.AddUint64("memory_limit", memoryLimit);
        writeQueryBuilder.AddUint64("memory_allocated", memoryAllocated);
        writeQueryBuilder.AddUint32("ic_port", icPort);
        writeQueryBuilder.AddString("node_address", nodeAddress);
        writeQueryBuilder.AddString("data_center", dataCenter);
        writeQueryBuilder.AddText(
            "UPSERT INTO `" NODES_TABLE_NAME "`\n"
            "(`" TENANT_COLUMN_NAME "`, `" NODE_ID_COLUMN_NAME "`, `" INSTANCE_ID_COLUMN_NAME "`,\n"
            "`" HOST_NAME_COLUMN_NAME "`, `" EXPIRE_AT_COLUMN_NAME "`, `" ACTIVE_WORKERS_COLUMN_NAME"`,\n"
            "`" MEMORY_LIMIT_COLUMN_NAME "`, `" MEMORY_ALLOCATED_COLUMN_NAME "`, `" INTERCONNECT_PORT_COLUMN_NAME "`,\n"
            "`" NODE_ADDRESS_COLUMN_NAME "`, `" DATA_CENTER_COLUMN_NAME"`)\n"
            "VALUES ($tenant ,$node_id, $instance_id, $hostname, $deadline, $active_workers, $memory_limit,\n"
            "$memory_allocated, $ic_port, $node_address, $data_center);\n"
        );
        const auto writeQuery = writeQueryBuilder.Build();
        return std::make_pair(writeQuery.Sql, writeQuery.Params);
    };

    const auto readQuery = readQueryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    TAsyncStatus status = ReadModifyWrite(readQuery.Sql, readQuery.Params, prepareParams, requestCounters, debugInfo);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvNodesHealthCheckResponse, Fq::Private::NodesHealthCheckResult>(
        "NodesHealthCheckRequest - NodesHealthCheckResult",
        NActors::TActivationContext::ActorSystem(),
        status,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(NodesHealthCheckRequest, tenant, nodeId, instanceId, hostName, delta, future.GetValue());
        });
}

} // NFq
