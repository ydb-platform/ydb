import argparse
from concurrent import futures
import logging
import os

import grpc

from ydb.public.api.grpc import (
    ydb_discovery_v1_pb2_grpc,
    ydb_federation_discovery_v1_pb2_grpc,
    ydb_scheme_v1_pb2_grpc,
)
from ydb.public.api.protos import ydb_federation_discovery_pb2, ydb_status_codes_pb2


def forward_to_cm(method, request, context):
    # Keep database/auth metadata and the caller's deadline intact.
    try:
        response, call = method.with_call(
            request,
            metadata=context.invocation_metadata(),
            timeout=context.time_remaining(),
        )
    except grpc.RpcError as error:
        context.set_trailing_metadata(error.trailing_metadata() or ())
        context.abort(error.code(), error.details())
    context.send_initial_metadata(call.initial_metadata() or ())
    context.set_trailing_metadata(call.trailing_metadata() or ())
    return response


class DiscoveryService(ydb_discovery_v1_pb2_grpc.DiscoveryServiceServicer):
    def ListEndpoints(self, request, context):
        # Ordinary SDK clients need CM's endpoints before loading metadata.
        with grpc.insecure_channel(f"localhost:{os.environ['CM_PORT']}") as channel:
            stub = ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub(channel)
            return forward_to_cm(stub.ListEndpoints, request, context)


class SchemeService(ydb_scheme_v1_pb2_grpc.SchemeServiceServicer):
    def DescribePath(self, request, context):
        # KQP determines the external entity type before discovering clusters.
        with grpc.insecure_channel(f"localhost:{os.environ['CM_PORT']}") as channel:
            stub = ydb_scheme_v1_pb2_grpc.SchemeServiceStub(channel)
            return forward_to_cm(stub.DescribePath, request, context)


class FederationDiscoveryService(ydb_federation_discovery_v1_pb2_grpc.FederationDiscoveryServiceServicer):
    def ListFederationDatabases(self, request, context):
        database = dict(context.invocation_metadata()).get("x-ydb-database", "")
        response = ydb_federation_discovery_pb2.ListFederationDatabasesResponse()
        response.operation.ready = True
        if database not in ("/logbroker-federation/prod", "/logbroker-federation/test"):
            response.operation.status = ydb_status_codes_pb2.StatusIds.BAD_REQUEST
            response.operation.issues.add(message=f"Unknown federation database: {database!r}")
            return response

        result = ydb_federation_discovery_pb2.ListFederationDatabasesResult(
            control_plane_endpoint=f"localhost:{os.environ['CM_PORT']}",
            self_location="cluster_a",
        )
        for name in ("cluster_a", "cluster_b"):
            result.federation_databases.add(
                name=name,
                id=name,
                path=f"/Root{database}",
                endpoint=f"localhost:{os.environ[f'{name}_port']}",
                location=name,
                status=ydb_federation_discovery_pb2.DatabaseInfo.AVAILABLE,
                weight=100,
            )
        response.operation.status = ydb_status_codes_pb2.StatusIds.SUCCESS
        response.operation.result.Pack(result)
        logging.info("Federation discovery for %s: %s", database, result)
        return response


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    ydb_discovery_v1_pb2_grpc.add_DiscoveryServiceServicer_to_server(DiscoveryService(), server)
    ydb_scheme_v1_pb2_grpc.add_SchemeServiceServicer_to_server(SchemeService(), server)
    ydb_federation_discovery_v1_pb2_grpc.add_FederationDiscoveryServiceServicer_to_server(
        FederationDiscoveryService(),
        server,
    )
    if not server.add_insecure_port(f"[::]:{args.port}"):
        raise RuntimeError(f"Failed to bind federation discovery port {args.port}")
    server.start()
    logging.info("Federation discovery listening on port %s", args.port)
    try:
        server.wait_for_termination()
    finally:
        server.stop(grace=0).wait()


if __name__ == "__main__":
    main()
