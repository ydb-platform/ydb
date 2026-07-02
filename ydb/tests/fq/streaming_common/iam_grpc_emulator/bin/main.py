import argparse
import logging
import time
from concurrent import futures

import grpc
from google.protobuf import timestamp_pb2

from ydb.public.api.client.yc_public.iam import iam_token_service_pb2
from ydb.public.api.client.yc_public.iam import iam_token_service_pb2_grpc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DEFAULT_TOKEN = "test-iam-token"
DEFAULT_EXPIRES_IN = 3600


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--port", type=int, required=True, help="gRPC port to listen on")
    parser.add_argument("--token", type=str, default=DEFAULT_TOKEN, help="IAM token to return")
    parser.add_argument("--expires-in", type=int, default=DEFAULT_EXPIRES_IN,
                        help="Token TTL in seconds")
    return parser.parse_args()


def make_response(token, expires_in):
    expires_at = timestamp_pb2.Timestamp()
    expires_at.seconds = int(time.time()) + expires_in
    return iam_token_service_pb2.CreateIamTokenResponse(
        iam_token=token,
        expires_at=expires_at,
    )


class IamTokenServicer(iam_token_service_pb2_grpc.IamTokenServiceServicer):
    def __init__(self, token, expires_in):
        self.token = token
        self.expires_in = expires_in

    def Create(self, request, context):
        logger.debug("IamTokenService.Create called")
        return make_response(self.token, self.expires_in)

    def CreateForServiceAccount(self, request, context):
        logger.debug("IamTokenService.CreateForServiceAccount called, sa_id=%s", request.service_account_id)
        return make_response(self.token, self.expires_in)


def main():
    args = parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    iam_token_service_pb2_grpc.add_IamTokenServiceServicer_to_server(
        IamTokenServicer(args.token, args.expires_in),
        server,
    )
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    logger.info("IAM gRPC emulator listening on port %d", args.port)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
