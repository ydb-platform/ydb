import argparse
import logging
import random
import time
from concurrent import futures
import os

import grpc
from google.protobuf import timestamp_pb2

from ydb.public.api.client.yc_private.iam import iam_token_service_pb2
from ydb.public.api.client.yc_private.iam import iam_token_service_pb2_grpc

if os.environ.get("USE_ACCESS_SERVICE_V2", "true") == "true":
    from ydb.public.api.client.yc_private.accessservice import access_service_pb2
    from ydb.public.api.client.yc_private.accessservice import access_service_pb2_grpc
else:
    from ydb.public.api.client.yc_private.servicecontrol import access_service_pb2
    from ydb.public.api.client.yc_private.servicecontrol import access_service_pb2_grpc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# PRNG
random.seed(0)

DEFAULT_TOKEN = "root@builtin"
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
        self.invocation = 0

    def _pick_token(self):
        return self.token

    def Create(self, request, context):
        logger.debug("IamTokenService.Create called")
        return make_response(self._pick_token(), self.expires_in)

    def CreateForServiceAccount(self, request, context):
        logger.debug("IamTokenService.CreateForServiceAccount called, sa_id=%s", request.service_account_id)
        return make_response(self._pick_token(), self.expires_in)

    def CreateForService(self, request, context):
        token =self._pick_token()
        logger.debug(
            "IamTokenService.CreateForService called, service_id=%s microservice_id=%s resource_id=%s target_sa=%s token=%s",
            request.service_id, request.microservice_id, request.resource_id, request.target_service_account_id, token
        )

        target_sa = request.target_service_account_id

        if target_sa == 'bad':
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details("Reject bad SA")

        if target_sa == 'flaky':
            if random.random() < 0.1:
                logger.debug("Simulating random failure")
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("Too busy to respond")

        if target_sa == 'odd':
            self.invocation += 1
            if self.invocation % 2 != 0:
                logger.debug("Simulating odd failure")
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("Too busy to respond")

        return make_response(token, self.expires_in)


def make_dummy_subject():
    return access_service_pb2.Subject(
        user_account=access_service_pb2.Subject.UserAccount(
            id='bob',
            federation_id='mock federation'
        )
    )


class AccessServicer(access_service_pb2_grpc.AccessServiceServicer):
    def __init__(self):
        self.invocation = 0
        pass

    def Authenticate(self, request, context):
        logger.debug("AccessService Authenticate called")
        return access_service_pb2.AuthenticateResponse(
            subject=make_dummy_subject()
        )

    def Authorize(self, request, context):
        logger.debug("AccessService Authorize called, iam_token=%s, permission=%s, resource_path=%s", request.iam_token, request.permission, repr(request.resource_path))

        if request.permission != 'iam.serviceAccounts.use':
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Don't know about this permission: " + request.permission)
            return access_service_pb2.AuthorizeResponse()

        for resource in request.resource_path:
            if resource.type != 'iam.serviceAccount':
                context.set_code(grpc.StatusCode.UNAUTHORIZED)
                context.set_details("Don't know about this type: " + resource.type)
                return access_service_pb2.AuthorizeResponse()

            if resource.id == 'odd':
                self.invocation += 1
                if self.invocation % 2 != 0:
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details("Too busy to respond for odd")
                    return access_service_pb2.AuthorizeResponse()

            if resource.id == 'flaky':
                if random.random() < 0.1:
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details("Too busy to respond for flaky")
                    return access_service_pb2.AuthorizeResponse()

            if resource.id == 'bad':
                context.set_code(grpc.StatusCode.UNAUTHORIZED)
                context.set_details("This one is bad")
                return access_service_pb2.AuthorizeResponse()

            return access_service_pb2.AuthorizeResponse(
                subject=make_dummy_subject()
            )

        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details("Don't know about those resources")
        return access_service_pb2.AuthorizeResponse()


def main():
    args = parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    access_service_pb2_grpc.add_AccessServiceServicer_to_server(
        AccessServicer(),
        server,
    )
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
