import argparse
import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

TOKEN_PATH = "/computeMetadata/v1/instance/service-accounts/default/token"
DEFAULT_TOKEN = "test-iam-token"
DEFAULT_EXPIRES_IN = 3600


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--port", type=int, required=True, help="HTTP port to listen on")
    parser.add_argument("--token", type=str, default=DEFAULT_TOKEN, help="IAM token to return")
    parser.add_argument("--expires-in", type=int, default=DEFAULT_EXPIRES_IN,
                        help="Token TTL in seconds")
    return parser.parse_args()


def make_handler(token, expires_in):
    class IamTokenHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != TOKEN_PATH:
                self.send_error(404, "Not Found")
                return

            flavor = self.headers.get("Metadata-Flavor", "")
            if flavor != "Google":
                logger.warning(f"Missing or wrong Metadata-Flavor header: '{flavor}'")
                self.send_error(400, "Metadata-Flavor: Google header required")
                return

            body = json.dumps({
                "access_token": token,
                "expires_in": expires_in,
                "token_type": "Bearer",
            }).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            logger.debug(f"Served token '{token}' to {self.client_address}")

        def log_message(self, format, *args):
            logger.debug(f"{self.client_address[0]} - {format % args}")

    return IamTokenHandler


def main():
    args = parse_args()
    handler_class = make_handler(args.token, args.expires_in)
    server = HTTPServer(("localhost", args.port), handler_class)
    logger.info(f"IAM emulator listening on port {args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
