import argparse
from ..auth import DEFAULT_FILE as DEFAULT_FILE, UserTokenCollection as UserTokenCollection, poll_for_token as poll_for_token, request_device_code as request_device_code
from ..config import REGIONS as REGIONS

def parse_auth(args: argparse.Namespace) -> None:
    """Authenticate with Logfire.

    This will authenticate your machine with Logfire and store the credentials.
    """
