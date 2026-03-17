from __future__ import annotations

import argparse
import sys
import webbrowser
from urllib.parse import urlparse

from ..auth import DEFAULT_FILE, UserTokenCollection, poll_for_token, request_device_code
from ..config import REGIONS


def parse_auth(args: argparse.Namespace) -> None:
    """Authenticate with Logfire.

    This will authenticate your machine with Logfire and store the credentials.
    """
    logfire_url: str | None = args.logfire_url

    tokens_collection = UserTokenCollection()
    logged_in = tokens_collection.is_logged_in(logfire_url)

    if logged_in:
        sys.stderr.writelines(
            (
                f'You are already logged in. (Your credentials are stored in {DEFAULT_FILE})\n',
                'If you would like to log in using a different account, use the --region argument:\n',
                'logfire --region <region> auth\n',
            )
        )
        return

    sys.stderr.writelines(
        (
            '\n',
            'Welcome to Logfire! 🔥\n',
            'Before you can send data to Logfire, we need to authenticate you.\n',
            '\n',
        )
    )
    if not logfire_url:
        selected_region = -1
        while not (1 <= selected_region <= len(REGIONS)):
            sys.stderr.write('Logfire is available in multiple data regions. Please select one:\n')
            for i, (region_id, region_data) in enumerate(REGIONS.items(), start=1):
                sys.stderr.write(f'{i}. {region_id.upper()} (GCP region: {region_data["gcp_region"]})\n')

            try:
                selected_region = int(
                    input(f'Selected region [{"/".join(str(i) for i in range(1, len(REGIONS) + 1))}]: ')
                )
            except ValueError:
                selected_region = -1
        logfire_url = list(REGIONS.values())[selected_region - 1]['base_url']

    device_code, frontend_auth_url = request_device_code(args._session, logfire_url)
    frontend_host = urlparse(frontend_auth_url).netloc

    # We are not using the `prompt` parameter from `input` here because we want to write to stderr.
    sys.stderr.write(f'Press Enter to open {frontend_host} in your browser...\n')
    input()

    try:
        webbrowser.open(frontend_auth_url, new=2)
    except webbrowser.Error:
        pass
    sys.stderr.writelines(
        (
            f"Please open {frontend_auth_url} in your browser to authenticate if it hasn't already.\n",
            'Waiting for you to authenticate with Logfire...\n',
        )
    )

    tokens_collection.add_token(logfire_url, poll_for_token(args._session, device_code, logfire_url))
    sys.stderr.write('Successfully authenticated!\n')
    sys.stderr.write(f'\nYour Logfire credentials are stored in {DEFAULT_FILE}\n')
