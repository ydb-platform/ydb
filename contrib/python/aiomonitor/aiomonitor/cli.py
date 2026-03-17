import argparse
import asyncio

from .monitor import MONITOR_HOST, MONITOR_TERMUI_PORT
from .telnet import TelnetClient


async def async_monitor_client(host: str, port: int) -> None:
    async with TelnetClient(host, port) as client:
        await client.interact()


def monitor_client(host: str, port: int) -> None:
    try:
        asyncio.run(async_monitor_client(host, port))
    except KeyboardInterrupt:
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-H",
        "--host",
        dest="monitor_host",
        default=MONITOR_HOST,
        type=str,
        help="monitor host ip",
    )
    parser.add_argument(
        "-p",
        "--port",
        dest="monitor_port",
        default=MONITOR_TERMUI_PORT,
        type=int,
        help="monitor port number",
    )
    args = parser.parse_args()
    monitor_client(args.monitor_host, args.monitor_port)


if __name__ == "__main__":
    main()
