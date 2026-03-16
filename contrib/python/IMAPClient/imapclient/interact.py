#!/usr/bin/python

# Copyright (c) 2020, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

import argparse
from getpass import getpass

from . import imapclient
from .config import create_client_from_config, get_config_defaults, parse_config_file


def command_line() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-H", "--host", dest="host", action="store", help="IMAP host connect to"
    )
    parser.add_argument(
        "-u",
        "--username",
        dest="username",
        action="store",
        help="Username to login with",
    )
    parser.add_argument(
        "-p",
        "--password",
        dest="password",
        action="store",
        help="Password to login with",
    )
    parser.add_argument(
        "-P",
        "--port",
        dest="port",
        action="store",
        type=int,
        default=None,
        help="IMAP port to use (default is 993 for TLS, or 143 otherwise)",
    )

    ssl_group = parser.add_mutually_exclusive_group()
    ssl_group.add_argument(
        "-s",
        "--ssl",
        dest="ssl",
        action="store_true",
        default=None,
        help="Use SSL/TLS connection (default)",
    )
    ssl_group.add_argument(
        "--insecure",
        dest="insecure",
        action="store_true",
        default=False,
        help="Use insecure connection (i.e. without SSL/TLS)",
    )

    parser.add_argument(
        "-f",
        "--file",
        dest="file",
        action="store",
        default=None,
        help="Config file (same as livetest)",
    )

    args = parser.parse_args()

    if args.file:
        if (
            args.host
            or args.username
            or args.password
            or args.port
            or args.ssl
            or args.insecure
        ):
            parser.error("If -f/--file is given no other options can be used")
        # Use the options in the config file
        args = parse_config_file(args.file)
        return args

    args.ssl = not args.insecure

    # Scan through arguments, filling in defaults and prompting when
    # a compulsory argument wasn't provided.
    compulsory_args = ("host", "username", "password")
    for name, default_value in get_config_defaults().items():
        value = getattr(args, name, default_value)
        if name in compulsory_args and value is None:
            value = getpass(name + ": ")
        setattr(args, name, value)

    return args


def main() -> int:
    args = command_line()
    print("Connecting...")
    client = create_client_from_config(args)
    print("Connected.")
    banner = '\nIMAPClient instance is "c"'

    def ptpython(c: imapclient.IMAPClient) -> None:
        from ptpython.repl import embed  # type: ignore[import-not-found]

        embed(globals(), locals())

    def ipython_400(c: imapclient.IMAPClient) -> None:
        from IPython.terminal.embed import (  # type: ignore[import-not-found]
            InteractiveShellEmbed,
        )

        ipshell = InteractiveShellEmbed(banner1=banner)
        ipshell("")

    def ipython_011(c: imapclient.IMAPClient) -> None:
        from IPython.frontend.terminal.embed import (  # type: ignore[import-not-found]
            InteractiveShellEmbed,
        )

        ipshell = InteractiveShellEmbed(banner1=banner)
        ipshell("")

    def ipython_010(c: imapclient.IMAPClient) -> None:
        from IPython.Shell import IPShellEmbed  # type: ignore[import-not-found]

        IPShellEmbed("", banner=banner)()

    def builtin(c: imapclient.IMAPClient) -> None:
        import code

        code.interact(banner, local={"c": c})

    shell_attempts = (
        ptpython,
        ipython_400,
        ipython_011,
        ipython_010,
        builtin,
    )
    for shell in shell_attempts:
        try:
            shell(client)
        except ImportError:
            pass
        else:
            break
    return 0


if __name__ == "__main__":
    main()
