#!/usr/bin/env python3
"""HTTP server for modbus simulator.

The modbus simulator contain 3 distinct parts:

- Datastore simulator, to define registers and their behaviour including actions: (simulator)(../../datastore/simulator.py)
- Modbus server: (server)(./http_server.py)
- HTTP server with REST API and web pages providing an online console in your browser

Multiple setups for different server types and/or devices are prepared in a (json file)(./setup.json), the detailed configuration is explained in (doc)(README.rst)

The command line parameters are kept to a minimum:

usage: main.py [-h] [--modbus_server MODBUS_SERVER]
               [--modbus_device MODBUS_DEVICE] [--http_host HTTP_HOST]
               [--http_port HTTP_PORT]
               [--log {critical,error,warning,info,debug}]
               [--json_file JSON_FILE]
               [--custom_actions_module CUSTOM_ACTIONS_MODULE]

Modbus server with REST-API and web server

options:
  -h, --help            show this help message and exit
  --modbus_server MODBUS_SERVER
                        use <modbus_server> from server_list in json file
  --modbus_device MODBUS_DEVICE
                        use <modbus_device> from device_list in json file
  --http_host HTTP_HOST
                        use <http_host> as host to bind http listen
  --http_port HTTP_PORT
                        use <http_port> as port to bind http listen
  --log {critical,error,warning,info,debug}
                        set log level, default is info
  --log_file LOG_FILE
                        name of server log file, default is "server.log"
  --json_file JSON_FILE
                        name of json_file, default is "setup.json"
  --custom_actions_module CUSTOM_ACTIONS_MODULE
                        python file with custom actions, default is none
"""
import argparse
import asyncio
import os

from pymodbus import pymodbus_apply_logging_config
from pymodbus.logging import Log
from pymodbus.server.simulator.http_server import ModbusSimulatorServer


def get_commandline(extras=None, cmdline=None):
    """Get command line arguments."""
    parser = argparse.ArgumentParser(
        description="Modbus server with REST-API and web server"
    )
    parser.add_argument(
        "--modbus_server",
        help="use <modbus_server> from server_list in json file",
        type=str,
    )
    parser.add_argument(
        "--modbus_device",
        help="use <modbus_device> from device_list in json file",
        type=str,
    )
    parser.add_argument(
        "--http_host",
        help="use <http_host> as host to bind http listen",
        type=str,
    )
    parser.add_argument(
        "--http_port",
        help="use <http_port> as port to bind http listen",
        type=str,
        default=8081,
    )
    parser.add_argument(
        "--log",
        choices=["critical", "error", "warning", "info", "debug"],
        help="set log level, default is info",
        default="info",
        type=str,
    )
    parser.add_argument(
        "--json_file",
        help='name of json file, default is "setup.json"',
        type=str,
        default=os.path.join(os.path.dirname(__file__), "setup.json"),
    )
    parser.add_argument(
        "--log_file",
        help='name of server log file, default is "server.log"',
        type=str,
    )
    parser.add_argument(
        "--custom_actions_module",
        help="python file with custom actions, default is none",
        type=str,
    )
    if extras:
        for extra in extras:
            parser.add_argument(extra[0], **extra[1])
    args = parser.parse_args(cmdline)
    pymodbus_apply_logging_config(args.log.upper())
    Log.info("Start simulator")
    cmd_args = {}
    for argument in args.__dict__:
        if argument == "log":
            continue
        if args.__dict__[argument] is not None:
            cmd_args[argument] = args.__dict__[argument]
    return cmd_args


async def run_main():
    """Run server async."""
    cmd_args = get_commandline()
    task = ModbusSimulatorServer(**cmd_args)
    await task.run_forever()


def main():
    """Run server."""
    asyncio.run(run_main(), debug=True)


if __name__ == "__main__":
    main()
