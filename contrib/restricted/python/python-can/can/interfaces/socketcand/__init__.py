"""
Interface to socketcand
see https://github.com/linux-can/socketcand

Copyright (C) 2021  DOMOLOGIC GmbH
http://www.domologic.de
"""

__all__ = [
    "SocketCanDaemonBus",
    "detect_beacon",
    "socketcand",
]

from .socketcand import SocketCanDaemonBus, detect_beacon
