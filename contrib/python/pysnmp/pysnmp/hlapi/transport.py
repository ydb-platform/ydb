#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from typing import Tuple


from pysnmp import error
from pysnmp.carrier.base import AbstractTransport
from pysnmp.entity.engine import SnmpEngine

__all__ = []


class AbstractTransportTarget:
    retries: int
    timeout: float
    transport: "AbstractTransport | None"
    transport_address: Tuple[str, int]

    TRANSPORT_DOMAIN = None
    PROTO_TRANSPORT = AbstractTransport

    def __init__(
        self,
        timeout: float = 1,
        retries: int = 5,
        tagList=b"",
    ):
        self.timeout = timeout
        self.retries = retries
        self.tagList = tagList
        self.iface = None
        self.transport = None

        if not hasattr(self, "transport_address"):
            raise Exception(
                f"Please call .create() to construct {self.__class__.__name__} object"
            )

    @classmethod
    async def create(cls, address: Tuple[str, int], *args, **kwargs):
        """
        Asynchronously creates an instance of the class with the given address.

        Args:
            cls (Type): The class to create an instance of.
            address (Tuple[str, int]): A tuple containing the address as a string and the port as an integer.
            *args: Additional positional arguments to pass to the class initializer.
            **kwargs: Additional keyword arguments to pass to the class initializer.

        Returns:
            An instance of the class with the resolved transport address.

        """
        self = cls.__new__(cls)
        transportAddr = address
        self.transport_address = await self._resolve_address(transportAddr)
        self.__init__(*args, **kwargs)
        return self

    def __repr__(self):
        return "{}({!r}, timeout={!r}, retries={!r}, tagList={!r})".format(
            self.__class__.__name__,
            self.transport_address,
            self.timeout,
            self.retries,
            self.tagList,
        )

    def get_transport_info(self):
        return self.TRANSPORT_DOMAIN, self.transport_address

    def set_local_address(self, iface):
        """Set source address.

        Parameters
        ----------
        iface : tuple
            Indicates network address of a local interface from which SNMP packets will be originated.
            Format is the same as of `transportAddress`.

        Returns
        -------
            self

        """
        self.iface = iface
        return self

    def open_client_mode(self):
        self.transport = self.PROTO_TRANSPORT().open_client_mode(self.iface)
        return self.transport

    def verify_dispatcher_compatibility(self, snmpEngine: SnmpEngine):
        if (
            snmpEngine.transport_dispatcher is None
            or not self.PROTO_TRANSPORT.is_compatible_with_dispatcher(
                snmpEngine.transport_dispatcher
            )
        ):
            raise error.PySnmpError(
                "Transport {!r} is not compatible with dispatcher {!r}".format(
                    self.PROTO_TRANSPORT, snmpEngine.transport_dispatcher
                )
            )

    async def _resolve_address(self, transportAddr: Tuple) -> Tuple[str, int]:
        raise NotImplementedError()
