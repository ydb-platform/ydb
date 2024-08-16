# -*- coding: utf-8 -*-
import abc
import os
import ydb.tests.library.common.yatest_common as yatest_common


class KikimrNodePortAllocatorInterface(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    @abc.abstractproperty
    def grpc_ssl_port(self):
        pass

    @abc.abstractproperty
    def mon_port(self):
        pass

    @abc.abstractproperty
    def grpc_port(self):
        pass

    @abc.abstractproperty
    def mbus_port(self):
        pass

    @abc.abstractproperty
    def ic_port(self):
        pass

    @abc.abstractproperty
    def sqs_port(self):
        pass

    @abc.abstractproperty
    def ext_port(self):
        pass

    @abc.abstractproperty
    def public_http_port(self):
        pass


class KikimrPortAllocatorInterface(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    @abc.abstractmethod
    def get_node_port_allocator(self, node_index):
        """
            Returns KikimrNodePortAllocatorInterface object
        """
        pass

    @abc.abstractmethod
    def get_slot_port_allocator(self, slot_index):
        """
            Returns KikimrNodePortAllocatorInterface object
        """
        pass

    @abc.abstractmethod
    def release_ports(self):
        pass

#
# Port manager allocator
#


class KikimrPortManagerNodePortAllocator(KikimrNodePortAllocatorInterface):
    def __init__(self, port_manager):
        super(KikimrPortManagerNodePortAllocator, self).__init__()
        self.__port_manager = port_manager
        self.__mon_port = None
        self.__grpc_port = None
        self.__mbus_port = None
        self.__ic_port = None
        self.__sqs_port = None
        self.__grpc_ssl_port = None
        self.__ext_port = None
        self.__public_http_port = None
        self.__pgwire_port = None

    @property
    def mon_port(self):
        if self.__mon_port is None:
            self.__mon_port = self.__port_manager.get_port()
        return self.__mon_port

    @property
    def grpc_port(self):
        if self.__grpc_port is None:
            self.__grpc_port = self.__port_manager.get_port()
        return self.__grpc_port

    @property
    def mbus_port(self):
        if self.__mbus_port is None:
            self.__mbus_port = self.__port_manager.get_port()
        return self.__mbus_port

    @property
    def ic_port(self):
        if self.__ic_port is None:
            self.__ic_port = self.__port_manager.get_port()
        return self.__ic_port

    @property
    def grpc_ssl_port(self):
        if self.__grpc_ssl_port is None:
            self.__grpc_ssl_port = self.__port_manager.get_port()
        return self.__grpc_ssl_port

    @property
    def sqs_port(self):
        if self.__sqs_port is None:
            self.__sqs_port = self.__port_manager.get_port()
        return self.__sqs_port

    @property
    def ext_port(self):
        if self.__ext_port is None:
            self.__ext_port = self.__port_manager.get_port()
        return self.__ext_port

    @property
    def pgwire_port(self):
        if self.__pgwire_port is None:
            self.__pgwire_port = self.__port_manager.get_port()
        return self.__pgwire_port

    @property
    def public_http_port(self):
        if self.__public_http_port is None:
            self.__public_http_port = self.__port_manager.get_port()
        return self.__public_http_port


class KikimrPortManagerPortAllocator(KikimrPortAllocatorInterface):
    def __init__(self, port_manager=None):
        super(KikimrPortManagerPortAllocator, self).__init__()
        self.__port_manager = yatest_common.PortManager() if port_manager is None else port_manager
        self.__nodes_allocators = []
        self.__slots_allocators = []

    def get_node_port_allocator(self, node_index):
        while len(self.__nodes_allocators) <= node_index:
            self.__nodes_allocators.append(KikimrPortManagerNodePortAllocator(self.__port_manager))
        return self.__nodes_allocators[node_index]

    def get_slot_port_allocator(self, slot_index):
        while len(self.__slots_allocators) <= slot_index:
            self.__slots_allocators.append(KikimrPortManagerNodePortAllocator(self.__port_manager))
        return self.__slots_allocators[slot_index]

    def release_ports(self):
        self.__port_manager.release()


#
# Fixed port allocator
#

class KikimrFixedNodePortAllocator(KikimrNodePortAllocatorInterface):

    def __init__(self, base_port_offset, mon_port=8765, grpc_port=2135, mbus_port=2134, ic_port=19001, sqs_port=8771, grpc_ssl_port=2137,
                 ext_port=2237, public_http_port=8766, pgwire_port=5432):
        super(KikimrFixedNodePortAllocator, self).__init__()

        self.base_port_offset = base_port_offset
        if os.getenv('MON_PORT') is not None:
            self.__mon_port = int(os.getenv('MON_PORT'))
        else:
            self.__mon_port = mon_port
        if os.getenv('GRPC_PORT') is not None:
            self.__grpc_port = int(os.getenv('GRPC_PORT'))
        else:
            self.__grpc_port = grpc_port
        self.__mbus_port = mbus_port
        if os.getenv('IC_PORT') is not None:
            self.__ic_port = int(os.getenv('IC_PORT'))
        else:
            self.__ic_port = ic_port
        self.__sqs_port = sqs_port
        if os.getenv('GRPC_TLS_PORT') is not None:
            self.__grpc_ssl_port = int(os.getenv('GRPC_TLS_PORT'))
        else:
            self.__grpc_ssl_port = grpc_ssl_port
        if os.getenv('GRPC_EXT_PORT') is not None:
            self.__ext_port = int(os.getenv('GRPC_EXT_PORT'))
        else:
            self.__ext_port = ext_port
        if os.getenv('PUBLIC_HTTP_PORT') is not None:
            self.__public_http_port = int(os.getenv('PUBLIC_HTTP_PORT'))
        else:
            self.__public_http_port = public_http_port
        if os.getenv('YDB_PGWIRE_PORT') is not None:
            self.__pgwire_port = int(os.getenv('YDB_PGWIRE_PORT'))
        else:
            self.__pgwire_port = pgwire_port

    @property
    def mon_port(self):
        return self.__mon_port + self.base_port_offset

    @property
    def grpc_ssl_port(self):
        return self.__grpc_ssl_port + self.base_port_offset

    @property
    def grpc_port(self):
        return self.__grpc_port + self.base_port_offset

    @property
    def mbus_port(self):
        return self.__mbus_port + self.base_port_offset

    @property
    def ic_port(self):
        return self.__ic_port + self.base_port_offset

    @property
    def sqs_port(self):
        return self.__sqs_port + self.base_port_offset

    @property
    def ext_port(self):
        return self.__ext_port + self.base_port_offset

    def public_http_port(self):
        return self.__public_http_port + self.base_port_offset

    @property
    def pgwire_port(self):
        return self.__pgwire_port + self.base_port_offset


class KikimrFixedPortAllocator(KikimrPortAllocatorInterface):
    def __init__(self,
                 base_port_offset,
                 nodes_port_allocators_list=(),
                 slots_port_allocators_list=()):
        super(KikimrFixedPortAllocator, self).__init__()
        self.__nodes_port_allocators_list = nodes_port_allocators_list
        self.__slots_port_allocators_list = slots_port_allocators_list
        self.__default_value = KikimrFixedNodePortAllocator(base_port_offset)

    def get_node_port_allocator(self, node_index):
        if node_index <= len(self.__nodes_port_allocators_list):
            return self.__nodes_port_allocators_list[node_index - 1]
        else:
            return self.__default_value

    def get_slot_port_allocator(self, slot_index):
        if slot_index <= len(self.__slots_port_allocators_list):
            return self.__slots_port_allocators_list[slot_index - 1]
        else:
            return self.__default_value

    def release_ports(self):
        pass
