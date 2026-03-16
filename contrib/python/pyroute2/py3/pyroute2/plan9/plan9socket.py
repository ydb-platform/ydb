from pyroute2.netlink.core import CoreSocket
from pyroute2.plan9 import Marshal9P


class Plan9Socket(CoreSocket):
    def __init__(self, use_socket=None):
        super().__init__(use_socket=use_socket)
        self.marshal = Marshal9P()
        self.spec['tag_field'] = 'tag'
