#!/usr/bin/python3
import argparse
import random
import string
import struct
import math
import sys


KB = 1024
MB = 1024 * KB
GB = 1024 * MB


def padding(x = 0, t = None):
    if t is None:
        return ' ' * (x * 4)
    else:
        lines = t.strip().split('\n')
        return '\n'.join(padding(1) + line for line in lines)


def human(value):
    return value + 1


class PrintLine:
    def __init__(self, key, value, encode=False):
        self.key = key
        self.value = value
        self.encode = encode

    def print(self, padding_size=0):
        result = padding(padding_size)
        result += str(self.key)
        result += ": "
        if self.encode: result += "\""
        result += str(self.value)
        if self.encode: result += "\""
        return result

    def __str__(self):
        return self.print()


class PrintNode:
    def __init__(self, name, inline=False):
        self.name = name
        self.children = []

    def add_line(self, key, value, encode=False):
        self.children.append(PrintLine(key, value, encode))

    def add_node(self, node):
        self.children.append(node)

    def print(self, padding_size=0):
        result = padding(padding_size) + self.name + " {\n"
        for child in self.children:
            result += child.print(padding_size + 1) + "\n"
        result += padding(padding_size) + "}"
        return result

    def __str__(self):
        return self.print()


class PDisk:
    def __init__(self, node_id, pdisk_id, box_id, disk_type='HDD'):
        self.node_id = node_id
        self.pdisk_id = pdisk_id
        self.path = "/dev/node{}/pdisk{}".format(node_id, pdisk_id)
        self.type = disk_type
        self.box_id = box_id
        self.num_static_slots = 0
        self.expected_slot_count = 16

    def print(self):
        printer = PrintNode("PDisk")
        printer.add_line("NodeId", self.node_id)
        printer.add_line("PDiskId", self.pdisk_id)
        printer.add_line("Path", self.path, encode=True)
        printer.add_line("Guid", ''.join(random.choice('123456789') for _ in range(9)))
        printer.add_line("BoxId", self.box_id)
        printer.add_line("NumStaticSlots", self.num_static_slots)
        printer.add_line("DriveStatus", "ACTIVE")
        printer.add_line("ExpectedSlotCount", self.expected_slot_count)
        return printer


class VSlot:
    def __init__(self, node_id, pdisk_id, vslot_id):
        self.node_id = node_id
        self.pdisk_id = pdisk_id
        self.vslot_id = vslot_id

    def print(self):
        printer = PrintNode("VSlotId")
        printer.add_line("NodeId", self.node_id)
        printer.add_line("PDiskId", self.pdisk_id)
        printer.add_line("VSlotId", self.vslot_id)
        return printer

    def __str__(self):
        return "VSlot({}, {}, {})".format(self.node_id, self.pdisk_id, self.vslot_id)

    def __repr__(self):
        return self.__str__()


class Group:
    def __init__(self, box_id, storage_pool_id, erasure_species='block-4-2'):
        self.erasure_species = erasure_species
        self.box_id = box_id
        self.storage_pool_id = storage_pool_id
        self.vslots = []

    def add_vslot(self, node_id, pdisk_id, vslot_id):
        self.vslots.append(VSlot(node_id, pdisk_id, vslot_id))

    def print(self):
        printer = PrintNode("Group")
        printer.add_line("GroupId", ''.join(random.choice('123456789') for _ in range(9)))
        printer.add_line("ErasureSpecies", self.erasure_species, encode=True)
        printer.add_line("BoxId", self.box_id)
        printer.add_line("StoragePoolId", self.storage_pool_id)
        for vslot in self.vslots:
            printer.add_node(vslot.print())
        return printer


class FailDomain:
    def __init__(self, dc=None, room=None, rack=None, body=None):
        self.dc = dc
        self.room = room
        self.rack = rack
        self.body = body

    def serialize(self):
        result = bytes()
        pack = lambda level, value: b'' if value is None else struct.pack('<BI', level, value)
        result += pack(10, self.dc)
        result += pack(20, self.room)
        result += pack(30, self.rack)
        result += pack(40, self.body)
        result = ''.join('\\{0:03o}'.format(x) for x in result)
        return result

    def deserialize(self, data):
        value_count = len(data) // 5
        data = struct.unpack('<' + 'BI' * value_count, data)
        for index in range(0, len(data), 2):
            if data[index] == 10:
                self.dc = data[index + 1]
            elif data[index] == 20:
                self.room = data[index + 1]
            elif data[index] == 30:
                self.rack = data[index + 1]
            else:
                self.body = data[index + 1]


class Node:
    def __init__(self, node_id, dc=None, room=None, rack=None, body=None):
        self.node_id = node_id
        self.physical_location = FailDomain(dc, room, rack, body)
        self.fqdn = "node{}.test.cluster.net".format(node_id)
        self.icport = 1337

    def print(self):
        printer = PrintNode("Node")
        printer.add_line("NodeId", self.node_id)
        printer.add_line("PhysicalLocation", self.physical_location.serialize(), encode=True)
        hostkey = PrintNode("HostKey")
        hostkey.add_line("Fqdn", self.fqdn, encode=True)
        hostkey.add_line("IcPort", self.icport)
        printer.add_node(hostkey)
        return printer


class Request:
    def __init__(self, algorithm, iterations):
        self.algorithm = algorithm
        self.iterations = iterations

    def print(self):
        printer = PrintNode("Request")
        printer.add_line("Algorithm", self.algorithm)
        printer.add_line("Iterations", self.iterations)
        return printer

    @staticmethod
    def choices():
        return ['ANNEALING', 'QUADRATIC', 'HUNGARIAN']


class BaseConfig:
    def __init__(self):
        self.nodes = []
        self.pdisks = []
        self.groups = []

    def print(self):
        printer = PrintNode("BaseConfig")
        for node in self.nodes:
            printer.add_node(node.print())
        for pdisk in self.pdisks:
            printer.add_node(pdisk.print())
        for group in self.groups:
            printer.add_node(group.print())
        return printer


class StoragePool:
    def __init__(self, groups, box_id, sp_id, erasure_species="block-4-2"):
        self.box_id = box_id
        self.storage_pool_id = sp_id
        self.name = "StoragePool{}:{}".format(box_id, sp_id)
        self.erasure_species = erasure_species
        self.groups = groups

    def print(self):
        printer = PrintNode("StoragePool")
        printer.add_line("BoxId", self.box_id)
        printer.add_line("StoragePoolId", self.storage_pool_id)
        printer.add_line("Name", self.name, encode=True)
        printer.add_line("ErasureSpecies", self.erasure_species, encode=True)
        printer.add_line("VDiskKind", "Default", encode=True)
        printer.add_line("Kind", "rot", encode=True)
        printer.add_line("NumGroups", self.groups)
        pdisk_filter = PrintNode("PDiskFilter")
        prop = PrintNode("Property")
        prop.add_line("Type", "ROT")
        pdisk_filter.add_node(prop)
        printer.add_node(pdisk_filter)
        return printer


class Metric:
    def __init__(self, vslot, size):
        self.vslot = vslot
        self.size = size

    def print(self):
        printer = PrintNode("Metric")
        printer.add_node(self.vslot.print())
        printer.add_line("Metric", self.size)
        return printer

    def __str__(self):
        return "Metric({}, {})".format(str(self.vslot), self.size)

    def __repr__(self):
        return self.__str__()


class MetricStrategy:
    def __init__(self, name):
        if name not in MetricStrategy.choices():
            raise Exception("wrong strategy typename")
        self.func = getattr(self, 'strategy_{}'.format(name))

    @staticmethod
    def choices():
        allowed_names = [name for name in dir(MetricStrategy) if name.startswith('strategy_')]
        allowed_names = [name[name.find('_') + 1:] for name in allowed_names]
        return allowed_names

    def execute(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def strategy_random(self, minimum=10*MB, maximum=10*GB, *args, **kwargs):
        return random.randint(minimum, maximum)

    def strategy_minmax(self, group_id, minimum=10*MB, maximum=10*GB, *args, **kwargs):
        if group_id % 2 == 0:
            return minimum
        return maximum


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test cluster crafting utility.')
    parser.add_argument('--nodes', dest='total_nodes', type=int, default=0, help='count of nodes in cluster')
    parser.add_argument('--groups', dest='groups', type=int, default=0, help='count of existing groups')
    parser.add_argument('--nodes-in-rack', dest='nodes_in_rack', type=int, default=8, help='count of nodes in one rack')
    parser.add_argument('--pdisks-per-node', dest='pdisks_per_node', type=int, default=1, help='count of pdisks on one node')
    parser.add_argument('--cluster-output', dest='cluster_output', type=str, default='cluster', help='name of file with clusterfit config')
    parser.add_argument('--metric-output', dest='metric_output', type=str, default='metric', help='name of file with vdisk metrics')
    parser.add_argument('--strategy', dest='strategy', type=str, default='random', choices=MetricStrategy.choices(), help='vslot metric assignment mechanism')
    parser.add_argument('--algorithm', dest='algorithm', type=str, choices=Request.choices(), help='clusterfit algorithm')
    parser.add_argument('--iterations', dest='iterations', type=int, default=10**5, help='count of iteration in anneling algorithm')
    args = parser.parse_args()

    racks = dict()
    node_usage = dict()
    strategy = MetricStrategy(args.strategy)

    nodes = []
    for id in range(args.total_nodes):
        rack_id = id // args.nodes_in_rack
        nodes.append(Node(human(id), 1, 1, human(id // args.nodes_in_rack), 1))
        node_usage[id] = [0 for _ in range(args.pdisks_per_node)]
        if rack_id in racks:
            racks[rack_id].append(id)
        else:
            racks[rack_id] = [id]

    pdisks = []
    for node in range(args.total_nodes):
        for index in range(args.pdisks_per_node):
            pdisk = PDisk(human(node), human(index), 1)
            pdisks.append(pdisk)

    def make_vslots():
        def find_vslot(rack_id):
            for node_id in racks[rack_id]:
                for pdisk_id in range(args.pdisks_per_node):
                    if node_usage[node_id][pdisk_id] < 8:
                        node_usage[node_id][pdisk_id] += 1
                        return VSlot(human(node_id), human(pdisk_id), node_usage[node_id][pdisk_id])
            return None
        result = []
        for rack_id in racks:
            if len(result) < 8:
                vslot = find_vslot(rack_id)
                if vslot is not None:
                    result.append(vslot)
        return result

    groups = []
    metrics = []
    for group_id in range(args.groups):
        group = Group(1, 1)
        group.vslots = make_vslots()
        for vslot in group.vslots:
            metrics.append(Metric(vslot, strategy.execute(group_id=group_id, vslot=vslot)))
        groups.append(group)

    storage_pools = []
    storage_pools.append(StoragePool(args.groups, 1, 1))

    base_config = BaseConfig()
    base_config.pdisks = pdisks
    base_config.groups = groups
    base_config.nodes = nodes

    with open(args.cluster_output, 'w') as cluster:
        cluster.write(str(base_config.print()) + "\n")
        for spool in storage_pools:
            cluster.write(str(spool.print()) + "\n")
        for metric in metrics:
            cluster.write(str(metric.print()) + "\n")
        cluster.write(str(Request(args.algorithm, args.iterations).print()) + "\n")

    with open(args.metric_output, 'w') as metrics_file:
        metrics_file.write(str(metrics))

