#!/usr/bin/python3
import argparse


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

    def key(self):
        return (self.node_id, self.pdisk_id, self.vslot_id)

    def __str__(self):
        return "VSlot({}, {}, {})".format(self.node_id, self.pdisk_id, self.vslot_id)

    def __repr__(self):
        return self.__str__()


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


class Statistics:
    def __init__(self):
        self.metrics = dict()
        self.moved = list()
        self.total_size = 0
        self.moved_size = 0

    def parse_metrics(self, metrics):
        for metric in metrics:
            self.metrics[metric.vslot.key()] = metric.size
            self.total_size += metric.size

    def move_vslot(self, vslot):
        self.moved.append(vslot)
        self.moved_size += self.metrics[vslot.key()]

    def __str__(self):
        return "Statistics(moved={:3f}%)".format(100 * self.moved_size / self.total_size)

    def __repr__(self):
        return self.__str__()


def parse_line(line):
    return int(line.strip().split(':')[-1].strip())


def read_file(result_filename, metrics_filename):
    with open(result_filename, 'r') as result_desc:
        result_content = result_desc.read().strip()
    with open(metrics_filename, 'r') as metrics_desc:
        metrics = eval(metrics_desc.read())

    stats = Statistics()
    stats.parse_metrics(metrics)

    result_lines = result_content.split('\n')
    current_line = 0
    while current_line < len(result_lines):
        if result_lines[current_line].strip().startswith("MoveCommand"):
            group_id        = parse_line(result_lines[current_line + 1])
            origin_node_id  = parse_line(result_lines[current_line + 2])
            origin_pdisk_id = parse_line(result_lines[current_line + 3])
            origin_vslot_id = parse_line(result_lines[current_line + 4])
            target_node_id  = parse_line(result_lines[current_line + 5])
            target_pdisk_id = parse_line(result_lines[current_line + 6])
            stats.move_vslot(VSlot(origin_node_id, origin_pdisk_id, origin_vslot_id))
            current_line += 7
        else:
            current_line += 1

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ClusterFit output statistics tool")
    parser.add_argument("--src", dest="result", type=str, help="text file with move commands and pdisk statistics")
    parser.add_argument("--metrics", dest="metrics", type=str, help="metrics for vslots")
    args = parser.parse_args()
    stats = read_file(args.result, args.metrics)
    print(stats)
