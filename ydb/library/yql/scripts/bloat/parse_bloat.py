#!/usr/bin/env python3
import sys
import json
import os

class Walker:
    def __init__(self):
        self.stats = {}

    def process(self, node):
        if node["type"] == "file":
            self.current_file = node["name"]
        if "children" in node:
            for child in node["children"]:
                self.process(child)
            return
        if node["type"] != "fn":
            return
        name = node["name"]
        inside_template = 0
        final_name = ""
        for c in name:
            if c == '<':
                inside_template += 1
                if inside_template == 1:
                    final_name += c
            elif c == '>':
                inside_template -= 1
                if inside_template == 0:
                    final_name += c
            else:
                if inside_template:
                    continue
                final_name += c
        if final_name not in self.stats:
            self.stats[final_name] = [0,0,set()]
        self.stats[final_name][0] += node["size"]
        self.stats[final_name][1] += 1
        self.stats[final_name][2].add(self.current_file)

def print_stat(f, p):
    print("{} size={} count={}".format(p[0],p[1][0],p[1][1]), file=f)
    for s in sorted(p[1][2]):
        print("    " + s, file=f)

def main():
    pgm = sys.argv[1]
    with open(pgm + ".json") as f:
        data = json.load(f)
    walker = Walker()
    walker.process(data["tree"])
    with open(pgm + ".by_size.txt","w") as f:
        for p in sorted(walker.stats.items(), key=lambda p: p[1][0], reverse=True):
            print_stat(f, p)
    with open(pgm + ".by_count.txt","w") as f:
        for p in sorted(walker.stats.items(), key=lambda p: p[1][1], reverse=True):
            print_stat(f, p)
    return 0

if __name__ == "__main__":
    sys.exit(main())

