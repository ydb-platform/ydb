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
            self.stats[final_name] = [0,0,set(),None,None,None]
        p = self.stats[final_name]
        p[0] += node["size"]
        p[1] += 1
        p[2].add(self.current_file)
        if not p[3]:
            p[3] = node["size"]
        else:
            p[3] = min(p[3],node["size"])
        if not p[4]:
            p[4] = node["size"]
            p[5] = name
        else:
            if node["size"] > p[4]:
                p[5] = name
            p[4] = max(p[4],node["size"])

def print_stat(f, d):
    p=d[1]
    print("group_name: "+d[0],file=f)
    print("    size={} count={} avg={:.2f} min={} max={}".format(p[0],p[1],p[0]/p[1],p[3],p[4]), file=f)
    if p[1] > 1:
        print("    name_for_max: " + p[5], file=f)
    print("    files:",file=f)
    for s in sorted(p[2]):
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

