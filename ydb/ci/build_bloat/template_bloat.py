#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import sys

THRESHHOLD_TO_SHOW_ON_TREE_VIEW = 1024*10 

def remove_brackets(name, b1, b2):
    inside_template = 0
    final_name = ""
    for c in name:
        if c == b1:
            inside_template += 1
            if inside_template == 1:
                final_name += c
        elif c == b2:
            inside_template -= 1
            if inside_template == 0:
                final_name += c
        else:
            if inside_template:
                continue
            final_name += c
    return final_name

def get_aggregation_key(name):
    final_name = name
    # remove member function specifiers
    final_name = final_name.removesuffix(" const")
    final_name = final_name.removesuffix(" &&")

    # remove thunks
    final_name = final_name.removeprefix("non-virtual thunk to ") 
    
    # remove spaces and brackets
    final_name = final_name.replace("(anonymous namespace)", "[anonymous_namespace]")
    
    # remove all inside brackets
    final_name = remove_brackets(final_name, "<", ">")
    final_name = remove_brackets(final_name, "(", ")")
    if " " in final_name:
        # it works not perfect but ok
        final_name = final_name.rsplit(" ")[1]
    return final_name

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
        final_name = get_aggregation_key(name)
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


def add_to_tree(tree, path, value, count):
    tree["name"] = path[0]
    if "children" not in tree:
        tree["children"] = {}
    if len(path) == 1:
        # paths can be the same, but return value differs
        # assert "size" not in tree
        if "size" not in tree:
            tree["size"] = 0
        tree["size"] += value
        tree["type"] = "function"
        tree["count"] = count
    else:
        tree["type"] = "namespace"
        if path[1] not in tree["children"]:
            tree["children"][path[1]] = {}
        add_to_tree(tree["children"][path[1]], path[1:], value, count)

def children_to_list(tree):
    if "children" not in tree:
        return
    tree["children"] = list(tree["children"].values())
    for child in tree["children"]:
        children_to_list(child)

def propogate_size(tree):
    if "size" not in tree:
        tree["size"] = 0
    for child in tree.get("children", []):
        tree["size"] += propogate_size(child)
    return tree["size"]

def enrich_names_with_sec(tree):
    area = 0
    for child_ in tree.get("children", []):
        enrich_names_with_sec(child_)

    tree["name"] = tree["name"] + " " + "{:_} KiB".format(int(tree["size"]/1024))
    if "count" in tree:
        tree["name"] += ", {} times".format(tree["count"])

def build_tree(items):
    tree = {}
    total_size = 0
    for name, (size, count, obj_files, avg, min, max) in items:
        # we skip small entities to order to make html view usable
        if size < THRESHHOLD_TO_SHOW_ON_TREE_VIEW:
            continue

        # use braces only for args
        total_size += size

        if '(' in name:
            prefix, args = name.split('(', 1)
            args = "(" + args
        else: 
            # some unmagled symbols present such as 
            # _ZN17NPrivateExceptionlsI10yexceptionA25_cEENSt4__y19enable_ifIXsr3std10is_base_ofINS_10yexceptionEu7__decayIT_EEE5valueEOS6_E4typeES8_RKT0_ 
            # for now i don't have any idea
            prefix, args = name, ""
        
        path = prefix.split("::")
        path[-1] += args

        if ' ' in path[0] and (len(path) > 1):
            # sometimes return value specified, so strip it 
            # signed 'char* NKikimr::NCHash::TListPool<>::GetList<>(unsigned long)'
            path[0] = path[0].rsplit(' ', 1)[1]

        root_name = "root (all function less than {} KiB are ommited)".format(THRESHHOLD_TO_SHOW_ON_TREE_VIEW // 1024)
        path = [root_name] + path

        add_to_tree(tree, path, size, count)
    children_to_list(tree)
    propogate_size(tree)
    enrich_names_with_sec(tree)
    print("Total size =", total_size)
    return tree


def parse_args():
    parser = argparse.ArgumentParser(
        description="""A tool for analyzing binary size."""
    )
    parser.add_argument(
        "-j",
        "--bloat-json",
        required=True,
        help="Path to json file created by 'ya tool bloat'",
    )
    parser.add_argument(
        "-o",
        "--output-prefix",
        required=False,
        help="Prefix for the output files",
    )
    parser.add_argument(
        "-t",
        "--html-template-bloat",
        required=False,
        help="Generate html output for template bloat",
    )
    return parser.parse_args()


def main():
    options = parse_args()
    json_path = options.bloat_json
    output_prefix = options.output_prefix
    with open(json_path) as f:
        data = json.load(f)
    walker = Walker()
    walker.process(data["tree"])
    items = walker.stats.items()  # [name, (size, count, avg, min, max)]
    if output_prefix:
        with open(output_prefix + ".by_size.txt","w") as f:
            for p in sorted(items, key=lambda p: p[1][0], reverse=True):
                print_stat(f, p)
        with open(output_prefix + ".by_count.txt","w") as f:
            for p in sorted(items, key=lambda p: p[1][1], reverse=True):
                print_stat(f, p)
    
    if options.html_template_bloat:
        output_dir = options.html_template_bloat
        current_script_dir = os.path.dirname(os.path.realpath(__file__))
        html_dir = os.path.join(current_script_dir, "html_template_bloat")

        tree = build_tree(items)

        shutil.copytree(html_dir, output_dir, dirs_exist_ok=True)


        with open(os.path.join(output_dir, "bloat.json"), "w") as f:
            f.write("kTree = ")
            json.dump(tree, f, indent=4)

    return 0

if __name__ == "__main__":
    sys.exit(main())