#!/usr/bin/env python3

import json
import os 

from jinja2 import Environment, FileSystemLoader, StrictUndefined

def add_to_tree(tree, path):
    current_name, current_type, current_size = path[0]
    tree["name"] = current_name
    if "children" not in tree:
        tree["children"] = {}
    if "size" not in tree:
        tree["size"] = 0
    
    tree["size"] += current_size
    tree["type"] = current_type

    if len(path) == 1:
        # paths can be the same, but return value differs
        # assert "size" not in tree
        pass
    else:
        next_name = path[1][0]
        if next_name not in tree["children"]:
            tree["children"][next_name] = {}
        add_to_tree(tree["children"][next_name], path[1:])

def children_to_list(tree):
    if "children" not in tree:
        return
    tree["children"] = list(tree["children"].values())
    for child in tree["children"]:
        children_to_list(child)

def propogate_size(tree):
    for child in tree.get("children", []):
        tree["size"] += propogate_size(child)
    return tree["size"]

def enrich_names_with_units(tree, unit_name, factor):
    area = 0
    for child_ in tree.get("children", []):
        enrich_names_with_units(child_, unit_name, factor)

    tree["name"] = tree["name"] + " " + "{:_} {}".format(int(tree["size"]*factor), unit_name)
    if "count" in tree:
        tree["name"] += ", {} times".format(tree["count"])

def build_tree_map(paths_to_add, unit_name, factor):
    tree = {}
    for path in paths_to_add:
        add_to_tree(tree, path)
    children_to_list(tree)
    propogate_size(tree)
    enrich_names_with_units(tree, unit_name, factor)
    return tree


def generate_tree_map_html(output_dir, tree_paths, unit_name, factor):
    current_script_dir = os.path.dirname(os.path.realpath(__file__))
    html_dir = os.path.join(current_script_dir, "html")

    tree = build_tree_map(tree_paths, unit_name, factor)

    env = Environment(loader=FileSystemLoader(html_dir), undefined=StrictUndefined)
    types = [
        ("namespace", "Namespace", "#66C2A5"),
        ("function", "Function", "#FC8D62"),
    ]
    file_names = os.listdir(html_dir)
    os.makedirs(output_dir, exist_ok=True)
    for file_name in file_names:
        data = env.get_template(file_name).render(types=types)

        dst_path = os.path.join(output_dir, file_name)
        with open(dst_path, "w") as f:
            f.write(data)

    with open(os.path.join(output_dir, "bloat.json"), "w") as f:
        f.write("kTree = ")
        json.dump(tree, f, indent=4)
