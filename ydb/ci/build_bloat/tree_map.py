#!/usr/bin/env python3

import json
import os 

from jinja2 import Environment, FileSystemLoader, StrictUndefined

def _add_to_tree(tree, path):
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
        _add_to_tree(tree["children"][next_name], path[1:])

def _children_to_list(tree):
    if "children" not in tree:
        return
    tree["children"] = list(tree["children"].values())
    for child in tree["children"]:
        _children_to_list(child)

def _propogate_size(tree):
    for child in tree.get("children", []):
        tree["size"] += _propogate_size(child)
    return tree["size"]

def _remove_less_then_threshold(tree, threshold, fix_size_threshold):
    new_children = []
    new_size = 0
    self_size = tree["size"]
    for child in tree.get("children", []):
        self_size -= child["size"]
        _remove_less_then_threshold(child, threshold, fix_size_threshold)
        if child["size"] >= threshold:
            new_children.append(child)
            new_size += child["size"]

    tree["children"] = new_children
    if fix_size_threshold:
        print(new_size + self_size)
        tree["size"] = new_size + self_size

def _intify_size(tree):
    for child in tree.get("children", []):
        _intify_size(child)
    tree["size"] = int(tree["size"])

def _enrich_names_with_units(tree, unit_name, factor):
    for child_ in tree.get("children", []):
        _enrich_names_with_units(child_, unit_name, factor)

    tree["name"] = tree["name"] + ", {:_} {}".format(int(tree["size"]*factor), unit_name)

def _build_tree_map(paths_to_add, unit_name, factor, threshold, fix_size_threshold):
    tree = {}
    for path in paths_to_add:
        _add_to_tree(tree, path)
    _children_to_list(tree)
    _propogate_size(tree)
    _remove_less_then_threshold(tree, threshold, fix_size_threshold)
    _intify_size(tree)
    _enrich_names_with_units(tree, unit_name, factor)
    return tree


def generate_tree_map_html(output_dir: str, tree_paths: list[tuple[str, str, int]], unit_name: str, factor: float, types: list[tuple[str, str, str]], threshold=0, fix_size_threshold=False):
    current_script_dir = os.path.dirname(os.path.realpath(__file__))
    html_dir = os.path.join(current_script_dir, "html")

    tree = _build_tree_map(tree_paths, unit_name, factor, threshold, fix_size_threshold)

    env = Environment(loader=FileSystemLoader(html_dir), undefined=StrictUndefined)
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
