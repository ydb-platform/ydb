#!/usr/bin/env python3

# Tool used to transform junit report. Performs the following:
# - adds classname with relative path to test in 'testcase' node
# - add 'url:logsdir' and other links with in 'testcase' node
# - mutes tests

import argparse
import re
import json
import os
import sys
import urllib.parse
import zipfile
from typing import Set
from xml.etree import ElementTree as ET

import tree_map

THRESHOLD = 5  # sec


def process(junit_path):
    tree = ET.parse(junit_path)
    root = tree.getroot()

    tree_paths = []

    for suite in root.findall("testsuite"):
        suite_name = suite.get("name")

        for case in suite.findall("testcase"):
            test_name = case.get("name")
            duration = float(case.get("time"))
            
            test_name = test_name.split("[")[0]

            root_name = "/"
            path = [root_name] + suite_name.split("/") + [test_name]
            path_with_info = [[chunk, "dir", 0] for chunk in path]
            path_with_info[-1][1] = "testcase"
            path_with_info[-1][2] = duration
            path_with_info[-2][1] = "testsuite"
            tree_paths.append(path_with_info)


    types = [
        ("dir", "Directory", "#66C2A5"),
        ("testsuite", "Test Suite", "#8DA0CB"),
        ("testcase", "Test Case", "#FC8D62"),
    ]
    tree_map.generate_tree_map_html("/home/maxim-yurchuk/test_bloat", tree_paths, unit_name="sec", factor=1.0, types=types, threshold=500, fix_size_threshold=True)


process("junit.xml")