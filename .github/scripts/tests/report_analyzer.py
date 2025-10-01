#!/usr/bin/env python3

"""
The tool used to analyze file created by "ya make ... --build-results-report <file>"
"""

import argparse
import sys
import json 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--report_file", 
        help="path to file received via 'ya make ... --build-results-report <file>'", 
        type=argparse.FileType("r"), 
        required=True
    )
    parser.add_argument(
        "--summary_file", 
        help="output file for summary", 
        type=argparse.FileType("w"), 
        default="-"
    )
    args = parser.parse_args()

    report_file = args.report_file
    summary_file = args.summary_file

    obj = json.load(report_file)

    all = []

    for result in obj["results"]:
        type_ = result["type"] 
        if type_ == "test" and result.get("chunk"):
            rss_consumtion = result["metrics"].get("suite_max_proc_tree_memory_consumption_kb", 0) / 1024 / 1024
            path = result["path"] + " " + result.get("subtest_name", "")
            all.append((rss_consumtion, path))

    all.sort()
    summary_file.write("RSS usage by tests, sorted\n\n")    
    for rss, path in all:
        summary_file.write("{} {:.2f} GiB\n".format(path, rss))
    summary_file.write("\n")
