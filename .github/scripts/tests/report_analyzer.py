#!/usr/bin/env python3

"""
The tool used to analyze file created by "ya make ... --build-results-report <file>"
"""

import sys
import json 

if __name__ == "__main__":
    report_path = sys.argv[1]
    summary_path = sys.argv[2]

    with open(report_path) as f:
        obj = json.loads(f.read())

    all = []

    for result in obj["results"]:
        type_ = result["type"] 
        if type_ == "test" and result.get("chunk"):
            rss_consumtion = result["metrics"].get("suite_max_proc_tree_memory_consumption_kb", 0) / 1024 / 1024
            path = result["path"] + " " + result.get("subtest_name", "")
            all.append((rss_consumtion, path))

    all.sort()
    with open (summary_path, "w") as f:
        f.write("RSS usage by tests, sorted\n\n")    
        for rss, path in all:
            f.write("{} {:.2f} GiB \n".format(path, rss))
        f.write("\n")
