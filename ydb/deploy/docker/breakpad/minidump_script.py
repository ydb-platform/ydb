#!/usr/bin/python3.8

import json
import subprocess
import argparse
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Minidump files processing"
    )
    parser.add_argument("succeeded", action="store")
    parser.add_argument("dmp_file", action="store")
    args = parser.parse_args()
    dmp_file = args.dmp_file
    core_file = args.dmp_file[:-3] + "core"
    json_file = args.dmp_file[:-3] + "json"
    succeeded = args.succeeded

    if succeeded == "true":
        elf_cmd = ["readelf", "-n", "/opt/ydb/bin/ydbd"]
        svnrev_cmd = ["/opt/ydb/bin/ydbd", "--svnrevision"]
        mndmp_cmd = ["/usr/bin/minidump-2-core", "-v", dmp_file, "-o", core_file]
        gdb_cmd = [
            "/usr/bin/gdb",
            "/opt/ydb/bin/ydbd",
            core_file,
            "-iex=set auto-load safe-path /",
            "-ex=thread apply all bt",
            "--batch",
            "-q"
        ]

        elf_resp = subprocess.check_output(elf_cmd).decode("utf-8")
        svnrev_resp = subprocess.check_output(svnrev_cmd).decode("utf-8")
        subprocess.run(mndmp_cmd)
        gdb_resp = subprocess.check_output(gdb_cmd).decode("utf-8")
        os.remove(dmp_file)
        os.remove(core_file)

        ret = json.dumps({"binary": "/opt/ydb/bin/ydbd", "readelf": elf_resp, "svnrevision": svnrev_resp, "stacktrace": gdb_resp})
        with open(json_file,"w") as out:
            out.write(ret)
