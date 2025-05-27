#!/usr/bin/python3.8

import json
import subprocess
import argparse
import signal
import os


def signal_info(minidump_text):
    minidump_lines = minidump_text.splitlines()

    signal_name = "UNKNOWN"
    signal_str = "no description"
    for line in minidump_lines:
        line = line.strip()
        if line.startswith('Crash|'):
            # "Crash|SIGSEGV|0x452e|0"
            signal_name = line.split('|')[1].split()[0]
            break

    try:
        signal_code = getattr(signal, signal_name)
        print(f"Signal name '{signal_name}' corresponds to signal number {signal_code}.")
    except AttributeError:
        print(f"Signal name '{signal_name}' is not a valid signal.")
    else:
        try:
            signal_str = signal.strsignal(signal_code)
            print(f"Signal code '{signal_code}' corresponds to signal description {signal_str}.")
        except ValueError:
            print(f"Signal code '{signal_code}' is not a valid value.")

    return "Program terminated with signal {}, {}.".format(signal_name, signal_str)


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
        stwlk_cmd = ["/usr/bin/minidump_stackwalk", "-m", dmp_file]
        gdb_cmd = [
            "/usr/bin/gdb",
            "-q",
            "-batch",
            "-iex=set auto-load safe-path /",
            "-ex=thread apply all bt 999",
            "/opt/ydb/bin/ydbd",
            core_file,
        ]

        elf_resp = subprocess.check_output(elf_cmd).decode("utf-8")
        svnrev_resp = subprocess.check_output(svnrev_cmd).decode("utf-8")

        subprocess.run(mndmp_cmd, stderr=subprocess.DEVNULL)
        stwlk_resp = subprocess.check_output(stwlk_cmd, stderr=subprocess.DEVNULL).decode("utf-8")
        gdb_resp = subprocess.check_output(gdb_cmd, stderr=subprocess.DEVNULL).decode("utf-8")
        stacktrace = []
        for line in gdb_resp.splitlines():
            stacktrace.append(line)
            if line.startswith("Core was generated"):
                stacktrace.append(signal_info(stwlk_resp))
        stacktrace_str = '\n'.join(stacktrace)

        os.remove(dmp_file)
        os.remove(core_file)

        ret = json.dumps({"binary": "/opt/ydb/bin/ydbd", "readelf": elf_resp, "svnrevision": svnrev_resp, "stacktrace": stacktrace_str})
        with open(json_file,"w") as out:
            out.write(ret)
