#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re


def main():
    REGEXP = """^("(.+)",|"",//(.+))$"""
    OUTPUT = "../tz_gen.h"
    OUTPUT_VALID = "../is_valid_gen.h"
    INPUT = OUTPUT
    CONTRIB_ZONES = "../../../../../contrib/libs/cctz/tzdata/ya.make.resources"
    RES_PREFIX = "/cctz/tzdata/"

    print("process %s into %s and %s" % (INPUT, OUTPUT, OUTPUT_VALID))
    with open(INPUT, "r") as inp:
        lines = inp.readlines()

    zones = []
    for line in lines:
        m = re.match(REGEXP, line)
        if not m:
            raise ValueError("invalid line: %s", line)
        zones.append(m.group(2) or m.group(3))

    szones = set()
    print("loaded %s zones" % len(zones))
    for zone in zones:
        if zone in szones:
            raise ValueError("duplicated zone: %s", zone)
        szones.add(zone)

    scontrib = set()
    with open(CONTRIB_ZONES, "r") as contrib:
        for line in contrib.readlines():
            if RES_PREFIX not in line:
                continue
            zone = line[line.index(RES_PREFIX) + len(RES_PREFIX):].strip()
            if zone in scontrib:
                raise ValueError("duplicated zone: %s", zone)
            scontrib.add(zone)
            if zone in szones:
                continue
            szones.add(zone)
            zones.append(zone)

    lines = []
    idx = 0
    pred = ""
    for zone in zones:
        if zone in scontrib:
            lines.append('"%s",\n' % zone)
        else:
            lines.append('"",//%s\n' % zone)
            pred += " && idx != %s" % idx
        idx += 1

    with open(OUTPUT, "w") as outp:
        outp.writelines(lines)

    with open(OUTPUT_VALID, "w") as outp:
        outp.write("// this file is generated, do not edit\n")
        outp.write("return idx < %s%s;\n" % (len(zones), pred))

    print("saved %s zones" % len(zones))


if __name__ == "__main__":
    main()
