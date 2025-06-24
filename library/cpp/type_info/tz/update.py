#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re


def main():
    REGEXP = """^("(.+)",|"",//(.+))$"""
    OUTPUT = "../tz.gen"
    INPUT = OUTPUT
    CONTRIB_ZONES = "../../../../../contrib/libs/cctz/tzdata/ya.make.resources"
    RES_PREFIX = "/cctz/tzdata/"

    print("process %s into %s" % (INPUT, OUTPUT))
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
    for zone in zones:
        if zone in scontrib:
            lines.append('"%s",\n' % zone)
        else:
            lines.append('"",//%s\n' % zone)

    with open(OUTPUT, "w") as outp:
        outp.writelines(lines)

    print("saved %s zones" % len(zones))


if __name__ == "__main__":
    main()
