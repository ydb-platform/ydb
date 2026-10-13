"""Fails the build of an executable that links the interface of a link slot
(//build/gn/link_slots.gni) without selecting a provider for it: the weak
references to the slot's functions would stay null, and a call crash at run
time.

Usage: link_slot_check.py --executable LABEL --interfaces FILE --stamp FILE
                          [--selected SLOT...]
FILE: the JSON list of "<slot>|<interface label>" that the executable's
dependencies declare (metadata link_slot_interfaces, collected by gn).
"""

import argparse
import json
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--executable", required=True)
    ap.add_argument("--interfaces", required=True)
    ap.add_argument("--selected", nargs="*", default=[])
    ap.add_argument("--stamp", required=True)
    args = ap.parse_args()

    with open(args.interfaces, encoding="utf-8") as f:
        entries = json.load(f)
    missing = {}
    for entry in entries:
        slot, _, label = entry.partition("|")
        if slot not in args.selected:
            missing.setdefault(slot, set()).add(label)
    if missing:
        sys.exit("%s links the interface of link slots it selects no provider for; add "
                 "\"<slot>=<provider>\" to its link_select (see build/gn/link_slots.gni):\n%s"
                 % (args.executable, "\n".join("  %s: %s" % (slot, " ".join(sorted(labels)))
                                               for slot, labels in sorted(missing.items()))))
    with open(args.stamp, "w"):
        pass


if __name__ == "__main__":
    main()
