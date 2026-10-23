"""Bounded old-blob recovery audit and independent SPIN execution of each edge."""

import itertools as it
import json
from pathlib import Path


def read_source(name):
    try:
        from library.python import resource
    except ImportError:
        return (Path(__file__).resolve().parent / name).read_bytes()
    value = resource.find("mirror3dc/" + name)
    if value is None:
        raise FileNotFoundError(name)
    return value


G = json.loads(read_source("graph.json"))
S = G['states']
WD = {i: state['write_durability'] for i, state in enumerate(S)}


def allocations(role, layout, cw=False):
    inc = [i for i, r in enumerate(role) if r == 'I']
    eligible = (
        (inc if 'J' in role and len(layout) <= len(inc) else [i for i, r in enumerate(role) if r in 'IJ'])
        if cw
        else [i for i, r in enumerate(role) if r in 'IJ']
    )
    out = set()
    for indices in it.permutations(eligible, len(layout)):
        v = [0] * 3
        for i, n in zip(indices, layout):
            v[i] = n
        out.add(tuple(v))
    return sorted(out)


def holds(p, vs):
    return any(all(a >= b for a, b in zip(p, v)) for v in vs)


def expr(vs):
    return (
        '(' + ' || '.join('(' + ' && '.join(f'c[{i}]>={n}' for i, n in enumerate(v)) + ')' for v in vs) + ')'
        if vs
        else '0'
    )


def successors(a, p, role=None):
    if a not in WD:
        return []
    role = role or S[a]['realms']
    vs = allocations(role, WD[a])
    if holds(p, vs) or not any(p[i] and r != 'E' for i, r in enumerate(role)):
        return []
    out = set()
    for v in vs:
        for i in range(3):
            if p[i] < v[i]:
                q = list(p)
                q[i] += 1
                out.add(tuple(q))
    return sorted(out)
