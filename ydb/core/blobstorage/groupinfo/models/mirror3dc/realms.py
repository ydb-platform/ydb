"""Physical-realm histories: realm IDs never move with diagram columns.

For each mode edge preserve the maximum number of realm roles. Enumerate every
choice of physical realm for the required role changes. In particular IIJ->IJE
is I->E, retaining J. Same-role mode changes cannot swap physical identities.
"""

import argparse
import itertools as it
import json
import re
from collections import deque
from pathlib import Path

from model_common import WD, allocations, holds, expr, successors, read_source


def generate(out):
    G = json.loads(read_source("graph.json"))
    S = G['states']

    WO = {i: state['write_ok'] for i, state in enumerate(S)}
    configs = [(i, ''.join(p)) for i, s in enumerate(S) for p in sorted(set(it.permutations(s['realms'])))]
    index = {c: i for i, c in enumerate(configs)}
    edges = []
    for a, b in G['edges']:
        for ci, (mode, roles) in enumerate(configs):
            if mode != a:
                continue
            candidates = [(j, r) for j, (m, r) in enumerate(configs) if m == b]
            distance = min(sum(x != y for x, y in zip(roles, r)) for _, r in candidates)
            edges.extend((ci, j) for j, r in candidates if sum(x != y for x, y in zip(roles, r)) == distance)

    def cw(ci):
        mode, roles = configs[ci]
        return [p for layout in S[mode]['cw'] for p in allocations(roles, layout, True)]

    def ack(ci):
        mode, roles = configs[ci]
        included = {i for i, r in enumerate(roles) if r == 'I'}
        return [
            p
            for layout in WO[mode]
            for p in allocations(roles, layout)
            if not any(p[i] and r == 'J' for i, r in enumerate(roles)) or all(p[i] for i in included)
        ]

    def replicate(ci, p):
        mode, roles = configs[ci]
        return successors(mode, p, roles)

    def loss(a, b, p):
        ar, br = configs[a][1], configs[b][1]
        choices = [range(n + 1) if ar[i] in 'EL' and br[i] == 'J' else [n] for i, n in enumerate(p)]
        return it.product(*choices)

    def returning_realms(a, b):
        return [i for i, (old, new) in enumerate(zip(configs[a][1], configs[b][1])) if old == 'E' and new == 'L']

    def memory_after(a, b, last):
        departed = [i for i, (old, new) in enumerate(zip(configs[a][1], configs[b][1])) if old == 'I' and new == 'E']
        assert len(departed) <= 1
        return departed[0] if departed else last

    def can_return(a, b, last):
        return all(i == last for i in returning_realms(a, b))

    adj = {i: [] for i in range(len(configs))}
    for k, (a, b) in enumerate(edges):
        adj[a].append((k, b))
    # Initialize pre-write metadata from actual mode/role histories, not arbitrary IDs.
    metadata = {(index[(0, 'III')], 3)}
    pending = deque(metadata)
    while pending:
        a, last = pending.popleft()
        for _, b in adj[a]:
            if can_return(a, b, last):
                nxt = (b, memory_after(a, b, last))
                if nxt not in metadata:
                    metadata.add(nxt)
                    pending.append(nxt)
    placements = list(it.product(range(3), repeat=3))
    births = [
        (i, p, last)
        for i, last in sorted(metadata)
        for p in placements
        if holds(p, ack(i)) and all(not p[j] for j, r in enumerate(configs[i][1]) if r in 'EL')
    ]
    parent = {s: None for s in births}
    queue = deque(births)
    unsafe = []
    while queue:
        state = queue.popleft()
        a, p, last = state
        for k, b in adj[a]:
            if not can_return(a, b, last):
                continue
            if holds(p, cw(b)):
                nexts = [((b, q, memory_after(a, b, last)), f'edge {k}') for q in loss(a, b, p)]
            else:
                nexts = [((a, q, last), f'replicate for edge {k}') for q in replicate(a, p)]
            for nxt, action in nexts:
                if not holds(nxt[1], cw(nxt[0])):
                    unsafe.append((state, nxt, action))
                    continue
                if nxt not in parent:
                    parent[nxt] = (state, action)
                    queue.append(nxt)

    def trace(state):
        out = []
        while state is not None:
            ci, p, last = state
            prev = parent[state]
            out.append(
                {
                    'mode': S[configs[ci][0]]['name'],
                    'roles': configs[ci][1],
                    'copies': p,
                    'last_left_I': last if last != 3 else None,
                    'action': prev[1] if prev else 'confirmed WriteOk write',
                }
            )
            state = prev[0] if prev else None
        return out[::-1]

    # Audit every requested outgoing edge from each reachable physical placement.
    blocked = []
    unrecoverable_cw0 = []
    for state in parent:
        a, p, last = state
        for k, b in adj[a]:
            if not can_return(a, b, last):
                continue
            seen, todo = {p}, [p]
            while todo:
                q = todo.pop()
                if holds(q, cw(b)):
                    continue
                nxt = replicate(a, q)
                if not nxt:
                    outcomes = unrecoverable_cw0 if S[configs[a][0]]['cw'] == [[]] else blocked
                    outcomes.append(
                        {
                            'edge': k,
                            'target': S[configs[b][0]]['name'],
                            'target_roles': configs[b][1],
                            'terminal': q,
                            'history': trace(state),
                        }
                    )
                    break
                for z in nxt:
                    if z not in seen:
                        seen.add(z)
                        todo.append(z)

    out = out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    summary = {
        'source_sha': G['source_sha'],
        'physical_configs': len(configs),
        'physical_edges': len(edges),
        'mode_edges': len(G['edges']),
        'reachable_pairs': len(parent),
        'unsafe_count': len(unsafe),
        'blocked_count': len(blocked),
        'blocked_examples': blocked[:5],
        'unrecoverable_cw0_count': len(unrecoverable_cw0),
        'unrecoverable_cw0_examples': unrecoverable_cw0[:5],
        'suspect_pending': [
            trace(s)
            for s in parent
            if S[configs[s[0]][0]]['name'] == 'SOLO_DC_RESURECTION_PENDING'
            and not any(s[1][i] for i, r in enumerate(configs[s[0]][1]) if r in 'IL')
        ],
        'configs': [{'mode': S[m]['name'], 'roles': r} for m, r in configs],
        'edges': edges,
    }

    # SPIN independently explores histories; no Python reachability filter.
    # lock selects a desired edge and runs source-WD recovery until its guard holds.
    t = (
        '/* One immutable blob; physical realm IDs, copy counts 0..2.\n'
        ' * See README.md for assumptions and properties. Generated by realms.py. */\n'
        '#define NO_REALM 3\n#define NO_EDGE (-1)\n\n'
        'byte c[3];\nbyte state;\nbyte last = NO_REALM;\nshort lock = NO_EDGE;\n\n'
        'init {\n    if\n'
    )
    for ci, p, last in births:
        t += '    :: state = %d;\n        c[0] = %d;\n        c[1] = %d;\n        c[2] = %d;\n        last = %d\n' % (
            (ci,) + p + (last,)
        )
    t += '    fi;\n    do\n'
    for k, (a, b) in enumerate(edges):
        return_guard = ''.join(f' && last=={i}' for i in returning_realms(a, b))
        t += f'    :: lock == NO_EDGE && state == {a}{return_guard} ->\n        lock = {k}\n'
        t += f'    :: lock == {k} && {expr(cw(b))} ->\n        atomic {{\n'
        for i in range(3):
            if configs[a][1][i] in 'EL' and configs[b][1][i] == 'J':
                t += (
                    '            if\n'
                    + ''.join(f'            :: c[{i}] >= {n} ->\n                c[{i}] = {n}\n' for n in range(3))
                    + '            fi;\n'
                )
        departed = [i for i, (old, new) in enumerate(zip(configs[a][1], configs[b][1])) if old == 'I' and new == 'E']
        if departed:
            t += f'            last = {departed[0]};\n'
        t += f'            state = {b};\n            assert({expr(cw(b))});\n            lock = NO_EDGE;\n        }}\n'
        mode, roles = configs[a]
        vs = allocations(roles, WD[mode])
        source = '(' + ' || '.join(f'c[{i}]>0' for i, r in enumerate(roles) if r != 'E') + ')'
        enabled = []
        for i in range(3):
            maximum = max(v[i] for v in vs)
            if maximum:
                guard = f'!{expr(cw(b))} && !{expr(vs)} && {source} && c[{i}]<{maximum}'
                enabled.append('(' + guard + ')')
                t += f'    :: lock == {k} && {guard} ->\n        c[{i}]++\n'
        t += f'    :: lock == {k} && !{expr(cw(b))} && !(' + ' || '.join(enabled) + ') ->\n'
        if S[mode]['cw'] == [[]]:
            t += '        /* CW 0 permits terminal inability to recover; target CW is still guarded. */\n'
            t += '        break;\n'
        else:
            t += '#ifdef CHECK_RECOVERY\n        assert(0);\n#else\n        break;\n#endif\n'
    t += '    :: else ->\n        break\n    od;\n}\n'

    # Promela may terminate a statement at a newline after ')'. Wrap after operators.
    def wrap_expression(line):
        parts = re.split(r"\s+(&&|\|\|)\s+", line)
        current = parts[0]
        continuation = " " * (len(line) - len(line.lstrip()) + 4)
        lines = []
        for operator, operand in zip(parts[1::2], parts[2::2]):
            current += " " + operator
            if len(current) + 1 + len(operand) > 100:
                lines.append(current)
                current = continuation + operand
            else:
                current += " " + operand
        return "\n".join([*lines, current])

    t = "\n".join(wrap_expression(line) for line in t.splitlines()) + "\n"
    (out / 'model.pml').write_text(t)
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(
        json.dumps({k: v for k, v in summary.items() if k not in ('configs', 'edges', 'suspect_pending')}, indent=2),
        flush=True,
    )

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    summary = generate(parser.parse_args().output)
    if summary['unsafe_count'] or summary['blocked_count']:
        raise SystemExit("Model audit found unsafe or blocked transitions; see summary.json")
