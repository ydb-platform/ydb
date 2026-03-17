__version__ = '2.0.0'

import sys
import json

from .symbols import *
from .symbols import Symbol

# rules
# - keys and strings which start with $ (or specified escape_str) are escaped to $$ (or escape_str * 2)
# - when source is dict and diff is a dict -> patch
# - when source is list and diff is a list patch dict -> patch
# - else -> replacement

# Python 2 vs 3
PY3 = sys.version_info[0] == 3

if PY3:
    string_types = str
else:
    string_types = basestring


class JsonDumper(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, obj, dest=None):
        if dest is None:
            return json.dumps(obj, **self.kwargs)
        else:
            return json.dump(obj, dest, **self.kwargs)


default_dumper = JsonDumper()


class JsonLoader(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, src):
        if isinstance(src, string_types):
            return json.loads(src, **self.kwargs)
        else:
            return json.load(src, **self.kwargs)


default_loader = JsonLoader()


class JsonDiffSyntax(object):
    def emit_set_diff(self, a, b, s, added, removed):
        raise NotImplementedError()

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        raise NotImplementedError()

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        raise NotImplementedError()

    def emit_value_diff(self, a, b, s):
        raise NotImplementedError()

    def patch(self, a, d):
        raise NotImplementedError()

    def unpatch(self, a, d):
        raise NotImplementedError()


class CompactJsonDiffSyntax(object):
    def emit_set_diff(self, a, b, s, added, removed):
        if s == 0.0 or len(removed) == len(a):
            return {replace: b} if isinstance(b, dict) else b
        else:
            d = {}
            if removed:
                d[discard] = removed
            if added:
                d[add] = added
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        if s == 0.0:
            return {replace: b} if isinstance(b, dict) else b
        elif s == 1.0:
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = [pos for pos, value in deleted]
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        if s == 0.0:
            return {replace: b} if isinstance(b, dict) else b
        elif s == 1.0:
            return {}
        else:
            changed.update(added)
            if removed:
                changed[delete] = list(removed.keys())
            return changed

    def emit_value_diff(self, a, b, s):
        if s == 1.0:
            return {}
        else:
            return {replace: b} if isinstance(b, dict) else b

    def patch(self, a, d):
        if isinstance(d, dict):
            if not d:
                return a
            if replace in d:
                return d[replace]
            if isinstance(a, dict):
                a = dict(a)
                for k, v in d.items():
                    if k is delete:
                        for kdel in v:
                            del a[kdel]
                    else:
                        av = a.get(k, missing)
                        if av is missing:
                            a[k] = v
                        else:
                            a[k] = self.patch(av, v)
                return a
            elif isinstance(a, (list, tuple)):
                original_type = type(a)
                a = list(a)
                if delete in d:
                    for pos in d[delete]:
                        a.pop(pos)
                if insert in d:
                    for pos, value in d[insert]:
                        a.insert(pos, value)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        a[k] = self.patch(a[k], v)
                if original_type is not list:
                    a = original_type(a)
                return a
            elif isinstance(a, set):
                a = set(a)
                if discard in d:
                    for x in d[discard]:
                        a.discard(x)
                if add in d:
                    for x in d[add]:
                        a.add(x)
                return a
        return d


class ExplicitJsonDiffSyntax(object):
    def emit_set_diff(self, a, b, s, added, removed):
        if s == 0.0 or len(removed) == len(a):
            return b
        else:
            d = {}
            if removed:
                d[discard] = removed
            if added:
                d[add] = added
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        if s == 0.0:
            return b
        elif s == 1.0:
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = [pos for pos, value in deleted]
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        if s == 0.0:
            return b
        elif s == 1.0:
            return {}
        else:
            d = {}
            if added:
                d[insert] = added
            if changed:
                d[update] = changed
            if removed:
                d[delete] = list(removed.keys())
            return d

    def emit_value_diff(self, a, b, s):
        if s == 1.0:
            return {}
        else:
            return b


class SymmetricJsonDiffSyntax(object):
    def emit_set_diff(self, a, b, s, added, removed):
        if s == 0.0 or len(removed) == len(a):
            return [a, b]
        else:
            d = {}
            if added:
                d[add] = added
            if removed:
                d[discard] = removed
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        if s == 0.0:
            return [a, b]
        elif s == 1.0:
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = deleted
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        if s == 0.0:
            return [a, b]
        elif s == 1.0:
            return {}
        else:
            d = changed
            if added:
                d[insert] = added
            if removed:
                d[delete] = removed
            return d

    def emit_value_diff(self, a, b, s):
        if s == 1.0:
            return {}
        else:
            return [a, b]

    def patch(self, a, d):
        if isinstance(d, list):
            _, b = d
            return b
        elif isinstance(d, dict):
            if not d:
                return a
            if isinstance(a, dict):
                a = dict(a)
                for k, v in d.items():
                    if k is delete:
                        for kdel, _ in v.items():
                            del a[kdel]
                    elif k is insert:
                        for kk, vv in v.items():
                            a[kk] = vv
                    else:
                        a[k] = self.patch(a[k], v)
                return a
            elif isinstance(a, (list, tuple)):
                original_type = type(a)
                a = list(a)
                if delete in d:
                    for pos, value in d[delete]:
                        a.pop(pos)
                if insert in d:
                    for pos, value in d[insert]:
                        a.insert(pos, value)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        a[k] = self.patch(a[k], v)
                if original_type is not list:
                    a = original_type(a)
                return a
            elif isinstance(a, set):
                a = set(a)
                if discard in d:
                    for x in d[discard]:
                        a.discard(x)
                if add in d:
                    for x in d[add]:
                        a.add(x)
                return a
        raise Exception("Invalid symmetric diff")

    def unpatch(self, b, d):
        if isinstance(d, list):
            a, _ = d
            return a
        elif isinstance(d, dict):
            if not d:
                return b
            if isinstance(b, dict):
                b = dict(b)
                for k, v in d.items():
                    if k is delete:
                        for kk, vv in v.items():
                            b[kk] = vv
                    elif k is insert:
                        for kk, vv in v.items():
                            del b[kk]
                    else:
                        b[k] = self.unpatch(b[k], v)
                return b
            elif isinstance(b, (list, tuple)):
                original_type = type(b)
                b = list(b)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        b[k] = self.unpatch(b[k], v)
                if insert in d:
                    for pos, value in reversed(d[insert]):
                        b.pop(pos)
                if delete in d:
                    for pos, value in reversed(d[delete]):
                        b.insert(pos, value)
                if original_type is not list:
                    b = original_type(b)
                return b
            elif isinstance(b, set):
                b = set(b)
                if discard in d:
                    for x in d[discard]:
                        b.add(x)
                if add in d:
                    for x in d[add]:
                        b.discard(x)
                return b
        raise Exception("Invalid symmetric diff")


builtin_syntaxes = {
    'compact': CompactJsonDiffSyntax(),
    'symmetric': SymmetricJsonDiffSyntax(),
    'explicit': ExplicitJsonDiffSyntax()
}


class JsonDiffer(object):

    class Options(object):
        pass

    def __init__(self, syntax='compact', load=False, dump=False, marshal=False,
                 loader=default_loader, dumper=default_dumper, escape_str='$'):
        self.options = JsonDiffer.Options()
        self.options.syntax = builtin_syntaxes.get(syntax, syntax)
        self.options.load = load
        self.options.dump = dump
        self.options.marshal = marshal
        self.options.loader = loader
        self.options.dumper = dumper
        self.options.escape_str = escape_str
        self._symbol_map = {
            escape_str + symbol.label: symbol
            for symbol in _all_symbols_
        }

    def _list_diff_0(self, C, X, Y):
        i, j = len(X), len(Y)
        r = []
        while True:
            if i > 0 and j > 0:
                d, s = self._obj_diff(X[i-1], Y[j-1])
                if s > 0 and C[i][j] == C[i-1][j-1] + s:
                    r.append((0, d, j-1, s))
                    i, j = i - 1, j - 1
                    continue
            if j > 0 and (i == 0 or C[i][j-1] >= C[i-1][j]):
                r.append((1, Y[j-1], j-1, 0.0))
                j = j - 1
                continue
            if i > 0 and (j == 0 or C[i][j-1] < C[i-1][j]):
                r.append((-1, X[i-1], i-1, 0.0))
                i = i - 1
                continue
            return reversed(r)

    def _list_diff(self, X, Y):
        # LCS
        m = len(X)
        n = len(Y)
        # An (m+1) times (n+1) matrix
        C = [[0 for j in range(n+1)] for i in range(m+1)]
        for i in range(1, m+1):
            for j in range(1, n+1):
                _, s = self._obj_diff(X[i-1], Y[j-1])
                # Following lines are part of the original LCS algorithm
                # left in the code in case modification turns out to be problematic
                #if X[i-1] == Y[j-1]:
                #    C[i][j] = C[i-1][j-1] + 1
                #else:
                C[i][j] = max(C[i][j-1], C[i-1][j], C[i-1][j-1] + s)
        inserted = []
        deleted = []
        changed = {}
        tot_s = 0.0

        for sign, value, pos, s in self._list_diff_0(C, X, Y):
            if sign == 1:
                inserted.append((pos, value))
            elif sign == -1:
                deleted.insert(0, (pos, value))
            elif sign == 0 and s < 1:
                changed[pos] = value
            tot_s += s
        tot_n = len(X) + len(inserted)
        if tot_n == 0:
            s = 1.0
        else:
            s = tot_s / tot_n
        return self.options.syntax.emit_list_diff(X, Y, s, inserted, changed, deleted), s

    def _set_diff(self, a, b):
        removed = a.difference(b)
        added = b.difference(a)
        if not removed and not added:
            return {}, 1.0
        ranking = sorted(
            (
                (self._obj_diff(x, y)[1], x, y)
                for x in removed
                for y in added
            ),
            reverse=True,
            key=lambda x: x[0]
        )
        r2 = set(removed)
        a2 = set(added)
        n_common = len(a) - len(removed)
        s_common = float(n_common)
        for s, x, y in ranking:
            if x in r2 and y in a2:
                r2.discard(x)
                a2.discard(y)
                s_common += s
                n_common += 1
            if not r2 or not a2:
                break
        n_tot = len(a) + len(added)
        s = s_common / n_tot if n_tot != 0 else 1.0
        return self.options.syntax.emit_set_diff(a, b, s, added, removed), s

    def _dict_diff(self, a, b):
        removed = {}
        nremoved = 0
        nadded = 0
        nmatched = 0
        smatched = 0.0
        added = {}
        changed = {}
        for k, v in a.items():
            w = b.get(k, missing)
            if w is missing:
                nremoved += 1
                removed[k] = v
            else:
                nmatched += 1
                d, s = self._obj_diff(v, w)
                if s < 1.0:
                    changed[k] = d
                smatched += 0.5 + 0.5 * s
        for k, v in b.items():
            if k not in a:
                nadded += 1
                added[k] = v
        n_tot = nremoved + nmatched + nadded
        s = smatched / n_tot if n_tot != 0 else 1.0
        return self.options.syntax.emit_dict_diff(a, b, s, added, changed, removed), s

    def _obj_diff(self, a, b):
        if a is b:
            return self.options.syntax.emit_value_diff(a, b, 1.0), 1.0
        if isinstance(a, dict) and isinstance(b, dict):
            return self._dict_diff(a, b)
        elif isinstance(a, tuple) and isinstance(b, tuple):
            return self._list_diff(a, b)
        elif isinstance(a, list) and isinstance(b, list):
            return self._list_diff(a, b)
        elif isinstance(a, set) and isinstance(b, set):
            return self._set_diff(a, b)
        elif a != b:
            return self.options.syntax.emit_value_diff(a, b, 0.0), 0.0
        else:
            return self.options.syntax.emit_value_diff(a, b, 1.0), 1.0

    def diff(self, a, b, fp=None):
        if self.options.load:
            a = self.options.loader(a)
            b = self.options.loader(b)

        d, s = self._obj_diff(a, b)

        if self.options.marshal or self.options.dump:
            d = self.marshal(d)

        if self.options.dump:
            return self.options.dumper(d, fp)
        else:
            return d

    def similarity(self, a, b):
        if self.options.load:
            a = self.options.loader(a)
            b = self.options.loader(b)

        d, s = self._obj_diff(a, b)

        return s

    def patch(self, a, d, fp=None):
        if self.options.load:
            a = self.options.loader(a)
            d = self.options.loader(d)

        if self.options.marshal or self.options.load:
            d = self.unmarshal(d)

        b = self.options.syntax.patch(a, d)

        if self.options.dump:
            return self.options.dumper(b, fp)
        else:
            return b

    def unpatch(self, b, d, fp=None):
        if self.options.load:
            b = self.options.loader(b)
            d = self.options.loader(d)

        if self.options.marshal or self.options.load:
            d = self.unmarshal(d)

        a = self.options.syntax.unpatch(b, d)

        if self.options.dump:
            return self.options.dumper(a, fp)
        else:
            return a


    def _unescape(self, x):
        if isinstance(x, string_types):
            sym = self._symbol_map.get(x, None)
            if sym is not None:
                return sym
            if x.startswith(self.options.escape_str):
                return x[1:]
        return x

    def unmarshal(self, d):
        if isinstance(d, dict):
            return {
                self._unescape(k): self.unmarshal(v)
                for k, v in d.items()
            }
        elif isinstance(d, (list, tuple)):
            return type(d)(
                self.unmarshal(x)
                for x in d
            )
        else:
            return self._unescape(d)

    def _escape(self, o):
        if type(o) is Symbol:
            return self.options.escape_str + o.label
        if isinstance(o, string_types) and o.startswith(self.options.escape_str):
            return self.options.escape_str + o
        return o

    def marshal(self, d):
        if isinstance(d, dict):
            return {
                self._escape(k): self.marshal(v)
                for k, v in d.items()
            }
        elif isinstance(d, (list, tuple)):
            return type(d)(
                self.marshal(x)
                for x in d
            )
        else:
            return self._escape(d)


def diff(a, b, fp=None, cls=JsonDiffer, **kwargs):
    return cls(**kwargs).diff(a, b, fp)


def patch(a, d, fp=None, cls=JsonDiffer, **kwargs):
    return cls(**kwargs).patch(a, d, fp)


def similarity(a, b, cls=JsonDiffer, **kwargs):
    return cls(**kwargs).similarity(a, b)


__all__ = [
    "similarity",
    "diff",
    "JsonDiffer",
    "JsonDumper",
    "JsonLoader",
]
