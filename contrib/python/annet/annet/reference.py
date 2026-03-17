from annet.annlib.patching import match_row_to_acl
from annet.annlib.rbparser.acl import compile_ref_acl_text


class RefMatchResult:
    def __init__(self, gen_cls=None, groups=None):
        self.elems = {}
        if gen_cls and groups:
            self.elems[gen_cls] = groups

    def gen_cls(self):
        return list(self.elems.keys())

    def groups(self):
        return list(sum(self.elems.values(), []))

    def __iter__(self):
        yield from self.elems.items()

    def __add__(self, other):
        ret = RefMatchResult()
        ret.elems.update(self.elems)
        ret.elems.update(other.elems)
        return ret


class RefMatch:
    def __init__(self, acl, gen_cls):
        self.acl = acl
        self.gen_cls = gen_cls

    def match(self, config):
        ret = RefMatchResult()
        passed = self._match(config, self.acl)
        if passed:
            ret = RefMatchResult(self.gen_cls, passed)
        return ret

    @classmethod
    def _match(cls, config, acl, _path=tuple()):
        ret = []
        for row, children in config.items():
            (rule, children_rules) = match_row_to_acl(row, acl)
            if rule and rule["attrs"]["match"]:
                ret.append(rule["attrs"]["match"])
            if rule and children and children_rules:
                ret += cls._match(children, children_rules, _path + (row,))
        return ret


class RefMatcher:
    def __init__(self):
        self.matches = []

    def match(self, config):
        ret = RefMatchResult()
        for rmatch in self.matches:
            ret += rmatch.match(config)
        return ret

    def add(self, acl_text, gen_class):
        acl = compile_ref_acl_text(acl_text)
        self.matches.append(RefMatch(acl, gen_class))


class RefTracker:
    class Root:
        pass

    def __init__(self):
        self.cfgs = {}
        self.mapidx = {}
        self.graph = Graph()
        self.root = self.__class__.Root
        self.map(self.root, self.graph.root_id)

    def map(self, newcls, node_id):
        self.mapidx[node_id] = newcls
        self.mapidx[newcls] = node_id

    def addcls(self, newcls):
        if newcls not in self.mapidx:
            self.map(newcls, self.graph.newnode())
        return self.mapidx[newcls]

    def add(self, refcls, defcls):
        if refcls not in self.mapidx:
            self.add(self.root, refcls)
        ridx = self.addcls(refcls)
        didx = self.addcls(defcls)
        self.graph.connect(ridx, didx)

    def config(self, newcls, config):
        self.cfgs[newcls] = config

    def walk(self):
        ret = []
        for didx, ridx in self.graph.walk():
            dc = self.mapidx[didx]
            rc = self.mapidx[ridx]
            yield dc, rc
        return ret

    def configs(self):
        ret = []
        for dc, rc in self.walk():
            if dc in self.cfgs and rc in self.cfgs:
                ret.append((self.cfgs[dc], self.cfgs[rc]))
        return ret


class Graph:
    def __init__(self):
        self.indices = []
        self.root_id = self.newnode()

    def newnode(self):
        node_id = len(self.indices)
        self.indices.append([0 for _ in range(len(self.indices))])
        for ind in self.indices:
            ind.append(0)
        return node_id

    def connect(self, ridx, didx):
        self.indices[ridx][didx] = 1

    def walk(self):
        def childs(ridx):
            ret = []
            for didx, is_ref in enumerate(self.indices[ridx]):
                if is_ref:
                    ret.append((ridx, didx))
            return ret

        def bfs(run, seen):
            ch = []
            for key in run:
                ridx, didx = key
                if key not in seen:
                    seen.add(key)
                    ch += childs(didx)
                    yield ridx, didx
            if ch:
                yield from bfs(ch, seen)

        for left, right in bfs(childs(self.root_id), set()):
            if left not in (self.root_id, right):
                yield left, right
