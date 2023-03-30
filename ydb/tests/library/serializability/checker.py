
import six
import time
import random
from collections import defaultdict


__all__ = [
    'SerializabilityError',
    'SerializabilityChecker',
]


class Node(object):
    def __init__(self, value):
        self.description = None
        self.value = value
        self.reads = {}  # key -> write node
        self.writes = set()  # set of keys
        self.dependencies = set()

        self.rev_reads = defaultdict(set)  # key -> set(read node...)
        self.rev_dependencies = set()


class KeyInfo(object):
    def __init__(self, initial_node):
        self.writes = {}  # value -> write node
        self.writes[initial_node.value] = initial_node
        self.committed = set()
        self.committed.add(initial_node)
        self.committed_writes = set()
        self.committed_writes.add(initial_node)


class Reason(object):
    class Base(object):
        def source_keys(self):
            return ()

        def source_orders(self):
            return ()

    class Initial(Base):
        def __init__(self, left, right):
            self.left = left
            self.right = right

    class Observed(Base):
        def __init__(self, key, left, right):
            self.key = key
            self.left = left
            self.right = right

        def source_keys(self):
            return (self.key,)

        def produced_order(self):
            return (self.left, self.right)

        def explain(self):
            return 'observed %r -> %r (read key %r)' % (self.left, self.right, self.key)

    class LinearizedRead(Base):
        def __init__(self, key, left, right):
            self.key = key
            self.left = left
            self.right = right

        def source_keys(self):
            return (self.key,)

        def produced_order(self):
            return (self.left, self.right)

        def explain(self):
            return 'linearized %r -> %r (reading key %r)' % (self.left, self.right, self.key)

    class LinearizedWrite(Base):
        def __init__(self, key, left, right):
            self.key = key
            self.left = left
            self.right = right

        def source_keys(self):
            return (self.key,)

        def produced_order(self):
            return (self.left, self.right)

        def explain(self):
            return 'linearized %r -> %r (writing key %r)' % (self.left, self.right, self.key)

    class LinearizedGlobal(Base):
        def __init__(self, left, right):
            self.left = left
            self.right = right

        def produced_order(self):
            return (self.left, self.right)

        def explain(self):
            return 'linearized %r -> %r (global)' % (self.left, self.right)

    class Flattened(Base):
        def __init__(self, left, middle, right):
            self.left = left
            self.middle = middle
            self.right = right

        def source_orders(self):
            return ((self.left, self.middle), (self.middle, self.right))

        def produced_order(self):
            return (self.left, self.right)

        def explain(self):
            return 'flattened %r -> %r (based on %r -> %r -> %r)' % (self.left, self.right, self.left, self.middle, self.right)

    class Flattened4(Base):
        def __init__(self, left, middle1, middle2, right):
            self.left = left
            self.middle1 = middle1
            self.middle2 = middle2
            self.right = right

        def source_orders(self):
            return ((self.left, self.middle1), (self.middle1, self.middle2), (self.middle2, self.right))

        def produced_order(self):
            return (self.left, self.right)

        def explain(self):
            return 'flattened %r -> %r (based on %r -> %r -> %r -> %r)' % (self.left, self.right, self.left, self.middle1, self.middle2, self.right)

    class OrderingWRW(Base):
        def __init__(self, key, left, middle, right):
            self.key = key
            self.left = left
            self.middle = middle
            self.right = right

        def source_keys(self):
            return (self.key,)

        def source_orders(self):
            return ((self.left, self.right), (self.left, self.middle))

        def produced_order(self):
            return (self.middle, self.right)

        def explain(self):
            return 'ordering %r -> %r by key %r (write %r -> read %r -> write %r)' % (
                self.middle, self.right, self.key,
                self.left, self.middle, self.right,
            )

    class OrderingWWR(Base):
        def __init__(self, key, left, middle, right):
            self.key = key
            self.left = left
            self.middle = middle
            self.right = right

        def source_keys(self):
            return (self.key,)

        def source_orders(self):
            return ((self.left, self.right), (self.middle, self.right))

        def produced_order(self):
            return (self.left, self.middle)

        def explain(self):
            return 'ordering %r -> %r by key %r (write %r -> write %r -> read %r)' % (
                self.left, self.middle, self.key,
                self.left, self.middle, self.right,
            )

    class OrderingRWR(Base):
        def __init__(self, key, left, middle, right):
            self.key = key
            self.left = left
            self.middle = middle
            self.right = right

        def source_keys(self):
            return (self.key,)

        def source_orders(self):
            return ((self.left, self.right), (self.middle, self.right))

        def produced_order(self):
            return (self.left, self.middle)

        def explain(self):
            return 'ordering %r -> %r by key %r (read %r -> write %r -> read %r)' % (
                self.left, self.middle, self.key,
                self.left, self.middle, self.right,
            )


class SerializabilityError(RuntimeError):
    pass


class SerializabilityChecker(object):
    def __init__(self, logger=None, debug=False, explain=False):
        self.logger = logger
        self.debug = debug
        self.explain = explain

        self.initial_node = Node(0)

        self.nodes = {}
        self.nodes[0] = self.initial_node
        self.committed = set()
        self.committed.add(self.initial_node)
        self.observed_commits = set()
        self.aborted = set()

        self._next_value = 1

        self.keys = {}

        self.writing_keys = defaultdict(int)

        self.reasons = defaultdict(list)

    def prepare_keys(self, cnt):
        for key in six.moves.range(0, cnt + 1):
            assert key not in self.keys, 'Multiple prepare calls'
            self.keys[key] = KeyInfo(self.initial_node)
            self.initial_node.writes.add(key)

    def select_read_keys(self, cnt=1):
        assert cnt >= 1
        return random.sample(sorted(self.keys.keys()), cnt)

    def select_write_keys(self, cnt=1):
        assert cnt >= 1
        keys = random.sample(sorted(self.keys.keys()), cnt)
        for key in keys:
            self.writing_keys[key] += 1
        return keys

    def release_write_keys(self, keys):
        for key in keys:
            was = self.writing_keys[key]
            if was > 1:
                self.writing_keys[key] = was - 1
            else:
                assert was == 1
                del self.writing_keys[key]

    def select_read_from_write_keys(self, cnt=1):
        assert cnt >= 1
        keys = self.writing_keys.keys()
        if len(keys) < cnt:
            # There are not enough keys
            return []
        return random.sample(sorted(keys), cnt)

    def get_next_node_value(self):
        return self._next_value

    def new_node(self):
        node = Node(self._next_value)
        self._next_value += 1
        node.dependencies.add(self.initial_node)
        self.nodes[node.value] = node
        return node

    def find_node(self, value):
        try:
            return self.nodes[value]
        except KeyError:
            raise SerializabilityError('Failed to find node with value %r' % (value,))

    def _add_reason(self, reason):
        """Remembers reason for some path, logs it when necessary"""
        if self.debug and self.logger is not None:
            self.logger.debug('%s', reason.explain())
        a, b = reason.produced_order()
        self.reasons[a, b].append(reason)
        for a, b in reason.source_orders():
            # N.B. we ignore node 0, because initial node may have no reasons
            assert (a, b) in self.reasons or not a or not b, 'Source ordering %r -> %r is not explained' % (a, b)

    def _explain_reason(self, a, b):
        """Logs an explanation for path a -> b"""

        keys = set()
        nodes = set()

        done = set()
        pending = []
        pending.append((a, b))

        if self.logger is not None:
            self.logger.info('Reasoning for %r -> %r:', a, b)
        while pending:
            a, b = pending.pop()
            if (a, b) in done:
                continue
            nodes.add(a)
            nodes.add(b)
            done.add((a, b))
            reasons = self.reasons.get((a, b))
            if reasons is None:
                self.logger.error('    no explanation for %r -> %r', a, b)
                continue
            for reason in reasons:
                self.logger.info('    %s', reason.explain())
                keys.update(reason.source_keys())
                pending.extend(reason.source_orders())

        return keys, nodes

    def ensure_linearizable_reads(self, node, keys):
        """Ensure reads from keys happen after committed reads or writes to keys

        This method must be called before starting operation.
        """
        if self.explain and self.debug and self.logger is not None:
            self.logger.debug('linearizing %r (reading keys %r)', node.value, keys)
        for key in keys:
            assert key in self.keys, 'Key %r was not prepared properly' % (key,)
            if self.explain:
                for dep in self.keys[key].committed:
                    self._add_reason(Reason.LinearizedRead(key, dep.value, node.value))
            node.dependencies.update(self.keys[key].committed)

    def ensure_linearizable_writes(self, node, keys):
        """Ensure writes to keys happen after committed reads or writes to keys

        This method must be called before starting operation.
        """
        if self.explain and self.debug and self.logger is not None:
            self.logger.debug('linearizing %r (writing keys %r)', node.value, keys)
        for key in keys:
            assert key in self.keys, 'Key %r was not prepared properly' % (key,)
            if self.explain:
                for dep in self.keys[key].committed:
                    self._add_reason(Reason.LinearizedWrite(key, dep.value, node.value))
            node.dependencies.update(self.keys[key].committed)

    def ensure_linearizable_globally(self, node):
        """For node ensures global linearizability

        This method must be called before starting operation.
        """
        if self.explain and self.debug and self.logger is not None:
            self.logger.debug('linearizing %r (global)', node.value)
        if self.explain:
            for dep in self.committed:
                if dep not in node.dependencies:
                    self._add_reason(Reason.LinearizedGlobal(dep.value, node.value))
        node.dependencies.update(self.committed)

    def ensure_read_value(self, node, key, value):
        """For node ensures that it reads key/value pair correctly"""
        assert key in self.keys, 'Key %r was not prepared properly' % (key,)
        write = self.find_node(value)
        if key not in write.writes:
            raise SerializabilityError('Node %r observes key %r = %r which was not written' % (node.value, key, value))
        assert key not in node.reads or node.reads[key] is write, 'Key %r has multiple conflicting reads' % (key,)
        node.reads[key] = write
        if self.explain:
            self._add_reason(Reason.Observed(key, write.value, node.value))
        node.dependencies.add(write)
        # We observed this write, so it must be committed
        self.observed_commits.add(write)

    def ensure_write_value(self, node, key):
        """For node ensure that it writes key/value pair correctly"""
        assert key in self.keys, 'Key %r was not prepared properly' % (key,)
        info = self.keys[key]
        info.writes[node.value] = node
        node.writes.add(key)

    def commit(self, node):
        for key, write in node.reads.items():
            assert write in node.dependencies
            assert key in self.keys, 'Key %r was not prepared properly' % (key,)
            info = self.keys[key]
            info.committed.add(node)
        for key in node.writes:
            assert key in self.keys, 'Key %r was not prepared properly' % (key,)
            info = self.keys[key]
            info.committed.add(node)
            info.committed_writes.add(node)
        self.committed.add(node)

    def abort(self, node):
        if node.reads:
            # We want to check reads are correct even in aborted transactions
            # To do that we pretend like we committed a readonly tx
            #
            # This assumes linearizable dependencies (calculated before tx is
            # started) are correct, which is true for ydb since successful
            # reads imply interactivity, and interactive transactions take a
            # global snapshot, linearizing with everything.
            for key in node.writes:
                assert key in self.keys
                info = self.keys[key]
                del info.writes[node.value]
            node.writes.clear()
            self.commit(node)
        else:
            self.aborted.add(node)

    def _process_observed_commits(self):
        """Makes sure all observed nodes are in a committed set"""
        for node in self.observed_commits:
            if node in self.aborted or not node.writes:
                raise SerializabilityError('Node %r was aborted but observed committed' % (node.value,))
            self.committed.add(node)

    def _extend_committed(self):
        """Makes sure all nodes reachable from committed set are in a committed set"""
        done = set()

        def dfs(node):
            if node not in done:
                done.add(node)
                self.committed.add(node)
                for dep in node.dependencies:
                    if dep in self.aborted:
                        if self.explain:
                            self._explain_reason(dep.value, node.value)
                        raise SerializabilityError('Dependency on aborted changes (%r -> %r)' % (dep.value, node.value))
                    dfs(dep)

        for node in list(self.committed):
            if node in self.aborted:
                raise SerializabilityError('Node %r is both committed and aborted' % (node.value,))
            dfs(node)

    def _flatten_dependencies(self):
        """Makes sure all committed nodes include all their reachable dependencies"""
        done = set()
        gray = set()
        last = [time.time()]
        changed = [False]

        def dfs(node):
            if node not in done:
                if node in gray:
                    raise SerializabilityError('Dependency cycle detected')
                gray.add(node)

                ext = set()
                for dep in node.dependencies:
                    if dep in ext:
                        continue  # already visited indirectly
                    dfs(dep)
                    if self.explain:
                        for k in dep.dependencies:
                            if k not in ext and k not in node.dependencies:
                                # new dependency
                                self._add_reason(Reason.Flattened(k.value, dep.value, node.value))
                    ext.update(dep.dependencies)
                before = len(node.dependencies)
                node.dependencies.update(ext)
                after = len(node.dependencies)
                if before != after:
                    changed[0] = True
                done.add(node)

                t = time.time()
                if (t - last[0]) >= 1.0:
                    # Report some progress once a second
                    if self.logger is not None:
                        self.logger.info('dependencies: %d/%d processed', len(done), len(self.committed))
                    last[0] = t

        for node in self.committed:
            dfs(node)

        return changed[0]

    def _fill_reverse_dependencies(self):
        """Makes sure all committed nodes have reverse dependencies initialized"""
        for node in self.committed:
            for key, write in node.reads.items():
                assert write in node.dependencies
                write.rev_reads[key].add(node)
            for dep in node.dependencies:
                dep.rev_dependencies.add(node)

    def _fill_extra_dependencies(self):
        pending = set(self.committed)

        def add_link(a, b):
            """Add raw link between a and b"""
            assert a not in b.dependencies
            assert b not in a.dependencies
            assert a not in b.rev_dependencies
            assert b not in a.rev_dependencies
            b.dependencies.add(a)
            a.rev_dependencies.add(b)

        def is_ordered(a, b):
            """Returns True if a ordered before b"""
            assert isinstance(a, Node)
            assert isinstance(b, Node)
            return a in b.dependencies

        def draw_table(rows):
            cell_sizes = [0] * len(rows[0])
            for row in rows:
                for i, col in enumerate(row):
                    cell_sizes[i] = max(cell_sizes[i], len(col))
            separator = None
            for line, row in enumerate(rows):
                parts = ['|']
                for i, col in enumerate(row):
                    if i == 0 or i == len(row) - 1:
                        col = col.ljust(cell_sizes[i])
                    else:
                        col = col.rjust(cell_sizes[i])
                    parts.append(col)
                    parts.append('|')
                row = ' '.join(parts)
                if line == 0:
                    separator = '|' + '-' * (len(row) - 2) + '|'
                    self.logger.info('%s', separator)
                self.logger.info('%s', ' '.join(parts))
                if line == 0:
                    self.logger.info('%s', separator)
            if separator is not None:
                self.logger.info('%s', separator)

        def describe_keys(keys):
            parts = []
            first = None
            last = None

            def flush():
                if first is not None:
                    if (last - first) > 1:
                        parts.append('%r-%r' % (first, last))
                    elif (last - first) == 1:
                        parts.append('%r' % (first,))
                        parts.append('%r' % (last,))
                    else:
                        parts.append('%r' % (first,))
            for key in keys:
                if first is None or key != last + 1:
                    flush()
                    first = key
                last = key
            flush()
            return ', '.join(parts)

        def explain_conflicts(a, b):
            keys1, nodes1 = self._explain_reason(a.value, b.value)
            keys2, nodes2 = self._explain_reason(b.value, a.value)
            keys = sorted(keys1 | keys2)
            nodes = sorted(nodes1 | nodes2)
            rows = [['tx \\ key'] + ['key %r' % (key,) for key in keys] + ['description']]
            for node_id in nodes:
                if node_id == 0:
                    continue  # don't care
                node = self.nodes.get(node_id)
                values_reads = []
                values_writes = []
                have_reads = False
                have_writes = False
                for key in keys:
                    if key in node.reads:
                        have_reads = True
                        values_reads.append('read %r' % (node.reads[key].value,))
                    else:
                        values_reads.append('')
                    if key in node.writes:
                        have_writes = True
                        values_writes.append('write %r' % (node.value,))
                    else:
                        values_writes.append('')
                if have_reads:
                    description = '%s: keys %s' % (node.description, describe_keys(sorted(node.reads)))
                    rows.append(['%d r' % node_id] + values_reads + [description])
                if have_writes:
                    description = '%s: keys %s' % (node.description, describe_keys(sorted(node.writes)))
                    rows.append(['%d w' % node_id] + values_writes + [description])
            draw_table(rows)

        def ensure_order(a, b):
            """Ensure a happens before b"""
            if a is b:
                raise RuntimeError('Ordering %r -> %r (same node)' % (a.value, b.value))
            if is_ordered(a, b):
                raise RuntimeError('Ordering %r -> %r (already exists)' % (a.value, b.value))
            if is_ordered(b, a):
                if self.explain and self.logger is not None:
                    explain_conflicts(a, b)
                    if self._flatten_dependencies():
                        self.logger.warning('WARNING: found some unexpected indirect dependencies')
                raise SerializabilityError('Dependency cycle detected (ensure %r -> %r, but have %r -> %r)' % (a.value, b.value, b.value, a.value))

            add_link(a, b)
            pending.add(a)
            pending.add(b)

            # Dependencies are cumulative at this stage
            # Must link all left <- a <- b <- right
            lefts = a.dependencies.difference(b.dependencies)
            rights = b.rev_dependencies.difference(a.rev_dependencies)

            for left in lefts:
                if self.explain:
                    self._add_reason(Reason.Flattened(left.value, a.value, b.value))
                add_link(left, b)
                pending.add(left)

            for right in rights:
                if self.explain:
                    self._add_reason(Reason.Flattened(a.value, b.value, right.value))
                add_link(a, right)
                pending.add(right)

            for left in lefts:
                # N.B. difference is much faster than manual is_ordered checks
                for right in rights.difference(left.rev_dependencies):
                    if self.explain:
                        self._add_reason(Reason.Flattened4(left.value, a.value, b.value, right.value))
                    add_link(left, right)

        def process_write(write):
            """Processes reads that observed this write and dependencies"""
            # Search for W->R->W2 ordering based on W->W2
            for write2 in list(write.rev_dependencies):
                for key, reads in write.rev_reads.items():
                    if key not in write2.writes:
                        continue  # no intersection
                    # Look for R that is not yet R->W2
                    for read in reads.difference(write2.dependencies):
                        if read is write2:
                            continue  # R is W2
                        if is_ordered(read, write2):
                            continue  # already ordered
                        # We must add R->W2
                        if self.explain:
                            self._add_reason(Reason.OrderingWRW(key, write.value, read.value, write2.value))
                        ensure_order(read, write2)

        def process_read(read):
            """Processes writes that this read observed and dependencies"""
            # Search for RW1->W->R ordering based on RW1->R
            for rw1 in list(read.dependencies):
                for key, write in read.reads.items():
                    if rw1 is write:
                        continue  # RW1 is W
                    if is_ordered(rw1, write):
                        continue  # already ordered
                    if key in rw1.writes:
                        # We must add W1->W (key was not overwritten)
                        if self.explain:
                            self._add_reason(Reason.OrderingWWR(key, rw1.value, write.value, read.value))
                        ensure_order(rw1, write)
                    elif key in rw1.reads and rw1.reads[key] is not write:
                        # We must add R1->W (key was observed differently)
                        if self.explain:
                            self._add_reason(Reason.OrderingRWR(key, rw1.value, write.value, read.value))
                        ensure_order(rw1, write)

        last = [time.time()]

        while pending:
            node = pending.pop()

            process_write(node)

            process_read(node)

            t = time.time()
            if (t - last[0]) >= 1.0:
                # Report some progress once a second
                if self.logger is not None:
                    self.logger.info('ordering: %d nodes left...', len(pending))
                last[0] = t

        if self.explain and self.logger is not None and self._flatten_dependencies():
            self.logger.warning('WARNING: found some unexpected indirect dependencies')

    def verify(self):
        self._process_observed_commits()
        self._extend_committed()
        self._flatten_dependencies()
        self._fill_reverse_dependencies()
        self._fill_extra_dependencies()
