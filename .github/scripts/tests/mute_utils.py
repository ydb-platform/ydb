import operator
import xml.etree.ElementTree as ET


class MutedTestCheck:
    def __init__(self, fn=None):
        self.classes = set()
        self.methods = set()

        if fn:
            self.populate(fn)

    def populate(self, fn):
        with open(fn, "r") as fp:
            for line in fp:
                if line.startswith("-"):
                    line = line[1:].rstrip()
                    if "::" in line:
                        cls, method = line.split("::", maxsplit=1)
                        self.methods.add((cls, method))
                    else:
                        self.classes.add(line)

    def __call__(self, cls, method=None):
        if cls in self.classes:
            return True

        if method and (cls, method) in self.methods:
            return True

        return False

    @property
    def has_rules(self):
        return len(self.classes) or len(self.methods)


class MutedShardCheck:
    def __init__(self, fn=None):
        self.muted = set()
        if fn:
            self.populate(fn)

    def populate(self, fn):
        with open(fn, "rt") as fp:
            for line in fp:
                target = line.strip()
                if target:
                    self.muted.add(target)

    def __call__(self, target):
        return target in self.muted


def mute_target(node):
    failure = node.find("failure")

    if failure is None:
        return False

    skipped = ET.Element("skipped", {"message": failure.attrib["message"]})
    node.remove(failure)
    node.append(skipped)

    return True


def remove_failure(node):
    failure = node.find("failure")

    if failure is not None:
        node.remove(failure)
        return True

    return False


def op_attr(node, attr, op, value):
    v = int(node.get(attr, 0))
    node.set(attr, str(op(v, value)))


def inc_attr(node, attr, value):
    return op_attr(node, attr, operator.add, value)


def dec_attr(node, attr, value):
    return op_attr(node, attr, operator.sub, value)


def update_suite_info(root, n_remove_failures=None, n_skipped=None):
    if n_remove_failures:
        dec_attr(root, "failures", n_remove_failures)

    if n_skipped:
        inc_attr(root, "skipped", n_skipped)


def recalc_suite_info(suite):
    tests = failures = skipped = 0
    elapsed = 0.0

    for case in suite.findall("testcase"):
        tests += 1
        elapsed += float(case.get("time"))
        if case.find("skipped"):
            skipped += 1
        if case.find("failure"):
            failures += 1

    suite.set("tests", str(tests))
    suite.set("failures", str(failures))
    suite.set("skipped", str(skipped))
    suite.set("time", str(elapsed))
