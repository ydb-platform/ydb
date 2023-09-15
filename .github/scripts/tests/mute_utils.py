import operator
import re
import xml.etree.ElementTree as ET
from junit_utils import add_junit_property


def pattern_to_re(pattern):
    res = []
    for c in pattern:
        if c == '*':
            res.append('.*')
        else:
            res.append(re.escape(c))

    return f"(?:^{''.join(res)}$)"


class MuteTestCheck:
    def __init__(self, fn):
        self.regexps = []

        with open(fn, 'r') as fp:
            for line in fp:
                line = line.strip()
                pattern = pattern_to_re(line)

                try:
                    self.regexps.append(re.compile(pattern))
                except re.error:
                    print(f"Unable to compile regex {pattern!r}")
                    raise

    def __call__(self, fullname):
        for r in self.regexps:
            if r.match(fullname):
                return True
        return False


def mute_target(testcase):
    err_text = []
    err_msg = None
    found = False

    for node_name in ('failure', 'error'):
        while 1:
            err_node = testcase.find(node_name)
            if err_node is None:
                break

            msg = err_node.get('message')
            if msg:
                if err_msg is None:
                    err_msg = msg
                else:
                    err_text.append(msg)

            if err_node.text:
                err_text.append(err_node.text)

            found = True
            testcase.remove(err_node)

    if not found:
        return False

    skipped = ET.Element("skipped")

    if err_msg:
        skipped.set('message', err_msg)

    if err_text:
        skipped.text = '\n'.join(err_text)
    testcase.append(skipped)

    add_junit_property(testcase, "mute", "automatically muted based on rules")

    return True


def remove_failure(node):
    while 1:
        failure = node.find("failure")
        if failure is None:
            break
        node.remove(failure)


def op_attr(node, attr, op, value):
    v = int(node.get(attr, 0))
    node.set(attr, str(op(v, value)))


def inc_attr(node, attr, value):
    return op_attr(node, attr, operator.add, value)


def dec_attr(node, attr, value):
    return op_attr(node, attr, operator.sub, value)


def update_suite_info(root, n_remove_failures=None, n_remove_errors=None, n_skipped=None):
    if n_remove_failures:
        dec_attr(root, "failures", n_remove_failures)

    if n_remove_errors:
        dec_attr(root, "errors", n_remove_errors)

    if n_skipped:
        inc_attr(root, "skipped", n_skipped)


def recalc_suite_info(suite):
    tests = failures = skipped = 0
    elapsed = 0.0

    for case in suite.findall("testcase"):
        tests += 1
        elapsed += float(case.get("time", 0))
        if case.find("skipped"):
            skipped += 1
        if case.find("failure"):
            failures += 1

    suite.set("tests", str(tests))
    suite.set("failures", str(failures))
    suite.set("skipped", str(skipped))
    suite.set("time", str(elapsed))
