#!/usr/bin/env python
"""
This module runs the performance tests to compare the ``re`` module with the
``re2`` module. You can just run it from the command line, assuming you have re2
installed, and it will output a table in ReST format comparing everything.

To add a test, you can add a function to the bottom of this page that uses the
@register_test() decorator. Alternatively, you can create a module that uses it and
import it.
"""
from timeit import Timer
import re2
import re
try:
    import regex
except ImportError:
    regex = None

import os
import gzip

re2.set_fallback_notification(re2.FALLBACK_EXCEPTION)

os.chdir(os.path.dirname(__file__) or '.')

tests = {}

setup_code = """\
import re2
import re
from __main__ import tests, current_re
test = tests[%r]
"""

current_re = [None]




def main():
    benchmarks = {}
    # Run all of the performance comparisons.
    for testname, method in tests.items():
        benchmarks[testname] = {}
        if regex is not None:
            modules = (re, re2, regex)
        else:
            modules = (re, re2)
        results = [None for module in modules]
        for i, module in enumerate(modules):
            # We pre-compile the pattern, because that's
            # what people do.
            current_re[0] = module.compile(method.pattern)

            results[i] = method(current_re[0], **method.data)

            # Run a test.
            t = Timer("test(current_re[0],**test.data)",
                      setup_code % testname)
            benchmarks[testname][module.__name__] = (t.timeit(method.num_runs),
                                                     method.__doc__.strip(),
                                                     method.pattern,
                                                     method.num_runs)
        for i in range(len(results) - 1):
            if results[i] != results[i + 1]:
                raise ValueError("re2 output is not the same as re output: %s" % testname)

    benchmarks_to_ReST(benchmarks)


def benchmarks_to_ReST(benchmarks):
    """
    Convert dictionary to a nice table for ReST.
    """
    if regex is not None:
        headers = ('Test', 'Description', '# total runs', '``re`` time(s)', '``re2`` time(s)', '% ``re`` time', '``regex`` time(s)', '% ``regex`` time')
    else:
        headers = ('Test', 'Description', '# total runs', '``re`` time(s)', '``re2`` time(s)', '% ``re`` time')
    table = [headers]
    f = lambda x: "%0.3f" % x
    p = lambda x: "%0.2f%%" % (x * 100)

    for test, data in benchmarks.items():
        row = [test, data["re"][1], str(data["re"][3]), f(data["re"][0]), f(data["re2"][0])]

        row.append(p(data["re2"][0] / data["re"][0]))
        if regex is not None:
            row.extend((f(data["regex"][0]), p(data["re2"][0] / data["regex"][0])))
        table.append(row)
    col_sizes = [0] * len(table[0])
    for col in range(len(table[0])):
        col_sizes[col] = max(len(row[col]) for row in table)

    def print_divider(symbol='-'):
        print('+' + '+'.join(symbol*col_size for col_size in col_sizes) + '+')
    def print_row(row):
        print('|' + '|'.join(item.ljust(col_sizes[i]) for i, item in
            enumerate(row)) + '|')

    print_divider()
    print_row(table[0])
    print_divider('=')
    for row in table[1:]:
        print_row(row)
        print_divider()





###############################################
# Tests for performance
###############################################


# Convenient decorator for registering a new test.
def register_test(name, pattern, num_runs = 100, **data):
    def decorator(method):
        tests[name] = method
        method.pattern = pattern.encode('utf-8')
        method.num_runs = num_runs
        method.data = data

        return method
    return decorator


# This is the only function to get data right now,
# but I could imagine other functions as well.
_wikidata = None
def getwikidata():
    global _wikidata
    if _wikidata is None:
        _wikidata = gzip.open('wikipages.xml.gz', 'rb').read()
    return _wikidata



@register_test("Findall URI|Email",
               r'([a-zA-Z][a-zA-Z0-9]*)://([^ /]+)(/[^ ]*)?|([^ @]+)@([^ @]+)',
               2,
               data=getwikidata())
def findall_uriemail(pattern, data):
    """
    Find list of '([a-zA-Z][a-zA-Z0-9]*)://([^ /]+)(/[^ ]*)?|([^ @]+)@([^ @]+)'
    """
    return len(pattern.findall(data))



@register_test("Replace WikiLinks",
               r'(\[\[(^\|)+.*?\]\])',
               data=getwikidata())
def replace_wikilinks(pattern, data):
    """
    This test replaces links of the form [[Obama|Barack_Obama]] to Obama.
    """
    return len(pattern.sub(r'\1'.encode('utf-8'), data))



@register_test("Remove WikiLinks",
               r'(\[\[(^\|)+.*?\]\])',
               data=getwikidata())
def remove_wikilinks(pattern, data):
    """
    This test replaces links of the form [[Obama|Barack_Obama]] to the empty string
    """
    return len(pattern.sub(r'', data))





@register_test("Remove WikiLinks",
               r'(<page[^>]*>)',
               data=getwikidata())
def split_pages(pattern, data):
    """
    This test splits the data by the <page> tag.
    """
    return len(pattern.split(data))


def getweblogdata():
    return open(os.path.join(os.path.dirname(__file__), 'access.log'), 'rb')

#@register_test("weblog scan",
#               #r'^(\S+) (\S+) (\S+) \[(\d{1,2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) -(\d{4})\] "(\S+) (\S+) (\S+)" (\d+) (\d+|-) "([^"]+)" "([^"]+)"\n',
##               '(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ? (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (".*?"|-) (\S+) (\S+) (\S+) (\S+)',
#               '(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ? (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+)',
#               data=getweblogdata())
def weblog_matches(pattern, data):
    """
    Match weblog data line by line.
    """
    total=0
    for line in data.read()[:20000].splitlines():
        p = pattern.search(line)
        #for p in pattern.finditer(data.read()[:20000]):
        if p:
            total += len(p.groups())
    data.seek(0)

    return 0

if __name__ == '__main__':
    main()
