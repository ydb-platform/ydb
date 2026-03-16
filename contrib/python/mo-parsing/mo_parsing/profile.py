# encoding: utf-8
from json import dumps as value2json

from mo_dots import to_data

from mo_parsing.core import ParserElement
from mo_parsing.utils import Log

try:
    from jx_python import jx
    from mo_files import File
    from mo_future import text, process_time
    from mo_times import Date
except Exception as casue:
    Log.note("please pip install jx-python and pyLibrary")


class Profiler(object):
    def __init__(self, file):
        """
        USE with Profiler("myfile.tab"): TO ENABLE PER-PARSER PROFILING
        :param file:
        """
        self.file = File(file).set_extension("tab")
        self.previous_parse = None

    def __enter__(self):
        timing.clear()
        self.previous_parse = ParserElement._parse
        ParserElement._parse = _profile_parse

    def __exit__(self, exc_type, exc_val, exc_tb):
        ParserElement._parse = self.previous_parse
        profile = jx.sort(
            [
                {
                    "parser": text(parser),
                    "cache_hits": cache,
                    "matches": match,
                    "failures": fail,
                    "call_count": match + fail + cache,
                    "total_parse": parse,
                    "total_overhead": all - parse,
                    "per_parse": parse / (match + fail),
                    "per_overhead": (all - parse) / (match + fail + cache),
                }
                for parser, (cache, match, fail, parse, all) in timing.items()
            ],
            {"total_parse": "desc"},
        )
        self.file.add_suffix(
            Date.now().format("%Y%m%d_%H%M%S")
        ).write(_list2tab(profile))


timing = {}


def _profile_parse(self, string, start, do_actions=True):
    all_start = process_time()
    try:
        try:
            # preloc = self.whitespace.skip(string, start)
            preloc = start
            parse_start = process_time()
            tokens = self.parse_impl(string, preloc, do_actions)
            parse_end = process_time()
            match = 1
        except Exception as cause:
            parse_end = process_time()
            match = 2
            self.parser_config.fail_action and self.parser_config.fail_action(
                self, start, string, cause
            )
            raise

        if self.parse_action and (do_actions or self.parser_config.callDuringTry):
            for fn in self.parse_action:
                tokens = fn(tokens, start, string)

        return tokens
    finally:
        timing_entry = timing.get(self)
        if timing_entry is None:
            timing_entry = timing[self] = [0, 0, 0, 0, 0]
        timing_entry[match] += 1  # cache
        timing_entry[3] += parse_end - parse_start  # parse time
        timing_entry[4] += process_time() - all_start  # all time


def _list2tab(rows):
    columns = set()
    for r in to_data(rows):
        columns |= set(k for k, v in r.leaves())
    keys = list(columns)

    output = []
    for r in to_data(rows):
        output.append("\t".join(value2json(r[k]) for k in keys))

    return "\t".join(keys) + "\n" + "\n".join(output)
