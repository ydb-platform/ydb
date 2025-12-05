#!/usr/bin/env python3

import subprocess
import json
import sys
import re
import difflib
import time
import logging
import os
import os.path

class RunResult:
    def __init__(self, wall_time, success, result_file, report_file, rusage):
        self.wall_time = wall_time
        self.success = success
        self.result_file = result_file
        self.report_file = report_file
        self.rusage = rusage

def run_query_text(query_text, suffix):
    query_file = 'query_%s.sql' % suffix
    with open(query_file, 'wt') as outf:
        outf.write(query_text + '\n')

    result_file = 'result_%s.json' % suffix
    err_file = 'out_%s.txt' % suffix
    rusage_file = 'rusage.txt'

    if os.path.exists(rusage_file):
        os.remove(rusage_file)

    # with tracing and optimizer logging
    # cmdline = './kqprun_with_rusage.py %s -c dq.conf -s s10.sql -p %s -C query --log KQP_YQL=trace -T script --result-file %s >%s 2>&1' % (rusage_file, query_file, result_file, err_file)

    # no tracing
    cmdline = './kqprun_with_rusage.py %s -c dq.conf -s s1.sql -p %s -C query --log KQP_YQL=trace -T script --result-file %s >%s 2>&1' % (rusage_file, query_file, result_file, err_file)

    logging.info('Cmdline: %s', cmdline)
    kqprun = subprocess.Popen(cmdline, shell=True, stdin=None, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    start_time = time.time()
    stdout, stderr = kqprun.communicate()
    end_time = time.time()
    rusage = None
    if os.path.exists(rusage_file):
        with open(rusage_file, 'rt') as rf:
            rusage_json = rf.read()
            rusage = json.loads(rusage_json.strip())

    wall_time = end_time - start_time
    success = True

    if kqprun.returncode:
        success = False
        logging.error('Subprocess returned %d, see %s for details', kqprun.returncode, err_file)

    return RunResult(wall_time=wall_time, success=success, result_file=result_file, report_file=err_file, rusage=rusage)

def get_score(result):
    return -result.wall_time

def run_query_text_n_times(query_text, suffix, trials=1):
    best_result = None
    for n in range(trials):
        logging.info('Running trial %d of %d' % (n + 1, trials))
        result = run_query_text(query_text, suffix)
        if not result.success:
            return result
        logging.info('Score: {}'.format(get_score(result)))
        if best_result is None:
            best_result = result
            continue
        if get_score(best_result) < get_score(result):
            best_result = result
    return best_result

def fix_json_result(result):
    if isinstance(result, float):
        return int(round(result))
    if not isinstance(result, dict):
        return result
    for k, v in list(result.items()):
        result[k] = fix_json_result(v)
    return result

def read_json_result(result_file):
    lines = []
    with open(result_file, 'rt') as inf:
        for line in inf:
            line = line.strip()
            if not line:
                continue
            json_item = json.loads(line)
            fix_json_result(json_item)
            line = json.dumps(json_item, sort_keys=True)
            lines.append(line)
    return lines

def report_ok(query_id, res_ref, res_new):
    json_report = {
        'query_id': query_id,
        'report': 'ok',
        'wallclock_ref': res_ref.wall_time,
        'wallclock_new': res_new.wall_time,
        'rusage_ref': res_ref.rusage,
        'rusage_new': res_new.rusage,
    }
    print(json.dumps(json_report, ensure_ascii=False))
    sys.stdout.flush()
    logging.info('\x1b[0;32mTest %d passed\x1b[0m', query_id)
    perf_color_code = '\x1b[0;32m' if (res_ref.wall_time > res_new.wall_time) else '\x1b[0;31m'
    logging.info('%sReference time: %.1f sec, new time: %.1f sec\x1b[0m', perf_color_code, res_ref.wall_time, res_new.wall_time)

def report_error(query_id, msg, fail_file):
    json_report = {
        'query_id': query_id,
        'report': 'error',
        'see_file': fail_file
    }
    print(json.dumps(json_report, ensure_ascii=False))
    sys.stdout.flush()
    logging.info('\x1b[0;31mTest %d failed: %s\x1b[0m', query_id, msg)

def run_query(query_file, query_id):
    logging.info('\x1b[0;33mProcessing query %d\x1b[0m', query_id)
    with open(query_file, 'rt') as inf:
        source_query_text = inf.read()
    source_query_text = source_query_text.replace('{path}', '')
    source_query_text = 'pragma kikimr.UseLlvm = "false";\n' + source_query_text

    suffix_ref = 'wide_combine_q%d' % query_id
    suffix_new = 'dq_hash_combine_q%d' % query_id

    run_query_text = "pragma ydb.UseDqHashCombine = \"false\";\n" + source_query_text
    res_ref = run_query_text_n_times(run_query_text, suffix_ref)
    if not res_ref.success:
        report_error(query_id, 'Failed to run test %s' % suffix_ref, res_ref.report_file)

    run_query_text = "pragma ydb.UseDqHashCombine = \"true\";\n" + source_query_text
    res_new = run_query_text_n_times(run_query_text, suffix_new)
    if not res_new.success:
        report_error(query_id, 'Failed to run test %s' % suffix_new, res_new.report_file)

    if not res_ref.success or not res_new.success:
        return False

    ref_items = read_json_result(res_ref.result_file)
    new_items = read_json_result(res_new.result_file)

    ref_items.sort()
    new_items.sort()

    logging.info('%d results reference, %d results test' % (len(ref_items), len(new_items)))
    # if len(ref_items) and len(new_items):
    #    logging.info('First result example:\n\t%s\nvs.\n\t%s' % (ref_items[0], new_items[0]))

    if ref_items != new_items:
        diff = list(difflib.context_diff(ref_items, new_items))

        logging.error('Difference is:')
        for line in diff:
            logging.error(line)

        diff_fname = 'diff_q%d' % query_id
        with open(diff_fname, 'wt') as diff_file:
            for line in diff:
                print(line, file=diff_file)
        report_error(query_id, 'Results differ', diff_fname)

        return False
    else:
        report_ok(query_id, res_ref, res_new)
        return True

def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    for q in range(1, 23):
        query_id = q
        query_file = '/home/pzuev/s/arc/contrib/ydb/library/benchmarks/queries/tpch/ydb/q%d.sql' % query_id
        run_query(query_file, query_id)

if __name__ == '__main__':
    main()
