#!/usr/bin/env python3
import signal
import traceback
import html
import math
import argparse
import re
import sys
import os
import itertools
from pathlib import Path

import cyson as yson

# 2024-05-28 19:23:57.817 INFO  dqrun(pid=59479, tid=0x00007A180B80B640) [default] mkql_wide_combine.cpp:439: switching Memory mode to Spilling
RE_SPILLING = re.compile(r'mkql_([a-z_]*)\.cpp:([0-9]+): (0x[0-9a-fA-F]+# )?[Ss]witching Memory mode to Spilling')
SPILLING_MAP = {
    'wide_combine': 'c',
    'wide_top_sort': 's',
    'grace_join': 'j',
}


def fmtchange(cur, ref, plus='bad', minus='good', threshold=10):
    ret = '{:.1f}'.format(cur)
    if ref == 0:
        return ret + ' N/A%'
    change = int((cur/ref - 1)*100)
    cls = ''
    if change > threshold:
        cls = ' class="{}"'.format(plus)
        if change > threshold*100:
            return '{}<span{}>&gt;&gt;&gt;&gt;</span>'.format(ret, cls)
    elif change < -threshold:
        cls = ' class="{}"'.format(minus)
        if change <= -99:
            return '{}<span{}>&lt;&lt;&lt;&lt;</span>'.format(ret, cls)

    return '{}<code{}> {:+3}%</code>'.format(ret, cls, change)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--blacklist-file', default=[], action='append', help='File with query name regexp that will be skipped for comparison')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--by-side', action='store_true', default=False)
    parser.add_argument('--include-q', default=[], action='append')
    parser.add_argument('--exclude-q', default=[], action='append')
    parser.add_argument('--include-dir', default=[], action='append')
    parser.add_argument('--exclude-dir', default=[], action='append')
    parser.add_argument('resultdir', nargs='+', help='Directories for comparison')

    args = parser.parse_args()

    blacklists = []
    for blacklist_file in args.blacklist_file:
        with open(blacklist_file) as f:
            for line in f:
                blacklists.append(re.compile(line.strip()))

    rdirs = args.resultdir
    data = []

    print('''
<html><head><style>
.signal { color: blue; }
.errcode { color: red; }
.ok { color: green; }
.mismatch { color: yellow; }
.tabnum { text-align: right; }
.good { color: green; }
.bad { color: red; }
.spilling { background-color: lightgray; }
code { white-space: pre; }
</style></head>
''')
    print('<table border="1">')

    filelists = [sorted(map(str, Path(dirname).glob('**/summary.tsv'))) for dirname in rdirs]
    if len(args.include_dir):
        outf = []
        for filelist in filelists:
            outl = []
            for file in filelist:
                include = False
                for r in args.include_dir:
                    if re.search(r, file):
                        include = True
                        break
                if include:
                    outl += [file]
            outf += [outl]
        filelists = outf
    if len(args.exclude_dir):
        outf = []
        for filelist in filelists:
            outl = []
            for file in filelist:
                include = True
                for r in args.exclude_dir:
                    if re.search(r, file):
                        include = False
                        break
                if include:
                    outl += [file]
            outf += [outl]
        filelists = outf
    if args.by_side:
        assert len(set(map(len, filelists))) == 1, "All dirs must have same layout"
        print('<tr><th>' + ''.join('<th colspan="{}">{}'.format(7*len(rdirs), html.escape(name[len(rdirs[0]) + 1:])) for name in filelists[0]))
    else:
        print('<tr><th>' + ''.join('<th colspan="{}">{}'.format(7*len(filelist), html.escape(dirname)) for dirname, filelist in zip(rdirs, filelists) if len(filelist)))
    print('<tr><th>')
    if args.by_side:
        dirfiles = map(lambda x: [x[0], [x[1]]], itertools.chain(*map(lambda x: zip(rdirs, x), zip(*filelists))))
    else:
        dirfiles = zip(rdirs, filelists)
    for dirname, filelist in dirfiles:
        for name in filelist:
            try:
                with open(name) as f:
                    coldata = []
                    cmdline = f.readline()
                    print('<th colspan="7"><span title="{}">{}</span>'.format(html.escape(cmdline, quote=True), html.escape(dirname if args.by_side else name[len(dirname) + 1:])))
                    for line in f:
                        line = line.split('\t')
                        (q, utime, stime, maxrss, exitcode, elapsed, minflt, majflt, inblock, oublock, nvcsw, nivcsv) = line[:12]
                        if len(args.include_q):
                            include = False
                            for r in args.include_q:
                                if re.search(r, q):
                                    include = True
                                    break
                            if not include:
                                continue
                        if len(args.exclude_q):
                            include = True
                            for r in args.exclude_q:
                                if re.search(r, q):
                                    include = False
                                    break
                            if not include:
                                continue
                        if len(line) >= 14:
                            rchar = line[12]
                            wchar = line[13]
                        else:
                            rchar = -1
                            wchar = -1
                        utime = float(utime)
                        stime = float(stime)
                        maxrss = int(maxrss)
                        inblock = int(inblock)
                        oublock = int(oublock)
                        exitcode = int(exitcode)
                        elapsed = int(elapsed)*1e-9
                        rchar = int(rchar)
                        wchar = int(wchar)
                        if len(data):
                            # assert data[0][len(coldata)][0] == q
                            if data[0][len(coldata)][0] != q:
                                pass
                        if wchar >= 0:
                            for spec in ['stderr.txt', 'stdout.txt', 'err.txt', 'stat.yson', 'result.yson', 'plan.yson', 'expr.txt']:
                                try:
                                    wchar -= os.stat('{}/{}-{}'.format(dirname, q, spec)).st_size
                                except Exception as ex:
                                    print(ex, file=sys.stderr)
                                    pass
                        coldata += [[dirname, q, elapsed, utime, stime, maxrss, exitcode, minflt, majflt, inblock, oublock, nvcsw, nivcsv, rchar, wchar]]
                    data += [coldata]
            except Exception:
                print(name, file=sys.stderr)
                raise
    print('<tr><th>Testcase' + '<th>Status<th>Real time, s<th>User time, s<th>RSS, MB<th>Input, MB<th>Output, MB<th>'*len(data) + '</tr>')
    refDatas = [None]*len(data[0])
    refTypes = [None]*len(data[0])
    for i in range(len(data[0])):
        q = data[0][i][1]
        print('<tr><td>{}'.format(html.escape(q)), end='')
        for c in range(len(data)):
            try:
                (dirname, q, elapsed, utime, stime, maxrss, exitcode, minflt, majflt, inblock, oublock, nvcsw, nivcsv, rchar, wchar) = data[c][i]
            except Exception:
                print('<td colspan="7">N/A')
                continue
            if c == 0:
                (refDirname, refQ, refElapsed, refUtime, refStime, refMaxrss, refExitcode, refMinflt, refMajflt, refInblock, refOublock, refNvcsw, refNivcsv, refRchar, refWchar) = data[c][i]
            cls = ''
            spilling = {}
            if args.verbose > 0:
                with open(dirname + '/' + q + '-stderr.txt') as errf:
                    for line in errf:
                        m = re.search(RE_SPILLING, line)
                        if m:
                            cls = 'spilling'
                            spilling_type = m.group(1)
                            spilling[spilling_type] = spilling.get(spilling_type, 0) + 1
            outname = dirname + '/' + q + '-result.yson'
            print('<td class="{}">'.format(cls))
            if os.access(dirname + '/' + q + '.svg', os.F_OK):
                print('<a href="{}">'.format(html.escape(dirname + '/' + q + '.svg')))
            if exitcode < 0:
                print('<span class="signal {}" title="{}">SIG</span>'.format(cls, html.escape(signal.strsignal(-exitcode), quote=True)))
            elif exitcode > 0:
                print('<span class="errcode {}" title="{}">ERR</span>'.format(cls, exitcode))
            else:
                print('<span class="ok {}">OK</span>'.format(cls))
            if spilling and args.verbose > 1:
                print('<span title="{}">+{}</span>'.format(', '.join('{}*{}'.format(t, spilling[t]) for t in spilling), ''.join(map(lambda t: SPILLING_MAP[t], sorted(spilling.keys())))))
            if c == 0:
                print('<td class="tabnum">{:.1f}<td class="tabnum">{:.1f}<td class="tabnum">{:.1f}<td class="tabnum">{:.1f}<td class="tabnum">{:.1f}'.format(
                    elapsed, utime, maxrss/1024, rchar/(1024*1024), wchar/(1024*1024)))
            else:
                print('<td class="tabnum">{}<td class="tabnum">{}<td class="tabnum">{}<td class="tabnum">{}<td class="tabnum">{}'.format(
                    fmtchange(elapsed, refElapsed),
                    fmtchange(utime, refUtime),
                    fmtchange(maxrss/1024, refMaxrss/1024),
                    fmtchange(rchar/(1024*1024), refRchar/(1024*1024)),
                    fmtchange(wchar/(1024*1024), refWchar/(1024*1024))))

            skip = False
            for blacklisted in blacklists:
                if re.fullmatch(blacklisted, q):
                    skip = True
                    break

            if skip:
                print('<td class="skipped"><span title="Query was blacklisted for comparison">SKIP</span>')
                continue

            if exitcode == 0:
                try:
                    valType = None
                    valData = None
                    with open(outname, 'rb') as f:
                        for result in yson.list_fragments(yson.InputStream.from_file(f)):
                            valType = result[0][b'Write'][0][b'Type']
                            valData = result[0][b'Write'][0][b'Data']
                            pass
                    if refDatas[i] is None:
                        refDatas[i] = valData
                        refTypes[i] = valType
                        print('<td>REF')
                    else:
                        assert valType[0] == b'ListType'
                        assert valType[1][0] == b'StructType'
                        stypes = valType[1][1]
                        ncols = len(stypes)
                        refType = refTypes[i]
                        assert refType is not None, "Reference missing"
                        refData = refDatas[i]
                        refstypes = refType[1][1]
                        assert ncols == len(refType[1][1]), 'Column number mismatch {} != {}'.format(ncols, len(refstypes))
                        nrows = len(valData)
                        assert nrows == len(refData), 'Row number mismatch {} != {}'.format(nrows, len(refData))
                        mismatches = []
                        for col in range(ncols):
                            stype = stypes[col][1]
                            # isOptional = False
                            if stype[0] == b'OptionalType':
                                stype = stype[1]
                                # isOptional = True
                            assert stype[0] in {b'DataType', b'PgType'}
                            isDouble = stype[1] in {b'Double', b'Decimal', b'numeric'}
                            for row in range(nrows):
                                val = valData[row][col]
                                ref = refData[row][col]
                                if ref is None:
                                    if val is not None:
                                        mismatches += ['{} != NULL at {}, {}'.format(val, row, col)]
                                    # assert val is None, '{} != NULL at {}, {}'.format(val, row, col)
                                    continue
                                # assert val is not None, 'NULL != {} at {}, {}'.format(ref, row, col)
                                if val is None:
                                    mismatches += ['NULL != {} at {}, {}'.format(ref, row, col)]
                                    continue
                                if type(ref) is list:
                                    ref = ref[0]
                                if type(val) is list:
                                    val = val[0]
                                if isDouble:
                                    val = float(val)
                                    ref = float(ref)
                                    if math.isnan(val):
                                        if not math.isnan(ref):
                                            mismatches += ['{} != {} at {}, {}'.format(val, ref, row, col)]
                                        # assert math.isnan(ref), '{} != {} at {}, {}'.format(val, ref, row, col)
                                        continue
                                    # assert not math.isnan(ref), '{} != {} at {}, {}'.format(val, ref, row, col)
                                    if math.isnan(ref):
                                        mismatches += '{} != {} at {}, {}'.format(val, ref, row, col)
                                        continue
                                    # assert abs(val - ref) <= 1e-5*max(abs(val), abs(ref), 1), 'abs({} - {}) >= eps at {}, {}'.format(val, ref, row, col)
                                    if abs(val - ref) > 1e-5*max(abs(val), abs(ref), 1):
                                        mismatches += ['abs({} - {}) >= eps at {}, {}'.format(val, ref, row, col)]
                                else:
                                    if val != ref:
                                        mismatches += ['{} != {} type {} at {}, {}'.format(val, ref, stypes[col][1][1], row, col)]
                                    # assert val == ref, '{} != {} type {} at {}, {}'.format(val, ref, stypes[col][1][1], row, col)
                        assert len(mismatches) == 0, str(mismatches)
                        print('<td class="ok">MATCH</td>')
                except Exception:
                    print('<td class="errcode">Comparison failed: ', traceback.format_exc())
            else:
                print('<td class="errcode">N/A')

        print('</tr>')

    print('</table>')
    print('</html>')


if __name__ == '__main__':
    main()
