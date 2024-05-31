#!/usr/bin/env python3
import signal
import traceback
import html
import math
import argparse
import re
import sys
from pathlib import Path

import cyson as yson


def fmtchange(cur, ref, plus='bad', minus='good', threshold=10):
    ret = '{:.1f}'.format(cur)
    if ref == 0:
        return ret + ' N/A%'
    change = int((cur/ref - 1)*100)
    cls = ''
    if change > threshold:
        cls = ' class="{}"'.format(plus)
    elif change < -threshold:
        cls = ' class="{}"'.format(minus)

    return ret + '<code{}> {:+3}%</code>'.format(cls, change)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--blacklist-file', default=[], action='append', help='File with query name regexp that will be skipped for comparison')
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
code { white-space: pre; }
</style></head>
''')
    print('<table border="1">')

    filelists = [sorted(map(str, Path(dirname).glob('**/summary.tsv'))) for dirname in rdirs]
    print('<tr><th>' + ''.join('<th colspan="{}">'.format(5*len(filelist)) + html.escape(dirname) for dirname, filelist in zip(rdirs, filelists)))
    print('<tr><th>')
    for dirname, filelist in zip(rdirs, filelists):
        for name in filelist:
            try:
                with open(name) as f:
                    coldata = []
                    cmdline = f.readline()
                    print('<th colspan="5"><span title="{}">{}</span>'.format(html.escape(cmdline, quote=True), html.escape(name[len(dirname) + 1:])))
                    for line in f:
                        line = line.split('\t')
                        (q, utime, stime, maxrss, exitcode, elapsed) = line[:6]
                        utime = float(utime)
                        stime = float(stime)
                        maxrss = int(maxrss)
                        exitcode = int(exitcode)
                        elapsed = int(elapsed)*1e-9
                        if len(data):
                            # assert data[0][len(coldata)][0] == q
                            if data[0][len(coldata)][0] != q:
                                pass
                        coldata += [[dirname, q, elapsed, utime, stime, maxrss, exitcode]]
                    data += [coldata]
            except Exception:
                print(name, file=sys.stderr)
                raise
    print('<tr><th>Testcase' + '<th>Status<th>Real time, s<th>User time, s<th>RSS, MB<th>'*len(data) + '</tr>')
    refDatas = [None]*len(data[0])
    refTypes = [None]*len(data[0])
    for i in range(len(data[0])):
        q = data[0][i][1]
        print('<tr><td>{}'.format(html.escape(q)), end='')
        for c in range(len(data)):
            try:
                (dirname, q, elapsed, utime, stime, maxrss, exitcode) = data[c][i]
            except Exception:
                print('<td colspan="5">N/A')
                continue
            if c == 0:
                (refDirname, refQ, refElapsed, refUtime, refStime, refMaxrss, refExitcode) = data[c][i]
            outname = dirname + '/' + q + '-result.yson'
            if exitcode < 0:
                print('<td><span class="signal" title="{}">SIG</span>'.format(html.escape(signal.strsignal(-exitcode), quote=True)))
            elif exitcode > 0:
                print('<td><span class="errcode" title="{}">ERR</span>'.format(exitcode))
            else:
                print('<td><span class="ok">OK</span>')
            if c == 0:
                print('<td class="tabnum">{:.1f}<td class="tabnum">{:.1f}<td class="tabnum">{:.1f}'.format(elapsed, utime, maxrss/1024))
            else:
                print('<td class="tabnum">{}<td class="tabnum">{}<td class="tabnum">{}'.format(fmtchange(elapsed, refElapsed), fmtchange(utime, refUtime), fmtchange(maxrss/1024, refMaxrss/1024)))

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
                            isOptional = False
                            if stype[0] == b'OptionalType':
                                stype = stype[1]
                                isOptional = True
                            assert stype[0] == b'DataType'
                            isDouble = stype[1] == b'Double' or stype[1] == b'Decimal'
                            for row in range(nrows):
                                val = valData[row][col]
                                ref = refData[row][col]
                                if isOptional:
                                    if ref is None:
                                        if val is not None:
                                            mismatches += ['{} != NULL at {}, {}'.format(val, row, col)]
                                        # assert val is None, '{} != NULL at {}, {}'.format(val, row, col)
                                        continue
                                    # assert val is not None, 'NULL != {} at {}, {}'.format(ref, row, col)
                                    if val is None:
                                        mismatches += ['NULL != {} at {}, {}'.format(ref, row, col)]
                                        continue
                                    ref = ref[0]
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
