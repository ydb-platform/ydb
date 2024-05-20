#!/usr/bin/env python3
import sys
import signal
import traceback
import html
import math
from pathlib import Path

import cyson as yson


def main():

    if len(sys.argv) < 2:
        print('Usage: {} resultdir... >report.htm'.format(sys.argv[0]), file=sys.stderr)
        sys.exit(1)

    rdirs = sys.argv[1:]
    data = []

    print('''
<html><head><style>.signal { color: blue; } .errcode { color: red; } .ok { color: green; } .mismatch { color: yellow; } .tabnum { text-align: right; } </style></head>
''')
    print('<table border="1">')
    print('<tr><th>' + ''.join(map(lambda x: '<th colspan="5">' + html.escape(rdirs[x]), range(len(rdirs)))))
    print('<tr><th>Testcase' + '<th>Status<th>Real time, s<th>User time, s<th>RSS, MB<th>'*len(rdirs) + '</tr>')
    print('<tr><th>')

    for dirname in rdirs:
        for name in sorted(map(str, Path(dirname).glob('**/summary.tsv'))):
            with open(name) as f:
                coldata = []
                cmdline = f.readline()
                print('<th colspan="4"><span title="{}">{}</span><th>'.format(html.escape(cmdline, quote=True), html.escape(name)))
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
    for i in range(len(data[0])):
        q = data[0][i][1]
        print('<tr><td>{}'.format(html.escape(q)), end='')
        for c in range(len(data)):
            (dirname, q, elapsed, utime, stime, maxrss, exitcode) = data[c][i]
            outname = dirname + '/' + q + '-result.yson'
            if exitcode < 0:
                print('<td><span class="signal" title="{}">SIG</span>'.format(html.escape(signal.strsignal(-exitcode), quote=True)))
            elif exitcode > 0:
                print('<td><span class="errcode" title="{}">ERR</span>'.format(exitcode))
            else:
                print('<td><span class="ok">OK</span>')
            print('<td class="tabnum">{:.1f}<td class="tabnum">{:.1f}<td class="tabnum">{:.1f}'.format(elapsed, utime, maxrss/1024))
            if exitcode == 0:
                try:
                    valType = None
                    valData = None
                    with open(outname, 'rb') as f:
                        for result in yson.list_fragments(yson.InputStream.from_file(f)):
                            valType = result[0][b'Write'][0][b'Type']
                            valData = result[0][b'Write'][0][b'Data']
                            pass
                    if c == 0:
                        data[c][i] += [valType, valData]
                        print('<td>')
                    else:
                        assert valType[0] == b'ListType'
                        assert valType[1][0] == b'StructType'
                        stypes = valType[1][1]
                        ncols = len(stypes)
                        refType = data[0][i][-2]
                        refData = data[0][i][-1]
                        refstypes = refType[1][1]
                        assert ncols == len(refType[1][1]), 'Column number mismatch {} != {}'.format(ncols, len(refstypes))
                        nrows = len(valData)
                        assert nrows == len(refData), 'Row number mismatch {} != {}'.format(nrows, len(refData))
                        for col in range(ncols):
                            stype = stypes[col][1]
                            isOptional = False
                            if stype[0] == b'OptionalType':
                                stype = stype[1]
                                isOptional = True
                            assert stype[0] == b'DataType'
                            isDouble = stype[1] == b'Double'
                            for row in range(nrows):
                                val = valData[row][col]
                                ref = refData[row][col]
                                if isOptional:
                                    if ref is None:
                                        assert val is None, 'NULL != NOT NULL at {}, {}'.format(row, col)
                                        continue
                                    assert val is not None, 'NOT NULL != NULL at {}, {}'.format(row, col)
                                    ref = ref[0]
                                    val = val[0]
                                if isDouble:
                                    val = float(val)
                                    ref = float(ref)
                                    if math.isnan(val):
                                        assert math.isnan(ref), '{} != {} at {}, {}'.format(val, ref, row, col)
                                        continue
                                    assert not math.isnan(ref), '{} != {} at {}, {}'.format(val, ref, row, col)
                                    assert abs(val - ref) <= 1e-5*max(abs(val), abs(ref), 1), 'abs({} - {}) >= eps at {}, {}'.format(val, ref, row, col)
                                else:
                                    assert val == ref, '{} != {} type {} at {}, {}'.format(val, ref, stypes[col][1][1], row, col)
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
