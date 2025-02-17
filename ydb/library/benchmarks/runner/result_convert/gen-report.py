#!/usr/bin/env python3
import sys
import signal
import html
from pathlib import Path


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

    return ret + '<span{}>{:+3}%</span>'.format(cls, change)


def main():
    if len(sys.argv) < 2:
        print('Usage: {} resultdir... >report.htm'.format(sys.argv[0]), file=sys.stderr)
        sys.exit(1)
    rdirs = sys.argv[1:]
    print('''
<html><head><style>
.signal { color: blue; }
.errcode { color: red; }
.ok { color: green; }
.mismatch { color: yellow; }
.tabnum { text-align: right; }
.good { color: green; }
.bad { color: red; }
</style></head>
''')
    print('<table border="1">')
    data = []
    filelists = [sorted(map(str, Path(dirname).glob('**/summary.tsv'))) for dirname in rdirs]
    print('<tr><th>' + ''.join('<th colspan="{}">'.format(4*len(filelist)) + html.escape(dirname) for dirname, filelist in zip(rdirs, filelists) if len(filelist)))
    print('<tr><th>')
    for dirname, filelist in zip(rdirs, filelists):
        for name in filelist:
            name = str(name)
            with open(name) as f:
                cmdline = f.readline()
                print('<th colspan="4"><span title="{}">{}</span>'.format(html.escape(cmdline, quote=True), html.escape(name[len(dirname)+1:])))
                coldata = []
                for line in f:
                    line = line.strip().split('\t')
                    (q, utime, stime, maxrss, exitcode, elapsed) = line[:6]
                    utime = float(utime)
                    stime = float(stime)
                    maxrss = int(maxrss)
                    elapsed = int(elapsed)*1e-9
                    exitcode = int(exitcode)
                    coldata += [(q, elapsed, utime, stime, maxrss, exitcode)]
                data += [coldata]

    print('<tr><th>Testcase' + '<th>Status<th>Real time, s<th>User time, s<th>RSS, MB'*len(data) + '</tr>')

    for i in range(len(data[0])):
        q = data[0][i][0]
        print('<tr><td>', q, end='')
        for c in range(len(data)):
            (q, elapsed, utime, stime, maxrss, exitcode) = data[c][i]
            if c == 0:
                (refQ, refElapsed, refUtime, refStime, refMaxrss, refExitcode) = data[c][i]
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
        print('</tr>')

    print("</table>")
    print("</html>")


if __name__ == "__main__":
    main()
