"""
Render a template using Genshi module.
"""

import pyperf

from genshi.template import MarkupTemplate, NewTextTemplate


BIGTABLE_XML = """\
<table xmlns:py="http://genshi.edgewall.org/">
<tr py:for="row in table">
<td py:for="c in row.values()" py:content="c"/>
</tr>
</table>
"""

BIGTABLE_TEXT = """\
<table>
{% for row in table %}<tr>
{% for c in row.values() %}<td>$c</td>{% end %}
</tr>{% end %}
</table>
"""


def bench_genshi(loops, tmpl_cls, tmpl_str):
    tmpl = tmpl_cls(tmpl_str)
    table = [dict(a=1, b=2, c=3, d=4, e=5, f=6, g=7, h=8, i=9, j=10)
             for _ in range(1000)]
    range_it = range(loops)
    t0 = pyperf.perf_counter()

    for _ in range_it:
        stream = tmpl.generate(table=table)
        stream.render()

    return pyperf.perf_counter() - t0


def add_cmdline_args(cmd, args):
    if args.benchmark:
        cmd.append(args.benchmark)


BENCHMARKS = {
    'xml': (MarkupTemplate, BIGTABLE_XML),
    'text': (NewTextTemplate, BIGTABLE_TEXT),
}


if __name__ == "__main__":
    runner = pyperf.Runner(add_cmdline_args=add_cmdline_args)
    runner.metadata['description'] = "Render a template using Genshi module"
    runner.argparser.add_argument("benchmark", nargs='?',
                                  choices=sorted(BENCHMARKS))

    args = runner.parse_args()
    if args.benchmark:
        benchmarks = (args.benchmark,)
    else:
        benchmarks = sorted(BENCHMARKS)

    for bench in benchmarks:
        name = 'genshi_%s' % bench
        tmpl_cls, tmpl_str = BENCHMARKS[bench]
        runner.bench_time_func(name, bench_genshi, tmpl_cls, tmpl_str)
