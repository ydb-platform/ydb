import sys
import csv
import json
import ydb.apps.dstool.lib.common as common
from itertools import groupby
from collections import defaultdict, OrderedDict

dialect_map = {
    'csv': 'excel',
    'tsv': 'excel-tab',
}


class TableOutput(object):
    def __init__(self, cols_order, default_sort_order=None, human_readable_fn=None, aggregations=None, aggr_drop=None,
                 col_units=None, col_names=None, right_align=None, default_visible_columns=None, default_values=None):
        self.rows = []
        self.cols_order = cols_order
        self.col_names = col_names if col_names is not None else {}
        self.default_sort_order = default_sort_order or self.cols_order
        self.human_readable_fn = human_readable_fn
        self.aggregations = aggregations if aggregations else {}
        self.aggr_drop = aggr_drop if aggr_drop else set()
        self.right_align = right_align if right_align else set()
        self.default_visible_columns = default_visible_columns if default_visible_columns else set()
        self.default_values = default_values if default_values else {}

        if col_units is not None:
            def make_it_human_readable(row):
                for key, value in col_units.items():
                    if key not in row:
                        continue
                    cell = row[key]
                    if value == 'bytes':
                        cell = f'{common.bytes_string(cell)}'
                    elif value == '%':
                        cell = '{0:.1%}'.format(cell) if cell is not None else 'None'
                    else:
                        assert False
                    row[key] = cell
                return human_readable_fn(row) if human_readable_fn else row
            self.human_readable_fn = make_it_human_readable

    def add_options(self, p):
        g = p.add_argument_group('Output format control')
        if self.aggregations:
            g.add_argument('--aggregate', '-a', type=str, nargs='*', choices=list(self.aggregations), help='Aggregate values in table')
        if self.human_readable_fn:
            g.add_argument('--human-readable', '-H', action='store_true', help='Show human-readable output')
        g.add_argument('--sort-by', type=str, default=','.join(self.default_sort_order), help='Sort order')
        g.add_argument('--reverse', action='store_true', help='Reverse sort')
        g.add_argument('--format', type=str, choices=['pretty', 'json', 'tsv', 'csv'], default='pretty', help='Output format control')
        g.add_argument('--no-header', action='store_true', help='Do not output header line')
        g.add_argument('--columns', nargs='*', help='Columns for show')
        g.add_argument('--all-columns', '-A', action='store_true', help='Show all columns')

    def dump(self, rows, args):
        if rows is None:
            return
        if self.aggregations:
            for aggr_name in args.aggregate or []:
                self.aggregate_table(rows, *self.aggregations[aggr_name])

        all_fields = {key for d in rows for key in d}
        if args.sort_by:
            columns_for_sort = [col for col in args.sort_by.split(',') if col in all_fields]

            def key_func(d):
                def getter(key):
                    return d.get(key, self.default_values.get(key))
                return tuple(map(getter, columns_for_sort))

            rows.sort(key=key_func, reverse=args.reverse)

        if self.human_readable_fn and args.human_readable:
            for d in rows:
                self.human_readable_fn(d)

        visible_columns = self.default_visible_columns
        if args.columns:
            visible_columns = set(args.columns)

        cols_order = [cn for cn in self.cols_order if cn in all_fields] + sorted(all_fields - set(self.cols_order))
        if visible_columns and not args.all_columns:
            cols_order = [column for column in cols_order if column in visible_columns]
        rows[:] = [
            OrderedDict((key, r.get(key)) for key in cols_order)
            for r in rows
        ]

        if args.format == 'pretty':
            maxw = defaultdict(int)
            for d in rows:
                for key, value in d.items():
                    maxw[key] = max(maxw[key], len(str(value)), 0 if args.no_header else len(self.col_names.get(key, key)))

            print('┌─', '─┬─'.join('─' * maxw[key] for key in cols_order), '─┐', sep='')
            if not args.no_header:
                print('│', ' │ '.join('%-*s' % (maxw[key], self.col_names.get(key, key)) for key in cols_order), '│')
            for idx, r in enumerate(rows):
                if not idx and not args.no_header:
                    print('├─', '─┼─'.join('─' * maxw[key] for key in cols_order), '─┤', sep='')
                print('│', ' │ '.join('%*s' % (maxw[key] * (1 if key in self.right_align else -1), value if value is not None else '') for key, value in r.items()), '│')
            print('└─', '─┴─'.join('─' * maxw[key] for key in cols_order), '─┘', sep='')
        elif args.format == 'json':
            json.dump(rows, sys.stdout, indent=2)
        elif args.format in ['tsv', 'csv']:
            writer = csv.writer(sys.stdout, dialect_map[args.format], lineterminator='\n')
            if not args.no_header:
                writer.writerow(cols_order)
            for r in rows:
                writer.writerow(r.values())

    def aggregate_table(self, rows, fields_to_aggr, aggr_fn):
        all_fields = {key for d in rows for key in d}
        prefix_fields = sorted(all_fields - set(fields_to_aggr) - set(self.aggr_drop))

        def prefix_getter(d):
            return tuple(map(d.get, prefix_fields))

        rows.sort(key=prefix_getter)

        rows[:] = [
            aggr_fn(dict(zip(prefix_fields, prefix)), row_group)
            for prefix, row_group in groupby(rows, prefix_getter)
        ]
