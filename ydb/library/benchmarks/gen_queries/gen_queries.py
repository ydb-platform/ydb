#!/usr/bin/env python3

import os
import argparse
from dataclasses import dataclass
from ydb.library.benchmarks.template import Builder


@dataclass
class Profile:
    syntax: str
    profile: str
    pragmas: str
    tables: str
    bindings: bool


def main():
    profiles = [
        Profile("yql", "dqrun", "pragmas_scalar.yql", "tables_bindings.jinja", True),
        Profile("yql", "dqrun_block", "pragmas_block.yql", "tables_bindings.jinja", True),
        Profile("pg", "dqrun", "pragmas_scalar_pg.yql", "tables_bindings.jinja", True),
        Profile("pg", "postgres", None, "tables_postgres.jinja", False),
        Profile("yql", "ytsaurus", "pragmas_ytsaurus.yql", "tables_postgres.jinja", False),
        Profile("pg", "ytsaurus", "pragmas_ytsaurus_pg.yql", "tables_ytsaurus_pg.jinja", False),
    ]
    parser = argparse.ArgumentParser()
    parser.add_argument('--syntax', default='yql', help='syntax "pg" or "yql"')
    parser.add_argument('--profile', default='dqrun', help='profile "dqrun" "dqrun_block" or "postgres"')
    parser.add_argument('--variant', default="h", help='variant "h" or "ds"')
    parser.add_argument('--output', default='q', help='output directory')
    parser.add_argument('--dataset-size', default='1', help='dataset size (1, 10, 100, ...)')
    parser.add_argument('--pragma', default=[], action='append', help='custom pragmas')
    parser.add_argument('--table-path-prefix', default=None, help='table path prefix')
    parser.add_argument('--cluster-name', default='hahn', help='YtSaurus cluster name')
    parser.add_argument('--decimal', default=False)
    args = parser.parse_args()
    profile = None
    for p in profiles:
        if p.syntax == args.syntax and p.profile == args.profile:
            profile = p
            break
    if profile is None:
        print("Cannot find syntax/profile pair")
        print("Awailable variants:")
        for p in profiles:
            print(f"  {p.syntax}/{p.profile}")
        return

    table_path_prefix = args.table_path_prefix
    if table_path_prefix is None:
        pg = '/'
        if args.syntax == 'pg':
            pg = '/pg/'
        table_path_prefix = f'home/yql_perf/tpc{args.variant}{args.dataset_size}s{pg}'

    pragma_keyword = "pragma"
    custom_pragmas = ""
    if args.syntax == "pg":
        pragma_keyword = "set"
    for pragma in args.pragma:
        k, v = pragma.split('=')
        custom_pragmas = custom_pragmas + f'{pragma_keyword} {k}="{v}";\n'

    path = f"{args.output}/{args.variant}"
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    b = Builder()
    b.add_link("bindings.json", f"bindings_{args.variant}_{args.syntax}.json")
    b.add_vars({"data": f"{args.variant}/{args.dataset_size}", "cluster_name": args.cluster_name, "table_path_prefix": table_path_prefix})
    if args.decimal:
        if args.variant == "h":
            b.add_vars({"numeric": '"Decimal", "12", "2"'})
        else:
            b.add_vars({"numeric": '"Decimal", "7", "2"'})
    else:
        b.add_vars({"numeric": '"Double"'})
    b.add("custom_pragmas", custom_pragmas)
    if p.pragmas:
        b.add_link("pragmas.sql", p.pragmas)
    else:
        b.add("pragmas.sql", "")
    if args.syntax == "yql":
        if args.decimal:
            b.add_link("consts.jinja", "consts_decimal.yql")
        else:
            b.add_link("consts.jinja", "consts.yql")
    b.add_link("tables.jinja", p.tables)
    queries = None
    if args.variant == "h":
        queries = range(1, 23)
    else:
        queries = range(1, 100)

    if p.bindings:
        with open(f"{path}/bindings.json", "w") as f:
            print("Generating bindings")
            json = b.build("bindings.json", True)
            f.write(json)

    for q in queries:
        with open(f"{path}/q{q}.sql", "w") as f:
            print(f"Generating {args.variant}/{p.syntax}/q{q}.sql")
            sql = b.build(f"{args.variant}/{p.syntax}/q{q}.sql", True)
            f.write(sql)


if __name__ == "__main__":
    main()
