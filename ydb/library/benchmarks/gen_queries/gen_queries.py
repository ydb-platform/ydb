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
    ]
    parser = argparse.ArgumentParser()
    parser.add_argument('--syntax', default='yql', help='syntax "pg" or "yql"')
    parser.add_argument('--profile', default='dqrun', help='profile "dqrun" "dqrun_block" or "postgres"')
    parser.add_argument('--variant', default="h", help='variant "h" or "ds"')
    parser.add_argument('--output', default='q', help='output directory')
    parser.add_argument('--dataset-size', default='1', help='dataset size (1, 10, 100, ...)')
    parser.add_argument('--pragma', default=[], action='append', help='custom pragmas')
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
    b.add_vars({"data": f"{args.variant}/{args.dataset_size}"})
    b.add("custom_pragmas", custom_pragmas)
    if p.pragmas:
        b.add_link("pragmas.sql", p.pragmas)
    else:
        b.add("pragmas.sql", "")
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
