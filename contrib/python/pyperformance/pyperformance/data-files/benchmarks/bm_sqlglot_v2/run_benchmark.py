import pyperf

from sqlglot import parse_one, transpile
from sqlglot.optimizer import optimize, normalize


SQL = """
select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
from
        (
                select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        extract(year from l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                from
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
group by
        supp_nation,
        cust_nation,
        l_year
order by
        supp_nation,
        cust_nation,
        l_year;
"""

TPCH_SCHEMA = {
    "lineitem": {
        "l_orderkey": "uint64",
        "l_partkey": "uint64",
        "l_suppkey": "uint64",
        "l_linenumber": "uint64",
        "l_quantity": "float64",
        "l_extendedprice": "float64",
        "l_discount": "float64",
        "l_tax": "float64",
        "l_returnflag": "string",
        "l_linestatus": "string",
        "l_shipdate": "date32",
        "l_commitdate": "date32",
        "l_receiptdate": "date32",
        "l_shipinstruct": "string",
        "l_shipmode": "string",
        "l_comment": "string",
    },
    "orders": {
        "o_orderkey": "uint64",
        "o_custkey": "uint64",
        "o_orderstatus": "string",
        "o_totalprice": "float64",
        "o_orderdate": "date32",
        "o_orderpriority": "string",
        "o_clerk": "string",
        "o_shippriority": "int32",
        "o_comment": "string",
    },
    "customer": {
        "c_custkey": "uint64",
        "c_name": "string",
        "c_address": "string",
        "c_nationkey": "uint64",
        "c_phone": "string",
        "c_acctbal": "float64",
        "c_mktsegment": "string",
        "c_comment": "string",
    },
    "part": {
        "p_partkey": "uint64",
        "p_name": "string",
        "p_mfgr": "string",
        "p_brand": "string",
        "p_type": "string",
        "p_size": "int32",
        "p_container": "string",
        "p_retailprice": "float64",
        "p_comment": "string",
    },
    "supplier": {
        "s_suppkey": "uint64",
        "s_name": "string",
        "s_address": "string",
        "s_nationkey": "uint64",
        "s_phone": "string",
        "s_acctbal": "float64",
        "s_comment": "string",
    },
    "partsupp": {
        "ps_partkey": "uint64",
        "ps_suppkey": "uint64",
        "ps_availqty": "int32",
        "ps_supplycost": "float64",
        "ps_comment": "string",
    },
    "nation": {
        "n_nationkey": "uint64",
        "n_name": "string",
        "n_regionkey": "uint64",
        "n_comment": "string",
    },
    "region": {
        "r_regionkey": "uint64",
        "r_name": "string",
        "r_comment": "string",
    },
}


def bench_parse(loops):
    elapsed = 0
    for _ in range(loops):
        t0 = pyperf.perf_counter()
        parse_one(SQL)
        elapsed += pyperf.perf_counter() - t0
    return elapsed


def bench_transpile(loops):
    elapsed = 0
    for _ in range(loops):
        t0 = pyperf.perf_counter()
        transpile(SQL, write="spark")
        elapsed += pyperf.perf_counter() - t0
    return elapsed


def bench_optimize(loops):
    elapsed = 0
    for _ in range(loops):
        t0 = pyperf.perf_counter()
        optimize(parse_one(SQL), TPCH_SCHEMA)
        elapsed += pyperf.perf_counter() - t0
    return elapsed


def bench_normalize(loops):
    elapsed = 0
    for _ in range(loops):
        conjunction = parse_one("(A AND B) OR (C AND D) OR (E AND F) OR (G AND H)")
        t0 = pyperf.perf_counter()
        normalize.normalize(conjunction)
        elapsed += pyperf.perf_counter() - t0
    return elapsed


BENCHMARKS = {
    "parse": bench_parse,
    "transpile": bench_transpile,
    "optimize": bench_optimize,
    "normalize": bench_normalize
}


def add_cmdline_args(cmd, args):
    cmd.append(args.benchmark)


def add_parser_args(parser):
    parser.add_argument(
        "benchmark",
        choices=BENCHMARKS,
        help="Which benchmark to run."
    )


if __name__ == "__main__":
    runner = pyperf.Runner(add_cmdline_args=add_cmdline_args)
    runner.metadata['description'] = "SQLGlot V2 benchmark"
    add_parser_args(runner.argparser)
    args = runner.parse_args()
    benchmark = args.benchmark

    runner.bench_time_func(f"sqlglot_v2_{benchmark}", BENCHMARKS[benchmark])
