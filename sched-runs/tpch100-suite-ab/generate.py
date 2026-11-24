#!/usr/bin/env python3
"""Generate all eight SF100 TPC-H tables with complete YDB-compatible columns."""
import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import json
from pathlib import Path
import shutil
import subprocess
import time

DUCK = Path('/home/mfilitov/tpch1-kqprun/bin/duckdb')
SCALE = 100
CHILDREN = 64
GIB = 1024 ** 3
DISK_FLOOR = 24 * GIB
LARGE = {'customer': 'c_acctbal::DOUBLE AS c_acctbal, c_address, c_comment, c_custkey::BIGINT AS c_custkey, c_mktsegment, c_name, c_nationkey::INTEGER AS c_nationkey, c_phone', 'lineitem': 'l_comment, l_commitdate, l_discount::DOUBLE AS l_discount, l_extendedprice::DOUBLE AS l_extendedprice, l_linenumber::INTEGER AS l_linenumber, l_linestatus, l_orderkey::BIGINT AS l_orderkey, l_partkey::BIGINT AS l_partkey, l_quantity::DOUBLE AS l_quantity, l_receiptdate, l_returnflag, l_shipdate, l_shipinstruct, l_shipmode, l_suppkey::BIGINT AS l_suppkey, l_tax::DOUBLE AS l_tax', 'orders': 'o_clerk, o_comment, o_custkey::BIGINT AS o_custkey, o_orderdate, o_orderkey::BIGINT AS o_orderkey, o_orderpriority, o_orderstatus, o_shippriority::INTEGER AS o_shippriority, o_totalprice::DOUBLE AS o_totalprice', 'part': 'p_brand, p_comment, p_container, p_mfgr, p_name, p_partkey::BIGINT AS p_partkey, p_retailprice::DOUBLE AS p_retailprice, p_size::INTEGER AS p_size, p_type', 'partsupp': 'ps_availqty::INTEGER AS ps_availqty, ps_comment, ps_partkey::BIGINT AS ps_partkey, ps_suppkey::BIGINT AS ps_suppkey, ps_supplycost::DOUBLE AS ps_supplycost', 'supplier': 's_acctbal::DOUBLE AS s_acctbal, s_address, s_comment, s_name, s_nationkey::INTEGER AS s_nationkey, s_phone, s_suppkey::BIGINT AS s_suppkey'}
SMALL = {'nation': 'n_comment, n_name, n_nationkey::INTEGER AS n_nationkey, n_regionkey::INTEGER AS n_regionkey', 'region': 'r_comment, r_name, r_regionkey::INTEGER AS r_regionkey'}


def literal(text):
    return "'" + str(text).replace("'", "''") + "'"


def write(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value,indent=2)+'\n')
    tmp.replace(path)


def step(root, n):
    done = root/'parts'/f'{n:04}.json'
    if done.exists():
        existing = json.loads(done.read_text())
        assert existing['scale'] == SCALE and existing['children'] == CHILDREN
        for table, stat in existing['tables'].items():
            assert (root/'data'/stat['file']).stat().st_size == stat['bytes'], (n, table)
        return existing
    assert shutil.disk_usage(root).free > DISK_FLOOR,'Disk reserve reached'
    start = time.monotonic()
    tables = LARGE
    catalog = {}
    sql = ['LOAD tpch;', 'SET threads = 1;', "SET memory_limit = '6GB';", f'CALL dbgen(sf={SCALE}, children={CHILDREN}, step={n});']
    for table, columns in tables.items():
        name = f'{table}/part-{n:04}.parquet'
        path = root/'data'/name
        temp = path.with_suffix('.parquet.partial')
        catalog[table] = name, path, temp
        sql.append(f'COPY (SELECT {columns} FROM {table}) TO {literal(temp)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 262144, PARQUET_VERSION V1);')
    counts_path=root/'parts'/f'{n:04}.rows'
    selects = [f"SELECT {literal(t)} AS tablename, count(*) AS rows FROM {t}" for t in tables]
    sql.append('COPY ('+' UNION ALL '.join(selects)+f') TO {literal(counts_path)} (FORMAT JSON, ARRAY true);')
    with (root/'parts'/f'{n:04}.log').open('w') as out:
        subprocess.run([str(DUCK), '-batch', '-bail', ':memory:'],input='\n'.join(sql)+'\n',
                       text=True, stdout=out, stderr=subprocess.STDOUT,check=True)
    count = {row['tablename']:row['rows'] for row in json.loads(counts_path.read_text())}
    result = dict(part=n, scale=SCALE, children=CHILDREN, elapsed_seconds=time.monotonic()-start,tables={})
    for table,(name,path,temp) in catalog.items():
        assert count[table]>0 and temp.stat().st_size>0, (n,table,count)
        temp.replace(path)
        result['tables'][table] = dict(rows=count[table],bytes=path.stat().st_size,file=name)
    write(done,result)
    return result


def dimensions(root):
    # Nation and Region are scale-independent. DuckDB shards these tables as
    # well as the large ones, so generate the tiny dimensions unpartitioned.
    out=root/'dimensions.json'
    if out.is_file():
        return json.loads(out.read_text())
    script=['LOAD tpch;', 'SET threads=1;', 'CALL dbgen(sf=0.001);']
    for table, columns in SMALL.items():
        path=root/'data'/f'{table}.parquet'
        script.append(f'COPY (SELECT {columns} FROM {table}) TO {literal(path)} (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE true);')
    counts=root/'parts'/'dimensions.rows'
    script.append(f"COPY (SELECT (SELECT count(*) FROM nation) AS nation, (SELECT count(*) FROM region) AS region) TO {literal(counts)} (FORMAT JSON, ARRAY true);")
    with (root/'dimensions.log').open('w') as log:
        subprocess.run([str(DUCK),'-batch','-bail',':memory:'],input='\n'.join(script)+'\n',
            text=True,stdout=log,stderr=subprocess.STDOUT,check=True)
    counts=json.loads(counts.read_text())[0]
    assert counts=={'nation':25,'region':5},counts
    result={t:dict(rows=counts[t],bytes=(root/'data'/f'{t}.parquet').stat().st_size,files=1) for t in SMALL}
    write(out,result)
    return result


def status(root, phase, done):
    combined=dict(phase=phase, partitions=len(done),expected_partitions=CHILDREN,
                  total_bytes=0,free_bytes=shutil.disk_usage(root).free,tables=dimensions(root))
    combined['total_bytes']=sum(t['bytes'] for t in combined['tables'].values())
    for part in done:
        for tab, row in part['tables'].items():
            totals=combined['tables'].setdefault(tab,dict(rows=0,bytes=0,files=0))
            totals['rows']+=row['rows']
            totals['bytes']+=row['bytes']
            totals['files']+=1
            combined['total_bytes']+=row['bytes']
    write(root/'dataset-status.json',combined)
    return combined


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('root',type=Path)
    ap.add_argument('--workers',type=int,default=8)
    ap.add_argument('--pilot',action='store_true')
    args=ap.parse_args()
    root=args.root.resolve()
    (root/'parts').mkdir(parents=True,exist_ok=True)
    for t in LARGE: (root/'data'/t).mkdir(parents=True,exist_ok=True)
    status(root,'pilot',[])
    first=[step(root,i) for i in range(2)]
    pilot=status(root,'pilot_complete',first)
    est_bytes=sum(pilot['tables'][t]['bytes'] for t in LARGE)*CHILDREN//len(first)
    print(f'Pilot: {pilot["total_bytes"]/GIB:.3f} GiB, estimated projected data {est_bytes/GIB:.1f} GiB, free {pilot["free_bytes"]/GIB:.1f} GiB',flush=True)
    write(root/'pilot-estimate.json',dict(estimated_dataset_bytes=est_bytes,disk_floor=DISK_FLOOR,parts=first))
    if args.pilot:return
    remaining_projected=est_bytes-pilot['total_bytes']-sum(
        f.stat().st_size for f in (root/'data').glob('*/*.parquet') if int(f.stem.removeprefix('part-'))>=2)
    assert remaining_projected*1.10+DISK_FLOOR < shutil.disk_usage(root).free,'Pilot does not leave disk space for the benchmark'
    done=list(first)
    it=iter(range(2,CHILDREN))
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        running={pool.submit(step,root,n):n for n in (next(it) for _ in range(args.workers))}
        while running:
            finished,_=wait(running,return_when=FIRST_COMPLETED)
            for job in sorted(finished,key=running.get):
                running.pop(job)
                done.append(job.result())
                metrics=status(root,'generating',done)
                if len(done)%8==0 or len(done)==CHILDREN:
                    print(f'{len(done)}/{CHILDREN}: {metrics["total_bytes"]/GIB:.2f} GiB',flush=True)
                n=next(it,None)
                if n is not None:
                    running[pool.submit(step,root,n)]=n
    totals=status(root,'validating',done)
    expected={'orders':150_000_000,'customer':15_000_000,'supplier':1_000_000,'part':20_000_000,'partsupp':80_000_000,'nation':25,'region':5}
    for table, rows in expected.items():
        assert totals['tables'][table]['rows']==rows,(table,totals['tables'][table]['rows'],rows)
    assert 599_000_000 < totals['tables']['lineitem']['rows'] < 601_000_000
    assert len(done)==CHILDREN and all(totals['tables'][t]['files']==CHILDREN for t in LARGE)
    status(root,'complete',done)


if __name__=='__main__':
    main()
