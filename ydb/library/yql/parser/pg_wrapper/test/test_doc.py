import yatest
from yqlrun import YQLRun
import cyson
from multiprocessing.pool import ThreadPool
import time


def run_one(item):
    line, input, output = item
    start_time = time.time()
    try:
        yqlrun_res = YQLRun(prov='yt',
                            use_sql2yql=False,
                            cfg_dir='ydb/library/yql/cfg/udf_test',
                            support_udfs=False).yql_exec(
            program="--!syntax_pg\n" + input,
            run_sql=True,
            check_error=True
        )

        dom = cyson.loads(yqlrun_res.results)
        elapsed_time = time.time() - start_time
        return (line, input, output, dom, None, elapsed_time)
    except Exception as e:
        elapsed_time = time.time() - start_time
        return (line, input, output, None, e, elapsed_time)


def test_doc():
    doc_src = yatest.common.source_path("ydb/library/yql/parser/pg_wrapper/functions.md")
    with open(doc_src) as f:
        doc_data = f.readlines()
    in_code = False
    queue = []
    for line in doc_data:
        line = line.strip()
        if line.startswith("```sql"):
            in_code = True
            continue
        elif in_code and line.startswith("```"):
            in_code = False
            continue
        if not in_code:
            continue
        input, output = [x.strip() for x in line.split("→")]
        if input.startswith("#"):
            continue
        if not input.startswith("SELECT"):
            input = "SELECT " + input
        queue.append((line, input, output))
    with ThreadPool(16) as pool:
        for res in pool.imap_unordered(run_one, queue):
            line, input, output, dom, e, elapsed_time = res
            print("TEST: " + line)
            print("INPUT: ", input)
            print("OUTPUT: ", output)
            print("ELAPSED: ", elapsed_time)
            if e is not None:
                raise e
            value = dom[0][b"Write"][0][b"Data"][0][0].decode("utf-8")
            print("VALUE: ", value)
            assert value == output, f"Expected '{output}' but got '{value}', test: {line}"
