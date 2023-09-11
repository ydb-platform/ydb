import yatest
from yqlrun import YQLRun
import cyson


def test_doc():
    doc_src = yatest.common.source_path("ydb/library/yql/parser/pg_wrapper/functions.md")
    with open(doc_src) as f:
        doc_data = f.readlines()
    in_code = False
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
        print("TEST: " + line)
        input, output = [x.strip() for x in line.split("→")]
        if not input.startswith("SELECT"):
            input = "SELECT " + input
        print("INPUT: ", input)
        print("OUTPUT: ", output)
        yqlrun_res = YQLRun(prov='yt', use_sql2yql=False, cfg_dir='ydb/library/yql/cfg/udf_test').yql_exec(
            program="--!syntax_pg\n" + input,
            run_sql=True,
            check_error=True
        )

        dom = cyson.loads(yqlrun_res.results)
        value = dom[0][b"Write"][0][b"Data"][0][0].decode("utf-8")
        print("VALUE: ", value)
        assert value == output, f"Expected '{output}' but got '{value}', test: {line}"
