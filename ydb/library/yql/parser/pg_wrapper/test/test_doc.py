import yatest
from yqlrun import YQLRun
import cyson
from multiprocessing.pool import ThreadPool
import time
import base64
import binascii
from yql_utils import get_param


def run_one(item):
    line, input, output, should_fail = item
    start_time = time.time()
    try:
        support_udfs = False
        if "LIKE" in input:
            support_udfs = True
        yqlrun_res = YQLRun(prov='yt',
                            use_sql2yql=False,
                            cfg_dir='ydb/library/yql/cfg/udf_test',
                            support_udfs=support_udfs).yql_exec(
            program="--!syntax_pg\n" + input,
            run_sql=True,
            check_error=True
        )

        dom = cyson.loads(yqlrun_res.results)
        elapsed_time = time.time() - start_time
        return (line, input, output, dom, None, elapsed_time, should_fail)
    except Exception as e:
        elapsed_time = time.time() - start_time
        return (line, input, output, None, e, elapsed_time, should_fail)


def convert_cell(cell, output):
    if cell is None:
        value = 'NULL'
    elif isinstance(cell, bytes):
        if output.startswith("\\x"):
            value = "\\x" + binascii.hexlify(cell).decode("utf-8")
        else:
            value = cell.decode("utf-8")
    else:
        value = "\\x" + binascii.hexlify(base64.b64decode(cell[0])).decode("utf-8")
    if output.startswith("~"):
        value = ''
        output = ''
    return (value, output)


def convert_value(data, output):
    if len(data) == 1:
        return convert_cell(data[0], output)
    lst = [convert_cell(x[0], x[1]) for x in zip(data, output.split(","))]
    return (",".join(x[0] for x in lst), ",".join(x[1] for x in lst))


def test_doc():
    skip_before = get_param("skip_before")
    stop_at = get_param("stop_at")
    if skip_before is not None:
        print("WILL SKIP TESTS BEFORE: ", skip_before)
    if stop_at is not None:
        print("WILL STOP AT: ", stop_at)
    doc_src = yatest.common.source_path("ydb/docs/ru/core/postgresql/_includes/functions.md")
    with open(doc_src) as f:
        doc_data = f.readlines()
    in_code = False
    queue = []
    total = 0
    skipped = 0
    skipped_exception = 0
    skipped_mismatch_res = 0
    skipped_same_res = 0
    set_of = None
    original_line = None
    original_input = None
    multiline = None
    skip_in_progress = skip_before is not None
    should_fail = None
    for raw_line in doc_data:
        line = raw_line.strip()
        if stop_at is not None and line.startswith("## " + stop_at):
            break
        if skip_in_progress:
            if line.startswith("## " + skip_before):
                skip_in_progress = False
            continue
        if set_of is not None:
            if line.startswith("]"):
                queue.append((original_line, original_input, set_of, should_fail))
                set_of = None
                original_line = None
                original_input = None
            else:
                set_of.append(line)
            continue
        if multiline is not None:
            if line.endswith('"""'):
                multiline.append(line[0:line.index('"""')])
                queue.append((original_line, original_input, "".join(multiline), should_fail))
                multiline = None
                original_line = None
                original_input = None
            else:
                multiline.append(raw_line)
            continue
        if line.startswith("```sql"):
            in_code = True
            continue
        elif in_code and line.startswith("```"):
            in_code = False
            continue
        if not in_code:
            continue
        if "→" not in line:
            continue
        total += 1
        line = line.replace("~→ ", "→ ~")
        input, output = [x.strip() for x in line.split("→")]
        should_fail = False
        if input.startswith("#") and not input.startswith("# "):
            should_fail = True
            skipped += 1
            input = input[1:]
        if not input.startswith("SELECT"):
            input = "SELECT " + input
        if "/*" in output:
            output = output[:output.index("/*")].strip()
        if output.startswith('"""'):
            multiline = [output[output.index('"""') + 3:] + "\n"]
            original_line = line
            original_input = input
            continue
        elif output.startswith("'") and output.endswith("'"):
            output = output[1:-1]
        elif output.endswith("["):
            set_of = []
            original_line = line
            original_input = input
            continue
        queue.append((line, input, output, should_fail))
    with ThreadPool(16) as pool:
        for res in pool.map(run_one, queue):
            line, input, output, dom, e, elapsed_time, should_fail = res
            print("TEST: " + line)
            print("INPUT: ", input)
            print("OUTPUT: ", output)
            print("ELAPSED: ", elapsed_time)
            if e is not None:
                if not should_fail:
                    raise e
                else:
                    skipped_exception += 1
                    print("SKIPPED, EXCEPTION")
            else:
                data = dom[0][b"Write"][0][b"Data"]
                print("DATA: ", data)
                if isinstance(output, list):
                    pairs = [convert_value(x[0], x[1]) for x in zip(data, output)]
                    value = [x[0] for x in pairs]
                    output = [x[1] for x in pairs]
                else:
                    value, output = convert_value(data[0], output)
                print("VALUE: ", value)
                try:
                    assert value == output, f"Expected '{output}' but got '{value}', test: {line}"
                except Exception as err:
                    if should_fail:
                        e = err
                        skipped_mismatch_res += 1
                        print("SKIPPED, MISMATCH RESULT")
                    else:
                        raise
            if should_fail and e is None:
                print("SKIPPED, SAME RESULT")
                skipped_same_res += 1
    print("TOTAL TESTS: ", total)
    print("SKIPPED TESTS: ", skipped)
    print("SKIPPED TESTS WITH EXCEPTION: ", skipped_exception)
    print("SKIPPED TESTS WITH MISMATCH RESULT: ", skipped_mismatch_res)
    print("SKIPPED TESTS WITH SAME RESULT: ", skipped_same_res)
