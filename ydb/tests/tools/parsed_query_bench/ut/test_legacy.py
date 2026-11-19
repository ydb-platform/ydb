import csv
import io
import json
import subprocess

import pytest
import yatest.common
from ydb.public.api.protos import ydb_query_pb2, ydb_value_pb2


TOOL = "ydb/tests/tools/parsed_query_bench/parsed_query_legacy_bench"
STAGES = ("text", "antlr_ast", "yql_ast")


def test_table_and_parameters(tmp_path):
    tool = yatest.common.binary_path(TOOL)
    sql = tmp_path / "query.sql"
    sql.write_text("--!syntax_v1\nSELECT payload AS first, id AS second FROM items WHERE id = $id;")
    columns = [
        dict(id=1, name="id", type="Int32", not_null=True, generated=False, has_default=False),
        dict(id=2, name="payload", type="String", not_null=False, generated=False, has_default=False),
    ]
    table = dict(path="/Root/items", owner_id=1, table_id=1, schema_version=1, storage="row",
                 has_write_indexes=False, key_columns=["id"], columns=columns)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps(dict(format_version=1, identity="fixture", cluster="benchmark",
                                       database="/Root", table_path_prefix="/Root", tables=[table])))
    request = ydb_query_pb2.ExecuteQueryRequest(
        session_id="offline", exec_mode=ydb_query_pb2.EXEC_MODE_EXECUTE, pool_id="test-pool",
        query_content=ydb_query_pb2.QueryContent(syntax=ydb_query_pb2.SYNTAX_YQL_V1, text=sql.read_text()),
    )
    request.tx_control.begin_tx.serializable_read_write.SetInParent()
    request.tx_control.commit_tx = True
    parameter = request.parameters["$id"]
    parameter.type.type_id = ydb_value_pb2.Type.INT32
    parameter.value.int32_value = 123
    template = tmp_path / "template.pb"
    template.write_bytes(request.SerializeToString())
    stages = (*STAGES, "kql_query_list")
    inputs = [tmp_path / (stage + ".pb") for stage in stages]
    cmd = [tool, "--catalog", catalog]
    subprocess.run([*cmd, "--template", template, "--export", sql, *inputs], check=True)
    assert ydb_query_pb2.ExecuteQueryRequest.FromString(inputs[0].read_bytes()) == request
    output = subprocess.check_output([*cmd, "--bench", "0", "2", *reversed(inputs)], text=True)
    rows = list(csv.DictReader(io.StringIO(output)))
    assert len(rows) == 2 * len(stages)
    assert {(r["stage"], r["iteration"]) for r in rows} == {(s, str(i)) for s in stages for i in range(2)}
    for row in rows:
        assert int(row["bytes"]) == inputs[stages.index(row["stage"])].stat().st_size
        for clock in ("wall_ns", "cpu_ns"):
            phases = [int(row[phase + clock]) for phase in ("decode_", "parse_or_import_", "frontend_")]
            assert all(phase >= 0 for phase in phases)
            assert sum(phases) == int(row["total_" + clock])
    data = subprocess.check_output([*cmd, "--template", template, "--client", "0", "1", sql, *stages], text=True)
    client = list(csv.DictReader(io.StringIO(data)))
    assert {r["stage"] for r in client} == set(stages)
    for sample in client:
        assert int(sample["bytes"]) == inputs[stages.index(sample["stage"])].stat().st_size
        assert int(sample["client_wall_ns"]) > 0

    # The grammar protobuf has only C++ bindings. Encode a one-node list whose child refers to itself.
    def message(field, content):
        return bytes([field * 8 + 2, len(content)]) + content

    envelope = ydb_query_pb2.ExecuteQueryRequest.FromString(inputs[0].read_bytes())
    envelope.ClearField("query_content")
    node = bytes([8, 2]) + message(4, bytes([1]))
    kql = bytes([8, 1]) + message(2, node)
    malformed = tmp_path / "forward.pb"
    malformed.write_bytes(envelope.SerializeToString() + message(5, message(6, kql)))
    failure = subprocess.run([*cmd, "--bench", "0", "1", *inputs[:3], malformed], text=True, capture_output=True)
    assert failure.returncode == 1
    assert "forward KQL child reference" in failure.stderr
    assert not failure.stdout

    request.parameters["$id"].value.int32_value += 1
    inputs[0].write_bytes(request.SerializeToString())
    failure = subprocess.run([*cmd, "--bench", "0", "1", *inputs], text=True, capture_output=True)
    assert failure.returncode == 1
    assert "Request envelope differs" in failure.stderr
    assert not failure.stdout


@pytest.mark.parametrize("stage", [1, 2])
def test_mismatched_script(tmp_path, stage):
    tool = yatest.common.binary_path(TOOL)
    inputs = []
    for suffix in ("2", "2ul"):
        sql = tmp_path / (suffix + ".sql")
        sql.write_text(f"SELECT 1 AS a; SELECT {suffix} AS b;")
        paths = [tmp_path / (name + suffix + ".pb") for name in STAGES]
        subprocess.run([tool, "--export", sql, *paths], check=True)
        inputs.append(paths)
    files = inputs[0]
    files[stage] = inputs[1][stage]
    failure = subprocess.run([tool, "--bench", "0", "1", *files], text=True, capture_output=True)
    assert failure.returncode == 1
    assert "differ" in failure.stderr
    assert not failure.stdout
