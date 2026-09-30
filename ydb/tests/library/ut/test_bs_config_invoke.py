from google.protobuf import text_format

import ydb.core.protos.blobstorage_config_pb2 as bs
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_runner import _ITEM_CONFIG_GENERATION_ANY
from ydb.tests.library.harness.kikimr_runner import _allow_applied_bs_config_retry


def _request():
    request = bs.TConfigRequest()
    host = request.Command.add().DefineHostConfig
    host.HostConfigId = 1
    host.Drive.add().Path = "/dev/disk"
    request.Command.add().DefineBox.BoxId = 1
    pool = request.Command.add().DefineStoragePool
    pool.BoxId = 1
    pool.StoragePoolId = 1
    pool.Name = "pool"
    return request


def _parse_invoke(cmd):
    proto_arg = cmd[-1]
    assert proto_arg.startswith("--proto=")
    parsed = bs.TConfigRequest()
    text_format.Parse(proto_arg[len("--proto="):], parsed)
    return parsed


def _invoke(request, fail_times):
    cluster = KiKiMR.__new__(KiKiMR)
    calls = []

    def fake_cli(cmd):
        calls.append(list(cmd))
        if len(calls) <= fail_times:
            raise RuntimeError("ItemConfigGeneration mismatch ItemConfigGenerationProvided# 0 ItemConfigGenerationExpected# 1")

    cluster._KiKiMR__call_kikimr_new_cli = fake_cli
    KiKiMR._bs_config_invoke(cluster, request)
    return calls


def test_bs_config_invoke_keeps_generation_when_first_attempt_succeeds(monkeypatch):
    monkeypatch.setattr("ydb.tests.library.harness.kikimr_runner.time.sleep", lambda _seconds: None)
    calls = _invoke(_request(), fail_times=0)

    assert len(calls) == 1
    parsed = _parse_invoke(calls[0])
    assert parsed.Command[0].DefineHostConfig.ItemConfigGeneration == 0
    assert parsed.Command[1].DefineBox.ItemConfigGeneration == 0
    assert parsed.Command[2].DefineStoragePool.ItemConfigGeneration == 0


def test_bs_config_invoke_bypasses_generation_after_failed_attempt(monkeypatch):
    monkeypatch.setattr("ydb.tests.library.harness.kikimr_runner.time.sleep", lambda _seconds: None)
    calls = _invoke(_request(), fail_times=1)

    assert len(calls) == 2
    first = _parse_invoke(calls[0])
    second = _parse_invoke(calls[1])
    assert first.Command[0].DefineHostConfig.ItemConfigGeneration == 0
    for command in second.Command:
        submessage = getattr(command, command.WhichOneof("Command"))
        assert submessage.ItemConfigGeneration == _ITEM_CONFIG_GENERATION_ANY


def test_retry_bypass_skips_commands_without_generation():
    request = bs.TConfigRequest()
    request.Command.add().DefineHostConfig.HostConfigId = 1
    request.Command.add().ReadHostConfig.HostConfigId.append(1)

    _allow_applied_bs_config_retry(request)

    assert request.Command[0].DefineHostConfig.ItemConfigGeneration == _ITEM_CONFIG_GENERATION_ANY
    assert request.Command[1].WhichOneof("Command") == "ReadHostConfig"
    assert "ItemConfigGeneration" not in request.Command[1].ReadHostConfig.DESCRIPTOR.fields_by_name


def test_bs_config_invoke_reraises_when_retries_are_exhausted(monkeypatch):
    monkeypatch.setattr("ydb.tests.library.harness.kikimr_runner.time.sleep", lambda _seconds: None)
    try:
        _invoke(_request(), fail_times=10 ** 9)
    except RuntimeError as e:
        assert "ItemConfigGeneration mismatch" in str(e)
    else:
        raise AssertionError("expected the original CLI error")
